use bytemuck::Zeroable;
use log::debug;
use std::collections::{HashMap, HashSet};

use crate::{
    page::Page,
    storage::{
        PageIndex, StorageError, Transaction, TransactionId,
        in_memory::{
            InMemoryPageReservation, InMemoryStorage,
            block::{PageGuard, UninitializedPageGuard},
            lock_manager::ManagedPageGuard,
        },
    },
};

#[derive(Debug)]
// TODO rename -> LockedPage
enum WritePage<'storage> {
    Read {
        guard: ManagedPageGuard<'storage>,
    },
    Modified {
        guard: ManagedPageGuard<'storage>,
        cow_guard: PageGuard<'storage>,
    },
    Inserted {
        guard: UninitializedPageGuard<'storage>,
        cow_guard: PageGuard<'storage>,
    },
    Deleted {
        guard: ManagedPageGuard<'storage>,
    },
}

impl WritePage<'_> {
    // TODO rename -> page() or something
    fn cow_guard(&self) -> Option<&Page> {
        match self {
            WritePage::Read { guard } => Some(guard),
            WritePage::Modified {
                guard: _,
                cow_guard: guard,
            }
            | WritePage::Inserted {
                guard: _,
                cow_guard: guard,
            } => Some(guard),
            WritePage::Deleted { guard: _ } => None,
        }
    }

    // TODO rename -> page_mut() or something
    fn cow_guard_mut(&mut self) -> Option<&mut Page> {
        match self {
            WritePage::Modified {
                guard: _,
                cow_guard,
            }
            | WritePage::Inserted {
                guard: _,
                cow_guard,
            } => Some(cow_guard),
            WritePage::Read { guard } => Some(guard),
            WritePage::Deleted { guard: _ } => None,
        }
    }
}

#[derive(Debug)]
pub struct InMemoryTransaction<'storage> {
    id: TransactionId,
    storage: &'storage InMemoryStorage,
    guards: HashMap<PageIndex, WritePage<'storage>>,
    finalized: bool,
    reserved_pages: HashSet<PageIndex>,
}

impl<'storage> InMemoryTransaction<'storage> {
    pub fn new(storage: &'storage InMemoryStorage) -> Self {
        Self {
            id: TransactionId::next(),
            storage,
            guards: HashMap::new(),
            finalized: false,
            reserved_pages: HashSet::new(),
        }
    }

    // TODO avoid passing by value?
    #[allow(clippy::large_types_passed_by_value)]
    fn copy_for_write(&self, page: Page) -> PageGuard<'storage> {
        self.storage
            .cow_copies
            .allocate(self.id)
            .initialize(page)
            .lock()
    }
}

impl Drop for InMemoryTransaction<'_> {
    fn drop(&mut self) {
        // TODO do an actual rollback
    }
}

// TODO we need to implement real MVCC, since right now we pages can change during a transaction,
// leading to inconsistencies.
// Example.
//  1. transaction A starts
//  2. transaction B starts
//  3. transaction A locks and reads page 1
//  4. transaction B locks and writes page 2
//  5. transaction B commits
//  6. transaction A locks and reads page 2
//      !!! this page is now inconsistent with the state we've, seen in step #3, TROUBLE !!!
impl<'storage> Transaction<'storage, InMemoryPageReservation<'storage>>
    for InMemoryTransaction<'storage>
{
    fn read<T, const N: usize>(
        &mut self,
        indices: impl Into<[PageIndex; N]>,
        read: impl FnOnce([&Page; N]) -> T,
    ) -> Result<T, StorageError> {
        let indices: [PageIndex; N] = indices.into();
        for index in indices {
            assert!(!self.reserved_pages.contains(&index));

            if self.guards.contains_key(&index) {
                continue;
            }

            self.guards.insert(
                index,
                WritePage::Read {
                    guard: self.storage.lock_manager.lock(self.id, index)?,
                },
            );
        }

        let guards = indices.map(|x| self.guards.get(&x).map(|x| x.cow_guard().unwrap()).unwrap());

        Ok(read(guards))
    }

    // TODO this method is hacky around replacing Read entry with Modified, can this be simplified?
    fn write<T, const N: usize>(
        &mut self,
        indices: impl Into<[PageIndex; N]>,
        write: impl FnOnce([&mut Page; N]) -> T,
    ) -> Result<T, StorageError> {
        let indices: [PageIndex; N] = indices.into();

        for index in indices {
            assert!(!self.reserved_pages.contains(&index));

            let create_copy = if let Some(entry) = self.guards.get(&index) {
                match entry {
                    WritePage::Read { .. } => true,
                    WritePage::Modified { .. } | WritePage::Inserted { .. } => false,
                    WritePage::Deleted { .. } => {
                        return Err(StorageError::PageNotFound(index));
                    }
                }
            } else {
                let guard = self.storage.lock_manager.lock(self.id, index).unwrap();
                let cow_guard = self.copy_for_write(*guard);

                self.guards
                    .insert(index, WritePage::Modified { guard, cow_guard });

                false
            };

            if create_copy {
                let guard = self.guards.remove(&index).unwrap();
                let WritePage::Read { guard } = guard else {
                    panic!();
                };
                let cow_guard = self.copy_for_write(*guard);

                self.guards
                    .insert(index, WritePage::Modified { guard, cow_guard });
            }
        }

        let guards = self
            .guards
            .get_disjoint_mut(indices.each_ref())
            .map(|x| &mut *x.unwrap().cow_guard_mut().unwrap());

        Ok(write(guards))
    }

    fn reserve(&mut self) -> Result<InMemoryPageReservation<'storage>, StorageError> {
        let page_guard = self.storage.lock_manager.reserve(self.id);
        self.reserved_pages.insert(page_guard.index());

        Ok(InMemoryPageReservation { page_guard })
    }

    fn insert_reserved(
        &mut self,
        reservation: InMemoryPageReservation<'storage>,
        page: Page,
    ) -> Result<(), StorageError> {
        let InMemoryPageReservation { page_guard } = reservation;
        let cow_guard = self
            .storage
            .cow_copies
            .allocate(self.id)
            .initialize(page)
            .lock();

        let index = page_guard.index();

        self.guards.insert(
            index,
            WritePage::Inserted {
                guard: page_guard,
                cow_guard,
            },
        );
        self.reserved_pages.remove(&index);

        Ok(())
    }

    fn insert(&mut self, page: Page) -> Result<PageIndex, StorageError> {
        let guard = self.storage.lock_manager.reserve(self.id);
        let cow_guard = self
            .storage
            .cow_copies
            .allocate(self.id)
            .initialize(page)
            .lock();
        let index = guard.index();

        self.guards
            .insert(index, WritePage::Inserted { guard, cow_guard });

        Ok(index)
    }

    fn delete(&mut self, page: PageIndex) -> Result<(), StorageError> {
        let guard = self.guards.get_mut(&page);

        match guard {
            Some(WritePage::Modified { .. } | WritePage::Read { .. }) | None => {
                let new_guard = self.storage.lock_manager.lock(self.id, page)?;

                self.guards
                    .insert(page, WritePage::Deleted { guard: new_guard });
            }
            Some(WritePage::Inserted { .. }) => {
                self.guards.remove(&page);
            }
            Some(WritePage::Deleted { .. }) => {}
        }

        Ok(())
    }

    fn commit(mut self) -> Result<(), StorageError> {
        debug!("[{:?}] committing transaction", self.id);

        self.finalized = true;

        // TODO make the commit consistent in event of a crash:
        //    1. write to a transaction log
        //    2. fsync the transaction log
        //    3. fsync the modified pages
        // TODO cleanup the cow_pages, once they're copied to the main storage

        for (_, guard) in self.guards.drain() {
            match guard {
                WritePage::Modified {
                    mut guard,
                    cow_guard,
                } => {
                    *guard = *cow_guard;
                }
                WritePage::Inserted { guard, cow_guard } => {
                    guard.initialize(*cow_guard);
                }
                WritePage::Deleted { mut guard } => {
                    // TODO really delete!
                    *guard = Page::zeroed();
                }
                WritePage::Read { .. } => {}
            }
        }

        Ok(())
    }

    fn rollback(mut self) -> Result<(), StorageError> {
        debug!("[{:?}] rolling back transaction", self.id);

        // TODO free all of the cow copies
        self.finalized = true;

        Ok(())
    }
}
