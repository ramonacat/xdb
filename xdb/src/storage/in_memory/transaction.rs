use bytemuck::Zeroable;
use log::debug;
use std::{
    collections::{HashMap, HashSet},
    ops::Deref as _,
};

use crate::{
    page::Page,
    storage::{
        PageIndex, StorageError, Transaction, TransactionId,
        in_memory::{
            InMemoryPageReservation, InMemoryStorage,
            block::{PageGuardMut, UninitializedPageGuard},
            lock_manager::{ManagedPageGuard, ManagedPageGuardMut},
        },
    },
};

#[derive(Debug)]
enum WritePage<'storage> {
    Modified {
        guard: ManagedPageGuardMut<'storage>,
        cow_guard: PageGuardMut<'storage>,
    },
    Inserted {
        guard: UninitializedPageGuard<'storage>,
        cow_guard: PageGuardMut<'storage>,
    },
    Deleted {
        guard: ManagedPageGuardMut<'storage>,
    },
}

impl<'storage> WritePage<'storage> {
    const fn cow_guard(&self) -> Option<&PageGuardMut<'storage>> {
        match self {
            WritePage::Modified {
                guard: _,
                cow_guard,
            }
            | WritePage::Inserted {
                guard: _,
                cow_guard,
            } => Some(cow_guard),
            WritePage::Deleted { guard: _ } => None,
        }
    }

    const fn cow_guard_mut(&mut self) -> Option<&mut PageGuardMut<'storage>> {
        match self {
            WritePage::Modified {
                guard: _,
                cow_guard,
            }
            | WritePage::Inserted {
                guard: _,
                cow_guard,
            } => Some(cow_guard),
            WritePage::Deleted { guard: _ } => None,
        }
    }
}

#[derive(Debug)]
pub struct InMemoryTransaction<'storage> {
    id: TransactionId,
    storage: &'storage InMemoryStorage,
    read_guards: HashMap<PageIndex, ManagedPageGuard<'storage>>,
    write_guards: HashMap<PageIndex, WritePage<'storage>>,
    finalized: bool,
    reserved_pages: HashSet<PageIndex>,
}

impl<'storage> InMemoryTransaction<'storage> {
    pub fn new(storage: &'storage InMemoryStorage) -> Self {
        Self {
            id: TransactionId::next(),
            storage,
            read_guards: HashMap::new(),
            write_guards: HashMap::new(),
            finalized: false,
            reserved_pages: HashSet::new(),
        }
    }

    // TODO avoid passing by value?
    #[allow(clippy::large_types_passed_by_value)]
    fn copy_for_write(&self, page: Page) -> PageGuardMut<'storage> {
        self.storage
            .cow_copies
            .allocate(self.id)
            .initialize(page)
            .get_mut()
    }
}

impl Drop for InMemoryTransaction<'_> {
    fn drop(&mut self) {
        // TODO do an actual rollback
    }
}

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

            if self.write_guards.contains_key(&index) {
                continue;
            }

            if !self.read_guards.contains_key(&index) {
                self.read_guards
                    .insert(index, self.storage.lock_manager.get_read(self.id, index)?);
            }
        }

        let guards = indices.map(|x| {
            self.write_guards.get(&x).map_or_else(
                || self.read_guards.get(&x).unwrap().deref(),
                |x| x.cow_guard().unwrap(),
            )
        });

        Ok(read(guards))
    }

    // TODO this method is a bit complex, simplify
    fn write<T, const N: usize>(
        &mut self,
        indices: impl Into<[PageIndex; N]>,
        write: impl FnOnce([&mut Page; N]) -> T,
    ) -> Result<T, StorageError> {
        let indices: [PageIndex; N] = indices.into();

        for index in indices {
            assert!(!self.reserved_pages.contains(&index));

            if let Some(read_guard) = self.read_guards.remove(&index) {
                let write_guard = read_guard.upgrade()?;

                let cow_guard = self.copy_for_write(*write_guard);

                self.write_guards.insert(
                    index,
                    WritePage::Modified {
                        guard: write_guard,
                        cow_guard,
                    },
                );
            }

            match self.write_guards.get(&index) {
                Some(WritePage::Deleted { .. }) => {
                    return Err(StorageError::PageNotFound(index));
                }
                Some(WritePage::Modified { .. } | WritePage::Inserted { .. }) => {}
                None => {
                    let guard = self.storage.lock_manager.get_write(self.id, index)?;
                    let cow_guard = self.copy_for_write(*guard);

                    self.write_guards
                        .insert(index, WritePage::Modified { guard, cow_guard });
                }
            }
        }

        let guards = self
            .write_guards
            .get_disjoint_mut(indices.each_ref())
            .map(|x| &mut **x.unwrap().cow_guard_mut().unwrap());

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
            .get_mut();

        let index = page_guard.index();

        self.write_guards.insert(
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
            .get_mut();
        let index = guard.index();

        self.write_guards
            .insert(index, WritePage::Inserted { guard, cow_guard });

        Ok(index)
    }

    fn delete(&mut self, page: PageIndex) -> Result<(), StorageError> {
        let guard = self.write_guards.get_mut(&page);

        match guard {
            Some(WritePage::Modified { .. }) | None => {
                let new_guard = self.storage.lock_manager.get_write(self.id, page)?;

                self.write_guards
                    .insert(page, WritePage::Deleted { guard: new_guard });
            }
            Some(WritePage::Inserted { .. }) => {
                self.write_guards.remove(&page);
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

        for (_, guard) in self.write_guards.drain() {
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
