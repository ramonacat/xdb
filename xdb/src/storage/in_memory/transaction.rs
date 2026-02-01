use log::debug;
use std::{
    collections::{HashMap, HashSet},
    ops::Deref as _,
};

use bytemuck::Zeroable as _;
use log::error;

use crate::{
    page::Page,
    storage::{
        PageIndex, StorageError, Transaction, TransactionId,
        in_memory::{
            InMemoryPageReservation, InMemoryStorage,
            lock_manager::{ManagedPageGuard, ManagedPageGuardMut},
        },
    },
};

#[derive(Debug)]
enum WritePage<'storage> {
    Modified {
        guard: ManagedPageGuardMut<'storage>,

        // TODO: we have to change this, so that the copy is what's modified, and then on commit
        // all the copies are transferred back to the real storage. With the current
        // implementation, other transactions can see the current transaction as non-atomic
        // (because another page can be read before we lock it for write)
        copy_index: PageIndex,
    },
    Inserted(ManagedPageGuardMut<'storage>),
}

impl<'storage> WritePage<'storage> {
    const fn guard(&self) -> &ManagedPageGuardMut<'storage> {
        match self {
            WritePage::Modified {
                guard,
                copy_index: _,
            }
            | WritePage::Inserted(guard) => guard,
        }
    }

    const fn guard_mut(&mut self) -> &mut ManagedPageGuardMut<'storage> {
        match self {
            WritePage::Modified {
                guard,
                copy_index: _,
            }
            | WritePage::Inserted(guard) => guard,
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
    fn copy_for_write(&self, page: Page) -> PageIndex {
        self.storage
            .rollback_copies
            .allocate(self.id)
            .initialize(page)
            .index()
    }

    fn do_rollback(&mut self) {
        for guard in self.write_guards.values_mut() {
            match guard {
                WritePage::Modified { guard, copy_index } => {
                    **guard = *self.storage.rollback_copies.get(*copy_index, self.id).get();
                }
                WritePage::Inserted(_) => {
                    // TODO delete from storage
                }
            }
        }
    }
}

impl Drop for InMemoryTransaction<'_> {
    fn drop(&mut self) {
        if self.finalized {
            return;
        }

        error!(
            "[{:?}] transaction dropped without being rolled back or comitted",
            self.id
        );
        self.do_rollback();
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
            self.write_guards
                .get(&x)
                .map_or_else(|| self.read_guards.get(&x).unwrap().deref(), |x| x.guard())
        });

        Ok(read(guards))
    }

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

                let copy_index = self.copy_for_write(*write_guard);

                self.write_guards.insert(
                    index,
                    WritePage::Modified {
                        guard: write_guard,
                        copy_index,
                    },
                );
            }

            if !self.write_guards.contains_key(&index) {
                let guard = self.storage.lock_manager.get_write(self.id, index)?;
                let copy_index = self.copy_for_write(*guard);

                self.write_guards
                    .insert(index, WritePage::Modified { guard, copy_index });
            }
        }

        let mut index_refs: [Option<&PageIndex>; N] = [None; N];
        for (i, idx) in indices.iter().enumerate() {
            index_refs[i] = Some(idx);
        }
        let guards = self
            .write_guards
            .get_disjoint_mut(index_refs.map(|x| x.unwrap()))
            .map(|x| &mut **x.unwrap().guard_mut());

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
        // TODO this is kinda yucky as we manage the pageref directly
        let InMemoryPageReservation { page_guard } = reservation;
        let index = page_guard.index();
        page_guard.initialize(page);

        self.write_guards.insert(
            index,
            WritePage::Inserted(self.storage.lock_manager.get_write(self.id, index)?),
        );
        self.reserved_pages.remove(&index);

        Ok(())
    }

    fn insert(&mut self, page: Page) -> Result<PageIndex, StorageError> {
        // TODO this API is kinda yucky, as we're managin the PageRef directly, instead of letting
        // LockManager deal with it
        let guard = self.storage.lock_manager.reserve(self.id).initialize(page);
        let index = guard.index();

        self.write_guards.insert(
            index,
            WritePage::Inserted(
                self.storage
                    .lock_manager
                    .get_write(self.id, guard.index())?,
            ),
        );

        Ok(index)
    }

    fn delete(&mut self, page: PageIndex) -> Result<(), StorageError> {
        // TODO actually delete the page, instead of just zeroing!

        let guard = self.write_guards.get_mut(&page);

        let guard = if let Some(g) = guard {
            g
        } else {
            let new_guard = self.storage.lock_manager.get_write(self.id, page)?;
            let copy_index = self.copy_for_write(*new_guard);

            self.write_guards.insert(
                page,
                WritePage::Modified {
                    guard: new_guard,
                    copy_index,
                },
            );

            self.write_guards.get_mut(&page).unwrap()
        };

        **guard.guard_mut() = Page::zeroed();

        Ok(())
    }

    fn commit(mut self) -> Result<(), StorageError> {
        debug!("[{:?}] committing transaction", self.id);

        self.finalized = true;

        // TODO delete all the pages from self.storage.rollback_copies
        Ok(())
    }

    fn rollback(mut self) -> Result<(), StorageError> {
        debug!("[{:?}] rolling back transaction", self.id);

        self.do_rollback();

        self.finalized = true;

        Ok(())
    }
}
