mod block;

use log::error;
use std::{collections::HashMap, ops::Deref};

use bytemuck::Zeroable;

use crate::{
    page::Page,
    storage::{
        PageIndex, PageReservation, Storage, StorageError, Transaction,
        in_memory::block::{Block, PageGuard, PageGuardMut, UninitializedPageGuard},
    },
};

// TODO impl Drop to return the page to free pool if it doesn't get written
pub struct InMemoryPageReservation<'storage> {
    page_guard: UninitializedPageGuard<'storage>,
}

impl<'storage> PageReservation<'storage> for InMemoryPageReservation<'storage> {
    fn index(&self) -> PageIndex {
        self.page_guard.index()
    }
}

#[derive(Debug)]
enum WritePage<'storage> {
    Modified {
        guard: PageGuardMut<'storage>,
        copy_index: PageIndex,
    },
    Inserted(PageGuardMut<'storage>),
}

impl<'storage> WritePage<'storage> {
    const fn guard(&self) -> &PageGuardMut<'storage> {
        match self {
            WritePage::Modified {
                guard,
                copy_index: _,
            }
            | WritePage::Inserted(guard) => guard,
        }
    }

    const fn guard_mut(&mut self) -> &mut PageGuardMut<'storage> {
        match self {
            WritePage::Modified {
                guard,
                copy_index: _,
            }
            | WritePage::Inserted(guard) => guard,
        }
    }
}

pub struct InMemoryTransaction<'storage> {
    storage: &'storage InMemoryStorage,
    read_guards: HashMap<PageIndex, PageGuard<'storage>>,
    write_guards: HashMap<PageIndex, WritePage<'storage>>,
    finalized: bool,
}

impl InMemoryTransaction<'_> {
    // TODO avoid passing by value?
    #[allow(clippy::large_types_passed_by_value)]
    fn copy_for_write(&self, page: Page) -> PageIndex {
        self.storage
            .rollback_copies
            .allocate()
            .initialize(page)
            .index()
    }

    fn do_rollback(&mut self) {
        for guard in self.write_guards.values_mut() {
            match guard {
                WritePage::Modified { guard, copy_index } => {
                    **guard = *self.storage.rollback_copies.get(*copy_index).get().unwrap();
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
        error!("transaction dropped without being rolled back or comitted");
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
            if self.write_guards.contains_key(&index) {
                continue;
            }

            match self.read_guards.entry(index) {
                std::collections::hash_map::Entry::Occupied(_) => {}
                std::collections::hash_map::Entry::Vacant(vacant_entry) => {
                    vacant_entry.insert(self.storage.pages.get(index).get()?);
                }
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
            if let Some(read_guard) = self.read_guards.remove(&index) {
                let write_guard = read_guard.upgrade();

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
                let guard = self.storage.pages.get(index).get_mut();
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

    fn reserve<'a>(&'a self) -> Result<InMemoryPageReservation<'storage>, StorageError> {
        let page_guard = self.storage.pages.allocate();

        Ok(InMemoryPageReservation { page_guard })
    }

    fn insert_reserved(
        &mut self,
        reservation: InMemoryPageReservation<'storage>,
        page: Page,
    ) -> Result<(), StorageError> {
        let InMemoryPageReservation { page_guard } = reservation;
        let index = page_guard.index();
        let guard = page_guard.initialize(page);

        self.write_guards
            .insert(index, WritePage::Inserted(guard.get_mut()));

        Ok(())
    }

    fn insert(&mut self, page: Page) -> Result<PageIndex, StorageError> {
        let guard = self.storage.pages.allocate().initialize(page);
        let index = guard.index();

        self.write_guards
            .insert(index, WritePage::Inserted(guard.get_mut()));

        Ok(index)
    }

    fn delete(&mut self, page: PageIndex) -> Result<(), StorageError> {
        // TODO actually delete the page, instead of just zeroing!

        let guard = self.write_guards.get_mut(&page);

        let guard = if let Some(g) = guard {
            g
        } else {
            let new_guard = self.storage.pages.get(page).get_mut();
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
        self.finalized = true;

        // TODO delete all the pages from self.storage.rollback_copies
        Ok(())
    }

    fn rollback(mut self) -> Result<(), StorageError> {
        self.do_rollback();

        self.finalized = true;

        Ok(())
    }
}

#[derive(Debug)]
pub struct InMemoryStorage {
    pages: Block,
    rollback_copies: Block,
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStorage {
    #[must_use]
    pub fn new() -> Self {
        Self {
            pages: Block::new(),
            rollback_copies: Block::new(),
        }
    }
}

impl Storage for InMemoryStorage {
    type Transaction<'a> = InMemoryTransaction<'a>;
    type PageReservation<'a> = InMemoryPageReservation<'a>;

    fn transaction(&self) -> Result<Self::Transaction<'_>, StorageError> {
        Ok(InMemoryTransaction {
            storage: self,
            read_guards: HashMap::new(),
            write_guards: HashMap::new(),
            finalized: false,
        })
    }
}
