use std::{collections::HashMap, ops::Deref as _};

use bytemuck::Zeroable as _;
use log::error;

use crate::{
    page::Page,
    storage::{
        PageIndex, StorageError, Transaction,
        in_memory::{
            InMemoryPageReservation, InMemoryStorage,
            block::{PageGuard, PageGuardMut, PageRef},
        },
    },
};

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

impl<'storage> InMemoryTransaction<'storage> {
    pub fn new(storage: &'storage InMemoryStorage) -> Self {
        Self {
            storage,
            read_guards: HashMap::new(),
            write_guards: HashMap::new(),
            finalized: false,
        }
    }
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

    fn read_page(&self, page: PageRef<'storage>) -> Result<PageGuard<'storage>, StorageError> {
        match page.get() {
            Ok(page) => Ok(page),
            Err(_) => todo!(),
        }
    }

    fn upgrade_page(
        &self,
        guard: PageGuard<'storage>,
    ) -> Result<PageGuardMut<'storage>, StorageError> {
        match guard.upgrade() {
            Ok(g) => Ok(g),
            Err(_) => todo!(),
        }
    }

    fn write_page(&self, page: PageRef<'storage>) -> Result<PageGuardMut<'storage>, StorageError> {
        match page.get_mut() {
            Ok(page) => Ok(page),
            Err(_) => todo!(),
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

            if !self.read_guards.contains_key(&index) {
                self.read_guards
                    .insert(index, self.read_page(self.storage.pages.get(index))?);
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
                let write_guard = self.upgrade_page(read_guard)?;

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
                let guard = self.write_page(self.storage.pages.get(index))?;
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
            .insert(index, WritePage::Inserted(self.write_page(guard)?));

        Ok(())
    }

    fn insert(&mut self, page: Page) -> Result<PageIndex, StorageError> {
        let guard = self.storage.pages.allocate().initialize(page);
        let index = guard.index();

        self.write_guards
            .insert(index, WritePage::Inserted(self.write_page(guard)?));

        Ok(index)
    }

    fn delete(&mut self, page: PageIndex) -> Result<(), StorageError> {
        // TODO actually delete the page, instead of just zeroing!

        let guard = self.write_guards.get_mut(&page);

        let guard = if let Some(g) = guard {
            g
        } else {
            let new_guard = self.write_page(self.storage.pages.get(page))?;
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
