mod mmaped_block;

use std::{cell::RefCell, collections::HashMap, ops::Deref};

use bytemuck::Zeroable;

use crate::{
    page::Page,
    storage::{
        PageIndex, PageReservation, Storage, StorageError, Transaction,
        in_memory::mmaped_block::{Block, PageGuard, PageGuardMut, UninitializedPageGuard},
    },
};

// TODO impl Drop to return the page to free pool if it doesn't get written
pub struct InMemoryPageReservation<'storage> {
    #[allow(unused)] // TODO remove?
    storage: &'storage InMemoryStorage,
    page_guard: UninitializedPageGuard<'storage>,
}

impl<'storage> PageReservation<'storage> for InMemoryPageReservation<'storage> {
    fn index(&self) -> PageIndex {
        self.page_guard.index()
    }
}

pub struct InMemoryTransaction<'storage> {
    storage: &'storage InMemoryStorage,
    read_guards: RefCell<HashMap<PageIndex, PageGuard<'storage>>>,
    write_guards: RefCell<HashMap<PageIndex, PageGuardMut<'storage>>>,
}

// TODO once a page is accessed in a transaction, we should keep the lock until the transaction
// ends
impl<'storage> Transaction<'storage, InMemoryPageReservation<'storage>>
    for InMemoryTransaction<'storage>
{
    fn read<T, const N: usize>(
        &self,
        indices: impl Into<[PageIndex; N]>,
        read: impl FnOnce([&Page; N]) -> T,
    ) -> Result<T, StorageError> {
        let indices: [PageIndex; N] = indices.into();
        for index in indices {
            if self.write_guards.borrow().contains_key(&index) {
                continue;
            }

            self.read_guards
                .borrow_mut()
                .entry(index)
                .or_insert_with(|| self.storage.pages.get(index).get());
        }

        let read_guards = self.read_guards.borrow();
        let write_guards = self.write_guards.borrow();

        let guards = indices.map(|x| {
            write_guards
                .get(&x)
                .map_or_else(|| read_guards.get(&x).unwrap().deref(), |x| &**x)
        });

        Ok(read(guards))
    }

    fn write<T, const N: usize>(
        &self,
        indices: impl Into<[PageIndex; N]>,
        write: impl FnOnce([&mut Page; N]) -> T,
    ) -> Result<T, StorageError> {
        let indices: [PageIndex; N] = indices.into();

        for index in indices {
            if let Some(read_guard) = self.read_guards.borrow_mut().remove(&index) {
                let write_guard = read_guard.upgrade();

                self.write_guards.borrow_mut().insert(index, write_guard);
            }

            self.write_guards
                .borrow_mut()
                .entry(index)
                .or_insert_with(|| self.storage.pages.get(index).get_mut());
        }

        let mut index_refs: [Option<&PageIndex>; N] = [None; N];
        for (i, idx) in indices.iter().enumerate() {
            index_refs[i] = Some(idx);
        }
        let mut write_guards = self.write_guards.borrow_mut();
        let guards = write_guards
            .get_disjoint_mut(index_refs.map(|x| x.unwrap()))
            .map(|x| &mut **x.unwrap());

        Ok(write(guards))
    }

    fn reserve<'a>(&'a self) -> Result<InMemoryPageReservation<'storage>, StorageError> {
        let page_guard = self.storage.pages.allocate();

        Ok(InMemoryPageReservation {
            storage: self.storage,
            page_guard,
        })
    }

    fn insert_reserved(
        &self,
        reservation: InMemoryPageReservation<'storage>,
        page: Page,
    ) -> Result<(), StorageError> {
        let InMemoryPageReservation {
            storage: _,
            page_guard,
        } = reservation;
        let index = page_guard.index();
        let guard = page_guard.initialize(page);
        self.write_guards
            .borrow_mut()
            .insert(index, guard.get_mut());

        Ok(())
    }

    fn insert(&self, page: Page) -> Result<PageIndex, StorageError> {
        let guard = self.storage.pages.allocate().initialize(page);
        let index = guard.index();

        self.write_guards
            .borrow_mut()
            .insert(index, guard.get_mut());

        Ok(index)
    }

    fn delete(&self, page: PageIndex) -> Result<(), StorageError> {
        // TODO actually delete the page, instead of just zeroing!

        let mut guard = self.storage.pages.get(page).get_mut();
        *guard = Page::zeroed();
        self.write_guards.borrow_mut().insert(page, guard);

        Ok(())
    }

    fn commit(self) -> Result<(), StorageError> {
        todo!()
    }
}

#[derive(Debug)]
pub struct InMemoryStorage {
    pages: mmaped_block::Block,
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
        }
    }
}

impl Storage for InMemoryStorage {
    type Transaction<'a> = InMemoryTransaction<'a>;
    type PageReservation<'a> = InMemoryPageReservation<'a>;

    fn transaction(&self) -> Result<Self::Transaction<'_>, StorageError> {
        Ok(InMemoryTransaction {
            storage: self,
            read_guards: RefCell::new(HashMap::new()),
            write_guards: RefCell::new(HashMap::new()),
        })
    }
}
