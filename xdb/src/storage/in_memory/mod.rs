mod mmaped_block;

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
}

// TODO once a page is accessed in a transaction, we should keep the lock until the transaction
// ends
impl<'storage> Transaction<'storage, InMemoryPageReservation<'storage>>
    for InMemoryTransaction<'storage>
{
    type TPage = PageGuard<'storage>;
    type TPageMut = PageGuardMut<'storage>;

    fn read<T, const N: usize>(
        &self,
        indices: impl Into<[PageIndex; N]>,
        read: impl FnOnce([Self::TPage; N]) -> T,
    ) -> Result<T, StorageError> {
        let pages = indices.into().map(|i| self.storage.pages.get(i).get());

        Ok(read(pages))
    }

    fn write<T, const N: usize>(
        &self,
        indices: impl Into<[PageIndex; N]>,
        write: impl FnOnce([Self::TPageMut; N]) -> T,
    ) -> Result<T, StorageError> {
        let pages = indices.into().map(|i| self.storage.pages.get(i).get_mut());

        Ok(write(pages))
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
        page_guard.initialize(page);

        Ok(())
    }

    fn insert(&self, page: Page) -> Result<PageIndex, StorageError> {
        Ok(self.storage.pages.allocate().initialize(page).index())
    }

    fn delete(&self, page: PageIndex) -> Result<(), StorageError> {
        // TODO actually delete the page, instead of just zeroing!

        *self.storage.pages.get(page).get_mut() = Page::zeroed();

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
        Ok(InMemoryTransaction { storage: self })
    }
}
