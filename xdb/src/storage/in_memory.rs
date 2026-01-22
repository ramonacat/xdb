use std::sync::RwLock;

use bytemuck::Zeroable;

use crate::{
    page::Page,
    storage::{PageIndex, PageReservation, Storage, StorageError, Transaction},
};

// TODO impl Drop to return the page to free pool if it doesn't get written
pub struct InMemoryPageReservation<'storage> {
    #[allow(unused)] // TODO use it?
    storage: &'storage InMemoryStorage,
    index: PageIndex,
}

impl<'storage> PageReservation<'storage> for InMemoryPageReservation<'storage> {
    fn index(&self) -> PageIndex {
        self.index
    }
}

pub struct InMemoryTransaction<'storage> {
    storage: &'storage InMemoryStorage,
}

impl<'storage> Transaction<'storage, InMemoryPageReservation<'storage>>
    for InMemoryTransaction<'storage>
{
    fn read<T, const N: usize>(
        &self,
        indices: impl Into<[PageIndex; N]>,
        read: impl FnOnce([&Page; N]) -> T,
    ) -> Result<T, StorageError> {
        let storage = self.storage.pages.read().unwrap();

        let pages = indices
            .into()
            .map(|i| storage.get(usize::try_from(i.0).unwrap()).unwrap());

        Ok(read(pages))
    }

    fn write<T, const N: usize>(
        &self,
        indices: impl Into<[PageIndex; N]>,
        write: impl FnOnce([&mut Page; N]) -> T,
    ) -> Result<T, StorageError> {
        let mut storage = self.storage.pages.write().unwrap();
        let pages = storage
            .get_disjoint_mut(indices.into().map(|x| usize::try_from(x.0).unwrap()))
            .unwrap();

        Ok(write(pages))
    }

    fn reserve<'a>(&'a self) -> Result<InMemoryPageReservation<'storage>, StorageError> {
        let mut storage = self.storage.pages.write().unwrap();
        storage.push(Page::zeroed());

        let index = PageIndex((storage.len() - 1) as u64);
        drop(storage);

        Ok(InMemoryPageReservation {
            storage: self.storage,
            index,
        })
    }

    fn insert_reserved(
        &self,
        reservation: InMemoryPageReservation<'storage>,
        page: Page,
    ) -> Result<(), StorageError> {
        *self
            .storage
            .pages
            .write()
            .unwrap()
            .get_mut(usize::try_from(reservation.index.0).unwrap())
            .ok_or_else(|| StorageError::PageNotFound(reservation.index()))? = page;

        Ok(())
    }

    fn insert(&self, page: Page) -> Result<PageIndex, StorageError> {
        let mut storage = self.storage.pages.write().unwrap();
        storage.push(page);

        Ok(PageIndex((storage.len() - 1) as u64))
    }

    fn delete(&self, page: PageIndex) -> Result<(), StorageError> {
        // TODO actually delete the page, instead of just zeroing!

        *self
            .storage
            .pages
            .write()
            .unwrap()
            .get_mut(usize::try_from(page.0).unwrap())
            .unwrap() = Page::zeroed();

        Ok(())
    }

    fn commit(self) -> Result<(), StorageError> {
        todo!()
    }
}

#[derive(Debug)]
pub struct InMemoryStorage {
    // TODO: per page locks
    pages: RwLock<Vec<Page>>,
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStorage {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            pages: RwLock::new(vec![]),
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
