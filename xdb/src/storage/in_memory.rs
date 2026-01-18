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
    fn write_many<T, const N: usize>(
        &self,
        indices: [PageIndex; N],
        write: impl FnOnce([&mut Page; N]) -> T,
    ) -> Result<T, StorageError> {
        let mut storage = self.storage.pages.write().unwrap();
        let pages = storage
            .get_disjoint_mut(indices.map(|x| x.0 as usize))
            .unwrap();

        Ok(write(pages))
    }

    fn reserve<'a>(&'a self) -> Result<InMemoryPageReservation<'storage>, StorageError> {
        let mut storage = self.storage.pages.write().unwrap();
        storage.push(Page::zeroed());

        let index = PageIndex((storage.len() - 1) as u64);

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
        let mut storage = self.storage.pages.write().unwrap();

        *storage
            .get_mut(reservation.index.0 as usize)
            .ok_or_else(|| StorageError::PageNotFound(reservation.index()))? = page;

        Ok(())
    }

    fn insert(&self, page: Page) -> Result<PageIndex, StorageError> {
        let mut storage = self.storage.pages.write().unwrap();
        storage.push(page);

        Ok(PageIndex((storage.len() - 1) as u64))
    }

    fn commit(self) -> Result<(), StorageError> {
        todo!()
    }

    fn read_many<T, const N: usize>(
        &self,
        indices: [PageIndex; N],
        read: impl FnOnce([&Page; N]) -> T,
    ) -> Result<T, StorageError> {
        let storage = self.storage.pages.read().unwrap();

        let pages = indices.map(|i| storage.get(i.0 as usize).unwrap());

        Ok(read(pages))
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
    pub fn new() -> Self {
        Self {
            pages: RwLock::new(vec![]),
        }
    }
}

impl Storage for InMemoryStorage {
    type Transaction<'a> = InMemoryTransaction<'a>;
    type PageReservation<'a> = InMemoryPageReservation<'a>;

    fn transaction<'storage>(&'storage self) -> Result<Self::Transaction<'storage>, StorageError> {
        Ok(InMemoryTransaction { storage: self })
    }
}

// TODO #[cfg(test)]
pub mod test {
    use std::{
        marker::PhantomData,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use super::*;
    use crate::storage::Storage;

    pub struct TestTransaction<'a, T: Transaction<'a, TStorage::PageReservation<'a>>, TStorage: Storage>(
        T,
        Arc<AtomicUsize>,
        PhantomData<&'a TStorage>,
    );

    impl<'a, TTx: Transaction<'a, TStorage::PageReservation<'a>>, TStorage: Storage>
        Transaction<'a, TStorage::PageReservation<'a>> for TestTransaction<'a, TTx, TStorage>
    {
        fn read_many<TReturn, const N: usize>(
            &self,
            indices: [PageIndex; N],
            read: impl FnOnce([&Page; N]) -> TReturn,
        ) -> Result<TReturn, StorageError> {
            self.0.read_many(indices, read)
        }

        fn write_many<TReturn, const N: usize>(
            &self,
            indices: [PageIndex; N],
            write: impl FnOnce([&mut Page; N]) -> TReturn,
        ) -> Result<TReturn, StorageError> {
            self.0.write_many(indices, write)
        }

        fn reserve(&self) -> Result<TStorage::PageReservation<'a>, StorageError> {
            self.0.reserve()
        }

        fn insert_reserved(
            &self,
            reservation: TStorage::PageReservation<'a>,
            page: Page,
        ) -> Result<(), StorageError> {
            self.1.fetch_add(1, Ordering::Relaxed);

            self.0.insert_reserved(reservation, page)
        }

        fn insert(&self, page: Page) -> Result<PageIndex, StorageError> {
            self.1.fetch_add(1, Ordering::Relaxed);

            self.0.insert(page)
        }

        fn commit(self) -> Result<(), StorageError> {
            self.0.commit()
        }
    }

    // TODO a storage that collects metrics should probably be a thing outside of tests
    pub struct TestStorage<T: Storage> {
        page_count: Arc<AtomicUsize>,
        inner: T,
    }

    impl<T: Storage> TestStorage<T> {
        pub fn new(inner: T, page_count: Arc<AtomicUsize>) -> Self {
            Self { page_count, inner }
        }
    }

    impl<T: Storage> Storage for TestStorage<T> {
        type Transaction<'a>
            = TestTransaction<'a, T::Transaction<'a>, T>
        where
            T: 'a;

        type PageReservation<'a>
            = T::PageReservation<'a>
        where
            T: 'a;

        fn transaction<'storage>(
            &'storage self,
        ) -> Result<Self::Transaction<'storage>, StorageError> {
            Ok(TestTransaction(
                self.inner.transaction()?,
                self.page_count.clone(),
                PhantomData,
            ))
        }
    }
}
