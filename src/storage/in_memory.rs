use crate::{
    page::Page,
    storage::{PageIndex, Storage, StorageError, Transaction},
};

pub struct InMemoryTransaction<'storage> {
    storage: &'storage mut InMemoryStorage,
}

impl<'storage> Transaction<'storage> for InMemoryTransaction<'storage> {
    fn commit(self) -> Result<(), StorageError> {
        todo!()
    }

    fn write<T>(
        &mut self,
        index: PageIndex,
        write: impl FnOnce(&mut Page) -> T,
    ) -> Result<T, StorageError> {
        self.storage.write(index, write)
    }

    fn insert(&mut self, page: Page) -> Result<PageIndex, StorageError> {
        self.storage.insert(page)
    }

    fn read<TReturn>(
        &self,
        index: PageIndex,
        read: impl FnOnce(&Page) -> TReturn,
    ) -> Result<TReturn, StorageError> {
        let page = self.storage.get(index)?;

        Ok(read(page))
    }
}

#[derive(Debug)]
pub struct InMemoryStorage {
    pages: Vec<Page>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self { pages: vec![] }
    }
}

impl InMemoryStorage {
    fn get(&self, index: PageIndex) -> Result<&Page, StorageError> {
        self.pages
            .get(index.0 as usize)
            .map_or_else(|| Err(StorageError::PageNotFound(index)), Ok)
    }

    fn write<T>(
        &mut self,
        index: PageIndex,
        write: impl FnOnce(&mut Page) -> T,
    ) -> Result<T, StorageError> {
        let page = self
            .pages
            .get_mut(index.0 as usize)
            .map_or_else(|| Err(StorageError::PageNotFound(index)), Ok)?;

        Ok(write(page))
    }

    fn insert(&mut self, page: Page) -> Result<PageIndex, StorageError> {
        self.pages.push(page);

        Ok(PageIndex((self.pages.len() - 1) as u64))
    }
}

impl Storage for InMemoryStorage {
    type Transaction<'a> = InMemoryTransaction<'a>;

    fn transaction<'storage>(
        &'storage mut self,
    ) -> Result<Self::Transaction<'storage>, StorageError> {
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

    pub struct TestTransaction<'a, T: Transaction<'a>>(T, Arc<AtomicUsize>, PhantomData<&'a T>);

    impl<'a, T: Transaction<'a>> Transaction<'a> for TestTransaction<'a, T> {
        fn write<TReturn>(
            &mut self,
            index: PageIndex,
            write: impl FnOnce(&mut Page) -> TReturn,
        ) -> Result<TReturn, StorageError> {
            self.0.write(index, write)
        }

        fn insert(&mut self, page: Page) -> Result<PageIndex, StorageError> {
            self.1.fetch_add(1, Ordering::Relaxed);

            self.0.insert(page)
        }

        fn commit(self) -> Result<(), StorageError> {
            self.0.commit()
        }

        fn read<TReturn>(
            &self,
            index: PageIndex,
            read: impl FnOnce(&Page) -> TReturn,
        ) -> Result<TReturn, StorageError> {
            self.0.read(index, read)
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
            = TestTransaction<'a, T::Transaction<'a>>
        where
            T: 'a;

        fn transaction<'storage>(
            &'storage mut self,
        ) -> Result<Self::Transaction<'storage>, StorageError> {
            Ok(TestTransaction(
                self.inner.transaction()?,
                self.page_count.clone(),
                PhantomData,
            ))
        }
    }
}
