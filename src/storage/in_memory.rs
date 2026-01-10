use crate::{
    page::Page,
    storage::{PageIndex, Storage, StorageError},
};

#[derive(Debug)]
pub struct InMemoryStorage {
    pages: Vec<Page>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self { pages: vec![] }
    }
}

impl Storage for InMemoryStorage {
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

// TODO #[cfg(test)]
pub mod test {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;
    use crate::storage::Storage;

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
        fn get(&self, index: PageIndex) -> Result<&Page, StorageError> {
            self.inner.get(index)
        }

        fn insert(&mut self, page: crate::page::Page) -> Result<PageIndex, StorageError> {
            self.page_count.fetch_add(1, Ordering::Relaxed);

            self.inner.insert(page)
        }

        fn write<TReturn>(
            &mut self,
            index: PageIndex,
            write: impl FnOnce(&mut Page) -> TReturn,
        ) -> Result<TReturn, StorageError> {
            self.inner.write(index, write)
        }
    }
}
