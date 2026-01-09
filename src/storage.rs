use std::fmt::Display;

use bytemuck::{Pod, Zeroable};
use thiserror::Error;

use crate::page::Page;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("The page at index {0:?} does not exist")]
    PageNotFound(PageIndex),
}

#[derive(Debug, Clone, Copy, Pod, Zeroable, PartialEq, Eq)]
#[repr(transparent)]
pub struct PageIndex(u64);

impl Display for PageIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub trait Storage {
    fn get(&self, index: PageIndex) -> Result<&Page, StorageError>;
    fn get_mut(&mut self, index: PageIndex) -> Result<&mut Page, StorageError>;
    fn insert(&mut self, page: Page) -> Result<PageIndex, StorageError>;
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

impl Storage for InMemoryStorage {
    fn get(&self, index: PageIndex) -> Result<&Page, StorageError> {
        self.pages
            .get(index.0 as usize)
            .map_or_else(|| Err(StorageError::PageNotFound(index)), Ok)
    }

    // TODO return some sort of a WritablePageHandle object, so that we can persist after the write
    // as neccessary
    fn get_mut(&mut self, index: PageIndex) -> Result<&mut Page, StorageError> {
        self.pages
            .get_mut(index.0 as usize)
            .map_or_else(|| Err(StorageError::PageNotFound(index)), Ok)
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
        fn get(&self, index: PageIndex) -> Result<&crate::page::Page, StorageError> {
            self.inner.get(index)
        }

        fn get_mut(&mut self, index: PageIndex) -> Result<&mut crate::page::Page, StorageError> {
            self.inner.get_mut(index)
        }

        fn insert(&mut self, page: crate::page::Page) -> Result<PageIndex, StorageError> {
            self.page_count.fetch_add(1, Ordering::Relaxed);

            self.inner.insert(page)
        }
    }
}
