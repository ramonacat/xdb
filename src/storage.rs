use bytemuck::{Pod, Zeroable};
use thiserror::Error;

use crate::page::Page;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("The page at index {0:?} does not exist")]
    PageNotFound(PageIndex),
}

#[derive(Debug, Clone, Copy, Pod, Zeroable)]
#[repr(transparent)]
pub struct PageIndex(u64);

pub trait Storage {
    fn get(&self, index: PageIndex) -> Result<&Page, StorageError>;
    fn get_mut(&mut self, index: PageIndex) -> Result<&mut Page, StorageError>;
    fn insert(&mut self, page: Page) -> Result<PageIndex, StorageError>;
}

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
