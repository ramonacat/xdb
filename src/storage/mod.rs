pub mod in_memory;

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

pub trait Transaction<'storage> {
    fn read<T>(&self, index: PageIndex, read: impl FnOnce(&Page) -> T) -> Result<T, StorageError>;

    // TODO take a non-mut self reference
    fn write<T>(
        &mut self,
        index: PageIndex,
        write: impl FnOnce(&mut Page) -> T,
    ) -> Result<T, StorageError>;
    // TODO take a non-mut self reference
    fn insert(&mut self, page: Page) -> Result<PageIndex, StorageError>;

    // TODO actually make this useful and ensure transactional consistency
    #[allow(unused)]
    fn commit(self) -> Result<(), StorageError>;
}

pub trait Storage {
    type Transaction<'a>: Transaction<'a>
    where
        Self: 'a;

    // TODO take a non-mut reference
    fn transaction<'storage>(
        &'storage mut self,
    ) -> Result<Self::Transaction<'storage>, StorageError>;
}
