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

pub trait Storage {
    fn get(&self, index: PageIndex) -> Result<&Page, StorageError>;
    // TODO take a non-mut self reference
    // TODO make this called write instead, take a closure and provide the mutable reference only
    // for the scope of the closure
    fn get_mut(&mut self, index: PageIndex) -> Result<&mut Page, StorageError>;
    // TODO take a non-mut self reference
    fn insert(&mut self, page: Page) -> Result<PageIndex, StorageError>;
}
