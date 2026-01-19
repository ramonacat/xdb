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

impl PageIndex {
    #[must_use]
    pub const fn zero() -> Self {
        Self(0)
    }
}

impl Display for PageIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub trait PageReservation<'storage> {
    fn index(&self) -> PageIndex;
}

impl From<PageIndex> for [PageIndex; 1] {
    fn from(value: PageIndex) -> Self {
        [value]
    }
}

pub trait Transaction<'storage, TPageReservation: PageReservation<'storage>> {
    fn read<T, const N: usize>(
        &self,
        indices: impl Into<[PageIndex; N]>,
        read: impl FnOnce([&Page; N]) -> T,
    ) -> Result<T, StorageError>;

    fn write<T, const N: usize>(
        &self,
        indices: impl Into<[PageIndex; N]>,
        write: impl FnOnce([&mut Page; N]) -> T,
    ) -> Result<T, StorageError>;

    fn reserve(&self) -> Result<TPageReservation, StorageError>;

    fn insert_reserved(
        &self,
        reservation: TPageReservation,
        page: Page,
    ) -> Result<(), StorageError>;

    fn insert(&self, page: Page) -> Result<PageIndex, StorageError>;

    fn delete(&self, page: PageIndex) -> Result<(), StorageError>;

    // TODO actually make this useful and ensure transactional consistency
    #[allow(unused)]
    fn commit(self) -> Result<(), StorageError>;
}

pub trait Storage {
    type PageReservation<'storage>: PageReservation<'storage>
    where
        Self: 'storage;
    type Transaction<'storage>: Transaction<'storage, Self::PageReservation<'storage>>
    where
        Self: 'storage;

    fn transaction(&self) -> Result<Self::Transaction<'_>, StorageError>
    where
        Self: Sized;
}
