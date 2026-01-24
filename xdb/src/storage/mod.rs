pub mod in_memory;
pub mod instrumented;

use bytemuck::{Pod, Zeroable};
use thiserror::Error;

use crate::page::Page;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("The page at index {0:?} does not exist")]
    PageNotFound(PageIndex),
}

#[derive(Debug, Clone, Copy, Pod, Zeroable, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct PageIndex(u64);

impl PageIndex {
    #[must_use]
    pub(crate) const fn zero() -> Self {
        Self(0)
    }

    #[cfg(test)]
    pub const fn from_value(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub(crate) const fn value(self) -> u64 {
        self.0
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

pub trait Transaction<'storage, TPageReservation: PageReservation<'storage>>: Send {
    fn read<T, const N: usize>(
        &mut self,
        indices: impl Into<[PageIndex; N]>,
        read: impl FnOnce([&Page; N]) -> T,
    ) -> Result<T, StorageError>;

    fn write<T, const N: usize>(
        &mut self,
        indices: impl Into<[PageIndex; N]>,
        write: impl FnOnce([&mut Page; N]) -> T,
    ) -> Result<T, StorageError>;

    fn reserve(&self) -> Result<TPageReservation, StorageError>;

    fn insert_reserved(
        &mut self,
        reservation: TPageReservation,
        page: Page,
    ) -> Result<(), StorageError>;

    fn insert(&mut self, page: Page) -> Result<PageIndex, StorageError>;

    fn delete(&mut self, page: PageIndex) -> Result<(), StorageError>;

    // TODO actually make this useful and ensure transactional consistency
    #[allow(unused)]
    fn commit(self) -> Result<(), StorageError>;
}

pub trait Storage: Send + Sync {
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
