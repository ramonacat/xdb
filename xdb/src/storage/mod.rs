pub mod in_memory;
pub mod instrumented;

use std::{
    fmt::Debug, num::NonZeroU64, sync::atomic::{AtomicU64, Ordering}
};

use bytemuck::{Pod, PodInOption, Zeroable, ZeroableInOption};
use thiserror::Error;

use crate::page::Page;

#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum StorageError {
    #[error("The page at index {0:?} does not exist")]
    PageNotFound(PageIndex),
    #[error("Would deadlock when locking {0:?}")]
    // TODO this should also have a transaction ID
    Deadlock(PageIndex),
}

#[derive(Debug, Clone, Copy, Pod, Zeroable, PartialEq, Eq, Hash)]
#[repr(transparent)]
// TODO should the index have some sort of storage id?
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

// TODO does numeric ordering always imply a happens-before relationship?
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct TransactionId(NonZeroU64);

unsafe impl PodInOption for TransactionId {}
unsafe impl ZeroableInOption for TransactionId {}

static LATEST_TRANSACTION_ID: AtomicU64 = AtomicU64::new(1);

impl TransactionId {
    fn next() -> Self {
        Self(NonZeroU64::new(LATEST_TRANSACTION_ID.fetch_add(1, Ordering::Relaxed)).unwrap())
    }
}

pub trait Transaction<'storage, TPageReservation: PageReservation<'storage>>: Send + Debug {
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

    fn reserve(&mut self) -> Result<TPageReservation, StorageError>;

    fn insert_reserved(
        &mut self,
        reservation: TPageReservation,
        page: Page,
    ) -> Result<(), StorageError>;

    fn insert(&mut self, page: Page) -> Result<PageIndex, StorageError>;

    fn delete(&mut self, page: PageIndex) -> Result<(), StorageError>;

    fn commit(self) -> Result<(), StorageError>;
    fn rollback(self) -> Result<(), StorageError>;
}

pub trait Storage: Send + Sync + Debug {
    type PageReservation<'storage>: PageReservation<'storage>
    where
        Self: 'storage;
    type Transaction<'storage>: Transaction<'storage, Self::PageReservation<'storage>>
    where
        Self: 'storage;

    fn transaction(&self) -> Result<Self::Transaction<'_>, StorageError>
    where
        Self: Sized;

    // TODO separte `StorageDebug` trait?
    fn debug_locks(&self, page: PageIndex) -> String;
}
