pub mod in_memory;
pub mod instrumented;

use crate::sync::atomic::{AtomicU64, Ordering};
use std::{fmt::Debug, num::NonZeroU64};

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

    #[error("out of space")]
    OutOfSpace,
}

#[derive(Debug, Clone, Copy, Pod, Zeroable, PartialEq, Eq, Hash)]
#[repr(transparent)]
// TODO we need to separate the type that's used for storage from the type we give out to users,
// that should simplify the matters of logical/physical indices
// TODO the index should have some sort of storage id (can we have a type-level [lifetime?] tag that ties it to
// an instance of a block?)
pub struct PageIndex(u64);

impl PageIndex {
    #[must_use]
    pub(crate) const fn max() -> Self {
        Self(u64::MAX)
    }

    #[must_use]
    pub(crate) const fn from_value(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub(crate) const fn value(self) -> u64 {
        self.0
    }

    const fn next(self) -> Self {
        Self(self.0.strict_add(1))
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

#[derive(Debug, Clone, Copy, Zeroable, Pod, PartialEq, PartialOrd, Eq, Ord)]
#[repr(transparent)]
pub struct TransactionalTimestamp(u64);

impl TransactionalTimestamp {
    #[must_use]
    pub(crate) const fn zero() -> Self {
        Self(0)
    }
}

pub trait Transaction<'storage>: Send + Debug {
    type Storage: Storage + 'storage;

    fn id(&self) -> TransactionId;
    // TODO unify get and write, don't take callbacks, return a ref with the same lifetime as &self
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

    fn reserve(
        &mut self,
    ) -> Result<<Self::Storage as Storage>::PageReservation<'storage>, StorageError>;

    fn insert_reserved(
        &mut self,
        reservation: <Self::Storage as Storage>::PageReservation<'storage>,
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
    type Transaction<'storage>: Transaction<'storage, Storage = Self>
    where
        Self: 'storage;

    fn transaction(&self) -> Result<Self::Transaction<'_>, StorageError>
    where
        Self: Sized;
}
