pub mod in_memory;
pub mod instrumented;
pub(super) mod page;

use std::fmt::Debug;
use std::hash::Hash;
use std::num::NonZeroU64;

use bytemuck::{AnyBitPattern, NoUninit, Pod, PodInOption, Zeroable, ZeroableInOption};
use thiserror::Error;

use crate::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum StorageError<T: PageId> {
    #[error("The page at index {0:?} does not exist")]
    PageNotFound(T),
    #[error("Would deadlock when locking {0:?}")]
    // TODO this should also have a transaction ID
    Deadlock(T),

    #[error("out of space")]
    OutOfSpace,
}

#[derive(Debug, Clone, Copy, Pod, Zeroable, PartialEq, Eq, Hash)]
#[repr(transparent)]
// TODO we need to separate the type that's used for storage from the type we give out to users,
// that should simplify the matters of logical/physical indices
// TODO the index should have some sort of storage id (can we have a type-level [lifetime?] tag that ties it to
// an instance of a block?)
// TODO move this deeper, this should be only used internally for the storage implementation
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
    type Storage: Storage;

    fn index(&self) -> <<Self as PageReservation<'storage>>::Storage as Storage>::PageId;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable, Hash)]
#[repr(transparent)]
#[must_use]
pub struct SerializedPageId([u8; 8]);

impl From<SerializedPageId> for [SerializedPageId; 1] {
    fn from(value: SerializedPageId) -> Self {
        [value]
    }
}

impl SerializedPageId {
    pub const fn new(raw: [u8; 8]) -> Self {
        Self(raw)
    }

    #[must_use]
    pub const fn raw(self) -> [u8; 8] {
        self.0
    }
}

pub const SENTINEL_PAGE_ID: SerializedPageId = SerializedPageId([0xFF; _]);
pub const FIRST_PAGE_ID: SerializedPageId = SerializedPageId([0x00; _]);

pub trait PageId: Debug + Into<[Self; 1]> + Eq + Hash {
    fn sentinel() -> Self;
    fn first() -> Self;

    // TODO should the serialze/deserialize be happening in transaction instead?
    fn serialize(&self) -> SerializedPageId;
    fn deserialize(raw: SerializedPageId) -> Self;
}

type PageIdOf<T> = <T as Storage>::PageId;
type ErrorOf<T> = StorageError<PageIdOf<T>>;

pub trait Page: Debug {
    fn from_data<T: AnyBitPattern + NoUninit>(data: T) -> Self;
    fn data<T: AnyBitPattern>(&self) -> &T;
    fn data_mut<T: AnyBitPattern + NoUninit>(&mut self) -> &mut T;
}

pub trait Transaction<'storage>: Send + Debug {
    type Storage: Storage + 'storage;

    fn id(&self) -> TransactionId;
    // TODO unify get and write, don't take callbacks, return a ref with the same lifetime as &self
    fn read<T, const N: usize>(
        &mut self,
        indices: impl Into<[PageIdOf<Self::Storage>; N]>,
        read: impl FnOnce([&<Self::Storage as Storage>::Page; N]) -> T,
    ) -> Result<T, ErrorOf<Self::Storage>>;

    fn write<T, const N: usize>(
        &mut self,
        indices: impl Into<[PageIdOf<Self::Storage>; N]>,
        write: impl FnOnce([&mut <Self::Storage as Storage>::Page; N]) -> T,
    ) -> Result<T, ErrorOf<Self::Storage>>;

    fn reserve(
        &mut self,
    ) -> Result<<Self::Storage as Storage>::PageReservation<'storage>, ErrorOf<Self::Storage>>;

    fn insert_reserved(
        &mut self,
        reservation: <Self::Storage as Storage>::PageReservation<'storage>,
        page: <Self::Storage as Storage>::Page,
    ) -> Result<(), ErrorOf<Self::Storage>>;

    fn insert(
        &mut self,
        page: <Self::Storage as Storage>::Page,
    ) -> Result<PageIdOf<Self::Storage>, ErrorOf<Self::Storage>>;

    fn delete(&mut self, page: PageIdOf<Self::Storage>) -> Result<(), ErrorOf<Self::Storage>>;

    fn commit(self) -> Result<(), ErrorOf<Self::Storage>>;
    fn rollback(self) -> Result<(), ErrorOf<Self::Storage>>;
}

pub trait Storage: Send + Sync + Debug {
    type PageReservation<'storage>: PageReservation<'storage, Storage = Self>
    where
        Self: 'storage;

    type Transaction<'storage>: Transaction<'storage, Storage = Self>
    where
        Self: 'storage;

    type PageId: PageId;
    type Page: Page;

    fn transaction(&self) -> Result<Self::Transaction<'_>, ErrorOf<Self>>
    where
        Self: Sized;
}
