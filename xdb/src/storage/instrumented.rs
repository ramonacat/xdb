use crate::{
    storage::PageReservation,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use std::marker::PhantomData;

use super::{StorageError, Transaction};
use crate::storage::Storage;

pub struct InstrumentedPageReservation<'a, TStorage: Storage + 'a>(TStorage::PageReservation<'a>);

impl<'a, TStorage: Storage + 'a> PageReservation<'a> for InstrumentedPageReservation<'a, TStorage> {
    type Storage = InstrumentedStorage<TStorage>;

    fn index(&self) -> <<Self as PageReservation<'a>>::Storage as Storage>::PageId {
        self.0.index()
    }
}

#[derive(Debug)]
pub struct InstrumentedTransaction<'a, TStorage: Storage>(
    TStorage::Transaction<'a>,
    Arc<AtomicUsize>,
    PhantomData<&'a TStorage>,
);

impl<'a, TStorage: Storage> Transaction<'a> for InstrumentedTransaction<'a, TStorage> {
    type Storage = InstrumentedStorage<TStorage>;

    fn read<TReturn, const N: usize>(
        &mut self,
        indices: impl Into<[TStorage::PageId; N]>,
        read: impl FnOnce([&TStorage::Page; N]) -> TReturn,
    ) -> Result<TReturn, StorageError<TStorage::PageId>> {
        self.0.read(indices, read)
    }

    fn write<TReturn, const N: usize>(
        &mut self,
        indices: impl Into<[TStorage::PageId; N]>,
        write: impl FnOnce([&mut TStorage::Page; N]) -> TReturn,
    ) -> Result<TReturn, StorageError<TStorage::PageId>> {
        self.0.write(indices, write)
    }

    fn reserve(
        &mut self,
    ) -> Result<InstrumentedPageReservation<'a, TStorage>, StorageError<TStorage::PageId>> {
        Ok(InstrumentedPageReservation(self.0.reserve()?))
    }

    fn insert_reserved(
        &mut self,
        reservation: InstrumentedPageReservation<'a, TStorage>,
        page: TStorage::Page,
    ) -> Result<(), StorageError<TStorage::PageId>> {
        self.1.fetch_add(1, Ordering::Relaxed);

        self.0.insert_reserved(reservation.0, page)
    }

    fn insert(
        &mut self,
        page: TStorage::Page,
    ) -> Result<TStorage::PageId, StorageError<TStorage::PageId>> {
        self.1.fetch_add(1, Ordering::Relaxed);

        self.0.insert(page)
    }

    fn delete(&mut self, page: TStorage::PageId) -> Result<(), StorageError<TStorage::PageId>> {
        self.0.delete(page)
    }

    fn commit(self) -> Result<(), StorageError<TStorage::PageId>> {
        self.0.commit()
    }

    fn rollback(self) -> Result<(), StorageError<TStorage::PageId>> {
        self.0.rollback()
    }

    fn id(&self) -> super::TransactionId {
        self.0.id()
    }
}

// TODO generalize this so more metrics can be extracted (transactions per second, total
// transactions, writes, reads, latency, etc.)
#[derive(Debug)]
pub struct InstrumentedStorage<T: Storage> {
    page_count: Arc<AtomicUsize>,
    inner: T,
}

impl<T: Storage> InstrumentedStorage<T> {
    pub const fn new(inner: T, page_count: Arc<AtomicUsize>) -> Self {
        Self { page_count, inner }
    }
}

impl<T: Storage> Storage for InstrumentedStorage<T> {
    type PageReservation<'a>
        = InstrumentedPageReservation<'a, T>
    where
        T: 'a;

    type Transaction<'a>
        = InstrumentedTransaction<'a, T>
    where
        T: 'a;

    type PageId = T::PageId;
    type Page = T::Page;

    fn transaction(&self) -> Result<Self::Transaction<'_>, StorageError<T::PageId>> {
        Ok(InstrumentedTransaction(
            self.inner.transaction()?,
            self.page_count.clone(),
            PhantomData,
        ))
    }
}
