use crate::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::marker::PhantomData;

use super::{Page, PageIndex, StorageError, Transaction};
use crate::storage::Storage;

pub struct InstrumentedTransaction<
    'a,
    T: Transaction<'a, TStorage::PageReservation<'a>>,
    TStorage: Storage,
>(T, Arc<AtomicUsize>, PhantomData<&'a TStorage>);

impl<'a, TTx: Transaction<'a, TStorage::PageReservation<'a>>, TStorage: Storage>
    Transaction<'a, TStorage::PageReservation<'a>> for InstrumentedTransaction<'a, TTx, TStorage>
{
    fn read<TReturn, const N: usize>(
        &mut self,
        indices: impl Into<[PageIndex; N]>,
        read: impl FnOnce([&Page; N]) -> TReturn,
    ) -> Result<TReturn, StorageError> {
        self.0.read(indices, read)
    }

    fn write<TReturn, const N: usize>(
        &mut self,
        indices: impl Into<[PageIndex; N]>,
        write: impl FnOnce([&mut Page; N]) -> TReturn,
    ) -> Result<TReturn, StorageError> {
        self.0.write(indices, write)
    }

    fn reserve(&self) -> Result<TStorage::PageReservation<'a>, StorageError> {
        self.0.reserve()
    }

    fn insert_reserved(
        &mut self,
        reservation: TStorage::PageReservation<'a>,
        page: Page,
    ) -> Result<(), StorageError> {
        self.1.fetch_add(1, Ordering::Relaxed);

        self.0.insert_reserved(reservation, page)
    }

    fn insert(&mut self, page: Page) -> Result<PageIndex, StorageError> {
        self.1.fetch_add(1, Ordering::Relaxed);

        self.0.insert(page)
    }

    fn delete(&mut self, page: PageIndex) -> Result<(), StorageError> {
        self.0.delete(page)
    }

    fn commit(self) -> Result<(), StorageError> {
        self.0.commit()
    }

    fn rollback(self) -> Result<(), StorageError> {
        self.0.rollback()
    }
}

// TODO generalize this so more metrics can be extracted (transactions per second, total
// transactions, writes, reads, latency, etc.)
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
    type Transaction<'a>
        = InstrumentedTransaction<'a, T::Transaction<'a>, T>
    where
        T: 'a;

    type PageReservation<'a>
        = T::PageReservation<'a>
    where
        T: 'a;

    fn transaction(&self) -> Result<Self::Transaction<'_>, StorageError> {
        Ok(InstrumentedTransaction(
            self.inner.transaction()?,
            self.page_count.clone(),
            PhantomData,
        ))
    }
}
