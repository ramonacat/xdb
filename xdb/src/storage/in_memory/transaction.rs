use log::debug;
use std::collections::HashSet;

use crate::{
    page::Page,
    storage::{
        PageIndex, StorageError, Transaction, TransactionId,
        in_memory::{
            InMemoryPageReservation, InMemoryStorage, lock_manager::VersionManagerTransaction,
        },
    },
};

#[derive(Debug)]
pub struct InMemoryTransaction<'storage> {
    id: TransactionId,
    version_manager: VersionManagerTransaction<'storage>,
    finalized: bool,
    reserved_pages: HashSet<PageIndex>,
}

impl<'storage> InMemoryTransaction<'storage> {
    pub fn new(storage: &'storage InMemoryStorage) -> Self {
        let id = TransactionId::next();

        Self {
            id,
            finalized: false,
            reserved_pages: HashSet::new(),
            version_manager: VersionManagerTransaction::new(id, storage),
        }
    }
}

impl Drop for InMemoryTransaction<'_> {
    fn drop(&mut self) {
        // TODO do an actual rollback
    }
}

impl<'storage> Transaction<'storage> for InMemoryTransaction<'storage> {
    type Storage = InMemoryStorage;

    fn read<T, const N: usize>(
        &mut self,
        indices: impl Into<[PageIndex; N]>,
        read: impl FnOnce([&Page; N]) -> T,
    ) -> Result<T, StorageError> {
        let indices: [PageIndex; N] = indices.into();

        let guards: [_; N] = indices
            .map(|x| self.version_manager.read(x))
            .into_iter()
            .collect::<Result<Vec<_>, StorageError>>()?
            .try_into()
            .unwrap();

        Ok(read(guards.each_ref().map(|x| &**x)))
    }

    // TODO do we actually need to differentiate between read() and write()???
    fn write<T, const N: usize>(
        &mut self,
        indices: impl Into<[PageIndex; N]>,
        write: impl FnOnce([&mut Page; N]) -> T,
    ) -> Result<T, StorageError> {
        let indices: [PageIndex; N] = indices.into();

        let mut guards: [_; N] = indices
            .map(|x| self.version_manager.read(x))
            .into_iter()
            .collect::<Result<Vec<_>, StorageError>>()?
            .try_into()
            .unwrap();

        Ok(write(guards.each_mut().map(|x| &mut **x)))
    }

    fn reserve(&mut self) -> Result<InMemoryPageReservation<'storage>, StorageError> {
        let page_guard = self.version_manager.reserve();
        self.reserved_pages.insert(page_guard.index());

        Ok(InMemoryPageReservation { page_guard })
    }

    fn insert_reserved(
        &mut self,
        reservation: InMemoryPageReservation<'storage>,
        page: Page,
    ) -> Result<(), StorageError> {
        let InMemoryPageReservation { page_guard } = reservation;

        self.reserved_pages.remove(&page_guard.index());
        self.version_manager.insert_reserved(page_guard, page);

        Ok(())
    }

    fn insert(&mut self, page: Page) -> Result<PageIndex, StorageError> {
        let reserved = self.version_manager.reserve();
        let index = reserved.index();
        self.version_manager.insert_reserved(reserved, page);

        Ok(index)
    }

    fn delete(&mut self, page: PageIndex) -> Result<(), StorageError> {
        self.version_manager.delete(page);

        Ok(())
    }

    fn commit(mut self) -> Result<(), StorageError> {
        debug!("[{:?}] committing transaction", self.id);

        self.version_manager.commit()?;

        debug!("[{:?}] commit succesful", self.id);

        Ok(())
    }

    fn rollback(mut self) -> Result<(), StorageError> {
        debug!("[{:?}] rolling back transaction", self.id);

        // TODO free all of the cow copies
        self.finalized = true;

        Ok(())
    }
}
