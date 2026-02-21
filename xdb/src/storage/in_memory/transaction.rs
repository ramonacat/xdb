use crate::storage::{
    StorageError, Transaction,
    in_memory::{
        InMemoryPageId, InMemoryPageReservation, InMemoryStorage,
        version_manager::{transaction::VersionManagedTransaction, versioned_page::VersionedPage},
    },
};

#[derive(Debug)]
pub struct InMemoryTransaction<'storage> {
    version_manager: VersionManagedTransaction<'storage>,
}

impl<'storage> InMemoryTransaction<'storage> {
    pub fn new(storage: &'storage InMemoryStorage) -> Self {
        Self {
            version_manager: storage.version_manager.start_transaction(),
        }
    }
}

impl<'storage> Transaction<'storage> for InMemoryTransaction<'storage> {
    type Storage = InMemoryStorage;

    fn id(&self) -> crate::storage::TransactionId {
        self.version_manager.id()
    }

    fn read<T, const N: usize>(
        &mut self,
        indices: impl Into<[InMemoryPageId; N]>,
        read: impl FnOnce([&VersionedPage; N]) -> T,
    ) -> Result<T, StorageError<InMemoryPageId>> {
        let indices: [InMemoryPageId; N] = indices.into();

        let guards: [_; N] = indices
            .map(|x| self.version_manager.read(x.0))
            .into_iter()
            .collect::<Result<Vec<_>, StorageError<InMemoryPageId>>>()?
            .try_into()
            .unwrap();

        Ok(read(guards.each_ref().map(|x| &**x)))
    }

    // TODO do we actually need to differentiate between read() and write()???
    fn write<T, const N: usize>(
        &mut self,
        indices: impl Into<[InMemoryPageId; N]>,
        write: impl FnOnce([&mut VersionedPage; N]) -> T,
    ) -> Result<T, StorageError<InMemoryPageId>> {
        let indices: [InMemoryPageId; N] = indices.into();

        let mut guards: [_; N] = indices
            .map(|x| self.version_manager.write(x.0))
            .into_iter()
            .collect::<Result<Vec<_>, StorageError<InMemoryPageId>>>()?
            .try_into()
            .unwrap();

        Ok(write(guards.each_mut().map(|x| &mut **x)))
    }

    fn reserve(
        &mut self,
    ) -> Result<InMemoryPageReservation<'storage>, StorageError<InMemoryPageId>> {
        let page_guard = self.version_manager.reserve()?;

        Ok(InMemoryPageReservation { page_guard })
    }

    fn insert_reserved(
        &mut self,
        reservation: InMemoryPageReservation<'storage>,
        page: VersionedPage,
    ) -> Result<(), StorageError<InMemoryPageId>> {
        let InMemoryPageReservation { page_guard } = reservation;

        self.version_manager.insert_reserved(page_guard, page)?;

        Ok(())
    }

    fn insert(
        &mut self,
        page: VersionedPage,
    ) -> Result<InMemoryPageId, StorageError<InMemoryPageId>> {
        let reserved = self.version_manager.reserve()?;
        let physical_index = reserved.physical_index();
        self.version_manager.insert_reserved(reserved, page)?;

        Ok(InMemoryPageId(physical_index))
    }

    fn delete(&mut self, page: InMemoryPageId) -> Result<(), StorageError<InMemoryPageId>> {
        self.version_manager.delete(page.0)?;

        Ok(())
    }

    fn commit(mut self) -> Result<(), StorageError<InMemoryPageId>> {
        self.version_manager.commit()?;

        Ok(())
    }

    fn rollback(mut self) -> Result<(), StorageError<InMemoryPageId>> {
        self.version_manager.rollback();

        Ok(())
    }
}
