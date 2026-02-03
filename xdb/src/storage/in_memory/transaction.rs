use crate::{
    page::Page,
    storage::{
        PageIndex, StorageError, Transaction,
        in_memory::{
            InMemoryPageReservation, InMemoryStorage,
            version_manager::transaction::VersionManagedTransaction,
        },
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
        let page_guard = self.version_manager.reserve()?;

        Ok(InMemoryPageReservation { page_guard })
    }

    fn insert_reserved(
        &mut self,
        reservation: InMemoryPageReservation<'storage>,
        page: Page,
    ) -> Result<(), StorageError> {
        let InMemoryPageReservation { page_guard } = reservation;

        self.version_manager.insert_reserved(page_guard, page)?;

        Ok(())
    }

    fn insert(&mut self, page: Page) -> Result<PageIndex, StorageError> {
        let reserved = self.version_manager.reserve()?;
        let index = reserved.index();
        self.version_manager.insert_reserved(reserved, page)?;

        Ok(index)
    }

    fn delete(&mut self, page: PageIndex) -> Result<(), StorageError> {
        self.version_manager.delete(page)?;

        Ok(())
    }

    fn commit(mut self) -> Result<(), StorageError> {
        self.version_manager.commit()?;

        Ok(())
    }

    fn rollback(mut self) -> Result<(), StorageError> {
        self.version_manager.rollback();

        Ok(())
    }
}
