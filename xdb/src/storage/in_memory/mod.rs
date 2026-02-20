mod bitmap;
mod block;
mod transaction;
mod version_manager;

use bytemuck::Zeroable;

use crate::storage::in_memory::bitmap::Bitmap;
use crate::storage::in_memory::version_manager::{VersionManager, VersionedBlock};
use crate::storage::{PageId, SerializedPageId};
use crate::sync::Arc;

use crate::storage::in_memory::transaction::InMemoryTransaction;

use crate::storage::{
    PageIndex, PageReservation, Storage, StorageError, in_memory::block::UninitializedPageGuard,
};

// TODO impl Drop to return the page to free pool if it doesn't get written
pub struct InMemoryPageReservation<'storage> {
    page_guard: UninitializedPageGuard<'storage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct InMemoryPageId(PageIndex);

impl PageId for InMemoryPageId {
    fn sentinel() -> Self {
        Self(PageIndex::max())
    }

    fn first() -> Self {
        Self(PageIndex::zeroed())
    }

    fn serialize(&self) -> SerializedPageId {
        SerializedPageId(self.0.value().to_le_bytes())
    }

    fn deserialize(raw: SerializedPageId) -> Self {
        Self(PageIndex::from_value(u64::from_le_bytes(raw.0)))
    }
}

impl InMemoryPageId {
    #[must_use]
    pub const fn from_value(value: u64) -> Self {
        Self(PageIndex::from_value(value))
    }
}

impl From<InMemoryPageId> for [InMemoryPageId; 1] {
    fn from(value: InMemoryPageId) -> Self {
        [value]
    }
}

impl<'storage> PageReservation<'storage> for InMemoryPageReservation<'storage> {
    type Storage = InMemoryStorage;

    fn index(&self) -> InMemoryPageId {
        InMemoryPageId(self.page_guard.physical_index())
    }
}

#[derive(Debug)]
// TODO should probably just wrap the whole thing in an Arc, instead of practically each field
pub struct InMemoryStorage {
    version_manager: VersionManager,
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStorage {
    #[must_use]
    // TODO give the InMemoryStorage a name so we can differentiate the blocks if we have
    // multiple storages?
    pub fn new() -> Self {
        Self {
            version_manager: VersionManager::new(Arc::new(VersionedBlock::new())),
        }
    }
}

impl Storage for InMemoryStorage {
    type PageReservation<'a> = InMemoryPageReservation<'a>;
    type Transaction<'a> = InMemoryTransaction<'a>;
    type PageId = InMemoryPageId;

    fn transaction(&self) -> Result<Self::Transaction<'_>, StorageError<Self::PageId>> {
        Ok(InMemoryTransaction::new(self))
    }
}
