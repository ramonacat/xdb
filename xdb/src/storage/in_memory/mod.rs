mod bitmap;
mod block;
mod transaction;
mod version_manager;

use crate::storage::in_memory::bitmap::Bitmap;
use crate::storage::in_memory::version_manager::VersionManager;
use crate::sync::Arc;

use crate::storage::in_memory::transaction::InMemoryTransaction;

use crate::storage::{
    PageIndex, PageReservation, Storage, StorageError,
    in_memory::block::{Block, UninitializedPageGuard},
};

// TODO impl Drop to return the page to free pool if it doesn't get written
pub struct InMemoryPageReservation<'storage> {
    page_guard: UninitializedPageGuard<'storage>,
}

impl<'storage> PageReservation<'storage> for InMemoryPageReservation<'storage> {
    fn index(&self) -> PageIndex {
        self.page_guard.physical_index()
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
        let freemap = Arc::new(Bitmap::new("freemap".into()));

        Self {
            version_manager: VersionManager::new(
                Arc::new(Block::new("main block".into())),
                freemap,
            ),
        }
    }
}

impl Storage for InMemoryStorage {
    type Transaction<'a> = InMemoryTransaction<'a>;
    type PageReservation<'a> = InMemoryPageReservation<'a>;

    fn transaction(&self) -> Result<Self::Transaction<'_>, StorageError> {
        Ok(InMemoryTransaction::new(self))
    }
}
