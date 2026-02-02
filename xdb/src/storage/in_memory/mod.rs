mod block;
mod lock_manager;
mod transaction;

use crate::storage::in_memory::lock_manager::LockManager;
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
        self.page_guard.index()
    }
}

#[derive(Debug)]
pub struct InMemoryStorage {
    cow_copies: Block,
    lock_manager: LockManager,
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStorage {
    #[must_use]
    pub fn new() -> Self {
        Self {
            cow_copies: Block::new(),
            lock_manager: LockManager::new(Block::new()),
        }
    }
}

impl Storage for InMemoryStorage {
    type Transaction<'a> = InMemoryTransaction<'a>;
    type PageReservation<'a> = InMemoryPageReservation<'a>;

    fn transaction(&self) -> Result<Self::Transaction<'_>, StorageError> {
        Ok(InMemoryTransaction::new(self))
    }

    fn debug_locks(&self, page: PageIndex) -> String {
        self.lock_manager.debug_locks(page)
    }
}
