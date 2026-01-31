mod block;
mod transaction;

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
    pages: Block,
    rollback_copies: Block,
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
            pages: Block::new(),
            rollback_copies: Block::new(),
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
