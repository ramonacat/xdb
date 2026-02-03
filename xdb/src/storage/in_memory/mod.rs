mod bitmap;
mod block;
mod lock_manager;
mod transaction;
mod vacuum;

use std::collections::BTreeSet;

use crate::storage::in_memory::bitmap::Bitmap;
use crate::storage::in_memory::vacuum::Vacuum;
use crate::sync::Arc;
use crate::sync::Mutex;

use crate::storage::TransactionId;
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
// TODO should probably just wrap the whole thing in an Arc, instead of practically each field
pub struct InMemoryStorage {
    data: Block,
    running_transactions: Arc<Mutex<BTreeSet<TransactionId>>>,

    cow_copies: Arc<Block>,
    cow_copies_freemap: Arc<Bitmap>,
    #[allow(unused)]
    vacuum: Vacuum,
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStorage {
    #[must_use]
    pub fn new() -> Self {
        let running_transactions = Arc::new(Mutex::new(BTreeSet::new()));
        let cow_copies = Arc::new(Block::new("cow copies".into()));
        let cow_copies_freemap = Arc::new(Bitmap::new("cow copies freemap".into()));

        let vacuum = Vacuum::start(
            running_transactions.clone(),
            cow_copies.clone(),
            cow_copies_freemap.clone(),
        );
        Self {
            // TODO give the InMemoryStorage a name so we can differentiate the blocks if we have
            // multiple storages?
            cow_copies,
            data: Block::new("main block".into()),
            running_transactions,
            cow_copies_freemap,
            vacuum,
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
