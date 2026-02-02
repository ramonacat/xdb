mod bitmap;
mod block;
mod lock_manager;
mod transaction;

use std::collections::BTreeSet;

use crate::storage::in_memory::bitmap::Bitmap;
use crate::sync::Arc;
use crate::sync::Mutex;
use crate::sync::atomic::AtomicBool;
use crate::sync::atomic::Ordering;
use crate::thread;

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
    vacuum_running: Arc<AtomicBool>,

    cow_copies: Arc<Block>,
    cow_copies_freemap: Arc<Bitmap>,
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
        let vacuum_running = Arc::new(AtomicBool::new(true));
        let cow_copies = Arc::new(Block::new("cow copies".into()));
        let cow_copies_freemap = Arc::new(Bitmap::new("cow copies freemap".into()));

        {
            let running_transactions = running_transactions.clone();
            let vacuum_running = vacuum_running.clone();
            let cow_copies = cow_copies.clone();
            let cow_copies_freemap = cow_copies_freemap.clone();

            thread::spawn(move || {
                while vacuum_running.load(Ordering::Relaxed) {
                    let Some(min_txid) = running_transactions.lock().unwrap().first().copied()
                    else {
                        thread::yield_now();
                        continue;
                    };

                    let mut index = PageIndex::from_value(1);

                    while let Some(page) = cow_copies.try_get(index) {
                        if let Ok(page) = page.lock_nowait()
                            && let Some(visible_until) = page.visible_until()
                            && visible_until < min_txid
                        {
                            cow_copies_freemap.set(index.0);
                        }

                        index = index.next();
                    }
                }
            });
        }

        Self {
            // TODO give the InMemoryStorage a name so we can differentiate the blocks if we have
            // multiple storages?
            cow_copies,
            data: Block::new("main block".into()),
            running_transactions,
            vacuum_running,
            cow_copies_freemap,
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

impl Drop for InMemoryStorage {
    fn drop(&mut self) {
        self.vacuum_running.store(false, Ordering::Relaxed);
    }
}
