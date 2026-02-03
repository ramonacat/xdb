use std::collections::BTreeSet;

use crate::storage::TransactionId;
use crate::storage::in_memory::Bitmap;
use crate::storage::in_memory::block::Block;
use crate::storage::in_memory::version_manager::transaction::VersionManagedTransaction;
use crate::storage::in_memory::version_manager::vacuum::Vacuum;
use crate::sync::{Arc, Mutex};

pub mod transaction;
mod vacuum;

#[derive(Debug)]
pub struct VersionManager {
    // TODO this block should be one with the cow_pages
    data: Block,
    #[allow(unused)]
    vacuum: Vacuum,
    running_transactions: Arc<Mutex<BTreeSet<TransactionId>>>,
    cow_pages: Arc<Block>,
    cow_pages_freemap: Arc<Bitmap>,
}

impl VersionManager {
    pub fn new(
        data: Block,
        running_transactions: Arc<Mutex<BTreeSet<TransactionId>>>,
        cow_pages: Arc<Block>,
        cow_pages_freemap: Arc<Bitmap>,
    ) -> Self {
        Self {
            data,
            vacuum: Vacuum::start(
                running_transactions.clone(),
                cow_pages.clone(),
                cow_pages_freemap.clone(),
            ),
            running_transactions,
            cow_pages,
            cow_pages_freemap,
        }
    }

    pub fn start_transaction(&self) -> VersionManagedTransaction<'_> {
        let id = TransactionId::next();

        self.running_transactions.lock().unwrap().insert(id);

        VersionManagedTransaction::new(id, self)
    }
}
