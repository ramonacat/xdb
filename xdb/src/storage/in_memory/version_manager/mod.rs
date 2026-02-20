use tracing::debug;
use tracing::error;

use crate::storage::StorageError;
use crate::storage::TransactionalTimestamp;
use crate::storage::in_memory::Bitmap;
use crate::storage::in_memory::InMemoryPageId;
use crate::storage::in_memory::block::Block;
use crate::storage::in_memory::block::PageReadGuard;
use crate::storage::in_memory::block::PageWriteGuard;
use crate::storage::in_memory::block::UninitializedPageGuard;
use crate::storage::in_memory::version_manager::committer::Committer;
use crate::storage::in_memory::version_manager::recycled_pages::Recycler;
use crate::storage::in_memory::version_manager::transaction::VersionManagedTransaction;
use crate::storage::in_memory::version_manager::transaction_log::TransactionLog;
use crate::storage::in_memory::version_manager::vacuum::Vacuum;
use crate::storage::{PageIndex, TransactionId};
use crate::sync::Arc;

mod committer;
mod recycled_pages;
pub mod transaction;
pub mod transaction_log;
mod vacuum;

#[derive(Debug)]
pub struct VersionedBlock {
    block: Block,
    freemap: Bitmap,
}

impl VersionedBlock {
    pub fn new() -> Self {
        Self {
            block: Block::new("storage".to_string()),
            freemap: Bitmap::new("freemap".to_string()),
        }
    }

    fn get(&'_ self, index: PageIndex) -> PageReadGuard<'_> {
        self.block.get(index)
    }

    fn get_at(
        &'_ self,
        logical_index: PageIndex,
        timestamp: TransactionalTimestamp,
    ) -> PageReadGuard<'_> {
        let mut locks = vec![];

        let mut main_lock = self.block.get(logical_index);
        assert!(main_lock.previous_version().is_none());

        while !main_lock.is_visible_at(timestamp) {
            debug!(
                "{logical_index:?}/{:?} not visible at {timestamp:?} ({:?}/{:?}), checking next",
                main_lock.physical_index(),
                main_lock.visible_from(),
                main_lock.visible_until(),
            );

            let Some(next) = main_lock.next_version() else {
                // TODO should we panic here? I think we should not be able to get to this
                // place if the database is in a valid state?
                error!(
                    latest_from = ?main_lock.visible_from(),
                    latest_until = ?main_lock.visible_until(),
                    physical_index = ?main_lock.physical_index(),
                    "page not found"
                );
                panic!(
                    "page {logical_index:?}/{:?} not found (transaction timestamp: {:?}, latest version visible: {:?}/{:?})",
                    main_lock.physical_index(),
                    timestamp,
                    main_lock.visible_from(),
                    main_lock.visible_until()
                );
            };

            let previous_version = main_lock.physical_index();

            locks.push(main_lock);
            main_lock = self.block.get(next);

            assert!(main_lock.previous_version() == Some(previous_version));
        }

        // keep all the locks till here to avoid situations where vacuum changes the
        // next_/previous_version links while we're looking at them
        drop(locks);

        debug!(
            physical_index = ?main_lock.physical_index(),
            visible_from = ?main_lock.visible_from(),
            visible_until = ?main_lock.visible_until(),
            "found",
        );

        main_lock
    }

    fn try_get(&'_ self, index: PageIndex) -> Option<PageReadGuard<'_>> {
        self.block.try_get(index)
    }

    fn get_uninitialized(&'_ self, index: PageIndex) -> UninitializedPageGuard<'_> {
        self.block.get_uninitialized(index)
    }

    fn allocate(&'_ self) -> Result<UninitializedPageGuard<'_>, StorageError<InMemoryPageId>> {
        self.block.allocate()
    }

    fn allocated_page_count(&self) -> u64 {
        self.block.allocated_page_count()
    }

    fn take_free_pages(&self, max_count: usize) -> Vec<PageIndex> {
        self.freemap
            .find_and_unset(max_count)
            .into_iter()
            .map(|x| PageIndex(x as u64))
            .collect()
    }

    fn free_page(&self, page_guard: PageWriteGuard) {
        debug!(
            physical_index=?page_guard.physical_index(),
            visible_until=?page_guard.visible_until(),
            visible_from=?page_guard.visible_from(),
            is_free=?page_guard.is_free(),

            "freeing page"
        );
        let physical_index = page_guard.physical_index();

        drop(page_guard.reset());

        self.freemap.set(physical_index.0).unwrap();
    }
}

#[derive(Debug)]
enum TransactionPageAction {
    Read,
    Delete,
    // TODO this is a physical index of the CoWed page, clean the types up so that it's obvious
    Update(PageIndex),
    Insert,
}

#[derive(Debug)]
struct TransactionPage {
    logical_index: PageIndex,
    action: TransactionPageAction,
}

#[derive(Debug)]
pub struct VersionManager {
    data: Arc<VersionedBlock>,
    // TODO rename -> freemap
    committer: Committer,
    transaction_log: Arc<TransactionLog>,
    // TODO instead of a mutex, we should probably have per-thread queues or something (a lock-free ring-buffer
    // perhaps?)
    // TODO sending raw pointers kinda sucks, we probably should just do PageIndices?
    recycled_pages: Recycler,
}

unsafe impl Send for VersionManager {}
unsafe impl Sync for VersionManager {}

impl VersionManager {
    pub fn new(data: Arc<VersionedBlock>) -> Self {
        let log = Arc::new(TransactionLog::new());
        let vacuum = Vacuum::start(log.clone(), data.clone());

        Self {
            committer: Committer::new(data.clone(), log.clone()),
            // TODO this should be an argument probably? and we should have some sorta storage
            // loader or something that'll load data from disk (or create new files/memory
            // structures)
            transaction_log: log,
            data: data.clone(),
            recycled_pages: Recycler::new(data, vacuum),
        }
    }

    pub fn start_transaction(&self) -> VersionManagedTransaction<'_> {
        let id = TransactionId::next();

        VersionManagedTransaction::new(id, self, self.transaction_log.start_transaction(id))
    }
}
