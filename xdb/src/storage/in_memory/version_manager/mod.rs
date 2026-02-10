use std::mem::MaybeUninit;
use std::ptr::NonNull;
use tracing::error;
use tracing::instrument;
use tracing::trace;

use crate::page::Page;
use crate::storage::TransactionalTimestamp;
use crate::storage::in_memory::Bitmap;
use crate::storage::in_memory::block::Block;
use crate::storage::in_memory::block::PageReadGuard;
use crate::storage::in_memory::version_manager::committer::Committer;
use crate::storage::in_memory::version_manager::transaction::VersionManagedTransaction;
use crate::storage::in_memory::version_manager::transaction_log::TransactionLog;
use crate::storage::in_memory::version_manager::vacuum::Vacuum;
use crate::storage::{PageIndex, TransactionId};
use crate::sync::{Arc, Mutex};

mod committer;
pub mod transaction;
pub mod transaction_log;
mod vacuum;

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
    data: Arc<Block>,
    // TODO rename -> freemap
    freemap: Arc<Bitmap>,
    #[allow(unused)]
    vacuum: Vacuum,
    committer: Committer,
    transaction_log: Arc<TransactionLog>,
    // TODO instead of a mutex, we should probably have per-thread queues or something (a lock-free ring-buffer
    // perhaps?)
    // TODO give it a better name, it is not really a queue
    // TODO sending raw pointers kinda sucks, we probably should just do PageIndices?
    recycled_page_queue: Mutex<Vec<(NonNull<MaybeUninit<Page>>, PageIndex)>>,
}

unsafe impl Send for VersionManager {}
unsafe impl Sync for VersionManager {}

impl VersionManager {
    pub fn new(data: Arc<Block>, freemap: Arc<Bitmap>) -> Self {
        let log = Arc::new(TransactionLog::new());
        Self {
            vacuum: Vacuum::start(log.clone(), data.clone(), freemap.clone()),
            committer: Committer::new(data.clone(), log.clone()),
            // TODO this should be an argument probably? and we should have some sorta storage
            // loader or something that'll load data from disk (or create new files/memory
            // structures)
            transaction_log: log,
            data,
            freemap,
            recycled_page_queue: Mutex::new(Vec::new()),
        }
    }

    pub fn start_transaction(&self) -> VersionManagedTransaction<'_> {
        let id = TransactionId::next();

        VersionManagedTransaction::new(id, self, self.transaction_log.start_transaction(id))
    }
}

#[instrument(skip(data))]
// TODO this should not be a free function, but instead a method on `VersionAwareBlock` or
// something like that.
fn get_matching_version(
    data: &'_ Block,
    logical_index: PageIndex,
    timestamp: TransactionalTimestamp,
) -> PageReadGuard<'_> {
    let mut locks = vec![];

    let mut main_lock = data.get(logical_index);
    assert!(main_lock.previous_version().is_none());

    while !main_lock.is_visible_at(timestamp) {
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
        main_lock = data.get(next);

        assert!(main_lock.previous_version() == Some(previous_version));
    }

    // keep all the locks till here to avoid situations where vacuum changes the
    // next_/previous_version links while we're looking at them
    drop(locks);

    trace!(
        physical_index = ?main_lock.physical_index(),
        "found",
    );

    main_lock
}
