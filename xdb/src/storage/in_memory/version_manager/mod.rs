use std::mem::MaybeUninit;
use std::ptr::NonNull;

use crate::page::Page;
use crate::storage::in_memory::Bitmap;
use crate::storage::in_memory::block::{Block, PageRef};
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
// TODO rename -> "TransactionPage" or something
struct CowPage<'storage> {
    main: PageRef<'storage>,
    cow: Option<PageRef<'storage>>,
    deleted: bool,
    inserted: bool,
}

impl CowPage<'_> {
    fn into_raw(self) -> RawCowPage {
        RawCowPage {
            main: (
                self.main.as_ptr(),
                self.main.logical_index(),
                self.main.physical_index(),
            ),
            cow: self
                .cow
                .map(|x| (x.as_ptr(), x.logical_index(), x.physical_index())),
            deleted: self.deleted,
            inserted: self.inserted,
        }
    }
}

#[derive(Debug)]
// TODO this is an awful hack, can we find a better way to work with the lifetime issues
// between threads?
struct RawCowPage {
    main: (NonNull<Page>, Option<PageIndex>, PageIndex),
    cow: Option<(NonNull<Page>, Option<PageIndex>, PageIndex)>,
    deleted: bool,
    inserted: bool,
}

unsafe impl Send for RawCowPage {}

impl RawCowPage {
    unsafe fn reconstruct(self, block: &'_ Block) -> CowPage<'_> {
        let main = unsafe { PageRef::new(self.main.0, block, self.main.1, self.main.2) };

        CowPage {
            main,
            cow: self
                .cow
                .map(|x| unsafe { PageRef::new(x.0, block, x.1, x.2) }),
            deleted: self.deleted,
            inserted: self.inserted,
        }
    }
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
