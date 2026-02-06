use std::mem::MaybeUninit;
use std::ptr::NonNull;

use crate::page::{Page, PageVersion};
use crate::storage::in_memory::Bitmap;
use crate::storage::in_memory::block::{Block, PageRef, UninitializedPageGuard};
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
enum MainPageRef<'storage> {
    Initialized(PageRef<'storage>),
    Uninitialized(UninitializedPageGuard<'storage>),
}

#[derive(Debug)]
// TODO rename -> "TransactionPage" or something
struct CowPage<'storage> {
    main: MainPageRef<'storage>,
    cow: PageRef<'storage>,
    version: PageVersion,
    deleted: bool,
}

impl CowPage<'_> {
    const fn into_raw(self) -> RawCowPage {
        RawCowPage {
            main: match self.main {
                MainPageRef::Initialized(r) => RawMainPage::Initialized(r.as_ptr(), r.index()),
                MainPageRef::Uninitialized(r) => RawMainPage::Uninitialized(r.as_ptr(), r.index()),
            },
            cow: (self.cow.as_ptr(), self.cow.index()),
            version: self.version,
            deleted: self.deleted,
        }
    }
}

#[derive(Debug)]
enum RawMainPage {
    Initialized(NonNull<Page>, PageIndex),
    Uninitialized(NonNull<MaybeUninit<Page>>, PageIndex),
}

unsafe impl Send for RawMainPage {}

#[derive(Debug)]
// TODO this is an awful hack, can we find a better way to work with the lifetime issues
// between threads?
struct RawCowPage {
    main: RawMainPage,
    cow: (NonNull<Page>, PageIndex),
    version: PageVersion,
    deleted: bool,
}

unsafe impl Send for RawCowPage {}

impl RawCowPage {
    const unsafe fn reconstruct(self, block: &'_ Block) -> CowPage<'_> {
        let main = match self.main {
            RawMainPage::Initialized(page, index) => {
                MainPageRef::Initialized(unsafe { PageRef::new(page, block, index) })
            }
            RawMainPage::Uninitialized(page, index) => MainPageRef::Uninitialized(unsafe {
                UninitializedPageGuard::new(block, page, index)
            }),
        };

        CowPage {
            main,
            cow: unsafe { PageRef::new(self.cow.0, block, self.cow.1) },
            version: self.version,
            deleted: self.deleted,
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
