use std::collections::BTreeSet;
use std::mem::MaybeUninit;
use std::ptr::NonNull;

use crate::page::{Page, PageVersion};
use crate::storage::in_memory::Bitmap;
use crate::storage::in_memory::block::{Block, PageRef, UninitializedPageGuard};
use crate::storage::in_memory::version_manager::committer::Committer;
use crate::storage::in_memory::version_manager::transaction::VersionManagedTransaction;
use crate::storage::in_memory::version_manager::vacuum::Vacuum;
use crate::storage::{PageIndex, TransactionId};
use crate::sync::{Arc, Mutex};

mod committer;
pub mod transaction;
mod vacuum;

#[derive(Debug)]
enum MainPageRef<'storage> {
    Initialized(PageRef<'storage>),
    Uninitialized(UninitializedPageGuard<'storage>),
}

#[derive(Debug)]
struct CowPage<'storage> {
    main: MainPageRef<'storage>,
    cow: PageRef<'storage>,
    version: PageVersion,
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
    // TODO get rid of the mutex (this is only ever used by vacuum to figure out the lowest running
    // transaction, so perhaps just having an AtomicU64 instead is enough?
    running_transactions: Arc<Mutex<BTreeSet<TransactionId>>>,
    // TODO instead of a mutex, we should probably have per-thread queues or something (a lock-free ring-buffer
    // perhaps?)
    // TODO give it a better name, it is not really a queue
    // TODO sending raw pointers kinda sucks, we probably should just do PageIndices?
    recycled_page_queue: Mutex<Vec<(NonNull<MaybeUninit<Page>>, PageIndex)>>,
}

unsafe impl Send for VersionManager {}
unsafe impl Sync for VersionManager {}

impl VersionManager {
    pub fn new(
        data: Arc<Block>,
        running_transactions: Arc<Mutex<BTreeSet<TransactionId>>>,
        freemap: Arc<Bitmap>,
    ) -> Self {
        Self {
            vacuum: Vacuum::start(running_transactions.clone(), data.clone(), freemap.clone()),
            committer: Committer::new(data.clone()),
            data,
            running_transactions,
            freemap,
            recycled_page_queue: Mutex::new(Vec::new()),
        }
    }

    pub fn start_transaction(&self) -> VersionManagedTransaction<'_> {
        let id = TransactionId::next();

        self.running_transactions.lock().unwrap().insert(id);

        VersionManagedTransaction::new(id, self)
    }
}
