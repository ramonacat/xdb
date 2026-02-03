use std::collections::BTreeSet;

use crate::page::PageVersion;
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
    fn to_id(self) -> CowPageId {
        CowPageId {
            main: match self.main {
                MainPageRef::Initialized(r) => MainPageId::Initialized(r.index()),
                MainPageRef::Uninitialized(r) => MainPageId::Uninitialized(r.index()),
            },
            cow: self.cow.index(),
            version: self.version,
        }
    }
}

#[derive(Debug)]
enum MainPageId {
    Initialized(PageIndex),
    Uninitialized(PageIndex),
}

#[derive(Debug)]
struct CowPageId {
    main: MainPageId,
    cow: PageIndex,
    version: PageVersion,
}
impl CowPageId {
    fn to_ref<'block>(self, block: &'block Block, cow_pages: &'block Block) -> CowPage<'block> {
        let main = match self.main {
            MainPageId::Initialized(page_index) => MainPageRef::Initialized(block.get(page_index)),
            MainPageId::Uninitialized(page_index) => {
                MainPageRef::Uninitialized(block.get_uninitialized(page_index))
            }
        };

        CowPage {
            main,
            cow: cow_pages.get(self.cow),
            version: self.version,
        }
    }
}

#[derive(Debug)]
pub struct VersionManager {
    // TODO this block should be one with the cow_pages
    data: Arc<Block>,
    #[allow(unused)]
    vacuum: Vacuum,
    committer: Committer,
    running_transactions: Arc<Mutex<BTreeSet<TransactionId>>>,
    cow_pages: Arc<Block>,
    cow_pages_freemap: Arc<Bitmap>,
}

impl VersionManager {
    pub fn new(
        data: Arc<Block>,
        running_transactions: Arc<Mutex<BTreeSet<TransactionId>>>,
        cow_pages: Arc<Block>,
        cow_pages_freemap: Arc<Bitmap>,
    ) -> Self {
        Self {
            vacuum: Vacuum::start(
                running_transactions.clone(),
                cow_pages.clone(),
                cow_pages_freemap.clone(),
            ),
            committer: Committer::new(data.clone(), cow_pages.clone()),
            data,
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
