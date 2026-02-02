mod state;

use crate::sync::RwLock;
use std::ops::{Deref, DerefMut};

use log::debug;

use crate::{
    page::Page,
    storage::{
        PageIndex, StorageError, TransactionId,
        in_memory::{
            block::{Block, PageGuard, UninitializedPageGuard},
            lock_manager::state::LockManagerState,
        },
    },
};

#[derive(Debug)]
pub struct ManagedPageGuard<'storage> {
    guard: Option<PageGuard<'storage>>,
    lock_manager: &'storage LockManager,
    txid: TransactionId,
}

unsafe impl Send for ManagedPageGuard<'_> {}

impl Drop for ManagedPageGuard<'_> {
    fn drop(&mut self) {
        debug!(
            "[{:?}] dropping read guard {:?}",
            self.txid,
            self.guard.as_ref().map(PageGuard::index)
        );

        if let Some(guard) = self.guard.take() {
            let index = guard.index();

            debug!("[{:?}] removing read lock {index:?}", self.txid);

            let mut state_guard = self.lock_manager.state.write().unwrap();

            drop(guard);

            let potentially_unlocked_pages = state_guard.remove_page(self.txid, index);

            self.lock_manager
                .wake_all(potentially_unlocked_pages, self.txid);
        }
    }
}

impl Deref for ManagedPageGuard<'_> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        self.guard.as_ref().unwrap().deref()
    }
}

impl DerefMut for ManagedPageGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.as_mut().unwrap().deref_mut()
    }
}

#[derive(Debug)]
pub struct LockManager {
    state: RwLock<LockManagerState>,
    block: Block,
}

impl LockManager {
    pub fn new(block: Block) -> Self {
        Self {
            block,
            state: RwLock::new(LockManagerState::new()),
        }
    }

    pub fn lock(
        &self,
        txid: TransactionId,
        index: PageIndex,
    ) -> Result<ManagedPageGuard<'_>, StorageError> {
        let mut state_guard = self.state.write().unwrap();
        let page = self.block.get(index, txid);

        if !state_guard.add_page(txid, page.index()) {
            return Err(StorageError::Deadlock(page.index()));
        }

        debug!(
            "[{txid:?}] locking for read {:?} edges: \n{}",
            page.index(),
            &state_guard.edges_debug(Some(page.index()))
        );

        drop(state_guard);

        Ok(ManagedPageGuard {
            guard: Some(page.lock()),
            lock_manager: self,
            txid,
        })
    }

    pub fn reserve(&self, txid: TransactionId) -> UninitializedPageGuard<'_> {
        self.block.allocate(txid)
    }

    fn wake_all(&self, pages: Vec<PageIndex>, txid: TransactionId) {
        debug!("[{txid:?}] waking potential waiters: {pages:?}");

        for page in pages {
            self.block.get(page, txid).wake();
        }
    }

    pub(crate) fn debug_locks(&self, page: PageIndex) -> String {
        self.state.write().unwrap().edges_debug(Some(page))
    }
}
