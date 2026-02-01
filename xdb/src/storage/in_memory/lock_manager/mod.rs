mod state;

use crate::sync::RwLock;
use std::ops::{Deref, DerefMut};

use log::debug;

use crate::{
    page::Page,
    storage::{
        PageIndex, StorageError, TransactionId,
        in_memory::{
            block::{Block, PageGuard, PageGuardMut, PageRef, UninitializedPageGuard},
            lock_manager::state::{LockKind, LockManagerState},
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

impl<'storage> ManagedPageGuard<'storage> {
    pub fn upgrade(mut self) -> Result<ManagedPageGuardMut<'storage>, StorageError> {
        let Self {
            ref mut guard,
            lock_manager,
            txid,
        } = self;

        let guard = self
            .lock_manager
            .lock_upgrade(txid, guard.take().unwrap())?;

        Ok(ManagedPageGuardMut {
            guard: Some(guard),
            lock_manager,
            txid,
        })
    }
}

impl Drop for ManagedPageGuard<'_> {
    fn drop(&mut self) {
        debug!(
            "[{:?}] dropping read guard {:?}",
            self.txid,
            self.guard.as_ref().map(PageGuard::index)
        );

        if let Some(guard) = self.guard.take() {
            self.lock_manager.unlock_read(self.txid, guard);
        }
    }
}

impl Deref for ManagedPageGuard<'_> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        self.guard.as_ref().unwrap().deref()
    }
}

#[derive(Debug)]
pub struct ManagedPageGuardMut<'storage> {
    guard: Option<PageGuardMut<'storage>>,
    lock_manager: &'storage LockManager,
    txid: TransactionId,
}

unsafe impl Send for ManagedPageGuardMut<'_> {}

impl Drop for ManagedPageGuardMut<'_> {
    fn drop(&mut self) {
        debug!(
            "[{:?}] dropping mut guard {:?}",
            self.txid,
            self.guard.as_ref().map(PageGuardMut::index)
        );

        self.lock_manager
            .unlock_write(self.txid, self.guard.take().unwrap());
    }
}

impl DerefMut for ManagedPageGuardMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.as_mut().unwrap().deref_mut()
    }
}

impl Deref for ManagedPageGuardMut<'_> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        self.guard.as_ref().unwrap().deref()
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

    fn lock_read<'storage>(
        &self,
        txid: TransactionId,
        page: PageRef<'storage>,
    ) -> Result<PageGuard<'storage>, StorageError> {
        let mut state_guard = self.state.write().unwrap();

        if !state_guard.add_page(txid, page.index(), LockKind::Read) {
            return Err(StorageError::Deadlock(page.index()));
        }

        debug!(
            "[{txid:?}] locking for read {:?} edges: \n{}",
            page.index(),
            &state_guard.edges_debug(Some(page.index()))
        );

        drop(state_guard);

        Ok(page.get())
    }

    fn lock_upgrade<'storage>(
        &self,
        txid: TransactionId,
        guard: PageGuard<'storage>,
    ) -> Result<PageGuardMut<'storage>, StorageError> {
        let mut state_guard = self.state.write().unwrap();

        if !state_guard.add_page(txid, guard.index(), LockKind::Upgrade) {
            return Err(StorageError::Deadlock(guard.index()));
        }

        debug!("[{txid:?}] upgrading lock {:?}", guard.index());

        drop(state_guard);

        Ok(guard.upgrade())
    }

    // TODO we should wrap the PageGuard into our own type to handle unlocks on drop
    pub fn get_read(
        &self,
        txid: TransactionId,
        index: PageIndex,
    ) -> Result<ManagedPageGuard<'_>, StorageError> {
        self.lock_read(txid, self.block.get(index, txid))
            .map(|guard| ManagedPageGuard {
                guard: Some(guard),
                lock_manager: self,
                txid,
            })
    }

    pub fn get_write(
        &self,
        txid: TransactionId,
        index: PageIndex,
    ) -> Result<ManagedPageGuardMut<'_>, StorageError> {
        self.lock_write(txid, self.block.get(index, txid))
            .map(|guard| ManagedPageGuardMut {
                guard: Some(guard),
                lock_manager: self,
                txid,
            })
    }

    pub fn reserve(&self, txid: TransactionId) -> UninitializedPageGuard<'_> {
        self.block.allocate(txid)
    }

    fn lock_write<'storage>(
        &self,
        txid: TransactionId,
        page: PageRef<'storage>,
    ) -> Result<PageGuardMut<'storage>, StorageError> {
        let mut state_guard = self.state.write().unwrap();

        if !state_guard.add_page(txid, page.index(), LockKind::Write) {
            return Err(StorageError::Deadlock(page.index()));
        }

        debug!("[{txid:?}] locking for write {:?}", page.index());

        drop(state_guard);

        Ok(page.get_mut())
    }

    // TODO we should probably deal with the guards internally here, so that it is impossible to
    // drop one without being accounted for
    pub fn unlock_read(&self, txid: TransactionId, page: PageGuard<'_>) {
        let index = page.index();

        debug!("[{txid:?}] removing read lock {index:?}");

        let mut state_guard = self.state.write().unwrap();

        drop(page);

        let potentially_unlocked_pages = state_guard.remove_page(txid, index, LockKind::Read);

        self.wake_all(potentially_unlocked_pages, txid);
    }

    pub fn unlock_write(&self, txid: TransactionId, page: PageGuardMut<'_>) {
        let index = page.index();

        debug!("[{txid:?}] removing write lock {index:?}");

        let mut state_guard = self.state.write().unwrap();

        drop(page);

        let potentially_unlocked_pages = state_guard.remove_page(txid, index, LockKind::Write);

        self.wake_all(potentially_unlocked_pages, txid);
    }

    fn wake_all(&self, pages: Vec<PageIndex>, txid: TransactionId) {
        debug!("[{txid:?}] waking potential waiters: {pages:?}");

        for page in pages {
            self.block.get(page, txid).wake();
        }
    }
}
