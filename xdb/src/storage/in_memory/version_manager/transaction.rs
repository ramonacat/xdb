use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::thread;
use std::time::{Duration, Instant};

use bytemuck::{must_cast, must_cast_mut, must_cast_ref};
use tracing::{debug, info, instrument, warn};

use crate::storage::in_memory::InMemoryPageId;
use crate::storage::in_memory::block::{
    LockError, PageReadGuard as RawPageReadGuard, PageWriteGuard as RawPageWriteGuard,
    UninitializedPageGuard as RawUnitializedPageGuard,
};
use crate::storage::in_memory::version_manager::transaction_log::StartedTransaction;
use crate::storage::in_memory::version_manager::{
    TransactionPage, TransactionPageAction, VersionManager, VersionedPage,
};
use crate::storage::{PageIndex, StorageError, TransactionId};

pub struct VersionManagedTransaction<'storage> {
    id: TransactionId,
    pages: HashMap<PageIndex, TransactionPage>,
    version_manager: &'storage VersionManager,
    log_entry: StartedTransaction,
    committed: bool,
}

impl Debug for VersionManagedTransaction<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VersionManagedTransaction")
            .field("id", &self.id)
            .field("log_entry", &self.log_entry)
            .field("committed", &self.committed)
            .finish()
    }
}

impl Drop for VersionManagedTransaction<'_> {
    fn drop(&mut self) {
        // TODO is it ok to just drop, or should we warn if there wasn't an explicit
        // rollback/commit call?
        // TODO the boolean is an awful hack, use the log_entry to figure out the state instead
        if !self.committed {
            debug!(
                id = ?self.id,
                "transaction dropped without being commited"
            );
            self.rollback();
        }
    }
}

#[derive(Debug)]
pub struct PageReadGuard<'storage>(RawPageReadGuard<'storage>);
impl<'storage> PageReadGuard<'storage> {
    pub const fn new(raw: RawPageReadGuard<'storage>) -> Self {
        Self(raw)
    }

    pub fn upgrade(self) -> PageWriteGuard<'storage> {
        PageWriteGuard(self.0.upgrade())
    }

    pub fn try_upgrade(self) -> Result<PageWriteGuard<'storage>, LockError> {
        self.0.try_upgrade().map(PageWriteGuard)
    }

    pub const fn physical_index(&self) -> PageIndex {
        self.0.physical_index()
    }
}

impl Deref for PageReadGuard<'_> {
    type Target = VersionedPage;

    fn deref(&self) -> &Self::Target {
        must_cast_ref(&*self.0)
    }
}

#[derive(Debug)]
pub struct PageWriteGuard<'storage>(RawPageWriteGuard<'storage>);

impl DerefMut for PageWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        must_cast_mut(&mut *self.0)
    }
}

impl Deref for PageWriteGuard<'_> {
    type Target = VersionedPage;

    fn deref(&self) -> &Self::Target {
        must_cast_ref(&*self.0)
    }
}

impl<'storage> PageWriteGuard<'storage> {
    pub const fn physical_index(&self) -> PageIndex {
        self.0.physical_index()
    }

    // TODO this name is confusing - it should be "is_explicitly_marked_as_free" or something idk
    pub fn is_free(&self) -> bool {
        self.0.is_free()
    }

    pub fn reset(self) -> UninitializedPageGuard<'storage> {
        UninitializedPageGuard(self.0.reset())
    }

    pub fn mark_free(&mut self) {
        self.0.mark_free();
    }
}

#[derive(Debug)]
pub struct UninitializedPageGuard<'storage>(RawUnitializedPageGuard<'storage>);

impl<'storage> UninitializedPageGuard<'storage> {
    // TODO don't pass the whole thing by value if we can?
    #[allow(clippy::large_types_passed_by_value)]
    fn initialize(self, page: VersionedPage) -> PageWriteGuard<'storage> {
        let raw_page_guard = self.0.initialize(must_cast(page));

        PageWriteGuard(raw_page_guard)
    }

    pub const fn physical_index(&self) -> PageIndex {
        self.0.physical_index()
    }

    pub(crate) const fn new(raw: RawUnitializedPageGuard<'storage>) -> Self {
        Self(raw)
    }
}

impl<'storage> VersionManagedTransaction<'storage> {
    pub fn new(
        id: TransactionId,
        version_manager: &'storage VersionManager,
        log_entry: StartedTransaction,
    ) -> Self {
        Self {
            id,
            pages: HashMap::new(),
            version_manager,
            log_entry,
            committed: false,
        }
    }

    // TODO avoid passing by value
    #[allow(clippy::large_types_passed_by_value)]
    fn allocate(&self) -> Result<UninitializedPageGuard<'storage>, StorageError<InMemoryPageId>> {
        if let Some(recycled) = self.version_manager.recycled_pages.get_recycled_page() {
            debug!(
                physical_index = ?recycled.physical_index(),
                "allocated a recycled page",
            );
            return Ok(recycled);
        }

        let allocation_result = self.version_manager.data.allocate();
        match allocation_result {
            Ok(guard) => {
                debug!(
                    physical_index = ?guard.physical_index(),
                    "allocated a fresh page",
                );
                Ok(guard)
            }
            Err(StorageError::OutOfSpace) => {
                // TODO we should have some mechanism to use to ask vacuum to wake us up when pages
                // are available
                let start = Instant::now();
                loop {
                    let waited = start.elapsed();

                    if let Some(page) = self.version_manager.recycled_pages.get_recycled_page() {
                        info!(
                            physical_index = ?page.physical_index(),
                            ?waited,
                            "allocated a recycled page",
                        );
                        return Ok(page);
                    }

                    thread::yield_now();

                    if waited > Duration::from_millis(100) {
                        warn!(?waited, "waiting for a free cow page");
                    }
                }
            }
            Err(e) => Err(e),
        }
    }

    #[instrument(skip(self, index), fields(logical_index=?index))]
    pub(crate) fn read(
        &mut self,
        index: PageIndex,
    ) -> Result<PageReadGuard<'storage>, StorageError<InMemoryPageId>> {
        if let Some(entry) = self.pages.get(&index) {
            assert!(entry.logical_index == index);

            match entry.action {
                crate::storage::in_memory::version_manager::TransactionPageAction::Read
                | crate::storage::in_memory::version_manager::TransactionPageAction::Insert => {
                    let lock = self
                        .version_manager
                        .data
                        .get_at(entry.logical_index, self.log_entry.started());

                    Ok(lock)
                }
                crate::storage::in_memory::version_manager::TransactionPageAction::Delete => Err(
                    StorageError::PageNotFound(InMemoryPageId(entry.logical_index)),
                ),
                crate::storage::in_memory::version_manager::TransactionPageAction::Update(
                    cow_index,
                ) => {
                    let lock = self.version_manager.data.get(cow_index);

                    Ok(lock)
                }
            }
        } else {
            self.pages.insert(
                index,
                TransactionPage {
                    logical_index: index,
                    action: TransactionPageAction::Read,
                },
            );

            let main = self
                .version_manager
                .data
                .get_at(index, self.log_entry.started());

            Ok(main)
        }
    }

    pub(crate) fn write(
        &mut self,
        index: PageIndex,
    ) -> Result<PageWriteGuard<'storage>, StorageError<InMemoryPageId>> {
        if let Some(entry) = self.pages.get_mut(&index) {
            assert!(entry.logical_index == index);

            match entry.action {
                TransactionPageAction::Read => {}
                TransactionPageAction::Delete => {
                    return Err(StorageError::PageNotFound(InMemoryPageId(index)));
                }
                TransactionPageAction::Update(cow_page_index) => {
                    let cow_page = self.version_manager.data.get(cow_page_index);

                    return Ok(cow_page.upgrade());
                }
                TransactionPageAction::Insert => {
                    let main = self
                        .version_manager
                        .data
                        .get_at(entry.logical_index, self.log_entry.started());

                    return Ok(main.upgrade());
                }
            }
        }

        let main = self
            .version_manager
            .data
            .get_at(index, self.log_entry.started());
        let versioned_page: &VersionedPage = must_cast_ref(&*main);

        if versioned_page.next_version().is_some() {
            // TODO not a deadlock, but optimistic concurrency failure
            return Err(StorageError::Deadlock(InMemoryPageId(index)));
        }

        let cow = self.allocate()?;
        let cow = cow.initialize(*versioned_page);

        self.pages.insert(
            index,
            TransactionPage {
                logical_index: index,
                action: TransactionPageAction::Update(cow.physical_index()),
            },
        );

        Ok(cow)
    }

    #[instrument(skip(self))]
    pub(crate) fn reserve(
        &self,
    ) -> Result<UninitializedPageGuard<'storage>, StorageError<InMemoryPageId>> {
        self.allocate()
    }

    // TODO can we avoid passing this by value?
    #[allow(clippy::large_types_passed_by_value)]
    #[instrument(skip(self), fields(physical_index = ?page_guard.physical_index()))]
    pub(crate) fn insert_reserved(
        &mut self,
        page_guard: UninitializedPageGuard<'storage>,
        page: VersionedPage,
    ) -> Result<(), StorageError<InMemoryPageId>> {
        let logical_index = page_guard.physical_index();
        page_guard.initialize(page);

        let previous = self.pages.insert(
            logical_index,
            TransactionPage {
                logical_index,
                action: TransactionPageAction::Insert,
            },
        );
        assert!(previous.is_none());

        Ok(())
    }

    #[instrument(skip(self), fields(logical_index = ?page))]
    pub(crate) fn delete(&mut self, page: PageIndex) -> Result<(), StorageError<InMemoryPageId>> {
        let inserted = self.pages.insert(
            page,
            TransactionPage {
                logical_index: page,
                action: TransactionPageAction::Delete,
            },
        );

        if let Some(previous) = inserted {
            match previous.action {
                TransactionPageAction::Update(cow) => {
                    let mut cow_page = self.version_manager.data.get(cow).upgrade();

                    cow_page.mark_free();
                }
                TransactionPageAction::Read
                | TransactionPageAction::Delete
                | TransactionPageAction::Insert => {}
            }
        }

        Ok(())
    }

    #[instrument(skip(self), fields(id = ?self.id))]
    pub(crate) fn commit(&mut self) -> Result<(), StorageError<InMemoryPageId>> {
        self.committed = true;

        // TODO make the commit consistent in event of a crash:
        //    1. write to a transaction log
        //    2. fsync the transaction log
        //    3. fsync the modified pages
        self.version_manager
            .committer
            .request(self.log_entry, self.pages.drain().collect())
    }

    #[instrument(skip(self), fields(id = ?self.id))]
    pub fn rollback(&mut self) {
        // TODO instead of dealing with this directly here, we should send a request to committer
        debug!("rolling back");
        for (index, page) in self.pages.drain() {
            match page.action {
                TransactionPageAction::Read
                | TransactionPageAction::Delete
                | TransactionPageAction::Insert => {}
                TransactionPageAction::Update(cow) => {
                    let cow_page = self.version_manager.data.get(cow);

                    debug!(
                        logical_index = ?index,
                        physical_index = ?cow_page.physical_index(),
                        "setting page up to be freed"
                    );
                    let mut cow_lock = cow_page.upgrade();
                    cow_lock.mark_free();
                }
            }
        }
    }

    pub const fn id(&self) -> TransactionId {
        self.id
    }
}
