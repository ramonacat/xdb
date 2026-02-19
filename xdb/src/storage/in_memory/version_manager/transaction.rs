use crate::storage::in_memory::{
    InMemoryPageId,
    block::{PageReadGuard, PageWriteGuard},
    version_manager::{
        TransactionPage, TransactionPageAction, get_matching_version,
        transaction_log::TransactionLogEntryHandle,
    },
};
use std::{
    collections::HashMap,
    fmt::Debug,
    thread,
    time::{Duration, Instant},
};

use tracing::{debug, info, instrument, warn};

use crate::{
    page::Page,
    storage::{
        PageIndex, StorageError, TransactionId,
        in_memory::{block::UninitializedPageGuard, version_manager::VersionManager},
    },
};

pub struct VersionManagedTransaction<'storage> {
    id: TransactionId,
    pages: HashMap<PageIndex, TransactionPage>,
    version_manager: &'storage VersionManager,
    log_entry: TransactionLogEntryHandle<'storage>,
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

impl<'storage> VersionManagedTransaction<'storage> {
    pub fn new(
        id: TransactionId,
        version_manager: &'storage VersionManager,
        log_entry: TransactionLogEntryHandle<'storage>,
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
                    let lock = get_matching_version(
                        &self.version_manager.data,
                        entry.logical_index,
                        self.log_entry.start_timestamp(),
                    );

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

            let main = get_matching_version(
                &self.version_manager.data,
                index,
                self.log_entry.start_timestamp(),
            );

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
                    let main = get_matching_version(
                        &self.version_manager.data,
                        entry.logical_index,
                        self.log_entry.start_timestamp(),
                    );

                    return Ok(main.upgrade());
                }
            }
        }

        let main = get_matching_version(
            &self.version_manager.data,
            index,
            self.log_entry.start_timestamp(),
        );

        if main.next_version().is_some() {
            // TODO not a deadlock, but optimistic concurrency failure
            return Err(StorageError::Deadlock(InMemoryPageId(index)));
        }

        let cow = self.allocate()?;
        let cow = cow.initialize(*main);
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
        page: Page,
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
            .request(self.id, self.pages.drain().collect())
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

        self.log_entry.rollback();
    }

    pub const fn id(&self) -> TransactionId {
        self.id
    }
}
