use crate::storage::in_memory::{
    block::{PageGuard, PageGuardRead, PageRef},
    version_manager::transaction_log::TransactionLogEntryHandle,
};
use std::{
    collections::HashMap,
    thread,
    time::{Duration, Instant},
};

use tracing::{debug, info_span, instrument, warn};

use crate::{
    page::Page,
    storage::{
        PageIndex, StorageError, TransactionId,
        in_memory::{
            block::UninitializedPageGuard,
            version_manager::{CowPage, VersionManager},
        },
    },
    sync::Mutex,
};

#[derive(Debug)]
pub struct VersionManagedTransaction<'storage> {
    id: TransactionId,
    pages: HashMap<PageIndex, CowPage<'storage>>,
    version_manager: &'storage VersionManager,
    span: tracing::Span,
    last_free_page_scan: Mutex<Option<Instant>>,
    log_entry: TransactionLogEntryHandle<'storage>,
    committed: bool,
}

impl Drop for VersionManagedTransaction<'_> {
    fn drop(&mut self) {
        // TODO is it ok to just drop, or should we warn if there wasn't an explicit
        // rollback/commit call?
        // TODO the boolean is an awful hack, use the log_entry to figure out the state instead
        if !self.committed {
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
        let span = info_span!("transaction", id = ?id, started = ?log_entry.start_timestamp());

        Self {
            id,
            pages: HashMap::new(),
            version_manager,
            span,
            last_free_page_scan: Mutex::new(None),
            log_entry,
            committed: false,
        }
    }

    fn get_matching_version(&self, index: PageIndex) -> PageRef<'storage> {
        debug!(
            "looking for a matching version of {index:?} for {:?}",
            self.log_entry.start_timestamp()
        );

        let mut locks = vec![]; //

        let mut main = self.version_manager.data.get(Some(index), index);
        let mut main_lock = main.lock_read();

        while !main_lock.is_visible_at(self.log_entry.start_timestamp()) {
            debug!(
                "page {index:?}: {:?} {:?}/{:?}",
                main.physical_index(),
                main_lock.visible_from(),
                main_lock.visible_until()
            );
            let Some(next) = main_lock.next_version() else {
                // TODO should we panic here? I think we should not be able to get to this
                // place if the database is in a valid state?
                panic!(
                    "page {index:?} not found (transaction timestamp: {:?}, latest version visible: {:?}/{:?})",
                    self.log_entry.start_timestamp(),
                    main_lock.visible_from(),
                    main_lock.visible_until()
                );
            };

            main = self.version_manager.data.get(Some(index), next);

            let next_version_lock = main.lock_read();
            locks.push(main_lock);

            main_lock = next_version_lock;
        }

        // keep all the locks till here to avoid situations where vacuum changes the
        // next_/previous_version links while we're looking at them
        drop(locks);

        debug!(
            "{index:?}@{:?} is {:?}",
            self.log_entry.start_timestamp(),
            main.physical_index()
        );

        main
    }

    // TODO all page allocations should go through this
    fn get_recycled_page(
        &self,
        logical_index: Option<PageIndex>,
    ) -> Option<UninitializedPageGuard<'storage>> {
        // don't bother with all this if there aren't many allocated pages (TODO figure out if this
        // number makes sense)
        if self.version_manager.data.page_count_lower_bound() < 50000 {
            debug!("not recycling pages, too few were allocated");

            return None;
        }

        let mut recycled_page_queue = self.version_manager.recycled_page_queue.lock().unwrap();

        if let Some(page) = recycled_page_queue.pop() {
            debug!(
                "got a page from recycled_page_queue (length: {})",
                recycled_page_queue.len()
            );
            return Some(unsafe {
                UninitializedPageGuard::new(
                    &self.version_manager.data,
                    page.0,
                    logical_index,
                    page.1,
                )
            });
        }

        let since_last_free_page_scan = self
            .last_free_page_scan
            .lock()
            .unwrap()
            .map_or(Duration::MAX, |x| x.elapsed());

        // TODO we should allow the scan to happen as often as it wants to if there's no space in
        // storage anymore
        if since_last_free_page_scan < Duration::from_secs(10) {
            debug!(
                "only {since_last_free_page_scan:?} elapsed since last free page scan, skipping"
            );

            return None;
        }

        // TODO we need a better API for this - we must stop vacuum from marking the page as unused
        // again before we have a chance to reuse it, potentially resulting in multiple threads
        // getting the same page
        let lock = self.version_manager.vacuum.freeze();

        for free_page in self
            .version_manager
            .freemap
            .find_and_unset(10000)
            .into_iter()
            .map(|index| unsafe {
                self.version_manager
                    .data
                    .get_uninitialized(logical_index, PageIndex(index as u64))
            })
        {
            recycled_page_queue.push((free_page.as_ptr(), free_page.physical_index()));
        }

        drop(lock);
        *self.last_free_page_scan.lock().unwrap() = Some(Instant::now());

        debug!("recycled {} pages", recycled_page_queue.len());

        recycled_page_queue.pop().map(|page| unsafe {
            UninitializedPageGuard::new(&self.version_manager.data, page.0, logical_index, page.1)
        })
    }

    // TODO avoid passing by value
    // TODO we lose a lot of performance by always creating a cow page, we should do this only
    // when there's an actual write
    // TODO rename -> recycle_or_allocate
    #[allow(clippy::large_types_passed_by_value)]
    fn allocate_cow_copy(
        &self,
        logical_index: Option<PageIndex>,
    ) -> Result<UninitializedPageGuard<'storage>, StorageError> {
        if let Some(recycled) = self.get_recycled_page(logical_index) {
            debug!(
                "allocated a recycled page: {:?} for {:?}",
                recycled.physical_index(),
                recycled.logical_index()
            );
            return Ok(recycled);
        }

        let allocation_result = self.version_manager.data.allocate(logical_index);
        match allocation_result {
            Ok(guard) => {
                debug!(
                    "allocated a fresh page {:?} for {:?}",
                    guard.physical_index(),
                    guard.logical_index()
                );
                Ok(guard)
            }
            Err(StorageError::OutOfSpace) => {
                // TODO we should have some mechanism to use to ask vacuum to wake us up when pages
                // are available
                let start = Instant::now();
                loop {
                    let waited = start.elapsed();

                    if let Some(page) = self.get_recycled_page(logical_index) {
                        debug!(
                            "allocated a recycled page {:?} for {:?} after waiting for {waited:?}",
                            page.physical_index(),
                            page.logical_index()
                        );
                        return Ok(page);
                    }

                    thread::yield_now();

                    if waited > Duration::from_millis(100) {
                        warn!("waited {waited:?} for a free cow page");
                    }
                }
            }
            Err(e) => Err(e),
        }
    }

    #[instrument(skip(self), parent = &self.span)]
    pub(crate) fn read(
        &mut self,
        index: PageIndex,
    ) -> Result<PageGuardRead<'storage>, StorageError> {
        if let Some(entry) = self.pages.get(&index) {
            if let Some(cow) = entry.cow {
                return Ok(cow.lock_read());
            }

            Ok(entry.main.lock_read())
        } else {
            let main = self.get_matching_version(index);

            self.pages.insert(
                index,
                CowPage {
                    main,
                    cow: None,
                    deleted: false,
                    inserted: false,
                },
            );

            Ok(main.lock_read())
        }
    }

    pub(crate) fn write(&mut self, index: PageIndex) -> Result<PageGuard<'storage>, StorageError> {
        if let Some(entry) = self.pages.get(&index) {
            if entry.inserted {
                return Ok(entry.main.lock());
            }

            if let Some(cow) = entry.cow {
                return Ok(cow.lock());
            }

            let cow = self.allocate_cow_copy(Some(index))?;
            let cow = cow.initialize(*entry.main.lock_read());
            let physical_index = entry.main.physical_index();
            self.pages.get_mut(&index).unwrap().cow = Some(cow);

            let mut cow_lock = cow.lock();
            cow_lock.set_previous_version(Some(physical_index));
            cow_lock.set_next_version(None);

            Ok(cow_lock)
        } else {
            let page = self.get_matching_version(index);
            let cow = self.allocate_cow_copy(Some(index))?;
            let page_lock = page.lock_read();
            if page_lock.next_version().is_some() {
                // TODO not a deadlock, but optimistic concurrency failure
                return Err(StorageError::Deadlock(index));
            }
            let cow = cow.initialize(*page_lock);

            self.pages.insert(
                index,
                CowPage {
                    main: page,
                    cow: Some(cow),
                    deleted: false,
                    inserted: false,
                },
            );

            // TODO clean this up - we either manage the next/previous links here, or in the
            // committer
            let mut cow_lock = cow.lock();
            cow_lock.set_previous_version(Some(page.physical_index()));
            cow_lock.set_next_version(None);
            Ok(cow_lock)
        }
    }

    #[instrument(skip(self), parent = &self.span)]
    pub(crate) fn reserve(&self) -> Result<UninitializedPageGuard<'storage>, StorageError> {
        self.allocate_cow_copy(None)
    }

    // TODO can we avoid passing this by value?
    #[allow(clippy::large_types_passed_by_value)]
    #[instrument(skip(self), parent = &self.span)]
    pub(crate) fn insert_reserved(
        &mut self,
        page_guard: UninitializedPageGuard<'storage>,
        page: Page,
    ) -> Result<(), StorageError> {
        self.pages.insert(
            page_guard
                .logical_index()
                .unwrap_or_else(|| page_guard.physical_index()),
            CowPage {
                main: page_guard.initialize(page),
                cow: None,
                deleted: false,
                inserted: true,
            },
        );

        Ok(())
    }

    #[instrument(skip(self), parent = &self.span)]
    pub(crate) fn delete(&mut self, page: PageIndex) -> Result<(), StorageError> {
        if let Some(page) = self.pages.get_mut(&page) {
            // TODO this should really be an enum describing various possible states of a page,
            // instead of this boolean flag
            page.deleted = true;

            return Ok(());
        }

        let main = self.get_matching_version(page);

        self.pages.insert(
            page,
            CowPage {
                main,
                cow: None,
                deleted: true,
                inserted: false,
            },
        );

        Ok(())
    }

    #[instrument(skip(self), parent = &self.span)]
    pub(crate) fn commit(&mut self) -> Result<(), StorageError> {
        self.committed = true;

        // TODO make the commit consistent in event of a crash:
        //    1. write to a transaction log
        //    2. fsync the transaction log
        //    3. fsync the modified pages
        self.version_manager
            .committer
            .request(self.id, self.pages.drain().collect())
    }

    #[instrument(skip(self), parent = &self.span)]
    #[allow(clippy::needless_pass_by_ref_mut)] // TODO make const if we really don't need it
    pub fn rollback(&mut self) {
        for (_, page) in self.pages.drain() {
            if let Some(cow) = page.cow {
                // TODO we should probably have a better way of freeing these pages
                let mut cow_lock = cow.lock();
                cow_lock.set_next_version(None);
                cow_lock.set_previous_version(None);
                cow_lock.set_visible_until(Some(self.log_entry.start_timestamp()));
            }
        }
        // TODO do we need to do anything more here?

        self.log_entry.rollback();
    }

    pub const fn id(&self) -> TransactionId {
        self.id
    }
}
