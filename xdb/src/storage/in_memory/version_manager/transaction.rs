use crate::storage::in_memory::version_manager::transaction_log::TransactionLogEntryHandle;
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
            block::{PageGuard, UninitializedPageGuard},
            version_manager::{CowPage, MainPageRef, VersionManager},
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
        let span = info_span!("transaction", id = ?id);

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

    // TODO all page allocations should go through this
    fn get_recycled_page(&self) -> Option<UninitializedPageGuard<'storage>> {
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
                UninitializedPageGuard::new(&self.version_manager.data, page.0, page.1)
            });
        }

        let since_last_free_page_scan = self
            .last_free_page_scan
            .lock()
            .unwrap()
            .map_or(Duration::MAX, |x| x.elapsed());

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
                    .get_uninitialized(PageIndex(index as u64))
            })
        {
            recycled_page_queue.push((free_page.as_ptr(), free_page.index()));
        }

        drop(lock);
        *self.last_free_page_scan.lock().unwrap() = Some(Instant::now());

        debug!("recycled {} pages", recycled_page_queue.len());

        recycled_page_queue.pop().map(|page| unsafe {
            UninitializedPageGuard::new(&self.version_manager.data, page.0, page.1)
        })
    }

    // TODO avoid passing by value
    // TODO we lose a lot of performance by always creating a cow page, we should do this only
    // when there's an actual write
    // TODO rename -> recycle_or_allocate
    #[allow(clippy::large_types_passed_by_value)]
    fn allocate_cow_copy(&self) -> Result<UninitializedPageGuard<'storage>, StorageError> {
        if let Some(recycled) = self.get_recycled_page() {
            return Ok(recycled);
        }

        let allocation_result = self.version_manager.data.allocate();
        match allocation_result {
            Ok(guard) => Ok(guard),
            Err(StorageError::OutOfSpace) => {
                // TODO we should have some mechanism to use to ask vacuum to wake us up when pages
                // are available
                let start = Instant::now();
                loop {
                    if let Some(page) = self.get_recycled_page() {
                        return Ok(page);
                    }

                    thread::yield_now();

                    let waited = start.elapsed();
                    if waited > Duration::from_millis(100) {
                        warn!("waited {waited:?} for a free cow page");
                    }
                }
            }
            Err(e) => Err(e),
        }
    }

    #[instrument(skip(self), parent = &self.span)]
    pub(crate) fn read(&mut self, index: PageIndex) -> Result<PageGuard<'storage>, StorageError> {
        // TODO what we have currently only implements "read comitted" isolation level. What we
        // really need is is "snapshot"
        //
        // T1 starts
        // T2 starts
        // T1 reads page 1
        // T1 writes page 1 and page 2
        // T1 commits
        //
        // T2 page 1 now points to page 2 which was modified by T1, when T2 reads page 2 it gets
        // the new version!!!

        if let Some(entry) = self.pages.get(&index) {
            Ok(entry.cow.lock())
        } else {
            let main = self.version_manager.data.get(index);
            let main_lock = main.lock();

            if !main_lock.is_visible_at(self.log_entry.start_timestamp()) {
                // TODO we should not be returning an error, but instead there should be multiple
                // versions of the page stored, each with link to the newer version
                return Err(StorageError::Deadlock(index));
            }

            let cow = self.allocate_cow_copy()?.initialize(*main_lock);

            self.pages.insert(
                index,
                CowPage {
                    main: MainPageRef::Initialized(main),
                    cow,
                    version: main_lock.version(),
                    deleted: false,
                },
            );

            Ok(self.pages.get(&index).unwrap().cow.lock())
        }
    }

    #[instrument(skip(self), parent = &self.span)]
    pub(crate) fn reserve(&self) -> Result<UninitializedPageGuard<'storage>, StorageError> {
        self.allocate_cow_copy()
    }

    // TODO can we avoid passing this by value?
    #[allow(clippy::large_types_passed_by_value)]
    #[instrument(skip(self), parent = &self.span)]
    pub(crate) fn insert_reserved(
        &mut self,
        page_guard: UninitializedPageGuard<'storage>,
        page: Page,
    ) -> Result<(), StorageError> {
        let cow = self.allocate_cow_copy()?.initialize(page);

        self.pages.insert(
            page_guard.index(),
            CowPage {
                main: MainPageRef::Uninitialized(page_guard),
                cow,
                version: page.version(),
                deleted: false,
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

        let main = self.version_manager.data.get(page);
        let main_lock = main.lock();

        let cow = self.allocate_cow_copy()?.initialize(*main_lock);

        self.pages.insert(
            page,
            CowPage {
                main: MainPageRef::Initialized(main),
                cow,
                version: main_lock.version(),
                deleted: true,
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
    pub fn rollback(&mut self) {
        // TODO this should also be happening in the committer thread
        let mut locks = vec![];

        for (_, page) in self.pages.drain() {
            locks.push(page.cow.lock());
        }

        for lock in &mut locks {
            lock.set_visible_until(self.log_entry.start_timestamp());
        }

        self.log_entry.rollback();
    }

    pub const fn id(&self) -> TransactionId {
        self.id
    }
}
