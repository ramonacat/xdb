use std::{collections::HashMap, thread};

use bytemuck::Zeroable;
use tracing::{info_span, instrument};

use crate::{
    page::Page,
    storage::{
        PageIndex, StorageError, TransactionId,
        in_memory::{
            block::{PageGuard, PageRef, UninitializedPageGuard},
            version_manager::{CowPage, MainPageRef, VersionManager},
        },
    },
};

#[derive(Debug)]
pub struct VersionManagedTransaction<'storage> {
    id: TransactionId,
    pages: HashMap<PageIndex, CowPage<'storage>>,
    version_manager: &'storage VersionManager,
    span: tracing::Span,
}

impl Drop for VersionManagedTransaction<'_> {
    fn drop(&mut self) {
        self.version_manager
            .running_transactions
            .lock()
            .unwrap()
            .remove(&self.id);
        // TODO do a rollback?
    }
}

impl<'storage> VersionManagedTransaction<'storage> {
    pub fn new(id: TransactionId, version_manager: &'storage VersionManager) -> Self {
        let span = info_span!("transaction", id = ?id);

        Self {
            id,
            pages: HashMap::new(),
            version_manager,
            span,
        }
    }

    fn get_recycled_cow_page(&self) -> Option<PageRef<'storage>> {
        // TODO we need a better API for this - we must stop vacuum from marking the page as unused
        // again before we have a chance to reuse it, potentially resulting in multiple threads
        // getting the same page
        // TODO freezing for every page we're getting is expensive AF, we should probably keep a
        // thousand pages or something, and only freeze once that store is empty
        let lock = self.version_manager.vacuum.freeze();

        self.version_manager
            .cow_pages_freemap
            .find_and_unset()
            .map(|index| {
                let recycled_page = self.version_manager.cow_pages.get(PageIndex(index as u64));
                *recycled_page.lock() = Page::zeroed();

                drop(lock);

                recycled_page
            })
    }

    // TODO avoid passing by value
    // TODO this should block and wait for vacuum if there are no pages available
    #[allow(clippy::large_types_passed_by_value)]
    fn allocate_cow_copy(&self, page: Page) -> Result<PageRef<'storage>, StorageError> {
        if let Some(recycled) = self.get_recycled_cow_page() {
            *recycled.lock() = page;

            return Ok(recycled);
        }

        let allocation_result = self.version_manager.cow_pages.allocate();
        match allocation_result {
            Ok(guard) => Ok(guard.initialize(page)),
            Err(StorageError::OutOfSpace) => {
                // TODO we should have some mechanism to use to ask vacuum to wake us up when pages
                // are available
                loop {
                    if let Some(page) = self.get_recycled_cow_page() {
                        return Ok(page);
                    }

                    thread::yield_now();
                }
            }
            Err(e) => Err(e),
        }
    }

    #[instrument(skip(self), parent = &self.span)]
    pub(crate) fn read(&mut self, index: PageIndex) -> Result<PageGuard<'storage>, StorageError> {
        if let Some(entry) = self.pages.get(&index) {
            Ok(entry.cow.lock())
        } else {
            let main = self.version_manager.data.get(index);
            let main_lock = main.lock();

            if !main_lock.is_visible_in(self.id) {
                // TODO this is not a deadlock, it's just optimisitc concurrency race
                return Err(StorageError::Deadlock(index));
            }

            let cow = self.allocate_cow_copy(*main_lock)?;

            self.pages.insert(
                index,
                CowPage {
                    main: MainPageRef::Initialized(main),
                    cow,
                    version: main_lock.version(),
                },
            );

            Ok(self.pages.get(&index).unwrap().cow.lock())
        }
    }

    #[instrument(skip(self), parent = &self.span)]
    pub(crate) fn reserve(&self) -> Result<UninitializedPageGuard<'storage>, StorageError> {
        self.version_manager.data.allocate()
    }

    // TODO can we avoid passing this by value?
    #[allow(clippy::large_types_passed_by_value)]
    #[instrument(skip(self), parent = &self.span)]
    pub(crate) fn insert_reserved(
        &mut self,
        page_guard: UninitializedPageGuard<'storage>,
        page: Page,
    ) -> Result<(), StorageError> {
        let cow = self.allocate_cow_copy(page)?;

        self.pages.insert(
            page_guard.index(),
            CowPage {
                main: MainPageRef::Uninitialized(page_guard),
                cow,
                version: page.version(),
            },
        );

        Ok(())
    }

    #[instrument(skip(self), parent = &self.span)]
    pub(crate) fn delete(&mut self, page: PageIndex) -> Result<(), StorageError> {
        if let Some(page) = self.pages.get(&page) {
            page.cow.lock().set_visible_until(self.id);

            return Ok(());
        }

        let main = self.version_manager.data.get(page);
        let main_lock = main.lock();

        let cow = self.allocate_cow_copy(*main_lock)?;

        cow.lock().set_visible_until(self.id);

        self.pages.insert(
            page,
            CowPage {
                main: MainPageRef::Initialized(main),
                cow,
                version: main_lock.version(),
            },
        );

        Ok(())
    }

    #[instrument(skip(self), parent = &self.span)]
    pub(crate) fn commit(&mut self) -> Result<(), StorageError> {
        // TODO commits should all happen in a single thread, to avoid deadlocks and races
        // TODO make the commit consistent in event of a crash:
        //    1. write to a transaction log
        //    2. fsync the transaction log
        //    3. fsync the modified pages
        // TODO cleanup the cow_pages, once they're copied to the main storage
        //
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
            lock.set_visible_until(self.id);
        }
    }

    pub const fn id(&self) -> TransactionId {
        self.id
    }
}
