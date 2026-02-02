// TODO this should really be called VersionManager

use crate::{
    page::{PAGE_DATA_SIZE, PageVersion},
    storage::in_memory::{
        InMemoryStorage,
        block::{LockError, PageRef},
    },
    thread,
};
use std::collections::HashMap;

use crate::{
    page::Page,
    storage::{
        PageIndex, StorageError, TransactionId,
        in_memory::block::{PageGuard, UninitializedPageGuard},
    },
};

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

#[derive(Debug)]
pub struct VersionManagerTransaction<'storage> {
    id: TransactionId,
    pages: HashMap<PageIndex, CowPage<'storage>>,
    storage: &'storage InMemoryStorage,
}

// TODO do we want a timeout here?
fn retry_contended<T>(callback: impl Fn() -> Result<T, LockError>) -> T {
    loop {
        match callback() {
            Ok(r) => return r,
            Err(error) => match error {
                LockError::Contended(_) => {}
            },
        }

        thread::yield_now();
    }
}

impl<'storage> VersionManagerTransaction<'storage> {
    pub fn new(id: TransactionId, storage: &'storage InMemoryStorage) -> Self {
        Self {
            id,
            pages: HashMap::new(),
            storage,
        }
    }

    // TODO avoid passing by value
    #[allow(clippy::large_types_passed_by_value)]
    fn allocate_cow_copy(&self, page: Page) -> PageRef<'storage> {
        if let Some(index) = self.storage.cow_copies_freemap.find_and_unset() {
            return self
                .storage
                .cow_copies
                .get(PageIndex(index as u64), Some(self.id));
        }

        self.storage
            .cow_copies
            .allocate(Some(self.id))
            .initialize(page)
    }

    pub(crate) fn read(&mut self, index: PageIndex) -> Result<PageGuard<'storage>, StorageError> {
        if let Some(entry) = self.pages.get(&index) {
            Ok(entry.cow.lock())
        } else {
            let main = self.storage.data.get(index, Some(self.id));
            let main_lock = main.lock();

            if !main_lock.is_visible_in(self.id) {
                return Err(StorageError::Deadlock(index));
            }

            let cow = self.allocate_cow_copy(*main_lock);

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

    pub(crate) fn reserve(&self) -> UninitializedPageGuard<'storage> {
        self.storage.data.allocate(Some(self.id))
    }

    // TODO can we avoid passing this by value?
    #[allow(clippy::large_types_passed_by_value)]
    pub(crate) fn insert_reserved(
        &mut self,
        page_guard: UninitializedPageGuard<'storage>,
        page: Page,
    ) {
        let cow = self
            .storage
            .cow_copies
            .allocate(Some(self.id))
            .initialize(page);

        self.pages.insert(
            page_guard.index(),
            CowPage {
                main: MainPageRef::Uninitialized(page_guard),
                cow,
                version: page.version(),
            },
        );
    }

    pub(crate) fn delete(&mut self, page: PageIndex) {
        self.pages.entry(page).or_insert_with(|| {
            let main = self.storage.data.get(page, Some(self.id));
            let main_lock = main.lock();

            let cow = self
                .storage
                .cow_copies
                .allocate(Some(self.id))
                .initialize(*main_lock);

            let mut cow_lock = cow.lock();
            cow_lock.set_visible_until(self.id);
            // TODO do we really care about zeroing, or do we just need to improve change
            // detection in the lock manager to consider header-only changes?
            *cow_lock.data_mut::<[u8; PAGE_DATA_SIZE.as_bytes()]>() = [0; _];

            CowPage {
                main: MainPageRef::Initialized(main),
                cow,
                version: main_lock.version(),
            }
        });
    }

    // TODO commits should all happen in a single thread, to avoid deadlocks and races
    pub(crate) fn commit(&mut self) -> Result<(), StorageError> {
        // TODO make the commit consistent in event of a crash:
        //    1. write to a transaction log
        //    2. fsync the transaction log
        //    3. fsync the modified pages
        // TODO cleanup the cow_pages, once they're copied to the main storage

        let mut locks = retry_contended(|| {
            let mut locks = HashMap::new();
            for (index, page) in &self.pages {
                match &page.main {
                    MainPageRef::Initialized(page_ref) => {
                        let lock = page_ref.lock_nowait()?;

                        if lock.version() != page.version {
                            return Ok(Err(StorageError::Deadlock(*index)));
                        }

                        locks.insert(*index, lock);
                    }
                    MainPageRef::Uninitialized(_) => {}
                }
            }
            Ok(Ok(locks))
        })?;

        for (index, page) in self.pages.drain() {
            match page.main {
                MainPageRef::Initialized(_) => {
                    // It's very tempting to change this `get_mut` to `remove`, but that would be
                    // incorrect, as we'd be unlocking locks while still modifying the stored data.
                    // We can only start unlocking after this loop is done.
                    let lock = locks.get_mut(&index).unwrap();

                    let mut modfied_copy = *page.cow.lock();

                    if modfied_copy.data::<[u8; PAGE_DATA_SIZE.as_bytes()]>()
                        != lock.data::<[u8; PAGE_DATA_SIZE.as_bytes()]>()
                    {
                        modfied_copy.set_visible_from(self.id);
                        modfied_copy.increment_version();

                        **lock = modfied_copy;
                    }
                }
                MainPageRef::Uninitialized(guard) => {
                    let mut page = *page.cow.lock();

                    page.set_visible_from(self.id);

                    guard.initialize(page);
                }
            }
        }

        Ok(())
    }
}
