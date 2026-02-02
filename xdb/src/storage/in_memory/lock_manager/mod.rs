// TODO this should really be called VersionManager

use crate::{
    page::{PAGE_DATA_SIZE, PageVersion},
    storage::in_memory::{
        InMemoryStorage,
        block::{LockError, PageRef},
    },
    thread,
};
use std::collections::{HashMap, hash_map::Entry};

use crate::{
    page::Page,
    storage::{
        PageIndex, StorageError, TransactionId,
        in_memory::block::{Block, PageGuard, UninitializedPageGuard},
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

    pub(crate) fn read(&mut self, index: PageIndex) -> Result<PageGuard<'storage>, StorageError> {
        let page = match self.pages.entry(index) {
            Entry::Occupied(occupied) => occupied.get().cow.lock(),
            Entry::Vacant(vacant) => {
                let main = self.storage.lock_manager.block.get(index, self.id);
                let main_lock = main.lock();

                if !main_lock.is_visible_in(self.id) {
                    return Err(StorageError::Deadlock(index));
                }

                // TODO we really need to free up the cow pages once we're done, as allocating for
                // every read eats the memory up extremely fast
                let cow = self
                    .storage
                    .cow_copies
                    .allocate(self.id)
                    .initialize(*main_lock);

                let inserted = vacant.insert(CowPage {
                    main: MainPageRef::Initialized(main),
                    cow,
                    version: main_lock.version(),
                });

                inserted.cow.lock()
            }
        };

        Ok(page)
    }

    pub(crate) fn reserve(&self) -> UninitializedPageGuard<'storage> {
        self.storage.lock_manager.block.allocate(self.id)
    }

    // TODO can we avoid passing this by value?
    #[allow(clippy::large_types_passed_by_value)]
    pub(crate) fn insert_reserved(
        &mut self,
        page_guard: UninitializedPageGuard<'storage>,
        page: Page,
    ) {
        let cow = self.storage.cow_copies.allocate(self.id).initialize(page);

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
            let main = self.storage.lock_manager.block.get(page, self.id);
            let main_lock = main.lock();

            let cow = self
                .storage
                .cow_copies
                .allocate(self.id)
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

#[derive(Debug)]
// TODO just get rid of this lol
pub struct LockManager {
    block: Block,
}

impl LockManager {
    pub const fn new(block: Block) -> Self {
        Self { block }
    }
}
