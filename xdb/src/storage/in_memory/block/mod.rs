mod page_state;

// TODO this should not be at all aware of InMemoryPageId
use std::fmt::Debug;
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::ptr::NonNull;
#[cfg(debug_assertions)]
use std::time::{Duration, Instant};

use thiserror::Error;
use tracing::{debug, error, instrument, warn};

use crate::Size;
use crate::platform::allocation::Allocation;
use crate::platform::allocation::uncommitted::UncommittedAllocation;
use crate::storage::in_memory::InMemoryPageId;
use crate::storage::in_memory::block::page_state::{PageState, PageStateValue};
use crate::storage::page::{PAGE_SIZE, Page};
use crate::storage::{PageIndex, StorageError};
use crate::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Error)]
// TODO make this just `struct LockContended`
pub enum LockError {
    #[error("contended")]
    Contended(PageStateValue),
}

#[derive(Debug)]
pub struct PageReadGuard<'block> {
    page: NonNull<Page>,
    block: &'block Block,
    physical_index: PageIndex,
    #[cfg(debug_assertions)]
    taken: Instant,
    lock_consumed: bool,
}

impl<'block> PageReadGuard<'block> {
    #[instrument]
    // TODO we can make this safe if we can assert that the page is from within the block
    // TODO rename to lock?
    unsafe fn new(page: NonNull<Page>, block: &'block Block, physical_index: PageIndex) -> Self {
        let housekeeping = block.housekeeping_for(physical_index);
        housekeeping.lock_read();

        Self {
            page,
            block,
            physical_index,
            #[cfg(debug_assertions)]
            taken: Instant::now(),
            lock_consumed: false,
        }
    }

    unsafe fn try_lock(
        page: NonNull<Page>,
        block: &'block Block,
        physical_index: PageIndex,
    ) -> Result<Self, LockError> {
        let housekeeping = block.housekeeping_for(physical_index);
        housekeeping.try_lock_read()?;

        Ok(Self {
            page,
            block,
            physical_index,
            #[cfg(debug_assertions)]
            taken: Instant::now(),
            lock_consumed: false,
        })
    }

    // TODO clean up naming, PageGuard->PageGuardMut, PageGuardRead->PageGuard
    pub fn try_upgrade(self) -> Result<PageWriteGuard<'block>, LockError> {
        let guard = PageWriteGuard::try_upgrade(self)?;

        Ok(guard)
    }

    pub fn upgrade(self) -> PageWriteGuard<'block> {
        PageWriteGuard::upgrade(self)
    }

    // TODO this should be just named index, as the concept of logical indices does not exist at
    // this level
    pub const fn physical_index(&self) -> PageIndex {
        self.physical_index
    }
}

impl Deref for PageReadGuard<'_> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        unsafe { self.page.as_ref() }
    }
}

impl Drop for PageReadGuard<'_> {
    fn drop(&mut self) {
        if self.lock_consumed {
            return;
        }

        #[cfg(debug_assertions)]
        {
            let elapsed = self.taken.elapsed();

            if elapsed > Duration::from_millis(100) {
                warn!(waited=?elapsed, "read lock held for too long");
            }
        }

        self.block
            .housekeeping_for(self.physical_index)
            .unlock_read();
    }
}

#[derive(Debug)]
pub struct PageWriteGuard<'block> {
    page: NonNull<Page>,
    block: &'block Block,
    physical_index: PageIndex,
    #[cfg(debug_assertions)]
    taken: Instant,
    lock_consumed: bool,
}

unsafe impl Send for PageWriteGuard<'_> {}

impl<'block> PageWriteGuard<'block> {
    #[instrument]
    fn upgrade(mut read_guard: PageReadGuard<'block>) -> Self {
        let housekeeping = read_guard.block.housekeeping_for(read_guard.physical_index);
        housekeeping.upgrade();

        read_guard.lock_consumed = true;

        Self {
            page: read_guard.page,
            block: read_guard.block,
            physical_index: read_guard.physical_index,
            #[cfg(debug_assertions)]
            taken: Instant::now(),
            lock_consumed: false,
        }
    }

    fn try_upgrade(
        // TODO do we want to return back the readguard if the upgrade fails?
        mut read_guard: PageReadGuard<'block>,
    ) -> Result<Self, LockError> {
        let housekeeping = read_guard.block.housekeeping_for(read_guard.physical_index);
        housekeeping.try_upgrade()?;

        read_guard.lock_consumed = true;

        Ok(Self {
            page: read_guard.page,
            block: read_guard.block,
            physical_index: read_guard.physical_index,
            #[cfg(debug_assertions)]
            taken: Instant::now(),
            lock_consumed: false,
        })
    }

    unsafe fn from_locked(
        page: NonNull<Page>,
        block: &'block Block,
        physical_index: PageIndex,
    ) -> Self {
        Self {
            page,
            block,
            physical_index,
            #[cfg(debug_assertions)]
            taken: Instant::now(),
            lock_consumed: false,
        }
    }

    pub fn reset(mut self) -> UninitializedPageGuard<'block> {
        let housekeeping = self.block.housekeeping_for(self.physical_index);
        housekeeping.mark_uninitialized();
        self.lock_consumed = true;

        unsafe {
            UninitializedPageGuard::from_locked(self.block, self.page.cast(), self.physical_index)
        }
    }

    pub const fn physical_index(&self) -> PageIndex {
        self.physical_index
    }
}

impl AsRef<Page> for PageWriteGuard<'_> {
    fn as_ref(&self) -> &Page {
        unsafe { self.page.as_ref() }
    }
}

impl Deref for PageWriteGuard<'_> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        unsafe { self.page.as_ref() }
    }
}

impl DerefMut for PageWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.page.as_mut() }
    }
}

impl Drop for PageWriteGuard<'_> {
    #[instrument(skip(self), fields(physical_index = ?self.physical_index, block = self.block.name))]
    fn drop(&mut self) {
        if self.lock_consumed {
            return;
        }

        #[cfg(debug_assertions)]
        {
            let elapsed = self.taken.elapsed();

            if elapsed > Duration::from_millis(100) {
                warn!(waited=?elapsed, "lock held for too long");
            }
        }

        self.block
            .housekeeping_for(self.physical_index)
            .unlock_write();
    }
}

#[derive(Debug)]
pub struct UninitializedPageGuard<'block> {
    block: &'block Block,
    page: NonNull<MaybeUninit<Page>>,
    physical_index: PageIndex,
    lock_consumed: bool,
}

unsafe impl Send for UninitializedPageGuard<'_> {}

impl<'block> UninitializedPageGuard<'block> {
    pub(super) unsafe fn new(
        block: &'block Block,
        page: NonNull<MaybeUninit<Page>>,
        physical_index: PageIndex,
    ) -> Self {
        let housekeeping = block.housekeeping_for(physical_index);
        match housekeeping.try_write() {
            Ok(()) => {}
            Err(LockError::Contended(previous)) => panic!(
                "lock contended for supposedly uninitialized page {physical_index:?}: {previous:?}"
            ),
        }

        unsafe { Self::from_locked(block, page, physical_index) }
    }

    const unsafe fn from_locked(
        block: &'block Block,
        page: NonNull<MaybeUninit<Page>>,
        physical_index: PageIndex,
    ) -> Self {
        Self {
            block,
            page,
            physical_index,
            lock_consumed: false,
        }
    }

    pub const fn physical_index(&self) -> PageIndex {
        self.physical_index
    }

    #[allow(clippy::large_types_passed_by_value)] // TODO create an API that allows us to avoid
    // this
    #[instrument(skip(self, page), fields(physical_index = ?self.physical_index()))]
    pub fn initialize(mut self, page: Page) -> PageWriteGuard<'block> {
        let housekeeping = self.block.housekeeping_for(self.physical_index);

        // we took the lock when creating this struct
        let initialized_page = NonNull::from_mut(unsafe { self.page.as_mut().write(page) });
        housekeeping.mark_initialized();
        self.lock_consumed = true;

        unsafe { PageWriteGuard::from_locked(initialized_page, self.block, self.physical_index) }
    }
}

impl Drop for UninitializedPageGuard<'_> {
    fn drop(&mut self) {
        debug!(
            physical_index = ?self.physical_index,
            lock_consumed = ?self.lock_consumed,
            "dropping uninitialized page"
        );

        if self.lock_consumed {
            return;
        }

        debug!(
            physical_index = ?self.physical_index,
            "unlocking uninitialized page"
        );

        let housekeeping = self.block.housekeeping_for(self.physical_index);
        housekeeping.unlock_write();
    }
}

pub struct Block {
    name: String,
    housekeeping: Box<dyn Allocation>,
    data: Box<dyn Allocation>,
    latest_page: AtomicU64,
    allocated_page_count: AtomicU64,
}

impl Debug for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Block")
            .field("name", &self.name)
            .field("housekeeping", &self.housekeeping)
            .field("data", &self.data)
            .field("latest_page", &self.latest_page)
            .field("allocated_page_count", &self.allocated_page_count)
            .finish_non_exhaustive()
    }
}

impl Block {
    const HOUSEKEEPING_BLOCK_SIZE: Size = Size::of::<PageState>().multiply(Self::PAGE_COUNT);
    const PAGE_COUNT: usize = Self::SIZE.divide(PAGE_SIZE);
    const SIZE: Size = if cfg!(miri) {
        Size::MiB(128)
    } else {
        Size::GiB(4)
    };

    pub fn new(name: String) -> Self {
        let housekeeping: Box<dyn Allocation> =
            Box::new(UncommittedAllocation::new(Self::HOUSEKEEPING_BLOCK_SIZE));
        let data: Box<dyn Allocation> = Box::new(UncommittedAllocation::new(Self::SIZE));

        Self {
            name,
            housekeeping,
            data,
            latest_page: AtomicU64::new(0),
            allocated_page_count: AtomicU64::new(0),
        }
    }

    pub fn allocated_page_count(&self) -> u64 {
        self.allocated_page_count.load(Ordering::Acquire)
    }

    #[instrument]
    pub fn get(&self, physical_index: PageIndex) -> PageReadGuard<'_> {
        let latest_initialized_page = self.allocated_page_count.load(Ordering::Acquire);
        assert!(
            physical_index.0 < latest_initialized_page,
            "trying to get page {physical_index:?}, but initialized only upto {latest_initialized_page:?}",
        );

        let housekeeping = self.housekeeping_for(physical_index);

        if !housekeeping.initialized() {
            error!(
                ?physical_index,
                ?housekeeping,
                "trying to get a page, but housekeeping is not initialized",
            );
            panic!(
                "[{}] trying to get {physical_index:?}, but housekeeping is not initialized",
                self.name
            );
        }

        let page = unsafe {
            self.data
                .base_address()
                .cast()
                .add(usize::try_from(physical_index.0).unwrap())
        };

        assert!(page.cast() < unsafe { self.data.base_address().byte_add(Self::SIZE.as_bytes()) });
        assert!(page.cast() >= self.data.base_address());

        unsafe { PageReadGuard::new(page, self, physical_index) }
    }

    pub fn get_uninitialized(&'_ self, physical_index: PageIndex) -> UninitializedPageGuard<'_> {
        let latest_initialized_page = self.allocated_page_count.load(Ordering::Acquire);
        assert!(
            physical_index.0 < latest_initialized_page,
            "trying to get page {physical_index:?}, but initialized only upto {latest_initialized_page:?}",
        );

        let housekeeping = self.housekeeping_for(physical_index);

        match housekeeping.try_write() {
            Ok(()) => {}
            Err(LockError::Contended(previous)) => panic!(
                "lock contended while trying to get supposedly uninitialized page {physical_index:?}: {previous:?}"
            ),
        }

        assert!(
            !housekeeping.initialized(),
            "[{}] trying to get as unitialized {physical_index:?}, but housekeeping says it's already initialized",
            self.name
        );

        let page = unsafe {
            self.data
                .base_address()
                .cast()
                .add(usize::try_from(physical_index.0).unwrap())
        };

        assert!(page.cast() < unsafe { self.data.base_address().byte_add(Self::SIZE.as_bytes()) });
        assert!(page.cast() >= self.data.base_address());

        unsafe { UninitializedPageGuard::from_locked(self, page, physical_index) }
    }

    #[instrument]
    pub fn get_or_allocate_zeroed(
        &'_ self,
        physical_index: PageIndex,
        // TODO do we want a version of this that returns a read-only lock?
    ) -> Result<PageWriteGuard<'_>, StorageError<InMemoryPageId>> {
        while physical_index.0 >= self.allocated_page_count.load(Ordering::Acquire) {
            let allocated = self.allocate()?.initialize(Page::new());

            if allocated.physical_index == physical_index {
                return Ok(allocated);
            }
        }

        debug_assert!(self.allocated_page_count.load(Ordering::Acquire) > physical_index.0);

        Ok(self.get(physical_index).upgrade())
    }

    #[instrument]
    // TODO this method is almost all copy-paste with get, clean up
    pub fn try_get(&'_ self, physical_index: PageIndex) -> Option<PageReadGuard<'_>> {
        let allocated_page_count = self.allocated_page_count.load(Ordering::Acquire);

        if physical_index.0 >= allocated_page_count {
            return None;
        }

        if !self.housekeeping_for(physical_index).initialized() {
            return None;
        }

        let latest_initialized_page = self.allocated_page_count.load(Ordering::Acquire);
        assert!(
            physical_index.0 < latest_initialized_page,
            "trying to get page {physical_index:?}, but initialized only upto {latest_initialized_page:?}",
        );

        let housekeeping = self.housekeeping_for(physical_index);

        if !housekeeping.initialized() {
            error!(
                ?physical_index,
                ?housekeeping,
                "trying to get a page, but housekeeping is not initialized",
            );
            panic!(
                "[{}] trying to get {physical_index:?}, but housekeeping is not initialized",
                self.name
            );
        }

        let page = unsafe {
            self.data
                .base_address()
                .cast()
                .add(usize::try_from(physical_index.0).unwrap())
        };

        assert!(page.cast() < unsafe { self.data.base_address().byte_add(Self::SIZE.as_bytes()) });
        assert!(page.cast() >= self.data.base_address());

        unsafe { PageReadGuard::try_lock(page, self, physical_index).ok() }
    }

    #[instrument]
    pub fn allocate(&self) -> Result<UninitializedPageGuard<'_>, StorageError<InMemoryPageId>> {
        let index = self.latest_page.fetch_add(1, Ordering::AcqRel);

        let index = PageIndex(index);
        self.allocate_housekeeping(index)?;

        self.allocated_page_count
            .fetch_max(index.0 + 1, Ordering::AcqRel);

        let page = unsafe {
            self.data
                .base_address()
                .cast()
                .add(usize::try_from(index.0).unwrap())
        };

        debug_assert!(
            page.cast() < unsafe { self.data.base_address().byte_add(Self::SIZE.as_bytes()) }
        );
        debug_assert!(page.cast() >= self.data.base_address());

        Ok(unsafe { UninitializedPageGuard::new(self, page, index) })
    }

    #[instrument]
    fn allocate_housekeeping(
        &self,
        index: PageIndex,
    ) -> Result<NonNull<PageState>, StorageError<InMemoryPageId>> {
        if index.0 >= Self::PAGE_COUNT as u64 {
            return Err(StorageError::OutOfSpace);
        }

        // TODO this way we do the mprotect multiple times, it doesn't really matter, but might
        // make sense to only do that when index%PAGE_SIZE == 0??? (though then we have to
        // always assume that we will be getting contiguous indices, which might make other
        // things harder...)
        let houskeeping_page = unsafe {
            self.housekeeping
                .base_address()
                .cast::<[u8; PAGE_SIZE.as_bytes()]>()
                .add(usize::try_from(index.0).unwrap() / (PAGE_SIZE / Size::of::<PageState>()))
        };
        self.housekeeping.commit_page(houskeeping_page.cast());

        let page = unsafe {
            self.data
                .base_address()
                .cast::<Page>()
                .add(index.0.try_into().unwrap())
        };
        self.data.commit_page(page.cast());

        let page_state = unsafe {
            self.housekeeping
                .base_address()
                .cast::<PageState>()
                .add(index.0.try_into().unwrap())
        };
        unsafe {
            page_state.write(PageState::new());
        };

        assert!(
            page_state.cast()
                < unsafe {
                    self.housekeeping
                        .base_address()
                        .byte_add(Self::HOUSEKEEPING_BLOCK_SIZE.as_bytes())
                }
        );
        assert!(page_state.cast() >= self.housekeeping.base_address());

        Ok(page_state)
    }

    fn housekeeping_for(&self, index: PageIndex) -> Pin<&PageState> {
        assert!(index.0 < self.allocated_page_count());

        let address = unsafe {
            self.housekeeping
                .base_address()
                .cast()
                .add(usize::try_from(index.0).unwrap())
        };

        assert!(
            address.cast()
                < unsafe {
                    self.housekeeping
                        .base_address()
                        .byte_add(Self::HOUSEKEEPING_BLOCK_SIZE.as_bytes())
                }
        );
        assert!(address.cast() >= self.housekeeping.base_address());

        unsafe { Pin::new_unchecked(address.as_ref()) }
    }
}
