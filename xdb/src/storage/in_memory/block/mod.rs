// TODO the logical_index thing is a hack, instead version_manager should wrap all pagerefs into
// it's own datastructures which have that information

mod page_state;

use crate::platform::allocation::Allocation;
use crate::platform::allocation::uncommitted::UncommittedAllocation;
use crate::storage::StorageError;
use crate::storage::in_memory::block::page_state::PageStateValue;
use std::ops::DerefMut;
#[cfg(debug_assertions)]
use std::time::{Duration, Instant};
use std::{fmt::Debug, mem::MaybeUninit, ops::Deref, pin::Pin, ptr::NonNull};
use thiserror::Error;
use tracing::{debug, error, instrument, warn};

use crate::{
    Size,
    page::{PAGE_SIZE, Page},
    storage::{PageIndex, in_memory::block::page_state::PageState},
    sync::atomic::{AtomicU64, Ordering},
};

#[derive(Debug, Error)]
pub enum LockError {
    #[error("contended")]
    Contended(PageStateValue),
}

#[derive(Debug)]
pub struct IdLock<'block> {
    physical_index: PageIndex,
    block: &'block Block,
}

impl<'block> IdLock<'block> {
    pub unsafe fn new(physical_index: PageIndex, block: &'block Block) -> Self {
        unsafe { block.housekeeping_for(physical_index) }.lock_id();

        Self {
            physical_index,
            block,
        }
    }
}

impl Clone for IdLock<'_> {
    fn clone(&self) -> Self {
        unsafe { self.block.housekeeping_for(self.physical_index) }.lock_id();

        Self {
            physical_index: self.physical_index,
            block: self.block,
        }
    }
}

impl Drop for IdLock<'_> {
    fn drop(&mut self) {
        unsafe { self.block.housekeeping_for(self.physical_index) }.unlock_id();
    }
}

#[derive(Debug, Clone)]
pub struct PageRef<'block> {
    page: NonNull<Page>,
    block: &'block Block,
    logical_index: Option<PageIndex>,
    physical_index: PageIndex,
    #[allow(unused)]
    id_lock: IdLock<'block>,
}

unsafe impl Send for PageRef<'_> {}

impl<'block> PageRef<'block> {
    #[instrument(skip(self), fields(logical_index = ?self.logical_index, physical_index = ?self.physical_index, block = self.block.name))]
    // TODO rename -> try_lock, change result to option
    pub fn lock_nowait(&self) -> Result<PageGuard<'block>, LockError> {
        unsafe {
            PageGuard::new_nowait(
                self.page,
                self.block,
                self.logical_index,
                self.physical_index,
            )
        }
    }

    #[instrument(skip(self), fields(logical_index = ?self.logical_index, physical_index = ?self.physical_index, block = self.block.name))]
    pub fn lock(&self) -> PageGuard<'block> {
        unsafe {
            PageGuard::new(
                self.page,
                self.block,
                self.logical_index,
                self.physical_index,
            )
        }
    }

    #[instrument(skip(self), fields(logical_index = ?self.logical_index, physical_index = ?self.physical_index, block = self.block.name))]
    pub fn lock_read(&self) -> PageGuardRead<'block> {
        unsafe {
            PageGuardRead::new(
                self.page,
                self.block,
                self.logical_index,
                self.physical_index,
            )
        }
    }

    pub const fn logical_index(&self) -> Option<PageIndex> {
        self.logical_index
    }

    pub const fn physical_index(&self) -> PageIndex {
        self.physical_index
    }

    pub(crate) fn lock_for_move(self) -> Result<PageGuard<'block>, Self> {
        let page = self.page;
        let block = self.block;
        let logical_index = self.logical_index;
        let physical_index = self.physical_index;

        unsafe { self.block.housekeeping_for(self.physical_index) }.lock_for_move(self)?;

        Ok(unsafe { PageGuard::from_locked(page, block, logical_index, physical_index) })
    }
}

#[derive(Debug)]
pub struct PageGuardRead<'block> {
    page: NonNull<Page>,
    block: &'block Block,
    #[allow(unused)] // TODO drop if we really don't need it
    logical_index: Option<PageIndex>,
    physical_index: PageIndex,
    #[cfg(debug_assertions)]
    taken: Instant,
}

impl<'block> PageGuardRead<'block> {
    #[instrument]
    unsafe fn new(
        page: NonNull<Page>,
        block: &'block Block,
        logical_index: Option<PageIndex>,
        physical_index: PageIndex,
    ) -> Self {
        let housekeeping = unsafe { block.housekeeping_for(physical_index) };
        housekeeping.lock_read();

        Self {
            page,
            block,
            logical_index,
            physical_index,
            #[cfg(debug_assertions)]
            taken: Instant::now(),
        }
    }
}

impl Deref for PageGuardRead<'_> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        unsafe { self.page.as_ref() }
    }
}

impl Drop for PageGuardRead<'_> {
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        {
            let elapsed = self.taken.elapsed();

            if elapsed > Duration::from_millis(100) {
                warn!(waited=?elapsed, "read lock held for too long");
            }
        }

        unsafe { self.block.housekeeping_for(self.physical_index) }.unlock_read();
    }
}

#[derive(Debug)]
pub struct PageGuard<'block> {
    page: NonNull<Page>,
    block: &'block Block,
    logical_index: Option<PageIndex>,
    physical_index: PageIndex,
    #[cfg(debug_assertions)]
    taken: Instant,
    lock_consumed: bool,
}

unsafe impl Send for PageGuard<'_> {}

impl<'block> PageGuard<'block> {
    #[instrument]
    unsafe fn new(
        page: NonNull<Page>,
        block: &'block Block,
        logical_index: Option<PageIndex>,
        physical_index: PageIndex,
    ) -> Self {
        let housekeeping = unsafe { block.housekeeping_for(physical_index) };
        housekeeping.lock();

        Self {
            page,
            block,
            logical_index,
            physical_index,
            #[cfg(debug_assertions)]
            taken: Instant::now(),
            lock_consumed: false,
        }
    }

    unsafe fn new_nowait(
        page: NonNull<Page>,
        block: &'block Block,
        logical_index: Option<PageIndex>,
        physical_index: PageIndex,
    ) -> Result<Self, LockError> {
        let housekeeping = unsafe { block.housekeeping_for(physical_index) };
        housekeeping.lock_nowait()?;

        Ok(Self {
            page,
            block,
            logical_index,
            physical_index,
            #[cfg(debug_assertions)]
            taken: Instant::now(),
            lock_consumed: false,
        })
    }

    unsafe fn from_locked(
        page: NonNull<Page>,
        block: &'block Block,
        logical_index: Option<PageIndex>,
        physical_index: PageIndex,
    ) -> Self {
        Self {
            page,
            block,
            logical_index,
            physical_index,
            #[cfg(debug_assertions)]
            taken: Instant::now(),
            lock_consumed: false,
        }
    }

    pub fn reset(mut self) -> UninitializedPageGuard<'block> {
        let housekeeping = unsafe { self.block.housekeeping_for(self.physical_index) };
        housekeeping.mark_uninitialized();
        self.lock_consumed = true;

        unsafe {
            UninitializedPageGuard::from_locked(
                self.block,
                self.page.cast(),
                self.logical_index,
                self.physical_index,
            )
        }
    }

    pub const fn physical_index(&self) -> PageIndex {
        self.physical_index
    }

    pub const fn logical_index(&self) -> Option<PageIndex> {
        self.logical_index
    }
}

impl AsRef<Page> for PageGuard<'_> {
    fn as_ref(&self) -> &Page {
        unsafe { self.page.as_ref() }
    }
}

impl Deref for PageGuard<'_> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        unsafe { self.page.as_ref() }
    }
}

impl DerefMut for PageGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.page.as_mut() }
    }
}

impl Drop for PageGuard<'_> {
    #[instrument(skip(self), fields(logical_index = ?self.logical_index, physical_index = ?self.physical_index, block = self.block.name))]
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

        unsafe { self.block.housekeeping_for(self.physical_index) }.unlock();
    }
}

#[derive(Debug)]
// TODO rename -> UninitializedPageRef
pub struct UninitializedPageGuard<'block> {
    block: &'block Block,
    page: NonNull<MaybeUninit<Page>>,
    logical_index: Option<PageIndex>,
    physical_index: PageIndex,
    lock_consumed: bool,
}

unsafe impl Send for UninitializedPageGuard<'_> {}

impl<'block> UninitializedPageGuard<'block> {
    pub(super) unsafe fn new(
        block: &'block Block,
        page: NonNull<MaybeUninit<Page>>,
        logical_index: Option<PageIndex>,
        physical_index: PageIndex,
    ) -> Self {
        let housekeeping = unsafe { block.housekeeping_for(physical_index) };
        match housekeeping.lock_nowait() {
            Ok(()) => {}
            Err(LockError::Contended(previous)) => panic!(
                "lock contended for supposedly unallocated page {logical_index:?}/{physical_index:?}: {previous:?}"
            ),
        }

        unsafe { Self::from_locked(block, page, logical_index, physical_index) }
    }

    const unsafe fn from_locked(
        block: &'block Block,
        page: NonNull<MaybeUninit<Page>>,
        logical_index: Option<PageIndex>,
        physical_index: PageIndex,
    ) -> Self {
        Self {
            block,
            page,
            logical_index,
            physical_index,
            lock_consumed: false,
        }
    }

    pub const fn logical_index(&self) -> Option<PageIndex> {
        // TODO should we return an option instead?
        self.logical_index
    }

    pub const fn physical_index(&self) -> PageIndex {
        self.physical_index
    }

    #[allow(clippy::large_types_passed_by_value)] // TODO create an API that allows us to avoid
    // this
    #[instrument(skip(self, page), fields(logical_index = ?self.logical_index(), physical_index = ?self.physical_index()))]
    pub fn initialize(mut self, page: Page) -> PageRef<'block> {
        let housekeeping = unsafe { self.block.housekeeping_for(self.physical_index) };

        // we took the lock when creating this struct
        let initialized_page = NonNull::from_mut(unsafe { self.page.as_mut().write(page) });
        housekeeping.mark_initialized();
        housekeeping.unlock();
        self.lock_consumed = true;

        PageRef {
            page: initialized_page,
            block: self.block,
            logical_index: self.logical_index,
            physical_index: self.physical_index,
            id_lock: unsafe { IdLock::new(self.physical_index, self.block) },
        }
    }

    pub(super) const fn as_ptr(&self) -> NonNull<MaybeUninit<Page>> {
        self.page
    }
}

impl Drop for UninitializedPageGuard<'_> {
    fn drop(&mut self) {
        debug!(
            logical_index = ?self.logical_index,
            physical_index = ?self.physical_index,
            lock_consumed = ?self.lock_consumed,
            "dropping uninitialized page"
        );

        if self.lock_consumed {
            return;
        }

        debug!(
            logical_index = ?self.logical_index,
            physical_index = ?self.physical_index,
            "unlocking uninitialized page"
        );

        let housekeeping = unsafe { self.block.housekeeping_for(self.physical_index) };
        housekeeping.unlock();
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
    const SIZE: Size = if cfg!(miri) {
        Size::MiB(128)
    } else {
        Size::GiB(4)
    };
    const PAGE_COUNT: usize = Self::SIZE.divide(PAGE_SIZE);
    const HOUSEKEEPING_BLOCK_SIZE: Size = Size::of::<PageState>().multiply(Self::PAGE_COUNT);

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

    // This will return a value, that is equal or lower than the count of allocated pages.
    //
    // TODO rename this, as this is actually an upper bound, and while it's guaranted that the
    // pages are allocated, it's not guaranteed that those pages are initialized
    #[instrument]
    pub fn page_count_lower_bound(&self) -> u64 {
        self.allocated_page_count.load(Ordering::Acquire)
    }

    #[instrument]
    pub fn get(&self, logical_index: Option<PageIndex>, physical_index: PageIndex) -> PageRef<'_> {
        let latest_initialized_page = self.allocated_page_count.load(Ordering::Acquire);
        assert!(
            physical_index.0 < latest_initialized_page,
            "trying to get page {logical_index:?}/{physical_index:?}, but initialized only upto {latest_initialized_page:?}",
        );

        let housekeeping = unsafe { self.housekeeping_for(physical_index) };

        if !housekeeping.is_initialized() {
            error!(
                ?logical_index,
                ?physical_index,
                ?housekeeping,
                "trying to get a page, but housekeeping is not initialized",
            );
            panic!(
                "[{}] trying to get {logical_index:?}/{physical_index:?}, but housekeeping is not initialized",
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

        PageRef {
            page,
            block: self,
            logical_index,
            physical_index,
            id_lock: unsafe { IdLock::new(physical_index, self) },
        }
    }

    // TODO it's unsafe because we don't want multiple UninitializedPageGuards, but do we really
    // care?
    pub unsafe fn get_uninitialized(
        &'_ self,
        logical_index: Option<PageIndex>,
        physical_index: PageIndex,
    ) -> UninitializedPageGuard<'_> {
        let latest_initialized_page = self.allocated_page_count.load(Ordering::Acquire);
        assert!(
            physical_index.0 < latest_initialized_page,
            "trying to get page {logical_index:?}/{physical_index:?}, but initialized only upto {latest_initialized_page:?}",
        );

        let housekeeping = unsafe { self.housekeeping_for(physical_index) };

        match housekeeping.lock_nowait() {
            Ok(()) => {}
            Err(LockError::Contended(previous)) => panic!(
                "lock contended while trying to get supposedly uninitialized page {physical_index:?}: {previous:?}"
            ),
        }

        assert!(
            !housekeeping.is_initialized(),
            "[{}] trying to get as unitialized {logical_index:?}/{physical_index:?}, but housekeeping says it's already initialized",
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

        unsafe { UninitializedPageGuard::from_locked(self, page, logical_index, physical_index) }
    }

    #[instrument]
    pub fn get_or_allocate_zeroed(
        &'_ self,
        logical_index: Option<PageIndex>,
        physical_index: PageIndex,
    ) -> Result<PageRef<'_>, StorageError> {
        while physical_index.0 >= self.allocated_page_count.load(Ordering::Acquire) {
            let allocated = self.allocate(logical_index)?.initialize(Page::new());

            if allocated.physical_index == physical_index {
                return Ok(allocated);
            }
        }

        debug_assert!(self.allocated_page_count.load(Ordering::Acquire) > physical_index.0);

        Ok(self.get(logical_index, physical_index))
    }

    #[instrument]
    pub fn try_get(
        &'_ self,
        logical_index: Option<PageIndex>,
        physical_index: PageIndex,
    ) -> Option<PageRef<'_>> {
        let allocated_page_count = self.allocated_page_count.load(Ordering::Acquire);

        if physical_index.0 >= allocated_page_count {
            return None;
        }

        if !unsafe { self.housekeeping_for(physical_index) }.is_initialized() {
            return None;
        }

        // TODO should we fliparound and define self.get in terms of try_get?
        Some(self.get(logical_index, physical_index))
    }

    #[instrument]
    pub fn allocate(
        &self,
        logical_index: Option<PageIndex>,
    ) -> Result<UninitializedPageGuard<'_>, StorageError> {
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

        Ok(unsafe { UninitializedPageGuard::new(self, page, logical_index, index) })
    }

    #[instrument]
    fn allocate_housekeeping(&self, index: PageIndex) -> Result<NonNull<PageState>, StorageError> {
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

    unsafe fn housekeeping_for(&self, index: PageIndex) -> Pin<&PageState> {
        assert!(index.0 < Self::PAGE_COUNT as u64);

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
