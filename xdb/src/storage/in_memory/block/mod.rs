mod page_state;

use crate::platform::allocation::Allocation;
use crate::platform::allocation::uncommitted::UncommittedAllocation;
use crate::storage::StorageError;
use crate::storage::in_memory::block::page_state::PageStateValue;
use bytemuck::Zeroable;
use std::ops::DerefMut;
#[cfg(debug_assertions)]
use std::time::{Duration, Instant};
use std::{fmt::Debug, mem::MaybeUninit, ops::Deref, pin::Pin, ptr::NonNull};
use thiserror::Error;
use tracing::{Span, debug, debug_span, info_span, instrument, warn};

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

#[derive(Debug, Clone, Copy)]
pub struct PageRef<'block> {
    page: NonNull<Page>,
    block: &'block Block,
    index: PageIndex,
}

unsafe impl Send for PageRef<'_> {}

impl<'block> PageRef<'block> {
    pub(super) const unsafe fn new(
        page: NonNull<Page>,
        block: &'block Block,
        index: PageIndex,
    ) -> Self {
        Self { page, block, index }
    }

    #[instrument(skip(self), fields(index = ?self.index, block = self.block.name))]
    // TODO rename -> try_lock, change result to option
    pub fn lock_nowait(&self) -> Result<PageGuard<'block>, LockError> {
        unsafe { PageGuard::new_nowait(self.page, self.block, self.index) }
    }

    #[instrument(skip(self), fields(index = ?self.index, block = self.block.name))]
    pub fn lock(&self) -> PageGuard<'block> {
        unsafe { PageGuard::new(self.page, self.block, self.index) }
    }

    // TODO does it have to be unsafe? do we really care if somebody else has a PageRef to this?
    pub unsafe fn reset(self) -> UninitializedPageGuard<'block> {
        let housekeeping = unsafe { self.block.housekeeping_for(self.index) };
        housekeeping.mark_uninitialized();

        unsafe { UninitializedPageGuard::new(self.block, self.page.cast(), self.index) }
    }

    pub const fn index(&self) -> PageIndex {
        self.index
    }

    pub(super) const fn as_ptr(&self) -> NonNull<Page> {
        self.page
    }
}

#[derive(Debug)]
pub struct PageGuard<'block> {
    page: NonNull<Page>,
    block: &'block Block,
    index: PageIndex,
    #[cfg(debug_assertions)]
    taken: Instant,
    span: Span,
}

unsafe impl Send for PageGuard<'_> {}

impl<'block> PageGuard<'block> {
    unsafe fn new(page: NonNull<Page>, block: &'block Block, index: PageIndex) -> Self {
        let span = info_span!("page guard", block = block.name, ?index);
        span.in_scope(|| {
            let housekeeping = unsafe { block.housekeeping_for(index) };
            housekeeping.lock();
        });

        Self {
            page,
            block,
            index,
            #[cfg(debug_assertions)]
            taken: Instant::now(),
            span,
        }
    }

    unsafe fn new_nowait(
        page: NonNull<Page>,
        block: &'block Block,
        index: PageIndex,
    ) -> Result<Self, LockError> {
        let span = debug_span!("page guard", block = block.name, ?index);
        span.in_scope(|| {
            let housekeeping = unsafe { block.housekeeping_for(index) };
            housekeeping.lock_nowait()?;

            Ok(())
        })?;

        Ok(Self {
            page,
            block,
            index,
            #[cfg(debug_assertions)]
            taken: Instant::now(),
            span,
        })
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
    #[instrument(skip(self), fields(index = ?self.index, block = self.block.name), follows_from = [&self.span])]
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        {
            let elapsed = self.taken.elapsed();

            if elapsed > Duration::from_millis(100) {
                warn!("lock held for too long: {elapsed:?}");
            }
        }

        unsafe { self.block.housekeeping_for(self.index) }.unlock();
    }
}

#[derive(Debug)]
// TODO rename -> UninitializedPageRef
pub struct UninitializedPageGuard<'block> {
    block: &'block Block,
    page: NonNull<MaybeUninit<Page>>,
    index: PageIndex,
}

unsafe impl Send for UninitializedPageGuard<'_> {}

impl<'block> UninitializedPageGuard<'block> {
    pub(super) const unsafe fn new(
        block: &'block Block,
        page: NonNull<MaybeUninit<Page>>,
        index: PageIndex,
    ) -> Self {
        Self { block, page, index }
    }

    pub const fn index(&self) -> PageIndex {
        self.index
    }

    #[allow(clippy::large_types_passed_by_value)] // TODO create an API that allows us to avoid
    // this
    pub fn initialize(mut self, page: Page) -> PageRef<'block> {
        let housekeeping = unsafe { self.block.housekeeping_for(self.index) };

        // we're taking a mutable reference, so we must lock so that there is only one, even if
        // there are multiple UninitializedPageGuards
        housekeeping.lock();
        let initialized_page = NonNull::from_mut(unsafe { self.page.as_mut().write(page) });
        housekeeping.mark_initialized();
        housekeeping.unlock();

        PageRef {
            page: initialized_page,
            block: self.block,
            index: self.index,
        }
    }

    pub(super) const fn as_ptr(&self) -> NonNull<MaybeUninit<Page>> {
        self.page
    }
}

pub struct Block {
    name: String,
    housekeeping: Box<dyn Allocation>,
    data: Box<dyn Allocation>,
    latest_page: AtomicU64,
    allocated_page_count: AtomicU64,
    span: tracing::Span,
}

impl Debug for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Block")
            .field("name", &self.name)
            .field("housekeeping", &self.housekeeping)
            .field("data", &self.data)
            .field("latest_page", &self.latest_page)
            .field(
                "allocated_page_count_lower_bound",
                &self.allocated_page_count,
            )
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
        let span = info_span!("block", name, base=?data.base_address(), housekeeping=?housekeeping.base_address());

        Self {
            name,
            housekeeping,
            data,
            latest_page: AtomicU64::new(0),
            allocated_page_count: AtomicU64::new(0),
            span,
        }
    }

    // This will return a value, that is equal or lower than the count of allocated pages.
    //
    // TODO rename this, as this is actually an upper bound, and while it's guaranted that the
    // pages are allocated, it's not guaranteed that those pages are initialized
    #[instrument(skip(self), parent = &self.span)]
    pub fn page_count_lower_bound(&self) -> u64 {
        self.allocated_page_count.load(Ordering::Acquire)
    }

    #[instrument(skip(self), parent = &self.span)]
    pub fn get(&self, index: PageIndex) -> PageRef<'_> {
        let latest_initialized_page = self.allocated_page_count.load(Ordering::Acquire);
        assert!(
            index.0 < latest_initialized_page,
            "trying to get page {index:?}, but initialized only upto {latest_initialized_page:?}",
        );

        let housekeeping = unsafe { self.housekeeping_for(index) };

        assert!(
            housekeeping.is_initialized(),
            "[{}] trying to get {index:?}, but housekeeping is not initialized",
            self.name
        );

        let page = unsafe {
            self.data
                .base_address()
                .cast()
                .add(usize::try_from(index.0).unwrap())
        };

        assert!(page.cast() < unsafe { self.data.base_address().byte_add(Self::SIZE.as_bytes()) });
        assert!(page.cast() >= self.data.base_address());

        PageRef {
            page,
            block: self,
            index,
        }
    }

    // TODO it's unsafe because we don't want multiple UninitializedPageGuards, but do we really
    // care?
    pub unsafe fn get_uninitialized(&'_ self, index: PageIndex) -> UninitializedPageGuard<'_> {
        let latest_initialized_page = self.allocated_page_count.load(Ordering::Acquire);
        assert!(
            index.0 < latest_initialized_page,
            "trying to get page {index:?}, but initialized only upto {latest_initialized_page:?}",
        );

        let housekeeping = unsafe { self.housekeeping_for(index) };

        assert!(
            !housekeeping.is_initialized(),
            "[{}] trying to get as unitialized {index:?}, but housekeeping says it's already initialized",
            self.name
        );

        let page = unsafe {
            self.data
                .base_address()
                .cast()
                .add(usize::try_from(index.0).unwrap())
        };

        assert!(page.cast() < unsafe { self.data.base_address().byte_add(Self::SIZE.as_bytes()) });
        assert!(page.cast() >= self.data.base_address());

        unsafe { UninitializedPageGuard::new(self, page, index) }
    }

    #[instrument(skip(self), parent = &self.span)]
    pub fn get_or_allocate_zeroed(&'_ self, index: PageIndex) -> Result<PageRef<'_>, StorageError> {
        while index.0 >= self.allocated_page_count.load(Ordering::Acquire) {
            let allocated = self.allocate()?.initialize(Page::zeroed());

            if allocated.index == index {
                return Ok(allocated);
            }
        }

        debug_assert!(self.allocated_page_count.load(Ordering::Acquire) > index.0);

        Ok(self.get(index))
    }

    #[instrument(skip(self), parent = &self.span)]
    pub fn try_get(&'_ self, index: PageIndex) -> Option<PageRef<'_>> {
        let allocated_page_count = self.allocated_page_count.load(Ordering::Acquire);
        if index.0 >= allocated_page_count {
            return None;
        }

        if !unsafe { self.housekeeping_for(index) }.is_initialized() {
            return None;
        }

        // TODO should we fliparound and define self.get in terms of try_get?
        Some(self.get(index))
    }

    #[instrument(skip(self), parent = &self.span)]
    pub fn allocate(&self) -> Result<UninitializedPageGuard<'_>, StorageError> {
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

    fn allocate_housekeeping(&self, index: PageIndex) -> Result<NonNull<PageState>, StorageError> {
        debug!("[{}] allocating housekeeping for {index:?}", self.name);

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
