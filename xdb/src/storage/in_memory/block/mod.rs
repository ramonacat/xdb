mod page_state;

use crate::platform::allocation::uncommitted::UncommittedAllocation;
use crate::storage::TransactionId;
use crate::storage::in_memory::block::page_state::PageStateValue;
use crate::{
    platform::allocation::Allocation, storage::in_memory::block::page_state::DebugContext,
};
use log::debug;
use std::ops::DerefMut;
use std::{fmt::Debug, mem::MaybeUninit, ops::Deref, pin::Pin, ptr::NonNull};
use thiserror::Error;

use crate::{
    Size,
    page::{PAGE_SIZE, Page},
    storage::{PageIndex, in_memory::block::page_state::PageState},
    sync::atomic::{AtomicU64, Ordering},
};

#[derive(Debug, Clone, Copy)]
pub struct PageRef<'block> {
    page: NonNull<Page>,
    block: &'block Block,
    index: PageIndex,
    txid: TransactionId,
}

unsafe impl Send for PageRef<'_> {}

#[derive(Debug, Error)]
pub enum LockError {
    #[error("contended")]
    Contended(PageStateValue),
}

impl<'block> PageRef<'block> {
    pub fn lock_nowait(&self) -> Result<PageGuard<'block>, LockError> {
        unsafe { PageGuard::new_nowait(self.page, self.block, self.index, self.txid) }
    }

    pub fn lock(&self) -> PageGuard<'block> {
        unsafe { PageGuard::new(self.page, self.block, self.index, self.txid) }
    }
}

#[derive(Debug)]
pub struct PageGuard<'block> {
    page: NonNull<Page>,
    block: &'block Block,
    index: PageIndex,
    txid: TransactionId,
}

unsafe impl Send for PageGuard<'_> {}

impl<'block> PageGuard<'block> {
    unsafe fn new(
        page: NonNull<Page>,
        block: &'block Block,
        index: PageIndex,
        txid: TransactionId,
    ) -> Self {
        let housekeeping = unsafe { block.housekeeping_for(index) };
        housekeeping.lock(DebugContext::new(txid, index));

        Self {
            page,
            block,
            index,
            txid,
        }
    }

    unsafe fn new_nowait(
        page: NonNull<Page>,
        block: &'block Block,
        index: PageIndex,
        txid: TransactionId,
    ) -> Result<Self, LockError> {
        let housekeeping = unsafe { block.housekeeping_for(index) };
        housekeeping.lock_nowait(DebugContext::new(txid, index))?;

        Ok(Self {
            page,
            block,
            index,
            txid,
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
    fn drop(&mut self) {
        unsafe { self.block.housekeeping_for(self.index) }
            .unlock(DebugContext::new(self.txid, self.index));
    }
}

#[derive(Debug)]
pub struct UninitializedPageGuard<'block> {
    block: &'block Block,
    page: NonNull<MaybeUninit<Page>>,
    index: PageIndex,
    txid: TransactionId,
}

unsafe impl Send for UninitializedPageGuard<'_> {}

impl<'block> UninitializedPageGuard<'block> {
    const fn new(
        block: &'block Block,
        page: NonNull<MaybeUninit<Page>>,
        index: PageIndex,
        txid: TransactionId,
    ) -> Self {
        Self {
            block,
            page,
            index,
            txid,
        }
    }

    pub const fn index(&self) -> PageIndex {
        self.index
    }

    #[allow(clippy::large_types_passed_by_value)] // TODO create an API that allows us to avoid
    // this
    pub fn initialize(mut self, page: Page) -> PageRef<'block> {
        let initialied = unsafe { &raw mut *self.page.as_mut().write(page) };

        let housekeeping = unsafe { self.block.housekeeping_for(self.index) };
        housekeeping.mark_initialized();

        PageRef {
            page: NonNull::new(initialied).unwrap(),
            block: self.block,
            index: self.index,
            txid: self.txid,
        }
    }
}

#[derive(Debug)]
// TODO give the blocks names for use in debugging
pub struct Block {
    name: String,
    housekeeping: Box<dyn Allocation>,
    data: Box<dyn Allocation>,
    latest_page: AtomicU64,
}

impl Block {
    const SIZE: Size = Size::GiB(4);
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
        }
    }

    pub fn get(&self, index: PageIndex, txid: TransactionId) -> PageRef<'_> {
        debug!("[{}] reading page {index:?}", self.name);

        assert!(index.0 < self.latest_page.load(Ordering::Acquire));

        let housekeeping = unsafe { self.housekeeping_for(index) };

        assert!(
            housekeeping.is_initialized(),
            "[{:?}] [{txid:?}] trying to get {index:?}, but housekeeping is not initialized",
            self.name
        );

        let page = unsafe {
            self.data
                .base_address()
                .cast()
                .add(usize::try_from(index.0).unwrap())
        };

        PageRef {
            page,
            block: self,
            index,
            txid,
        }
    }

    pub fn allocate(&self, txid: TransactionId) -> UninitializedPageGuard<'_> {
        let index = self.latest_page.fetch_add(1, Ordering::Acquire);

        let index = PageIndex(index);
        self.allocate_housekeeping(index, txid);

        let page = unsafe {
            self.data
                .base_address()
                .cast()
                .add(usize::try_from(index.0).unwrap())
        };

        UninitializedPageGuard::new(self, page, index, txid)
    }

    fn allocate_housekeeping(&self, index: PageIndex, txid: TransactionId) -> NonNull<PageState> {
        debug!("[{}] allocating housekeeping for {index:?}", self.name);

        assert!(
            index.0 < Self::PAGE_COUNT as u64,
            "[{:?}] [{txid:?}] [{index:?}] index too high? latest_page: {:?}",
            self.name,
            self.latest_page.load(Ordering::Relaxed)
        );

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

        page_state
    }

    unsafe fn housekeeping_for(&self, index: PageIndex) -> Pin<&PageState> {
        assert!(index.0 < Self::PAGE_COUNT as u64);

        let address = unsafe {
            self.housekeeping
                .base_address()
                .cast()
                .add(usize::try_from(index.0).unwrap())
        };

        unsafe { Pin::new_unchecked(address.as_ref()) }
    }
}
