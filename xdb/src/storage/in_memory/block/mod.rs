mod page_state;

use crate::platform::allocation::uncommitted::UncommittedAllocation;
use crate::{platform::allocation::Allocation, storage::in_memory::block::page_state::LockError};
use std::{
    fmt::Debug,
    mem::{ManuallyDrop, MaybeUninit},
    ops::{Deref, DerefMut},
    pin::Pin,
    ptr::NonNull,
};

use crate::{
    Size,
    page::{PAGE_SIZE, Page},
    storage::{PageIndex, StorageError, in_memory::block::page_state::PageState},
    sync::atomic::{AtomicU64, Ordering},
};

#[derive(Debug)]
pub struct PageRef<'block> {
    page: NonNull<Page>,
    block: &'block Block,
    index: PageIndex,
}

#[derive(Debug)]
pub struct PageGuard<'block> {
    page: NonNull<Page>,
    block: &'block Block,
    index: PageIndex,
}

unsafe impl Send for PageGuard<'_> {}

const fn handle_lock_error(error: &LockError, index: PageIndex) -> StorageError {
    match error {
        LockError::Deadlock => StorageError::Deadlock(index),
    }
}

impl<'block> PageGuard<'block> {
    unsafe fn new(
        page: NonNull<Page>,
        block: &'block Block,
        index: PageIndex,
    ) -> Result<Self, StorageError> {
        let housekeeping = unsafe { block.housekeeping_for(index) };

        match housekeeping.lock_read() {
            Ok(()) => Ok(Self { page, block, index }),
            Err(error) => Err(handle_lock_error(&error, index)),
        }
    }

    pub fn upgrade(self) -> Result<PageGuardMut<'block>, StorageError> {
        let Self { page, block, index } = self;

        match unsafe { block.housekeeping_for(index) }.lock_upgrade() {
            Ok(()) => {}
            Err(e) => return Err(handle_lock_error(&e, index)),
        }

        // Do not drop self, as this would make us unlock the read lock, which is incorrect, as it
        // is now a write lock
        let _ = ManuallyDrop::new(self);

        Ok(PageGuardMut { page, block, index })
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

impl Drop for PageGuard<'_> {
    fn drop(&mut self) {
        unsafe { self.block.housekeeping_for(self.index) }.unlock_read();
    }
}

#[derive(Debug)]
pub struct PageGuardMut<'block> {
    page: NonNull<Page>,
    block: &'block Block,
    index: PageIndex,
}

unsafe impl Send for PageGuardMut<'_> {}

impl<'block> PageGuardMut<'block> {
    unsafe fn new(
        page: NonNull<Page>,
        block: &'block Block,
        index: PageIndex,
    ) -> Result<Self, StorageError> {
        let housekeeping = unsafe { block.housekeeping_for(index) };
        housekeeping.lock_write().map_err(|e| match e {
            // TODO this mapping is repeated in a few places, clean it up
            LockError::Deadlock => StorageError::Deadlock(index),
        })?;

        Ok(Self { page, block, index })
    }
}

impl AsMut<Page> for PageGuardMut<'_> {
    fn as_mut(&mut self) -> &mut Page {
        unsafe { self.page.as_mut() }
    }
}

impl Deref for PageGuardMut<'_> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        unsafe { self.page.as_ref() }
    }
}

impl DerefMut for PageGuardMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.page.as_mut() }
    }
}

impl Drop for PageGuardMut<'_> {
    fn drop(&mut self) {
        unsafe { self.block.housekeeping_for(self.index) }.unlock_write();
    }
}

impl<'block> PageRef<'block> {
    pub const fn index(&self) -> PageIndex {
        self.index
    }

    pub fn get(&self) -> Result<PageGuard<'block>, StorageError> {
        unsafe { PageGuard::new(self.page, self.block, self.index) }
    }

    pub fn get_mut(&self) -> Result<PageGuardMut<'block>, StorageError> {
        unsafe { PageGuardMut::new(self.page, self.block, self.index) }
    }
}

pub struct UninitializedPageGuard<'block> {
    block: &'block Block,
    page: NonNull<MaybeUninit<Page>>,
    index: PageIndex,
}

impl<'block> UninitializedPageGuard<'block> {
    const fn new(block: &'block Block, page: NonNull<MaybeUninit<Page>>, index: PageIndex) -> Self {
        Self { block, page, index }
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
        }
    }
}

#[derive(Debug)]
pub struct Block {
    housekeeping: Box<dyn Allocation>,
    data: Box<dyn Allocation>,
    latest_page: AtomicU64,
}

impl Block {
    const SIZE: Size = Size::GiB(4);
    const PAGE_COUNT: usize = Self::SIZE.divide(PAGE_SIZE);
    const HOUSEKEEPING_BLOCK_SIZE: Size = Size::of::<PageState>().multiply(Self::PAGE_COUNT);

    pub fn new() -> Self {
        let housekeeping: Box<dyn Allocation> =
            Box::new(UncommittedAllocation::new(Self::HOUSEKEEPING_BLOCK_SIZE));
        let data: Box<dyn Allocation> = Box::new(UncommittedAllocation::new(Self::SIZE));

        Self {
            housekeeping,
            data,
            latest_page: AtomicU64::new(0),
        }
    }

    pub fn get(&self, index: PageIndex) -> PageRef<'_> {
        assert!(index.0 < self.latest_page.load(Ordering::Acquire));

        let housekeeping = unsafe { self.housekeeping_for(index) };

        assert!(housekeeping.is_initialized());

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
        }
    }

    pub fn allocate(&self) -> UninitializedPageGuard<'_> {
        let index = self.latest_page.fetch_add(1, Ordering::Acquire);

        let index = PageIndex(index);
        self.allocate_housekeeping(index);

        let page = unsafe {
            self.data
                .base_address()
                .cast()
                .add(usize::try_from(index.0).unwrap())
        };

        UninitializedPageGuard::new(self, page, index)
    }

    fn allocate_housekeeping(&self, index: PageIndex) -> NonNull<PageState> {
        assert!(index.0 < Self::PAGE_COUNT as u64);

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
