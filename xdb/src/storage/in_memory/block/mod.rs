// TODO separate the miri and non-miri code into modules

mod static_allocation;
mod uncommitted_allocation;

use std::{
    fmt::Debug,
    mem::{ManuallyDrop, MaybeUninit},
    ops::{Deref, DerefMut},
    ptr::NonNull,
    sync::atomic::{AtomicU64, Ordering},
};

use crate::{
    page::{PAGE_SIZE, Page},
    storage::{
        PageIndex,
        in_memory::block::{
            static_allocation::StaticAllocation, uncommitted_allocation::UncommittedAllocation,
        },
    },
};

trait Allocation: Debug + Send {
    fn commit_page(&self, address: NonNull<u8>);
    fn base_address(&self) -> NonNull<u8>;
}

#[derive(Debug)]
#[repr(transparent)]
// TODO can we fit everything in u32 or even u16?
struct PageState(AtomicU64);

const fn mask(start_bit: u64, end_bit: u64) -> u64 {
    assert!(end_bit <= start_bit);

    if start_bit == end_bit {
        return 1 << start_bit;
    }

    1 << start_bit | mask(start_bit - 1, end_bit)
}

impl PageState {
    const MASK_IS_INITIALIZED: u64 = 1 << 63;
    #[allow(unused)]
    const MASK_READERS_WAITING: u64 = 1 << 62;
    #[allow(unused)]
    const MASK_WRITERS_WAITING: u64 = 1 << 61;
    const SHIFT_READER_COUNT: u64 = 44;
    const MASK_READER_COUNT: u64 = mask(60, Self::SHIFT_READER_COUNT);
    const MASK_HAS_WRITER: u64 = 1 << 43;

    const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    fn mark_initialized(&self) {
        let previous_state = self
            .0
            .fetch_or(Self::MASK_IS_INITIALIZED, Ordering::Release);
        assert!(previous_state & Self::MASK_IS_INITIALIZED == 0);
    }

    fn is_initialized(&self) -> bool {
        self.0.load(Ordering::Acquire) & Self::MASK_IS_INITIALIZED > 0
    }

    fn lock_write(&self) {
        assert!(self.is_initialized());

        self.0
            .fetch_update(Ordering::Acquire, Ordering::Relaxed, |f| {
                if f & Self::MASK_READER_COUNT >> Self::SHIFT_READER_COUNT > 0 {
                    return None;
                }

                if f & Self::MASK_HAS_WRITER > 0 {
                    return None;
                }

                Some(f | Self::MASK_HAS_WRITER)
            })
            .expect("cannot lock for write, already locked");
    }

    fn unlock_write(&self) {
        assert!(self.is_initialized());

        self.0
            .fetch_update(Ordering::Release, Ordering::Relaxed, |x| {
                Some(x & !Self::MASK_HAS_WRITER)
            })
            .unwrap();
    }

    fn lock_read(&self) {
        assert!(self.is_initialized());

        self.0
            .fetch_update(Ordering::Acquire, Ordering::Relaxed, |x| {
                if x & Self::MASK_HAS_WRITER > 0 {
                    return None;
                }

                let reader_count = (x & Self::MASK_READER_COUNT) >> Self::SHIFT_READER_COUNT;
                let new_reader_count = reader_count + 1;
                let shifted_new_reader_count = new_reader_count << Self::SHIFT_READER_COUNT;

                assert!(shifted_new_reader_count & !Self::MASK_READER_COUNT == 0);

                Some((x & !Self::MASK_READER_COUNT) | shifted_new_reader_count)
            })
            .expect("cannot block for read, as there already is a writer");
    }

    fn unlock_read(&self) {
        assert!(self.is_initialized());

        self.0
            .fetch_update(Ordering::Release, Ordering::Relaxed, |x| {
                assert!(x & Self::MASK_HAS_WRITER == 0);

                let reader_count = (x & Self::MASK_READER_COUNT) >> Self::SHIFT_READER_COUNT;
                assert!(reader_count > 0);
                let shifted_new_reader_count = (reader_count - 1) << Self::SHIFT_READER_COUNT;

                Some((x & !Self::MASK_READER_COUNT) | shifted_new_reader_count)
            })
            .unwrap();
    }

    fn lock_upgrade(&self) {
        self.0
            .fetch_update(Ordering::Acquire, Ordering::Acquire, |x| {
                // TODO we should wait on a futex here instead once we have multiple threads
                assert!((x & Self::MASK_READER_COUNT) >> Self::SHIFT_READER_COUNT == 1);
                assert!(x & Self::MASK_HAS_WRITER == 0);

                Some((x & !Self::MASK_READER_COUNT) | Self::MASK_HAS_WRITER)
            })
            .unwrap();
    }
}

const _: () = assert!(size_of::<PageState>() == size_of::<u64>());

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

impl<'block> PageGuard<'block> {
    unsafe fn new(page: NonNull<Page>, block: &'block Block, index: PageIndex) -> Self {
        let housekeeping = unsafe { block.housekeeping_for(index).as_ref() };
        housekeeping.lock_read();

        Self { page, block, index }
    }

    pub fn upgrade(self) -> PageGuardMut<'block> {
        let Self { page, block, index } = self;

        let housekeeping = unsafe { block.housekeeping_for(index).as_ref() };
        housekeeping.lock_upgrade();

        // Do not drop self, as this would make us unlock the read lock, which is incorrect, as it
        // is now a write lock
        let _ = ManuallyDrop::new(self);

        PageGuardMut { page, block, index }
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
        unsafe { self.block.housekeeping_for(self.index).as_ref() }.unlock_read();
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
    unsafe fn new(page: NonNull<Page>, block: &'block Block, index: PageIndex) -> Self {
        let housekeeping = unsafe { block.housekeeping_for(index).as_ref() };
        housekeeping.lock_write();

        Self { page, block, index }
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
        unsafe { self.block.housekeeping_for(self.index).as_ref() }.unlock_write();
    }
}

impl<'block> PageRef<'block> {
    pub const fn index(&self) -> PageIndex {
        self.index
    }

    pub fn get(&self) -> PageGuard<'block> {
        unsafe { PageGuard::new(self.page, self.block, self.index) }
    }

    pub fn get_mut(&self) -> PageGuardMut<'block> {
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
        unsafe { housekeeping.as_ref() }.mark_initialized();

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

unsafe impl Sync for Block {}
unsafe impl Send for Block {}

impl Block {
    // TODO make this Size::GiB(4).to_bytes() or something
    const SIZE: usize = 4 * 1024 * 1024 * 1024;
    const PAGE_COUNT: usize = Self::SIZE / PAGE_SIZE;
    const HOUSEKEEPING_BLOCK_SIZE: usize = Self::PAGE_COUNT * size_of::<PageState>();

    pub fn new() -> Self {
        let housekeeping: Box<dyn Allocation> = if cfg!(miri) {
            Box::new(StaticAllocation::new())
        } else {
            Box::new(UncommittedAllocation::new(Self::HOUSEKEEPING_BLOCK_SIZE))
        };
        let data: Box<dyn Allocation> = if cfg!(miri) {
            Box::new(StaticAllocation::new())
        } else {
            Box::new(UncommittedAllocation::new(Self::SIZE))
        };

        Self {
            housekeeping,
            data,
            latest_page: AtomicU64::new(0),
        }
    }

    pub fn get(&self, index: PageIndex) -> PageRef<'_> {
        assert!(index.0 < self.latest_page.load(Ordering::Acquire));

        let housekeeping = unsafe { self.housekeeping_for(index) };

        assert!(unsafe { housekeeping.as_ref() }.is_initialized());

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
                .cast::<[u8; PAGE_SIZE]>()
                .add(usize::try_from(index.0).unwrap() / (PAGE_SIZE / size_of::<PageState>()))
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

    unsafe fn housekeeping_for(&self, index: PageIndex) -> NonNull<PageState> {
        assert!(index.0 < Self::PAGE_COUNT as u64);

        unsafe {
            self.housekeeping
                .base_address()
                .cast()
                .add(usize::try_from(index.0).unwrap())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn mask_tests() {
        assert_eq!(mask(7, 0), 0b1111_1111);
        assert_eq!(mask(15, 8), 0b1111_1111_0000_0000);
    }
}
