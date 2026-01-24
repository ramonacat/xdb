// TODO separate the miri and non-miri code into modules

use std::{
    mem::MaybeUninit,
    ops::{Deref, DerefMut},
    ptr::NonNull,
    sync::atomic::{AtomicU64, Ordering},
};

#[cfg(not(miri))]
use std::ffi::CStr;

#[cfg(not(miri))]
use libc::{
    __errno_location, _SC_PAGE_SIZE, MAP_ANONYMOUS, MAP_FAILED, MAP_NORESERVE, MAP_PRIVATE,
    PROT_NONE, PROT_READ, PROT_WRITE, mmap, mprotect, munmap, strerror,
};

use crate::{
    page::{PAGE_SIZE, Page},
    storage::PageIndex,
};

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

#[allow(unused)]
impl PageState {
    const MASK_IS_INITIALIZED: u64 = 1 << 63;
    const MASK_READERS_WAITING: u64 = 1 << 62;
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

                Some(f | Self::MASK_HAS_WRITER)
            })
            .expect("cannot lock for write, as readers exist");
    }

    fn unlock_write(&self) {
        assert!(self.is_initialized());

        self.0
            .fetch_update(Ordering::Release, Ordering::Relaxed, |x| {
                Some(x & !Self::MASK_HAS_WRITER)
            });
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
            });
    }
}

const _: () = assert!(size_of::<PageState>() == size_of::<u64>());

pub struct PageRef<'block> {
    page: NonNull<Page>,
    block: &'block Block,
    index: PageIndex,
}

// TODO we need a way to upgrade a read-only guard to one that can be written to
pub struct PageGuard<'block> {
    page: NonNull<Page>,
    block: &'block Block,
    index: PageIndex,
}

impl<'block> PageGuard<'block> {
    unsafe fn new(page: NonNull<Page>, block: &'block Block, index: PageIndex) -> Self {
        let housekeeping = unsafe { block.housekeeping_for(index).as_ref() };
        housekeeping.lock_read();

        Self { page, block, index }
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

pub struct PageGuardMut<'block> {
    page: NonNull<Page>,
    block: &'block Block,
    index: PageIndex,
}

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
    housekeeping: NonNull<MaybeUninit<PageState>>,
    data: NonNull<MaybeUninit<Page>>,
    latest_page: AtomicU64,
}

#[cfg(miri)]
#[repr(C, align(4096))]
struct Memory([u8; Block::SIZE]);

#[cfg(miri)]
const _: () = assert!(align_of::<Memory>() == PAGE_SIZE);

impl Block {
    #[cfg(not(miri))]
    const SIZE: usize = 4 * 1024 * 1024 * 1024;
    #[cfg(miri)]
    const SIZE: usize = 64 * 1024 * 1024;
    const PAGE_COUNT: usize = Self::SIZE / PAGE_SIZE;
    const HOUSEKEEPING_BLOCK_SIZE: usize = Self::PAGE_COUNT * size_of::<PageState>();

    pub fn new() -> Self {
        #[cfg(not(miri))]
        let memory = {
            assert!(unsafe { usize::try_from(libc::sysconf(_SC_PAGE_SIZE)).unwrap() == PAGE_SIZE });

            let memory = unsafe {
                mmap(
                    std::ptr::null_mut(),
                    Self::SIZE, // 4GiB TODO create a type that can store a Size and has
                    // constructors for different prefixes
                    PROT_NONE,
                    // TODO support file backed mappings
                    MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE,
                    -1,
                    0,
                )
            };

            if memory == MAP_FAILED {
                panic_on_errno();
            }

            NonNull::new(memory).unwrap()
        };

        #[cfg(miri)]
        let memory = {
            let memory = Box::leak(Box::new(Memory([0; _])));

            NonNull::from_mut(memory)
        };

        Self {
            housekeeping: memory.cast(),
            data: unsafe { memory.byte_add(Self::HOUSEKEEPING_BLOCK_SIZE).cast() },
            latest_page: AtomicU64::new(0),
        }
    }

    pub fn get(&self, index: PageIndex) -> PageRef<'_> {
        assert!(index.0 < self.latest_page.load(Ordering::Acquire));

        let housekeeping = unsafe { self.housekeeping_for(index) };

        assert!(unsafe { housekeeping.as_ref() }.is_initialized());

        let page = unsafe { self.data.add(usize::try_from(index.0).unwrap()).cast() };

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

        let page = unsafe { (self.data).add(usize::try_from(index.0).unwrap()) };

        UninitializedPageGuard::new(self, page, index)
    }

    fn allocate_housekeeping(&self, index: PageIndex) -> *const PageState {
        assert!(index.0 < Self::PAGE_COUNT as u64);

        #[cfg(not(miri))]
        unsafe {
            let houskeeping_page = (self.housekeeping.cast::<[u8; PAGE_SIZE]>())
                .add(usize::try_from(index.0).unwrap() / (PAGE_SIZE / size_of::<PageState>()));
            // TODO this way we do the mprotect multiple times, it doesn't really matter, but might
            // make sense to only do that when index%PAGE_SIZE == 0??? (though then we have to
            // always assume that we will be getting contiguous indices, which might make other
            // things harder...)
            if mprotect(
                houskeeping_page.cast().as_ptr(),
                PAGE_SIZE,
                PROT_READ | PROT_WRITE,
            ) != 0
            {
                panic_on_errno();
            }

            let page = (self.data).add(index.0.try_into().unwrap());
            if mprotect(page.cast().as_ptr(), PAGE_SIZE, PROT_READ | PROT_WRITE) != 0 {
                panic_on_errno();
            }
        }

        let mut page_state = unsafe { self.housekeeping.add(index.0.try_into().unwrap()) };
        unsafe { &raw const *(page_state.as_mut().write(PageState::new())) }
    }

    unsafe fn housekeeping_for(&self, index: PageIndex) -> NonNull<PageState> {
        assert!(index.0 < Self::PAGE_COUNT as u64);

        unsafe { (self.housekeeping.cast()).add(usize::try_from(index.0).unwrap()) }
    }
}

#[cfg(not(miri))]
fn panic_on_errno() -> ! {
    let errno = unsafe { *__errno_location() };

    panic!("failed to mmap memory: {}", unsafe {
        CStr::from_ptr(strerror(errno)).to_string_lossy()
    });
}

impl Drop for Block {
    fn drop(&mut self) {
        #[cfg(not(miri))]
        unsafe {
            munmap(self.housekeeping.cast().as_ptr(), Self::SIZE)
        };
        #[cfg(miri)]
        unsafe {
            drop(Box::<Memory>::from_raw(self.housekeeping.cast().as_ptr()))
        };
    }
}

#[cfg(test)]
mod test {
    use crate::storage::in_memory::mmaped_block::mask;

    #[test]
    fn mask_tests() {
        assert_eq!(mask(7, 0), 0b1111_1111);
        assert_eq!(mask(15, 8), 0b1111_1111_0000_0000);
    }
}
