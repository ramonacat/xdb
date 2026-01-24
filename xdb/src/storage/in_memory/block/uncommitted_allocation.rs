use std::{ffi::CStr, ptr::NonNull};

use libc::{
    __errno_location, _SC_PAGE_SIZE, MAP_ANONYMOUS, MAP_FAILED, MAP_NORESERVE, MAP_PRIVATE,
    PROT_NONE, PROT_READ, PROT_WRITE, mmap, mprotect, munmap, strerror,
};

use crate::{page::PAGE_SIZE, storage::in_memory::block::Allocation};

#[derive(Debug)]
pub(super) struct UncommittedAllocation {
    address: NonNull<u8>,
    size: usize,
}
unsafe impl Send for UncommittedAllocation {}

impl Drop for UncommittedAllocation {
    fn drop(&mut self) {
        unsafe { munmap(self.address.as_ptr().cast(), self.size) };
    }
}

impl UncommittedAllocation {
    pub fn new(size: usize) -> Self {
        assert!(unsafe { usize::try_from(libc::sysconf(_SC_PAGE_SIZE)).unwrap() == PAGE_SIZE });

        let memory = unsafe {
            mmap(
                std::ptr::null_mut(),
                size,
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

        Self {
            address: NonNull::new(memory).unwrap().cast(),
            size,
        }
    }
}

impl Allocation for UncommittedAllocation {
    fn commit_page(&self, address: NonNull<u8>) {
        assert!(address.align_offset(PAGE_SIZE) == 0);
        assert!(address >= self.address && address < unsafe { self.address.byte_add(self.size) });

        unsafe {
            let page = address;
            if mprotect(page.cast().as_ptr(), PAGE_SIZE, PROT_READ | PROT_WRITE) != 0 {
                panic_on_errno();
            }
        }
    }

    fn base_address(&self) -> NonNull<u8> {
        self.address.cast()
    }
}

fn panic_on_errno() -> ! {
    let errno = unsafe { *__errno_location() };

    panic!("failed to mmap memory: {}", unsafe {
        CStr::from_ptr(strerror(errno)).to_string_lossy()
    });
}
