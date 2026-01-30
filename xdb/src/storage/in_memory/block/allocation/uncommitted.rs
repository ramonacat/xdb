use std::ptr::NonNull;

use libc::{
    _SC_PAGE_SIZE, MAP_ANONYMOUS, MAP_FAILED, MAP_NORESERVE, MAP_PRIVATE, PROT_NONE, PROT_READ,
    PROT_WRITE, mmap, mprotect, munmap,
};

use crate::{
    Size, page::PAGE_SIZE, platform::panic_on_errno,
    storage::in_memory::block::allocation::Allocation,
};

#[derive(Debug)]
pub struct UncommittedAllocation {
    address: NonNull<u8>,
    size: Size,
}
unsafe impl Send for UncommittedAllocation {}
unsafe impl Sync for UncommittedAllocation {}

impl Drop for UncommittedAllocation {
    fn drop(&mut self) {
        unsafe { munmap(self.address.as_ptr().cast(), self.size.as_bytes()) };
    }
}

impl UncommittedAllocation {
    pub fn new(size: Size) -> Self {
        let size = if cfg!(miri) { Size::MiB(8) } else { size };
        assert!(unsafe {
            usize::try_from(libc::sysconf(_SC_PAGE_SIZE)).unwrap() == PAGE_SIZE.as_bytes()
        });

        let memory = unsafe {
            mmap(
                std::ptr::null_mut(),
                size.as_bytes(),
                if cfg!(miri) {
                    PROT_READ | PROT_WRITE
                } else {
                    PROT_NONE
                },
                // TODO support file backed mappings
                MAP_PRIVATE | MAP_ANONYMOUS | if cfg!(miri) { 0 } else { MAP_NORESERVE },
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
        assert!(address.align_offset(PAGE_SIZE.as_bytes()) == 0);
        assert!(
            address >= self.address
                && address < unsafe { self.address.byte_add(self.size.as_bytes()) }
        );

        if !cfg!(miri) {
            unsafe {
                let page = address;
                if mprotect(
                    page.cast().as_ptr(),
                    PAGE_SIZE.as_bytes(),
                    PROT_READ | PROT_WRITE,
                ) != 0
                {
                    panic_on_errno();
                }
            }
        }
    }

    fn base_address(&self) -> NonNull<u8> {
        self.address.cast()
    }
}
