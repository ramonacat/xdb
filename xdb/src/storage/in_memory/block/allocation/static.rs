use std::ptr::NonNull;

use crate::{page::PAGE_SIZE, storage::in_memory::block::allocation::Allocation};

#[repr(C, align(4096))]
struct Memory([u8; 128 * 1024 * 1024]);
const _: () = assert!(align_of::<Memory>() == PAGE_SIZE.as_bytes());

#[derive(Debug)]
pub struct StaticAllocation {
    data: NonNull<u8>,
}
unsafe impl Send for StaticAllocation {}
unsafe impl Sync for StaticAllocation {}

impl Drop for StaticAllocation {
    fn drop(&mut self) {
        drop(unsafe { Box::<Memory>::from_raw(self.data.as_ptr().cast()) });
    }
}

impl StaticAllocation {
    #[allow(clippy::large_stack_arrays, clippy::large_stack_frames)] // TODO use vec or something here to avoid allocating on
    // the stack
    pub fn new() -> Self {
        Self {
            data: NonNull::new(Box::into_raw(Box::new(Memory([0; _]))).cast()).unwrap(),
        }
    }
}

impl Allocation for StaticAllocation {
    fn commit_page(&self, _address: NonNull<u8>) {}

    fn base_address(&self) -> NonNull<u8> {
        self.data
    }
}
