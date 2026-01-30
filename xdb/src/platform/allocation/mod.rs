use std::fmt::Debug;
use std::ptr::NonNull;

pub mod uncommitted;

pub trait Allocation: Debug + Send + Sync {
    fn commit_page(&self, address: NonNull<u8>);
    fn base_address(&self) -> NonNull<u8>;
}
