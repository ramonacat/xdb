use crate::platform::futex::FutexError;
use crate::sync::atomic::AtomicU32;
use std::{ffi::c_void, marker::PhantomPinned, pin::Pin, ptr};

use libc::{EAGAIN, EFAULT, EINTR, EINVAL, ETIMEDOUT, FUTEX_WAIT, FUTEX_WAKE, SYS_futex, syscall};

use crate::platform::errno;

#[derive(Debug)]
#[repr(transparent)]
pub struct Futex(AtomicU32, PhantomPinned);

impl Futex {
    pub const fn new(value: u32) -> Self {
        Self(AtomicU32::new(value), PhantomPinned)
    }

    pub fn wait(self: Pin<&Self>, value: u32) -> Result<(), FutexError> {
        let result = unsafe {
            syscall(
                SYS_futex,
                &raw const self.0,
                FUTEX_WAIT,
                value,
                ptr::null::<c_void>(),
            )
        };

        if result == 0 {
            return Ok(());
        }

        match errno() {
            // EAGAIN means value was not the expectedd one, EINTR means we were interrupted by a
            // signal
            EAGAIN | EINTR => Err(FutexError::Race),

            ETIMEDOUT => unreachable!("futex timeout despite no timeout being set"),
            EFAULT => unreachable!("timespec address did not point to a valid address"),
            EINVAL => unreachable!("timespec nanoseconds were over 1s"),
            e => unreachable!("unexpected error: {e}"),
        }
    }

    pub fn wake(self: Pin<&Self>, count: u32) -> u64 {
        let callers_woken_up = unsafe { syscall(SYS_futex, &raw const self.0, FUTEX_WAKE, count) };
        if callers_woken_up == -1 {
            match errno() {
                EINVAL => unreachable!("inconsistent futex state"),
                e => unreachable!("unexpected error: {e}"),
            }
        }

        u64::try_from(callers_woken_up).unwrap()
    }

    pub const fn atomic(self: Pin<&Self>) -> &AtomicU32 {
        &self.get_ref().0
    }
}
