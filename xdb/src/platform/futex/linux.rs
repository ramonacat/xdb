use crate::platform::futex::FutexError;
use crate::sync::atomic::AtomicU32;
use std::{marker::PhantomPinned, pin::Pin, ptr, time::Duration};

use libc::{
    EAGAIN, EFAULT, EINTR, EINVAL, ETIMEDOUT, FUTEX_WAIT, FUTEX_WAKE, SYS_futex, syscall, timespec,
};

use crate::platform::errno;

#[derive(Debug)]
#[repr(transparent)]
pub struct Futex(AtomicU32, PhantomPinned);

#[allow(unused)] // TODO remvove if really unused
impl Futex {
    pub const fn new(value: u32) -> Self {
        Self(AtomicU32::new(value), PhantomPinned)
    }

    pub fn wait(self: Pin<&Self>, value: u32, timeout: Option<Duration>) -> Result<(), FutexError> {
        let timespec = timeout.map(|x| timespec {
            tv_sec: x.as_secs().cast_signed(),
            tv_nsec: i64::from(x.subsec_nanos()),
        });

        let result = unsafe {
            syscall(
                SYS_futex,
                &raw const self.0,
                FUTEX_WAIT,
                value,
                timespec.as_ref().map_or(ptr::null(), ptr::from_ref),
            )
        };

        if result == 0 {
            return Ok(());
        }

        match errno() {
            // EAGAIN means value was not the expectedd one, EINTR means we were interrupted by a
            // signal
            EAGAIN | EINTR => Err(FutexError::Race),

            ETIMEDOUT => Err(FutexError::Timeout),
            EFAULT => unreachable!("timespec address did not point to a valid address"),
            EINVAL => unreachable!("timespec nanoseconds were over 1s"),
            e => unreachable!("unexpected error: {e}"),
        }
    }

    pub fn wake(self: Pin<&Self>, count: u32) -> Result<u64, FutexError> {
        let callers_woken_up = unsafe { syscall(SYS_futex, &raw const self.0, FUTEX_WAKE, count) };
        if callers_woken_up == -1 {
            match errno() {
                // TODO do we really really want to expose this?
                EINVAL => return Err(FutexError::InconsistentState),
                e => unreachable!("unexpected error: {e}"),
            }
        }

        Ok(u64::try_from(callers_woken_up).unwrap())
    }

    pub const fn atomic(self: Pin<&Self>) -> &AtomicU32 {
        &self.get_ref().0
    }
}
