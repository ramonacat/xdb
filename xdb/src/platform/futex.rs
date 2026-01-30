use std::{marker::PhantomPinned, pin::Pin, ptr, sync::atomic::AtomicU32, time::Duration};

use libc::{EAGAIN, ETIMEDOUT, FUTEX_WAIT, FUTEX_WAKE, SYS_futex, syscall, timespec};
use thiserror::Error;

use crate::platform::{errno, panic_on_errno};

#[derive(Debug, Error)]
pub enum FutexError {
    #[error("the value has changed while the wait was attempted")]
    Race,
    #[error("timed out")]
    Timeout,
}

#[derive(Debug)]
#[repr(transparent)]
pub struct Futex(AtomicU32, PhantomPinned);

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
            EAGAIN => Err(FutexError::Race),
            ETIMEDOUT => Err(FutexError::Timeout),
            // TODO handle all the possible errors here
            _ => panic_on_errno(),
        }
    }

    #[allow(clippy::unnecessary_wraps)] // TODO handle errors for real
    pub fn wake(self: Pin<&Self>, count: u32) -> Result<(), FutexError> {
        if unsafe { syscall(SYS_futex, &raw const self.0, FUTEX_WAKE, count) } == -1 {
            panic_on_errno();
        }

        Ok(())
    }

    pub const fn atomic(self: Pin<&Self>) -> &AtomicU32 {
        &self.get_ref().0
    }
}
