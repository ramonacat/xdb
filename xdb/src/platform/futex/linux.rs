use crate::sync::atomic::AtomicU32;
use std::{marker::PhantomPinned, pin::Pin, ptr, time::Duration};

use libc::{
    EAGAIN, EFAULT, EINTR, EINVAL, ETIMEDOUT, FUTEX_WAIT, FUTEX_WAKE, SYS_futex, syscall, timespec,
};

use crate::platform::errno;

#[derive(Debug)]
#[repr(transparent)]
pub struct Futex(AtomicU32, PhantomPinned);

// TODO we should pass FUTEX_PRIVATE to all the futex calls
impl Futex {
    pub const fn new(value: u32) -> Self {
        Self(AtomicU32::new(value), PhantomPinned)
    }

    pub fn wait(self: Pin<&Self>, value: u32, timeout: Option<Duration>) {
        let timeout_spec = timeout.map(|x| timespec {
            tv_sec: i64::try_from(x.as_secs()).unwrap(),
            tv_nsec: i64::from(x.subsec_nanos()),
        });

        let result = unsafe {
            syscall(
                SYS_futex,
                &raw const self.0,
                FUTEX_WAIT,
                value,
                timeout_spec.as_ref().map_or_else(ptr::null, ptr::from_ref),
            )
        };

        if result == 0 {
            return;
        }

        match errno() {
            // EAGAIN means value was not the expectedd one, EINTR means we were interrupted by a
            // signal
            // TODO do we want to/need to let the caller know if it was a timeout?
            EAGAIN | EINTR | ETIMEDOUT => {}

            EFAULT => unreachable!("timespec address did not point to a valid address"),
            EINVAL => unreachable!("timespec nanoseconds were over 1s"),
            e => unreachable!("unexpected error: {e}"),
        }
    }

    pub fn wake_one(self: Pin<&Self>) -> bool {
        self.wake(1) > 0
    }

    pub fn wake_all(self: Pin<&Self>) -> u64 {
        self.wake(u32::MAX)
    }

    fn wake(self: Pin<&Self>, count: u32) -> u64 {
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
