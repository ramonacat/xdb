use crate::sync::atomic::{AtomicU32, Ordering};
use std::{pin::Pin, time::Duration};

use crate::platform::futex::FutexError;

#[allow(unused)]
#[derive(Debug)]
pub struct Futex {
    value: AtomicU32,
}

#[allow(unused)]
impl Futex {
    pub const fn new(value: u32) -> Self {
        Self {
            value: AtomicU32::new(value),
        }
    }

    pub fn wait(self: Pin<&Self>, value: u32, timeout: Option<Duration>) -> Result<(), FutexError> {
        loop {
            crate::thread::yield_now();

            if self.value.load(Ordering::SeqCst) == value {
                continue;
            }

            return Ok(());
        }
    }

    #[allow(clippy::unused_self, clippy::unnecessary_wraps)]
    pub fn wake(self: Pin<&Self>, count: u32) -> Result<u64, FutexError> {
        Ok(u64::from(count))
    }

    pub const fn atomic(self: Pin<&Self>) -> &AtomicU32 {
        &self.get_ref().value
    }
}
