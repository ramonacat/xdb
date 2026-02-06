use crate::sync::atomic::{AtomicU32, Ordering};
use crate::thread;
use std::pin::Pin;
use std::time::Duration;

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

    pub fn wait(self: Pin<&Self>, value: u32, timeout: Option<Duration>) {
        thread::yield_now();

        loop {
            thread::yield_now();

            if self.value.load(Ordering::SeqCst) == value {
                continue;
            }

            return;
        }
    }

    #[allow(
        clippy::unused_self,
        clippy::unnecessary_wraps,
        clippy::missing_const_for_fn
    )]
    pub fn wake_one(self: Pin<&Self>) -> u64 {
        1
    }

    #[allow(
        clippy::unused_self,
        clippy::unnecessary_wraps,
        clippy::missing_const_for_fn
    )]
    pub fn wake_all(self: Pin<&Self>) -> u64 {
        1
    }

    pub const fn atomic(self: Pin<&Self>) -> &AtomicU32 {
        &self.get_ref().value
    }
}
