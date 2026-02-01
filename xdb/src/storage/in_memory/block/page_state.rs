use log::debug;

use crate::storage::{PageIndex, TransactionId};
use crate::sync::atomic::{AtomicU32, Ordering};
use std::fmt::Debug;
use std::time::Duration;
use std::{marker::PhantomPinned, pin::Pin};

use crate::platform::futex::Futex;

const fn mask32(start_bit: u32, end_bit: u32) -> u32 {
    assert!(end_bit <= start_bit);

    if start_bit == end_bit {
        return 1 << start_bit;
    }

    1 << start_bit | mask32(start_bit - 1, end_bit)
}

#[derive(Debug, Clone, Copy)]
#[allow(
    unused,
    reason = "the whole point of this struct is to just be for debug printing"
)]
pub struct DebugContext {
    transaction: TransactionId,
    page: PageIndex,
}

impl DebugContext {
    pub const fn new(transaction: TransactionId, page: PageIndex) -> Self {
        Self { transaction, page }
    }
}

#[derive(Debug)]
#[repr(transparent)]
pub struct PageState(Futex, PhantomPinned);

#[cfg(not(feature = "shuttle"))]
const _: () = assert!(size_of::<PageState>() == size_of::<u32>());

#[must_use]
#[derive(Clone, Copy)]
struct PageStateValue(u32);

impl Debug for PageStateValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageStateValue")
            .field("is_initialized", &self.is_initialized())
            .field("readers", &self.readers())
            .field("has_writer", &self.has_writer())
            .field("raw", &format!("{:#b}", self.0))
            .finish()
    }
}

impl PageStateValue {
    const MASK_IS_INITIALIZED: u32 = 1 << 31;
    const _UNUSED1: u32 = 1 << 30;
    const _UNUSED2: u32 = 1 << 29;
    const SHIFT_READER_COUNT: u32 = 12;
    const MASK_READER_COUNT: u32 = mask32(28, Self::SHIFT_READER_COUNT);
    const MASK_HAS_WRITER: u32 = 1 << 11;

    const fn is_initialized(self) -> bool {
        self.0 & Self::MASK_IS_INITIALIZED != 0
    }

    const fn readers(self) -> u32 {
        (self.0 & Self::MASK_READER_COUNT) >> Self::SHIFT_READER_COUNT
    }

    const fn with_readers(self, new_count: u32) -> Self {
        let shifted_new_count = new_count << Self::SHIFT_READER_COUNT;
        // TODO should we just treat this as any other lock contention?
        assert!(
            (shifted_new_count & !Self::MASK_READER_COUNT) == 0,
            "too many readers"
        );

        Self((self.0 & !Self::MASK_READER_COUNT) | shifted_new_count)
    }

    const fn has_writer(self) -> bool {
        self.0 & Self::MASK_HAS_WRITER != 0
    }

    const fn with_writer(self) -> Self {
        Self(self.0 | Self::MASK_HAS_WRITER)
    }

    const fn without_writer(self) -> Self {
        Self(self.0 & !Self::MASK_HAS_WRITER)
    }
}

impl PageState {
    pub const fn new() -> Self {
        Self(Futex::new(0), PhantomPinned)
    }

    const fn futex(self: Pin<&Self>) -> Pin<&Futex> {
        unsafe { Pin::new_unchecked(&self.get_ref().0) }
    }

    const fn atomic(self: Pin<&Self>) -> &AtomicU32 {
        self.futex().atomic()
    }

    pub fn mark_initialized(self: Pin<&Self>) {
        let previous_state = self
            .atomic()
            .fetch_or(PageStateValue::MASK_IS_INITIALIZED, Ordering::Release);

        assert!(!PageStateValue(previous_state).is_initialized());
    }

    pub fn is_initialized(self: Pin<&Self>) -> bool {
        PageStateValue(self.atomic().load(Ordering::Acquire)).is_initialized()
    }

    pub fn lock_write(self: Pin<&Self>, debug_context: DebugContext) {
        debug_assert!(self.is_initialized());

        match self
            .atomic()
            .fetch_update(Ordering::Acquire, Ordering::Acquire, |f| {
                let f = PageStateValue(f);

                if f.readers() > 0 {
                    return None;
                }

                if f.has_writer() {
                    return None;
                }

                Some(f.with_writer().0)
            }) {
            Ok(previous) => {
                let previous = PageStateValue(previous);

                debug!("[{debug_context:?}] locked for write, previous {previous:?}");
            }
            Err(previous) => {
                let previous = PageStateValue(previous);

                debug!("[{debug_context:?}] failed to lock for write, previous {previous:?}");

                // TODO drop the timeout once we're confident in deadlock detection
                match self.futex().wait(previous.0, Some(Duration::from_secs(5))) {
                    Ok(()) => {
                        self.lock_write(debug_context);
                    }
                    Err(error) => match error {
                        crate::platform::futex::FutexError::Race => todo!(),
                        // TODO we should stop this from happening at all!
                        crate::platform::futex::FutexError::Timeout => todo!(),
                        crate::platform::futex::FutexError::InconsistentState => todo!(),
                    },
                }
            }
        }
    }

    pub fn unlock_write(self: Pin<&Self>, debug_context: DebugContext) {
        debug_assert!(self.is_initialized());

        match self
            .atomic()
            .fetch_update(Ordering::Release, Ordering::Acquire, |x| {
                let x = PageStateValue(x);
                Some(x.without_writer().0)
            }) {
            Ok(previous) => {
                let previous = PageStateValue(previous);

                debug!("[{debug_context:?}] write unlocked from {previous:?}");

                self.wake();
            }
            Err(_) => todo!(),
        }
    }

    pub fn wake(self: Pin<&Self>) {
        // TODO probably should be more optimized as to choosing whether to wake up readers
        // or writers
        self.futex().wake(u32::MAX).unwrap();
    }

    pub fn lock_read(self: Pin<&Self>, debug_context: DebugContext) {
        debug_assert!(self.is_initialized());

        let result = self
            .atomic()
            .fetch_update(Ordering::Acquire, Ordering::Acquire, |x| {
                let x = PageStateValue(x);
                if x.has_writer() {
                    return None;
                }

                Some(x.with_readers(x.readers() + 1).0)
            });

        match result {
            Ok(previous) => {
                let previous = PageStateValue(previous);

                debug!("[{debug_context:?}] read locked from {previous:?}");
            }
            Err(previous) => {
                let previous = PageStateValue(previous);

                debug!("[{debug_context:?}] failed to lock for read from {previous:?}");

                // TODO drop the timeout once we're confident in deadlock detection
                // TODO this match around wait is repeated in a few places, clean it up
                match self.futex().wait(previous.0, Some(Duration::from_secs(5))) {
                    Ok(()) => {
                        self.lock_read(debug_context);
                    }
                    Err(error) => match error {
                        crate::platform::futex::FutexError::Race => todo!(),
                        // TODO we should stop this from happening at all!
                        crate::platform::futex::FutexError::Timeout => todo!(),
                        crate::platform::futex::FutexError::InconsistentState => todo!(),
                    },
                }
            }
        }
    }

    pub fn unlock_read(self: Pin<&Self>, debug_context: DebugContext) {
        debug_assert!(self.is_initialized());

        match self
            .atomic()
            .fetch_update(Ordering::Release, Ordering::Acquire, |x| {
                let x = PageStateValue(x);

                if x.has_writer() {
                    return None;
                }

                let reader_count = x.readers();

                if reader_count == 0 {
                    return None;
                }

                Some(x.with_readers(reader_count - 1).0)
            }) {
            Ok(previous) => {
                let previous = PageStateValue(previous);

                debug!("[{debug_context:?}] reader removed from {previous:?}");

                if previous.readers() == 1 {
                    self.wake();
                }
            }
            Err(previous) => {
                let previous = PageStateValue(previous);

                panic!(
                    "[{debug_context:?}] [{previous:?}] unlocking failed because of invalid state"
                );
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn mask_tests() {
        assert_eq!(mask32(7, 0), 0b1111_1111);
        assert_eq!(mask32(15, 8), 0b1111_1111_0000_0000);
    }
}
