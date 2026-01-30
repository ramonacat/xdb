use crate::sync::atomic::{AtomicU32, Ordering};
use std::{marker::PhantomPinned, pin::Pin, time::Duration};

use thiserror::Error;

use crate::platform::futex::{Futex, FutexError};

const fn mask32(start_bit: u32, end_bit: u32) -> u32 {
    assert!(end_bit <= start_bit);

    if start_bit == end_bit {
        return 1 << start_bit;
    }

    1 << start_bit | mask32(start_bit - 1, end_bit)
}

#[derive(Debug)]
#[repr(transparent)]
pub struct PageState(Futex, PhantomPinned);

#[cfg(not(feature = "shuttle"))]
const _: () = assert!(size_of::<PageState>() == size_of::<u32>());

#[derive(Debug, Error)]
pub enum LockError {
    #[error("would deadlock")]
    Deadlock,
}

impl PageState {
    const MASK_IS_INITIALIZED: u32 = 1 << 31;
    #[allow(unused)]
    const MASK_READERS_WAITING: u32 = 1 << 30;
    #[allow(unused)]
    const MASK_WRITERS_WAITING: u32 = 1 << 29;
    const SHIFT_READER_COUNT: u32 = 12;
    const MASK_READER_COUNT: u32 = mask32(28, Self::SHIFT_READER_COUNT);
    const MASK_HAS_WRITER: u32 = 1 << 11;

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
            .fetch_or(Self::MASK_IS_INITIALIZED, Ordering::Release);
        assert!(previous_state & Self::MASK_IS_INITIALIZED == 0);
    }

    pub fn is_initialized(self: Pin<&Self>) -> bool {
        self.atomic().load(Ordering::Acquire) & Self::MASK_IS_INITIALIZED > 0
    }

    pub fn lock_write(self: Pin<&Self>) -> Result<(), LockError> {
        debug_assert!(self.is_initialized());

        match self
            .atomic()
            .fetch_update(Ordering::Acquire, Ordering::Acquire, |f| {
                if ((f & Self::MASK_READER_COUNT) >> Self::SHIFT_READER_COUNT) > 0 {
                    return None;
                }

                if (f & Self::MASK_HAS_WRITER) > 0 {
                    return None;
                }

                Some(f | Self::MASK_HAS_WRITER)
            }) {
            Ok(_) => {}
            Err(old) => {
                self.wait(old)?;

                self.lock_write()?;
            }
        }

        Ok(())
    }

    pub fn unlock_write(self: Pin<&Self>) {
        debug_assert!(self.is_initialized());

        match self
            .atomic()
            .fetch_update(Ordering::Release, Ordering::Acquire, |x| {
                Some(x & !Self::MASK_HAS_WRITER)
            }) {
            Ok(_) => {
                self.futex().wake(1).unwrap();
            }
            Err(_) => todo!(),
        }
    }

    pub fn lock_read(self: Pin<&Self>) -> Result<(), LockError> {
        debug_assert!(self.is_initialized());

        let result = self
            .atomic()
            .fetch_update(Ordering::Acquire, Ordering::Acquire, |x| {
                if x & Self::MASK_HAS_WRITER > 0 {
                    return None;
                }

                let reader_count = (x & Self::MASK_READER_COUNT) >> Self::SHIFT_READER_COUNT;
                let new_reader_count = reader_count + 1;
                let shifted_new_reader_count = new_reader_count << Self::SHIFT_READER_COUNT;

                assert!(shifted_new_reader_count & !Self::MASK_READER_COUNT == 0);

                Some((x & !Self::MASK_READER_COUNT) | shifted_new_reader_count)
            });

        match result {
            Ok(_) => Ok(()),
            Err(old) => {
                self.wait(old)?;

                self.lock_read()
            }
        }
    }

    fn wait(self: Pin<&Self>, old: u32) -> Result<(), LockError> {
        // TODO 1s is a lot of time, do a deadlock detection instead
        match self.futex().wait(old, Some(Duration::from_secs(1))) {
            Ok(()) => Ok(()),
            Err(FutexError::Timeout) => Err(LockError::Deadlock),
            Err(FutexError::Race) => self.lock_read(),
            Err(FutexError::InconsistentState) => {
                unreachable!("futex was in an inconsistent state")
            }
        }
    }

    pub fn unlock_read(self: Pin<&Self>) {
        debug_assert!(self.is_initialized());

        match self
            .atomic()
            .fetch_update(Ordering::Release, Ordering::Acquire, |x| {
                assert!(x & Self::MASK_HAS_WRITER == 0);

                let reader_count = (x & Self::MASK_READER_COUNT) >> Self::SHIFT_READER_COUNT;
                assert!(reader_count > 0);
                let shifted_new_reader_count = (reader_count - 1) << Self::SHIFT_READER_COUNT;

                Some((x & !Self::MASK_READER_COUNT) | shifted_new_reader_count)
            }) {
            Ok(_) => {
                self.futex().wake(1).unwrap();
            }
            Err(_) => todo!(),
        }
    }

    pub fn lock_upgrade(self: Pin<&Self>) -> Result<(), LockError> {
        match self
            .atomic()
            .fetch_update(Ordering::Acquire, Ordering::Acquire, |x| {
                if (x & Self::MASK_READER_COUNT) >> Self::SHIFT_READER_COUNT != 1 {
                    return None;
                }

                assert!(x & Self::MASK_HAS_WRITER == 0);

                Some((x & !Self::MASK_READER_COUNT) | Self::MASK_HAS_WRITER)
            }) {
            Ok(_) => Ok(()),
            Err(old) => {
                self.wait(old)?;

                self.lock_upgrade()
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
