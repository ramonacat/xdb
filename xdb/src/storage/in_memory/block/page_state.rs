use std::sync::atomic::{AtomicU32, Ordering};

use libc::{ETIMEDOUT, FUTEX_WAIT, FUTEX_WAKE, SYS_futex, syscall, timespec};
use thiserror::Error;

use crate::platform::{errno, panic_on_errno};

const fn mask32(start_bit: u32, end_bit: u32) -> u32 {
    assert!(end_bit <= start_bit);

    if start_bit == end_bit {
        return 1 << start_bit;
    }

    1 << start_bit | mask32(start_bit - 1, end_bit)
}

#[derive(Debug)]
#[repr(transparent)]
pub struct PageState(AtomicU32);

const _: () = assert!(size_of::<PageState>() == size_of::<u32>());

#[derive(Debug, Error)]
pub enum LockError {
    #[error("would deadlock")]
    Deadlock,
}

// TODO move all the futex code into some neat abstraction that correctly handles errors, etc.
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
        Self(AtomicU32::new(0))
    }

    pub fn mark_initialized(&self) {
        let previous_state = self
            .0
            .fetch_or(Self::MASK_IS_INITIALIZED, Ordering::Release);
        assert!(previous_state & Self::MASK_IS_INITIALIZED == 0);
    }

    pub fn is_initialized(&self) -> bool {
        self.0.load(Ordering::Acquire) & Self::MASK_IS_INITIALIZED > 0
    }

    pub fn lock_write(&self) {
        assert!(self.is_initialized());

        match self
            .0
            .fetch_update(Ordering::Acquire, Ordering::Acquire, |f| {
                if f & Self::MASK_READER_COUNT >> Self::SHIFT_READER_COUNT > 0 {
                    return None;
                }

                if f & Self::MASK_HAS_WRITER > 0 {
                    return None;
                }

                Some(f | Self::MASK_HAS_WRITER)
            }) {
            Ok(_) => {
                if unsafe { syscall(SYS_futex, &raw const self.0, FUTEX_WAKE, 1u32) } == -1 {
                    panic_on_errno();
                }
            }
            Err(_) => todo!(),
        }
    }

    pub fn unlock_write(&self) {
        assert!(self.is_initialized());

        match self
            .0
            .fetch_update(Ordering::Release, Ordering::Acquire, |x| {
                Some(x & !Self::MASK_HAS_WRITER)
            }) {
            Ok(_) => {
                if unsafe { syscall(SYS_futex, &raw const self.0, FUTEX_WAKE, 1u32) } == -1 {
                    panic_on_errno();
                }
            }
            Err(_) => todo!(),
        }
    }

    pub fn lock_read(&self) -> Result<(), LockError> {
        assert!(self.is_initialized());

        let result = self
            .0
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
            Ok(_) => {}
            Err(old) => {
                // TODO 1s is a lot of time
                // TODO this might return EAGAIN if the value has changed before the call, handle that
                let timeout = timespec {
                    tv_sec: 1,
                    tv_nsec: 0,
                };

                if unsafe {
                    syscall(
                        SYS_futex,
                        &raw const self.0,
                        FUTEX_WAIT,
                        old,
                        &raw const timeout,
                    )
                } != 0
                {
                    if errno() == ETIMEDOUT {
                        // TODO real deadlock detection, instead of waiting ages for a timeout
                        return Err(LockError::Deadlock);
                    }
                    panic_on_errno();
                }

                self.lock_read()?;
            }
        }

        Ok(())
    }

    pub fn unlock_read(&self) {
        assert!(self.is_initialized());

        match self
            .0
            .fetch_update(Ordering::Release, Ordering::Acquire, |x| {
                assert!(x & Self::MASK_HAS_WRITER == 0);

                let reader_count = (x & Self::MASK_READER_COUNT) >> Self::SHIFT_READER_COUNT;
                assert!(reader_count > 0);
                let shifted_new_reader_count = (reader_count - 1) << Self::SHIFT_READER_COUNT;

                Some((x & !Self::MASK_READER_COUNT) | shifted_new_reader_count)
            }) {
            Ok(_) => {
                if unsafe { syscall(SYS_futex, &raw const self.0, FUTEX_WAKE, 1u32) } == -1 {
                    panic_on_errno();
                }
            }
            Err(_) => todo!(),
        }
    }

    pub fn lock_upgrade(&self) {
        self.0
            .fetch_update(Ordering::Acquire, Ordering::Acquire, |x| {
                // TODO we should wait on a futex here instead once we have multiple threads
                assert!((x & Self::MASK_READER_COUNT) >> Self::SHIFT_READER_COUNT == 1);
                assert!(x & Self::MASK_HAS_WRITER == 0);

                Some((x & !Self::MASK_READER_COUNT) | Self::MASK_HAS_WRITER)
            })
            .unwrap();
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
