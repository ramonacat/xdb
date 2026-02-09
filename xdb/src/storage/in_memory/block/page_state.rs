use tracing::{error, warn};

use crate::storage::in_memory::block::LockError;
use crate::sync::atomic::{AtomicU32, Ordering};
use std::fmt::Debug;
use std::time::Duration;
use std::{marker::PhantomPinned, pin::Pin};

use crate::platform::futex::Futex;

#[derive(Debug)]
#[repr(transparent)]
// TODO log warnings when a lock is held for a long time!
pub struct PageState(Futex, PhantomPinned);

#[cfg(not(feature = "shuttle"))]
const _: () = assert!(size_of::<PageState>() == size_of::<u32>());

#[must_use]
#[derive(Clone, Copy)]
pub struct PageStateValue(u32);

impl Debug for PageStateValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageStateValue")
            .field("is_initialized", &self.is_initialized())
            .field("is_locked", &self.has_writer())
            .field("readers", &self.readers())
            .field("raw", &format!("{:#b}", self.0))
            .finish()
    }
}

impl PageStateValue {
    const MASK_IS_INITIALIZED: u32 = 1 << 31;
    const MASK_IS_LOCKED: u32 = 1 << 30;

    const MASK_READERS: u32 = 0x0000_FFFF;

    const fn is_initialized(self) -> bool {
        (self.0 & Self::MASK_IS_INITIALIZED) != 0
    }

    const fn mark_uninitialized(self) -> Self {
        Self(self.0 & !Self::MASK_IS_INITIALIZED)
    }

    const fn has_writer(self) -> bool {
        (self.0 & Self::MASK_IS_LOCKED) != 0
    }

    const fn lock_write(self) -> Self {
        Self(self.0 | Self::MASK_IS_LOCKED)
    }

    const fn unlock_write(self) -> Self {
        Self(self.0 & !Self::MASK_IS_LOCKED)
    }

    fn readers(self) -> u16 {
        u16::try_from(self.0 & Self::MASK_READERS).unwrap()
    }

    fn with_readers(self, readers: u16) -> Self {
        assert!(!self.has_writer());

        Self((self.0 & !Self::MASK_READERS) | u32::from(readers))
    }
}

// TODO there's a lot of copy-paste here, can we simplify?
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

        debug_assert!(!PageStateValue(previous_state).is_initialized());
    }

    pub fn mark_uninitialized(self: Pin<&Self>) {
        match self
            .atomic()
            .fetch_update(Ordering::Release, Ordering::Acquire, |x| {
                let x = PageStateValue(x);

                assert!(x.has_writer());
                assert!(x.readers() == 0);

                Some(x.mark_uninitialized().0)
            }) {
            Ok(_) => {}
            Err(_) => todo!(),
        }
    }

    pub fn is_initialized(self: Pin<&Self>) -> bool {
        PageStateValue(self.atomic().load(Ordering::Acquire)).is_initialized()
    }

    pub fn try_upgrade(self: Pin<&Self>) -> Result<(), LockError> {
        match self
            .atomic()
            .fetch_update(Ordering::Acquire, Ordering::Acquire, |f| {
                let f = PageStateValue(f);

                if f.readers() != 1 {
                    return None;
                }

                if f.has_writer() {
                    return None;
                }

                Some(f.with_readers(0).lock_write().0)
            }) {
            Ok(_) => Ok(()),
            Err(previous) => {
                let previous = PageStateValue(previous);

                Err(LockError::Contended(previous))
            }
        }
    }

    pub fn try_write(self: Pin<&Self>) -> Result<(), LockError> {
        match self
            .atomic()
            .fetch_update(Ordering::Acquire, Ordering::Acquire, |f| {
                let f = PageStateValue(f);

                if f.readers() != 0 {
                    return None;
                }

                if f.has_writer() {
                    return None;
                }

                Some(f.with_readers(0).lock_write().0)
            }) {
            Ok(_) => Ok(()),
            Err(previous) => {
                let previous = PageStateValue(previous);

                Err(LockError::Contended(previous))
            }
        }
    }

    pub fn upgrade(self: Pin<&Self>) {
        let start = std::time::Instant::now();

        loop {
            match self.try_upgrade() {
                Ok(()) => {
                    return;
                }
                Err(LockError::Contended(previous)) => {
                    self.wait(previous);

                    // TODO do we want to keep this in non-debug builds?
                    let lock_duration = start.elapsed();
                    if lock_duration > Duration::from_millis(100) {
                        warn!(waited=?lock_duration, "waited for too long");
                    }
                }
            }
        }
    }

    fn wait(self: Pin<&Self>, previous: PageStateValue) {
        self.futex().wait(previous.0, None);
    }

    pub fn unlock_write(self: Pin<&Self>) {
        match self
            .atomic()
            .fetch_update(Ordering::Release, Ordering::Acquire, |x| {
                let x = PageStateValue(x);

                assert!(x.readers() == 0);
                assert!(x.has_writer());

                Some(x.unlock_write().0)
            }) {
            Ok(_) => {
                self.wake_all();
            }
            Err(_) => todo!(),
        }
    }

    pub fn lock_read(self: Pin<&Self>) {
        if !self.is_initialized() {
            error!(state = ?self.atomic().load(Ordering::Acquire), "trying to lock for read, but the page is uninitialized");
        }

        debug_assert!(self.is_initialized());

        match self
            .atomic()
            .fetch_update(Ordering::Acquire, Ordering::Acquire, |x| {
                let x = PageStateValue(x);

                if x.has_writer() {
                    return None;
                }

                Some(x.with_readers(x.readers() + 1).0)
            }) {
            Ok(_) => {}
            Err(previous) => {
                let previous = PageStateValue(previous);

                // TODO add a warning if we're waiting for too long
                self.wait(previous);
                self.lock_read();
            }
        }
    }

    pub fn unlock_read(self: Pin<&Self>) {
        debug_assert!(self.is_initialized());

        match self
            .atomic()
            .fetch_update(Ordering::Release, Ordering::Acquire, |x| {
                let x = PageStateValue(x);

                assert!(!x.has_writer());
                assert!(x.readers() > 0);

                Some(x.with_readers(x.readers() - 1).0)
            }) {
            Ok(_) => {
                self.wake_all();
            }
            Err(_) => todo!(),
        }
    }

    // TODO we should probably have some way of choosing who to wake (waiters for write, waiters
    // for read, etc.)
    fn wake_all(self: Pin<&Self>) {
        self.futex().wake_all();
    }
}

#[cfg(test)]
mod test {
    use crate::storage::in_memory::block::page_state::PageStateValue;

    #[test]
    fn page_state_value() {
        let lock = PageStateValue(PageStateValue::MASK_IS_INITIALIZED);
        assert!(lock.readers() == 0);

        assert!(!lock.with_readers(1).has_writer());
        assert!(lock.lock_write().readers() == 0);
    }
}
