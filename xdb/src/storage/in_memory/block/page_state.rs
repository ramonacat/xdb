use tracing::{debug, warn};

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
            .field("is_locked", &self.is_locked())
            .field("raw", &format!("{:#b}", self.0))
            .finish()
    }
}

impl PageStateValue {
    const MASK_IS_INITIALIZED: u32 = 1 << 31;
    const MASK_IS_LOCKED: u32 = 1 << 30;

    const MASK_READERS: u32 = 0x0000_FFFF;

    const fn is_initialized(self) -> bool {
        self.0 & Self::MASK_IS_INITIALIZED != 0
    }

    // TODO rename -> has_writer
    const fn is_locked(self) -> bool {
        self.0 & Self::MASK_IS_LOCKED != 0
    }

    // TODO rename -> lock_write
    const fn lock(self) -> Self {
        Self(self.0 | Self::MASK_IS_LOCKED)
    }

    // TODO rename -> unlock_write
    const fn unlock(self) -> Self {
        Self(self.0 & !Self::MASK_IS_LOCKED)
    }

    const fn mark_uninitialized(self) -> Self {
        Self(self.0 & !Self::MASK_IS_INITIALIZED)
    }

    fn readers(self) -> u16 {
        u16::try_from(self.0 & Self::MASK_READERS).unwrap()
    }

    fn with_readers(self, readers: u16) -> Self {
        assert!(!self.is_locked());

        Self((self.0 & !Self::MASK_READERS) | u32::from(readers))
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

        debug_assert!(!PageStateValue(previous_state).is_initialized());
    }

    pub fn mark_uninitialized(self: Pin<&Self>) {
        match self
            .atomic()
            .fetch_update(Ordering::Release, Ordering::Acquire, |x| {
                let x = PageStateValue(x);

                assert!(x.is_locked());
                assert!(x.readers() == 0);

                Some(x.mark_uninitialized().unlock().0)
            }) {
            Ok(_) => {}
            Err(_) => todo!(),
        }
    }

    pub fn is_initialized(self: Pin<&Self>) -> bool {
        PageStateValue(self.atomic().load(Ordering::Acquire)).is_initialized()
    }

    // TODO rename -> try_lock, change result to bool
    pub fn lock_nowait(self: Pin<&Self>) -> Result<(), LockError> {
        match self
            .atomic()
            .fetch_update(Ordering::Acquire, Ordering::Acquire, |f| {
                let f = PageStateValue(f);

                if f.readers() > 0 {
                    return None;
                }

                if f.is_locked() {
                    return None;
                }

                Some(f.lock().0)
            }) {
            Ok(previous) => {
                let previous = PageStateValue(previous);

                debug!("locked, previous {previous:?}");

                Ok(())
            }
            Err(previous) => {
                let previous = PageStateValue(previous);

                debug!("failed to lock, previous {previous:?}");

                Err(LockError::Contended(previous))
            }
        }
    }

    pub fn lock(self: Pin<&Self>) {
        let start = std::time::Instant::now();

        loop {
            match self.lock_nowait() {
                Ok(()) => {
                    return;
                }
                Err(LockError::Contended(previous)) => {
                    self.wait(previous);

                    // TODO do we want to keep this in non-debug builds?
                    let lock_duration = start.elapsed();
                    if lock_duration > Duration::from_millis(100) {
                        warn!("lock waited for too long: {lock_duration:?}");
                    }
                }
            }
        }
    }

    fn wait(self: Pin<&Self>, previous: PageStateValue) {
        self.futex().wait(previous.0, None);
    }

    pub fn unlock(self: Pin<&Self>) {
        debug_assert!(self.is_initialized());

        match self
            .atomic()
            .fetch_update(Ordering::Release, Ordering::Acquire, |x| {
                let x = PageStateValue(x);

                assert!(x.readers() == 0);
                assert!(x.is_locked());

                Some(x.unlock().0)
            }) {
            Ok(previous) => {
                let previous = PageStateValue(previous);

                debug!("unlocked from {previous:?}");

                self.wake();
            }
            Err(_) => todo!(),
        }
    }

    pub fn lock_read(self: Pin<&Self>) {
        debug_assert!(self.is_initialized());

        match self
            .atomic()
            .fetch_update(Ordering::Acquire, Ordering::Acquire, |x| {
                let x = PageStateValue(x);

                if x.is_locked() {
                    return None;
                }

                Some(x.with_readers(x.readers() + 1).0)
            }) {
            Ok(_) => {}
            Err(previous) => {
                let previous = PageStateValue(previous);

                warn!("waiting for a read lock {previous:?}");

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

                assert!(!x.is_locked());
                assert!(x.readers() > 0);

                Some(x.with_readers(x.readers() - 1).0)
            }) {
            Ok(_) => {
                self.wake();
            }
            Err(_) => todo!(),
        }
    }

    // TODO rename -> wake_one?
    // TODO does this need to be pub?
    pub fn wake(self: Pin<&Self>) {
        let awoken = self.futex().wake_one();
        debug!("awoken a waiter? {awoken} ");
    }
}
