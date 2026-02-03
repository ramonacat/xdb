use tracing::debug;

use crate::storage::in_memory::block::LockError;
use crate::sync::atomic::{AtomicU32, Ordering};
use std::fmt::Debug;
use std::time::Duration;
use std::{marker::PhantomPinned, pin::Pin};

use crate::platform::futex::{Futex, FutexError};

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

    const fn is_initialized(self) -> bool {
        self.0 & Self::MASK_IS_INITIALIZED != 0
    }

    const fn is_locked(self) -> bool {
        self.0 & Self::MASK_IS_LOCKED != 0
    }

    const fn lock(self) -> Self {
        Self(self.0 | Self::MASK_IS_LOCKED)
    }

    const fn unlock(self) -> Self {
        Self(self.0 & !Self::MASK_IS_LOCKED)
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

    pub fn lock_nowait(self: Pin<&Self>) -> Result<(), LockError> {
        match self
            .atomic()
            .fetch_update(Ordering::Acquire, Ordering::Acquire, |f| {
                let f = PageStateValue(f);

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
                    if std::time::Instant::now() - start > Duration::from_millis(100) {
                        // TODO enable in debug: panic!("lock held exceedingly long");
                    }
                }
            }
        }
    }

    fn wait(self: Pin<&Self>, previous: PageStateValue) {
        match self.futex().wait(previous.0) {
            Ok(()) | Err(FutexError::Race) => {}
        }
    }

    pub fn unlock(self: Pin<&Self>) {
        debug_assert!(self.is_initialized());

        match self
            .atomic()
            .fetch_update(Ordering::Release, Ordering::Acquire, |x| {
                let x = PageStateValue(x);

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

    pub fn wake(self: Pin<&Self>) {
        let awoken = self.futex().wake(1);
        debug!("awoken {awoken} waiters");
    }
}
