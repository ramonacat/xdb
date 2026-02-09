use tracing::{debug, error, warn};

use crate::storage::in_memory::block::{LockError, PageRef};
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

    const MASK_ID_LOCKS: u32 = 0b0011_1111_1111_1111_0000_0000_0000_0000;
    const SHIFT_ID_LOCKS: u32 = 16;

    const MASK_READERS: u32 = 0x0000_FFFF;

    const fn is_initialized(self) -> bool {
        (self.0 & Self::MASK_IS_INITIALIZED) != 0
    }

    // TODO rename -> has_writer
    const fn is_locked(self) -> bool {
        (self.0 & Self::MASK_IS_LOCKED) != 0
    }

    // TODO rename -> lock_write
    const fn lock(self) -> Self {
        Self(self.0 | Self::MASK_IS_LOCKED)
    }

    // TODO rename -> unlock_write
    const fn unlock(self) -> Self {
        Self(self.0 & !Self::MASK_IS_LOCKED)
    }

    const fn id_locks(self) -> u32 {
        (self.0 & Self::MASK_ID_LOCKS) >> Self::SHIFT_ID_LOCKS
    }

    // TODO do we actually care about locking the id, while not taking a read or write lock? or
    // should we just use PageGuard (allowing it to upgrade if needed) and get rid of PageRef entirely?
    const fn lock_id(self) -> Self {
        let locks = self.id_locks() + 1;
        let locks = locks << Self::SHIFT_ID_LOCKS;

        assert!((locks & !Self::MASK_ID_LOCKS) == 0);

        Self((self.0 & !Self::MASK_ID_LOCKS) | locks)
    }

    const fn unlock_id(self) -> Self {
        let locks = self.id_locks();
        assert!(locks > 0);
        let locks = locks - 1;

        Self((self.0 & !Self::MASK_ID_LOCKS) | (locks << Self::SHIFT_ID_LOCKS))
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

                Some(x.mark_uninitialized().0)
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
            Ok(_) => Ok(()),
            Err(previous) => {
                let previous = PageStateValue(previous);

                Err(LockError::Contended(previous))
            }
        }
    }

    pub fn lock_for_move<'block>(
        self: Pin<&Self>,
        page_ref: PageRef<'block>,
    ) -> Result<(), PageRef<'block>> {
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

                if f.id_locks() != 1 {
                    return None;
                }

                Some(f.lock().0)
            }) {
            Ok(_) => {
                drop(page_ref);

                Ok(())
            }
            Err(previous) => {
                let previous = PageStateValue(previous);
                debug!(?previous, "failed to lock for move");

                Err(page_ref)
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
                        warn!(waited=?lock_duration, "waited for too long");
                    }
                }
            }
        }
    }

    fn wait(self: Pin<&Self>, previous: PageStateValue) {
        self.futex().wait(previous.0, None);
    }

    pub fn unlock(self: Pin<&Self>) {
        match self
            .atomic()
            .fetch_update(Ordering::Release, Ordering::Acquire, |x| {
                let x = PageStateValue(x);

                assert!(x.readers() == 0);
                assert!(x.is_locked());

                Some(x.unlock().0)
            }) {
            Ok(_) => {
                self.wake();
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

                if x.is_locked() {
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
        self.futex().wake_all();
    }

    pub fn lock_id(self: Pin<&Self>) {
        match self
            .atomic()
            .fetch_update(Ordering::Acquire, Ordering::Acquire, |x| {
                let x = PageStateValue(x);

                Some(x.lock_id().0)
            }) {
            Ok(_) => {}
            Err(_) => todo!(),
        }
    }

    pub fn unlock_id(self: Pin<&Self>) {
        match self
            .atomic()
            .fetch_update(Ordering::Acquire, Ordering::Acquire, |x| {
                let x = PageStateValue(x);

                Some(x.unlock_id().0)
            }) {
            Ok(_) => {
                self.wake();
            }
            Err(_) => todo!(),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::storage::in_memory::block::page_state::PageStateValue;

    #[test]
    fn page_state_value() {
        let lock = PageStateValue(PageStateValue::MASK_IS_INITIALIZED);
        assert!(lock.readers() == 0);

        assert!(!lock.with_readers(1).is_locked());
        assert!(lock.lock().readers() == 0);
    }
}
