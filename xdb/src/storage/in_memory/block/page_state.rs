use tracing::warn;
use xdb_proc_macros::atomic_state;

use crate::storage::in_memory::block::LockError;
use crate::sync::atomic::Ordering;
use std::fmt::Debug;
use std::pin::Pin;
use std::time::Duration;

#[cfg(not(feature = "shuttle"))]
const _: () = assert!(size_of::<PageState>() == size_of::<u32>());

atomic_state!(
    pub PageState {
        initialized: 1,
        locked: 1,
        readers: 16,
    }

    pub query initialized(Ordering::Acquire);

    pub update(Ordering::Acquire, Ordering::Acquire)
        mark_initialized()
    {
        action: {
            debug_assert!(!state.initialized());

            Some(state.with_initialized(true))
        }
    }

    pub update(Ordering::Release, Ordering::Acquire)
        mark_uninitialized()
    {
        action: {
            assert!(state.locked());
            assert!(state.readers() == 0);

            Some(state.with_initialized(false))
        }
    }

    pub update(Ordering::Acquire, Ordering::Acquire)
        try_upgrade() -> Result<(), LockError>
    {
        action: {
            if state.readers() != 1 || state.locked() {
                return None;
            }

            Some(state.with_readers(0).with_locked(true))
        },
        ok: { Ok(()) },
        err: { Err(LockError::Contended(state)) },
    }

    pub update(Ordering::Acquire, Ordering::Acquire)
        try_write() -> Result<(), LockError>
    {
        action: {
            if state.readers() != 0 || state.locked() {
                return None;
            }

            Some(state.with_readers(0).with_locked(true))
        },
        ok: { Ok(()) },
        err: { Err(LockError::Contended(state)) },
    }

    pub update(Ordering::Release, Ordering::Acquire)
        unlock_write()
    {
        action: {
            assert!(state.readers() == 0);
            assert!(state.locked());

            Some(state.with_locked(false))
        },
        ok: { self.wake_all(); }
    }

    pub update(Ordering::Acquire, Ordering::Acquire)
        try_lock_read() -> Result<(), LockError>
    {
        action: {
            assert!(state.initialized());

            if state.locked() {
                return None;
            }

            Some(state.with_readers(state.readers() + 1))
        },
        ok: { Ok(()) },
        err: { Err(LockError::Contended(state)) },
    }

    pub update(Ordering::Release, Ordering::Acquire)
        unlock_read()
    {
        action: {
            assert!(!state.locked());
            assert!(state.readers() > 0);

            Some(state.with_readers(state.readers() - 1))
        },
        ok: { self.wake_all(); }
    }
);

// TODO there's a lot of copy-paste here, can we simplify?
impl PageState {
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

    pub fn lock_read(self: Pin<&Self>) {
        loop {
            match self.try_lock_read() {
                Ok(()) => return,
                Err(LockError::Contended(previous)) => {
                    self.wait(previous);
                }
            }
        }
    }
}
