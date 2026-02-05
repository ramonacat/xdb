use std::{
    fmt::Debug,
    pin::Pin,
    time::{Duration, Instant},
};

use tracing::{debug, instrument};

use crate::{
    platform::futex::{Futex, FutexError},
    sync::{Mutex, atomic::Ordering},
};

#[must_use]
pub enum RequestedState {
    Exit,
    Run,
}

pub struct FreezeGuard<'scheduler> {
    scheduler: &'scheduler Scheduler,
}

impl Drop for FreezeGuard<'_> {
    fn drop(&mut self) {
        // TODO create an api for change+wake?
        self.scheduler.state.request_unfreeze();
    }
}

#[derive(Clone, Copy)]
struct SchedulerStateValue(u32);

impl Debug for SchedulerStateValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchedulerStateValue")
            .field("is_running", &self.is_running())
            .field("exit_requested", &self.is_exit_requested())
            .field("freeze_requests", &self.freeze_requests())
            .field("raw", &format!("{:#b}", self.0))
            .finish()
    }
}

impl SchedulerStateValue {
    const MASK_IS_RUNNING: u32 = 1 << 31;
    const MASK_EXIT_REQUESTED: u32 = 1 << 30;

    const MASK_FREEZE_REQUESTS: u32 = 0x0000_FFFF;

    fn freeze_requests(self) -> u16 {
        u16::try_from(self.0 & Self::MASK_FREEZE_REQUESTS).unwrap()
    }

    fn set_freeze_requests(self, value: u16) -> Self {
        Self((self.0 & !Self::MASK_FREEZE_REQUESTS) | u32::from(value))
    }

    fn request_unfreeze(self) -> Self {
        self.set_freeze_requests(self.freeze_requests().strict_sub(1))
    }

    fn request_freeze(self) -> Self {
        self.set_freeze_requests(self.freeze_requests().strict_add(1))
    }

    const fn request_exit(self) -> Self {
        Self(self.0 | Self::MASK_EXIT_REQUESTED)
    }

    const fn set_running(self, value: bool) -> Self {
        if value {
            Self(self.0 | Self::MASK_IS_RUNNING)
        } else {
            Self(self.0 & !Self::MASK_IS_RUNNING)
        }
    }

    const fn is_exit_requested(self) -> bool {
        self.0 & Self::MASK_EXIT_REQUESTED > 0
    }

    fn is_freeze_requested(self) -> bool {
        self.freeze_requests() > 0
    }

    const fn is_running(self) -> bool {
        self.0 & Self::MASK_IS_RUNNING > 0
    }
}

impl SchedulerStateValue {}

#[derive(Debug)]
struct SchedulerState(Pin<Box<Futex>>);

impl SchedulerState {
    fn new() -> Self {
        Self(Box::pin(Futex::new(0)))
    }

    fn request_unfreeze(&self) {
        match self
            .0
            .as_ref()
            .atomic()
            .fetch_update(Ordering::Release, Ordering::Acquire, |v| {
                let v = SchedulerStateValue(v);

                Some(v.request_unfreeze().0)
            }) {
            Ok(_) => {
                self.0.as_ref().wake(u32::MAX);
            }
            Err(_) => todo!(),
        }
    }

    fn current(&self) -> SchedulerStateValue {
        SchedulerStateValue(self.0.as_ref().atomic().load(Ordering::Acquire))
    }

    fn freeze(&self) {
        match self
            .0
            .as_ref()
            .atomic()
            .fetch_update(Ordering::Release, Ordering::Acquire, |v| {
                let v = SchedulerStateValue(v);

                Some(v.request_freeze().0)
            }) {
            Ok(previous) => {
                let mut previous = SchedulerStateValue(previous);
                debug!("unfreezed from {previous:?}");

                while previous.is_running() {
                    self.0.as_ref().wake(u32::MAX);

                    match self.0.as_ref().wait(previous.0, None) {
                        // TODO ::Race is practically equivalent with Ok(()) for all purposes,
                        // Futex should just not return an error
                        Ok(()) | Err(FutexError::Race) => {}
                    }

                    previous =
                        SchedulerStateValue(self.0.as_ref().atomic().load(Ordering::Acquire));
                }
            }
            Err(_) => todo!(),
        }
    }

    fn wait(
        &self,
        current_state: SchedulerStateValue,
        timeout: Option<Duration>,
    ) -> Result<(), FutexError> {
        debug!("waiting to change state from {current_state:?}");

        self.0.as_ref().wait(current_state.0, timeout)
    }

    fn request_exit(&self) {
        match self
            .0
            .as_ref()
            .atomic()
            .fetch_update(Ordering::Release, Ordering::Acquire, |x| {
                let x = SchedulerStateValue(x);

                Some(x.request_exit().0)
            }) {
            Ok(_) => {
                self.0.as_ref().wake(u32::MAX);
            }
            Err(_) => todo!(),
        }
    }

    fn set_running(&self, is_running: bool) {
        match self
            .0
            .as_ref()
            .atomic()
            .fetch_update(Ordering::Release, Ordering::Acquire, |x| {
                let x = SchedulerStateValue(x);

                Some(x.set_running(is_running).0)
            }) {
            Ok(_) => {
                self.0.as_ref().wake(u32::MAX);
            }
            Err(_) => todo!(),
        }
    }
}

#[derive(Debug)]
pub(super) struct Scheduler {
    state: SchedulerState,
    last_finished_at: Mutex<Option<Instant>>,
}

impl Scheduler {
    const PAUSE_LENGTH: Duration = Duration::from_secs(30);

    pub fn new() -> Self {
        Self {
            state: SchedulerState::new(),
            last_finished_at: Mutex::new(None),
        }
    }

    pub(super) fn block_if_needed(&self) -> RequestedState {
        loop {
            let current_state = self.state.current();

            if current_state.is_exit_requested() {
                return RequestedState::Exit;
            } else if current_state.is_freeze_requested()
                // TODO make the pause duration configurable
                // TODO support for forced requests (when we need to block on vacuum to free pages for
                // allocation)
                || self.last_finished_at.lock().unwrap().map_or(Duration::MAX, |x| x.elapsed()) >= Self::PAUSE_LENGTH
            {
                if current_state.is_running() {
                    self.state.set_running(false);
                }

                match self.state.wait(
                    current_state,
                    self.last_finished_at
                        .lock()
                        .unwrap()
                        .map(|x| Self::PAUSE_LENGTH.saturating_sub(x.elapsed())),
                ) {
                    Ok(()) | Err(FutexError::Race) => {}
                }
            } else {
                // TODO should this be managed externally?
                self.set_running(true);
                return RequestedState::Run;
            }
        }
    }

    #[instrument()]
    pub(super) fn request_freeze(&'_ self) -> FreezeGuard<'_> {
        self.state.freeze();

        FreezeGuard { scheduler: self }
    }

    pub(super) fn request_exit(&'_ self) {
        self.state.request_exit();
    }

    pub(super) fn set_running(&self, value: bool) {
        self.state.set_running(value);
    }
}
