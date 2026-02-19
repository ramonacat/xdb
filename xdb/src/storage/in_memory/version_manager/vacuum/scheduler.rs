use std::{
    fmt::Debug,
    pin::Pin,
    time::{Duration, Instant},
};

use tracing::{instrument, trace};
use xdb_proc_macros::atomic_state;

use crate::sync::{Mutex, atomic::Ordering};

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
        self.scheduler.state.as_ref().request_unfreeze();
    }
}

atomic_state!(
    SchedulerState {
        running: 1,
        exit_requested: 1,
        freeze_requests: 16,
    }

    pub query freeze_requests(Ordering::Acquire);
    pub query exit_requested(Ordering::Acquire);

    pub update(Ordering::Release, Ordering::Acquire)
        request_unfreeze()
    {
        action: { Some(state.with_freeze_requests(state.freeze_requests() - 1)) },
        ok: { self.wake_all(); }
    }

    pub update(Ordering::Release, Ordering::Acquire)
        freeze()
    {
        action: { Some(state.with_freeze_requests(state.freeze_requests() + 1)) },
        ok: {
            let mut state = state;

            while state.running() {
                self.wake_all();
                self.wait(state);

                state = SchedulerStateValue(self.futex().atomic().load(Ordering::Acquire));
            }
        },
    }

    pub update(Ordering::Release, Ordering::Acquire)
        request_exit()
    {
        action: { Some(state.with_exit_requested(true)) },
        ok: { self.wake_all(); }
    }

    pub update(Ordering::Release, Ordering::Acquire)
        set_running(running: bool)
    {
        action: { Some(state.with_running(running)) },
        ok: { self.wake_all(); }
    }
);

impl SchedulerState {
    fn current(self: Pin<&Self>) -> SchedulerStateValue {
        SchedulerStateValue(self.futex().atomic().load(Ordering::Acquire))
    }
}

#[derive(Debug)]
pub(super) struct Scheduler {
    state: Pin<Box<SchedulerState>>,
    last_finished_at: Mutex<Option<Instant>>,
}

impl Scheduler {
    const PAUSE_LENGTH: Duration = Duration::from_secs(10);

    pub fn new() -> Self {
        Self {
            state: Box::pin(SchedulerState::new()),
            last_finished_at: Mutex::new(None),
        }
    }

    pub(super) fn block_if_unscheduled(&self) -> RequestedState {
        match self.block_if_frozen() {
            RequestedState::Exit => return RequestedState::Exit,
            RequestedState::Run => {}
        }

        loop {
            let elapsed_since_last_run = self
                .last_finished_at
                .lock()
                .unwrap()
                .map_or(Duration::MAX, |x| x.elapsed());

            let current_state = self.state.as_ref().current();

            if current_state.exit_requested() {
                return RequestedState::Exit;
            } else if current_state.freeze_requests() > 0 {
                self.set_running(false);

                self.state.as_ref().wait(current_state);
            } else if elapsed_since_last_run < Self::PAUSE_LENGTH {
                trace!("{elapsed_since_last_run:?} since last run, waiting");

                let timeout = self
                    .last_finished_at
                    .lock()
                    .unwrap()
                    .map_or(Self::PAUSE_LENGTH, |x| {
                        Self::PAUSE_LENGTH.saturating_sub(x.elapsed())
                    });
                self.state.as_ref().wait_timeout(current_state, timeout);
            } else {
                self.set_running(true);
                return RequestedState::Run;
            }
        }
    }

    #[instrument]
    pub(super) fn block_if_frozen(&self) -> RequestedState {
        loop {
            let current_state = self.state.as_ref().current();

            if current_state.exit_requested() {
                return RequestedState::Exit;
            } else if current_state.freeze_requests() > 0 {
                self.set_running(false);
                self.state.as_ref().wait(current_state);
            } else {
                // TODO should this be managed in the vacuum thread itself?
                self.set_running(true);
                return RequestedState::Run;
            }
        }
    }

    #[instrument()]
    pub(super) fn request_freeze(&'_ self) -> FreezeGuard<'_> {
        self.state.as_ref().freeze();

        FreezeGuard { scheduler: self }
    }

    pub(super) fn request_exit(&'_ self) {
        self.state.as_ref().request_exit();
    }

    pub fn start_full_run(&self) {
        *self.last_finished_at.lock().unwrap() = Some(Instant::now());
    }

    fn set_running(&self, value: bool) {
        self.state.as_ref().set_running(value);
    }
}
