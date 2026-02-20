use std::{
    fmt::Debug,
    pin::Pin,
    time::{Duration, Instant},
};

use tracing::trace;
use xdb_proc_macros::atomic_state;

use crate::sync::{Mutex, atomic::Ordering};

#[must_use]
pub enum RequestedState {
    Exit,
    Run,
}

atomic_state!(
    SchedulerState {
        running: 1,
        exit_requested: 1,
    }

    pub query exit_requested(Ordering::Acquire);

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
        loop {
            let elapsed_since_last_run = self
                .last_finished_at
                .lock()
                .unwrap()
                .map_or(Duration::MAX, |x| x.elapsed());

            let current_state = self.state.as_ref().current();

            if current_state.exit_requested() {
                return RequestedState::Exit;
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

    pub(super) fn request_exit(&'_ self) {
        self.state.as_ref().request_exit();
    }

    pub(super) fn requested_state(&self) -> RequestedState {
        if self.state.as_ref().exit_requested() {
            RequestedState::Exit
        } else {
            RequestedState::Run
        }
    }

    pub fn start_full_run(&self) {
        *self.last_finished_at.lock().unwrap() = Some(Instant::now());
    }

    fn set_running(&self, value: bool) {
        self.state.as_ref().set_running(value);
    }
}
