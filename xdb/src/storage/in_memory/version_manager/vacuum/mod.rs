mod scheduler;

use std::time::Duration;

use tracing::debug;

use crate::{
    storage::{
        PageIndex,
        in_memory::{
            Bitmap,
            block::Block,
            version_manager::{
                transaction_log::TransactionLog,
                vacuum::scheduler::{FreezeGuard, Scheduler},
            },
        },
    },
    sync::Arc,
    thread::{self, JoinHandle},
};

struct VacuumThread {
    // TODO we should prolly just have an Arc<VersionManager> here?
    log: Arc<TransactionLog>,
    data: Arc<Block>,
    freemap: Arc<Bitmap>,
    scheduler: Arc<Scheduler>,
}

impl VacuumThread {
    // TODO log a warning if a freeze is taking too long
    pub fn run(&self) {
        loop {
            match self.scheduler.block_if_unscheduled() {
                scheduler::RequestedState::Exit => break,
                scheduler::RequestedState::Run => {}
            }

            self.scheduler.start_full_run();

            debug!("requesting running transactions");

            let Some(min_timestamp) = self.log.minimum_active_timestamp() else {
                // TODO we need a smarter way of scheduling vacuum (based on usage of the
                // block)
                if cfg!(any(fuzzing, test)) {
                    thread::yield_now();
                } else {
                    thread::sleep(Duration::from_secs(10));
                }
                continue;
            };

            debug!("minimum active timestamp: {min_timestamp:?}");

            let mut index = PageIndex::from_value(1);

            let mut i = 0u64;
            let mut freed_count = 0u64;
            let mut checked_count = 0u64;

            let pages_to_check = self.data.page_count_lower_bound();
            debug!("vacuum will iterate over {pages_to_check} pages");
            while i < pages_to_check {
                // TODO this is another scheduling issue, if there's a lot of pages,
                // vacuum can spend a lot of time in this loop, preventing freezes and
                // preventing exit when done
                if i.is_multiple_of(10000) {
                    debug!("vacuum checking for freezes and exit requests");

                    match self.scheduler.block_if_frozen() {
                        scheduler::RequestedState::Exit => break,
                        scheduler::RequestedState::Run => {}
                    }
                }

                index = index.next();
                i += 1;

                let Some(page) = self.data.try_get(None, index) else {
                    continue;
                };

                if let Ok(mut page_guard) = page.lock_nowait() {
                    if page_guard.previous_version().is_some() {
                        continue;
                    }

                    checked_count += 1;

                    if let Some(visible_until) = page_guard.visible_until()
                        && min_timestamp > visible_until
                    {
                        debug!(
                            "page {index:?} needs to be cleaned up next:{:?}, previous:{:?}",
                            page_guard.next_version(),
                            page_guard.previous_version()
                        );

                        if page_guard.previous_version().is_some() {
                            continue;
                        }

                        if let Some(next_version_index) = page_guard.next_version() {
                            if let Some(next_version) = self.data.try_get(None, next_version_index)
                                && let Ok(next_version) = next_version.lock_nowait()
                            {
                                let mut next_next =
                                    if let Some(next_next_index) = next_version.next_version() {
                                        if let Some(next_next) =
                                            self.data.try_get(None, next_next_index)
                                            && let Ok(next_next) = next_next.lock_nowait()
                                        {
                                            Some(next_next)
                                        } else {
                                            continue;
                                        }
                                    } else {
                                        None
                                    };

                                *page_guard = *next_version;
                                page_guard.set_previous_version(None);

                                if let Some(ref mut next_next) = next_next {
                                    next_next.set_previous_version(Some(index));
                                }

                                next_version.reset();

                                drop(next_next);
                                drop(page_guard);

                                self.freemap.set(next_version_index.0).unwrap();
                                freed_count += 1;
                            }
                        } else {
                            debug!("freeing page {:?}", index);
                            page_guard.reset();

                            self.freemap.set(index.0).unwrap();
                            freed_count += 1;
                        }
                    }
                }
            }

            debug!(
                "vacuum scan finished, freed/checked/scanned/total {freed_count}/{checked_count}/{i}/{}",
                self.data.page_count_lower_bound()
            );
        }

        debug!("exiting vacuum thread...");
    }
}

#[derive(Debug)]
pub struct Vacuum {
    handle: Option<JoinHandle<()>>,
    scheduler: Arc<Scheduler>,
}

impl Vacuum {
    pub fn start(log: Arc<TransactionLog>, data: Arc<Block>, freemap: Arc<Bitmap>) -> Self {
        let scheduler = Arc::new(Scheduler::new());

        let handle = {
            let scheduler = scheduler.clone();

            thread::Builder::new()
                .name("vacuum".into())
                .spawn(move || {
                    let runner = VacuumThread {
                        log,
                        data,
                        freemap,
                        scheduler,
                    };
                    runner.run();
                })
                .unwrap()
        };
        Self {
            handle: Some(handle),
            scheduler,
        }
    }

    pub(crate) fn freeze(&'_ self) -> FreezeGuard<'_> {
        debug!("requesting vacuum freeze");
        let guard = self.scheduler.request_freeze();
        debug!("freeze succesfuly started");

        guard
    }
}

impl Drop for Vacuum {
    fn drop(&mut self) {
        debug!("dropping vacuum...");
        self.scheduler.request_exit();

        let handle = self.handle.take();

        if let Some(handle) = handle {
            handle.join().unwrap();
        }
    }
}
