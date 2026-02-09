mod scheduler;

use std::time::Duration;

use tracing::{debug, info, info_span, trace};

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
    #[allow(clippy::too_many_lines)] // TODO there's a lot of room for improvement here
    pub fn run(&self) {
        loop {
            let _ = info_span!("vaccum").entered();

            match self.scheduler.block_if_unscheduled() {
                scheduler::RequestedState::Exit => break,
                scheduler::RequestedState::Run => {}
            }

            self.scheduler.start_full_run();

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

            let mut index = PageIndex::from_value(1);

            let mut i = 0u64;
            let mut freed_count = 0u64;
            let mut checked_count = 0u64;

            let pages_to_check = self.data.page_count_lower_bound();

            debug!(
                minimum_active_timestamp = ?min_timestamp,
                pages_count = ?pages_to_check,
                "starting scan"
            );

            while i < pages_to_check {
                let _ = info_span!("page", physical_index = ?index).entered();
                // TODO this is another scheduling issue, if there's a lot of pages,
                // vacuum can spend a lot of time in this loop, preventing freezes and
                // preventing exit when done
                if i.is_multiple_of(10000) {
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
                    if page_guard.is_free() {
                        debug!(physical_index=?index, "freeing page");
                        drop(page_guard.reset());

                        self.freemap.set(index.0).unwrap();
                        freed_count += 1;

                        continue;
                    }

                    if page_guard.previous_version().is_some() {
                        continue;
                    }

                    checked_count += 1;

                    let Some(visible_until) = page_guard.visible_until() else {
                        continue;
                    };
                    if visible_until < min_timestamp {
                        continue;
                    }

                    trace!(physical_index = ?index, "trying to clean up");

                    if let Some(next_version_index) = page_guard.next_version() {
                        assert!(next_version_index != index);

                        if let Some(next_version) = self.data.try_get(None, next_version_index)
                            && let Ok(next_version) = next_version.lock_for_move()
                        {
                            assert!(
                                next_version.previous_version() == Some(index),
                                "previous_version of {next_version_index:?} expected to be {index:?}, but was {:?}",
                                next_version.previous_version()
                            );

                            let mut next_next = if let Some(next_next_index) =
                                next_version.next_version()
                            {
                                if let Some(next_next) = self.data.try_get(None, next_next_index)
                                    && let Ok(next_next) = next_next.lock_nowait()
                                {
                                    assert!(
                                        next_next.previous_version() == Some(next_version_index)
                                    );

                                    Some(next_next)
                                } else {
                                    continue;
                                }
                            } else {
                                None
                            };

                            assert!(page_guard.previous_version().is_none());

                            *page_guard = *next_version;
                            page_guard.set_previous_version(None);

                            if let Some(ref mut next_next) = next_next {
                                assert!(
                                    page_guard.next_version() == Some(next_next.physical_index())
                                );

                                next_next.set_previous_version(Some(index));
                            } else {
                                assert!(page_guard.next_version().is_none());
                            }

                            assert!(next_version_index != index);
                            assert!(next_version.physical_index() != index);

                            debug!(
                                physical_index=?next_version_index,
                                logical_index = ?index,
                                min_visible_timestamp = ?min_timestamp,
                                next.visible_until = ?next_version.visible_until(),
                                next.visible_since = ?next_version.visible_from(),
                                visible_until = ?page_guard.visible_until(),
                                visible_from = ?page_guard.visible_from(),
                                "freeing page"
                            );
                            drop(next_version.reset());

                            drop(next_next);
                            drop(page_guard);

                            self.freemap.set(next_version_index.0).unwrap();
                            freed_count += 1;
                        }
                    } else {
                        debug!(physical_index=?index, "freeing page");
                        drop(page_guard.reset());

                        self.freemap.set(index.0).unwrap();
                        freed_count += 1;
                    }
                }
            }

            debug!(
                freed_count = ?freed_count,
                checked_count = ?checked_count,
                scanned_count = ?i,
                total_count = ?self.data.page_count_lower_bound(),
                "vacuum scan finished",
            );
        }

        info!("exiting vacuum thread");
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
        trace!("requesting vacuum freeze");
        let guard = self.scheduler.request_freeze();
        trace!("freeze succesfuly started");

        guard
    }
}

impl Drop for Vacuum {
    fn drop(&mut self) {
        info!("exiting vacuum...");

        self.scheduler.request_exit();

        let handle = self.handle.take();

        if let Some(handle) = handle {
            handle.join().unwrap();
        }
    }
}
