mod scheduler;

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use tracing::{debug, info, info_span, instrument, trace};

use crate::{
    storage::{
        PageIndex, TransactionalTimestamp,
        in_memory::{
            Bitmap,
            block::{Block, PageWriteGuard},
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

    freed_count: AtomicU64,
    checked_count: u64,
}

impl VacuumThread {
    // TODO log a warning if a freeze is taking too long
    pub fn run(&mut self) {
        loop {
            let _ = info_span!("vaccum").entered();

            match self.scheduler.block_if_unscheduled() {
                scheduler::RequestedState::Exit => break,
                scheduler::RequestedState::Run => {}
            }

            self.scheduler.start_full_run();

            let Some(min_timestamp) = self.log.minimum_active_timestamp() else {
                // TODO we need a smarter way of scheduling vacuum (based on usage of the
                // block and allocation pressure)
                if cfg!(any(fuzzing, test)) {
                    thread::yield_now();
                } else {
                    thread::sleep(Duration::from_secs(10));
                }
                continue;
            };

            let mut index = PageIndex::from_value(1);

            let mut i = 0u64;
            self.freed_count.store(0, Ordering::Release);
            self.checked_count = 0u64;

            let pages_to_check = self.data.allocated_page_count();

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

                self.vacuum_page(index, min_timestamp);
            }

            debug!(
                freed_count = ?self.freed_count,
                checked_count = ?self.checked_count,
                scanned_count = ?i,
                total_count = ?self.data.allocated_page_count(),
                "vacuum scan finished",
            );
        }

        info!("exiting vacuum thread");
    }

    #[instrument(skip(self))]
    fn vacuum_page(&mut self, index: PageIndex, min_live_timestamp: TransactionalTimestamp) {
        let Some(page) = self.data.try_get(index) else {
            return;
        };

        let Ok(mut page_guard) = page.try_upgrade() else {
            return;
        };

        self.checked_count += 1;

        if page_guard.is_free() {
            self.free_page(page_guard);

            return;
        }

        if page_guard.previous_version().is_some() {
            // we will also see the first page in the chain at some point here, and we can look
            // from there (as the versions are in a growing order, it doesn't really matter from
            // correctness standpoint, but simplifies the implementation a lot)
            return;
        }

        let Some(visible_until) = page_guard.visible_until() else {
            return;
        };

        if visible_until >= min_live_timestamp {
            return;
        }

        trace!(physical_index = ?index, "trying to clean up");

        let Some(next_version_index) = page_guard.next_version() else {
            self.free_page(page_guard);
            return;
        };
        assert!(next_version_index != index);

        let Some(next_version) = self.data.try_get(next_version_index) else {
            return;
        };
        let Ok(next_version) = next_version.try_upgrade() else {
            return;
        };

        assert!(
            next_version.previous_version() == Some(index),
            "previous_version of {next_version_index:?} expected to be {index:?}, but was {:?}",
            next_version.previous_version()
        );

        let mut next_next = if let Some(next_next_index) = next_version.next_version() {
            if let Some(next_next) = self.data.try_get(next_next_index)
                && let Ok(next_next) = next_next.try_upgrade()
            {
                assert!(next_next.previous_version() == Some(next_version_index));

                Some(next_next)
            } else {
                return;
            }
        } else {
            None
        };

        debug!(
            physical_index=?next_version_index,
            logical_index = ?index,
            next.visible_until = ?next_version.visible_until(),
            next.visible_since = ?next_version.visible_from(),
            visible_until = ?page_guard.visible_until(),
            visible_from = ?page_guard.visible_from(),
            "moving page"
        );

        *page_guard = *next_version;
        page_guard.set_previous_version(None);

        if let Some(ref mut next_next) = next_next {
            assert!(page_guard.next_version() == Some(next_next.physical_index()));

            next_next.set_previous_version(Some(index));
        } else {
            assert!(page_guard.next_version().is_none());
        }

        assert!(next_version_index != index);
        assert!(next_version.physical_index() != index);

        self.free_page(next_version);

        drop(next_next);
        drop(page_guard);
    }

    #[instrument(skip(self))]
    fn free_page(&self, page_guard: PageWriteGuard) {
        debug!(
            physical_index=?page_guard.physical_index(),
            visible_until=?page_guard.visible_until(),
            visible_from=?page_guard.visible_from(),
            is_free=?page_guard.is_free(),

            "freeing page"
        );
        let physical_index = page_guard.physical_index();

        drop(page_guard.reset());

        self.freemap.set(physical_index.0).unwrap();
        self.freed_count.fetch_add(1, Ordering::Relaxed);
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
                    let mut runner = VacuumThread {
                        log,
                        data,
                        freemap,
                        scheduler,
                        checked_count: 0,
                        freed_count: AtomicU64::new(0),
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
