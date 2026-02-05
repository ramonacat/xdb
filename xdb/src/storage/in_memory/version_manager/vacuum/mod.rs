mod scheduler;

use std::{collections::BTreeSet, time::Duration};

use tracing::debug;

use crate::{
    storage::{
        PageIndex, TransactionId,
        in_memory::{
            Bitmap,
            block::Block,
            version_manager::vacuum::scheduler::{FreezeGuard, Scheduler},
        },
    },
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
};

struct VacuumThread {
    running_transactions: Arc<Mutex<BTreeSet<TransactionId>>>,
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

            let running_transactions = self.running_transactions.lock().unwrap();
            let Some(min_txid) = running_transactions.first().copied() else {
                drop(running_transactions);
                // TODO we need a smarter way of scheduling vacuum (based on usage of the
                // block)
                if cfg!(any(fuzzing, test)) {
                    thread::yield_now();
                } else {
                    thread::sleep(Duration::from_secs(10));
                }
                continue;
            };
            drop(running_transactions);

            debug!("min txid: {min_txid:?}");

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

                let Some(page) = self.data.try_get(index) else {
                    continue;
                };

                if let Ok(page_guard) = page.lock_nowait() {
                    checked_count += 1;

                    if let Some(visible_until) = page_guard.visible_until()
                        && visible_until < min_txid
                    {
                        drop(page_guard); // ensure the lock is dropped before we uninitialize it
                        unsafe {
                            page.reset();
                        }

                        self.freemap.set(index.0).unwrap();
                        freed_count += 1;
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
    pub fn start(
        running_transactions: Arc<Mutex<BTreeSet<TransactionId>>>,
        data: Arc<Block>,
        freemap: Arc<Bitmap>,
    ) -> Self {
        let scheduler = Arc::new(Scheduler::new());

        let handle = {
            let scheduler = scheduler.clone();

            thread::Builder::new()
                .name("vacuum".into())
                .spawn(move || {
                    let runner = VacuumThread {
                        running_transactions,
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
