use std::{collections::BTreeSet, pin::Pin, time::Duration};

use tracing::debug;

use crate::{
    platform::futex::{Futex, FutexError},
    storage::{
        PageIndex, TransactionId,
        in_memory::{Bitmap, block::Block},
    },
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
};

struct VacuumThread {
    running: Arc<AtomicBool>,
    freeze_requests: Pin<Arc<Futex>>,
    is_currently_frozen: Pin<Arc<Futex>>,
    running_transactions: Arc<Mutex<BTreeSet<TransactionId>>>,
    cow_copies: Arc<Block>,
    cow_copies_freemap: Arc<Bitmap>,
}

impl VacuumThread {
    fn freeze_if_needed(&self) {
        debug!("checking freeze requests");

        let freeze_requests = self.freeze_requests.as_ref();
        let is_currently_frozen = self.is_currently_frozen.as_ref();

        // TODO add a time limit for freezes?
        loop {
            let requests = freeze_requests.atomic().load(Ordering::Acquire);

            if requests == 0 {
                debug!("no freeze requests, continuing");
                is_currently_frozen.atomic().store(0, Ordering::Release);
                is_currently_frozen.wake(u32::MAX);

                break;
            }

            is_currently_frozen.atomic().store(1, Ordering::Release);
            let unfreezed = is_currently_frozen.wake(u32::MAX);

            debug!("waiting for {requests} freeze requests ({unfreezed} threads unfreezed)");
            match freeze_requests.wait(requests) {
                Ok(()) | Err(FutexError::Race) => {}
            }
        }

        debug!("exiting freeze_if_needed");
    }

    pub fn run(&self) {
        while self.running.load(Ordering::Relaxed) {
            self.freeze_if_needed();

            debug!("requesting running transactions");

            let running_transactions = self.running_transactions.lock().unwrap();
            let Some(min_txid) = running_transactions.first().copied() else {
                drop(running_transactions);
                // TODO we need a smarter way of scheduling vacuum (based on usage of the
                // block)
                thread::sleep(Duration::from_secs(10));
                continue;
            };
            drop(running_transactions);

            debug!("min txid: {min_txid:?}");

            let mut index = PageIndex::from_value(1);

            let mut i = 0u64;
            let mut freed_count = 0u64;

            let pages_to_check = self.cow_copies.page_count_lower_bound();
            debug!("vacuum will iterate over {pages_to_check} pages");
            while i < pages_to_check {
                // TODO this is another scheduling issue, if there's a lot of pages,
                // vacuum can spend a lot of time in this loop, preventing freezes and
                // preventing exit when done
                if i.is_multiple_of(10000) {
                    debug!("vacuum checking for freezes and exit requests");

                    self.freeze_if_needed();

                    if !self.running.load(Ordering::Relaxed) {
                        debug!("exit requested");
                        break;
                    }
                }

                i += 1;

                let Some(page) = self.cow_copies.try_get(index) else {
                    continue;
                };

                if let Ok(page) = page.lock_nowait()
                    && let Some(visible_until) = page.visible_until()
                    && visible_until < min_txid
                {
                    self.cow_copies_freemap.set(index.0).unwrap();
                    freed_count += 1;
                }

                index = index.next();
            }

            debug!("vacuum scan finished, {freed_count}/{i} pages marked as free");
        }

        debug!("exiting vacuum thread...");
    }
}

#[derive(Debug)]
pub struct Vacuum {
    running: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
    freeze_requests: Pin<Arc<Futex>>,
    is_currently_frozen: Pin<Arc<Futex>>,
}

impl Vacuum {
    pub fn start(
        running_transactions: Arc<Mutex<BTreeSet<TransactionId>>>,
        cow_copies: Arc<Block>,
        cow_copies_freemap: Arc<Bitmap>,
    ) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let freeze_requests = Arc::pin(Futex::new(0));
        let is_currently_frozen = Arc::pin(Futex::new(0));

        let handle = {
            let running = running.clone();
            let freeze_requests = freeze_requests.clone();
            let is_currently_frozen = is_currently_frozen.clone();

            thread::Builder::new()
                .name("vacuum".into())
                .spawn(move || {
                    let runner = VacuumThread {
                        running,
                        freeze_requests,
                        is_currently_frozen,
                        running_transactions,
                        cow_copies,
                        cow_copies_freemap,
                    };
                    runner.run();
                })
                .unwrap()
        };
        Self {
            running,
            handle: Some(handle),
            freeze_requests,
            is_currently_frozen,
        }
    }

    pub(crate) fn freeze(&'_ self) -> VacuumFreeze<'_> {
        debug!("requesting vacuum freeze");
        self.freeze_requests
            .as_ref()
            .atomic()
            .fetch_add(1, Ordering::AcqRel);
        self.freeze_requests.as_ref().wake(u32::MAX);

        let is_currently_frozen = self.is_currently_frozen.as_ref();
        debug!("not currently frozen, waiting...");

        loop {
            if is_currently_frozen.atomic().load(Ordering::Acquire) == 1 {
                break;
            }

            match is_currently_frozen.wait(0) {
                Ok(()) | Err(FutexError::Race) => {}
            }
        }

        debug!("freeze succesfuly started");

        VacuumFreeze { vacuum: self }
    }
}

pub struct VacuumFreeze<'vacuum> {
    vacuum: &'vacuum Vacuum,
}

impl Drop for VacuumFreeze<'_> {
    fn drop(&mut self) {
        let freeze_requests = self.vacuum.freeze_requests.as_ref();

        let before = freeze_requests.atomic().fetch_sub(1, Ordering::AcqRel);
        freeze_requests.wake(u32::MAX);

        debug!("freeze request dropped, from {before}");
    }
}

impl Drop for Vacuum {
    fn drop(&mut self) {
        debug!("dropping vacuum...");
        self.running.store(false, Ordering::Relaxed);

        let handle = self.handle.take();

        if let Some(handle) = handle {
            handle.join().unwrap();
        }
    }
}
