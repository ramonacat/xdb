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
    }

    pub fn run(&self) {
        while self.running.load(Ordering::Relaxed) {
            self.freeze_if_needed();

            let running_transactions = self.running_transactions.lock().unwrap();
            let Some(min_txid) = running_transactions.first().copied() else {
                // TODO we need a smarter way of scheduling vacuum (based on usage of the
                // block)
                thread::sleep(Duration::from_millis(10));
                continue;
            };
            drop(running_transactions);

            let mut index = PageIndex::from_value(1);

            let mut i = 0u64;
            let mut freed_count = 0u64;

            while let Some(page) = self.cow_copies.try_get(index) {
                if let Ok(page) = page.lock_nowait()
                    && let Some(visible_until) = page.visible_until()
                    && visible_until < min_txid
                {
                    self.cow_copies_freemap.set(index.0).unwrap();
                    freed_count += 1;
                }

                // TODO this is another scheduling issue, if there's a lot of pages,
                // vacuum can spend a lot of time in this loop, preventing freezes and
                // preventing exit when done
                if i.is_multiple_of(10000) {
                    debug!("vacuum checking for freezes and exit requests");

                    self.freeze_if_needed();

                    if !self.running.load(Ordering::Relaxed) {
                        break;
                    }

                    break;
                }

                i += 1;
                index = index.next();
            }

            debug!("vacuum scan finished, {freed_count}/{i} pages marked as free");
        }
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
        self.freeze_requests
            .as_ref()
            .atomic()
            .fetch_add(1, Ordering::AcqRel);
        self.freeze_requests.as_ref().wake(u32::MAX);

        let is_currently_frozen = self.is_currently_frozen.as_ref();

        loop {
            if is_currently_frozen.atomic().load(Ordering::Acquire) == 1 {
                break;
            }

            match is_currently_frozen.wait(0) {
                Ok(()) | Err(FutexError::Race) => {}
            }
        }

        VacuumFreeze { vacuum: self }
    }
}

pub struct VacuumFreeze<'vacuum> {
    vacuum: &'vacuum Vacuum,
}

impl Drop for VacuumFreeze<'_> {
    fn drop(&mut self) {
        let freeze_requests = self.vacuum.freeze_requests.as_ref();

        freeze_requests.atomic().fetch_sub(1, Ordering::AcqRel);
        freeze_requests.wake(u32::MAX);
    }
}

impl Drop for Vacuum {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);

        let handle = self.handle.take();

        if let Some(handle) = handle {
            handle.join().unwrap();
        }
    }
}
