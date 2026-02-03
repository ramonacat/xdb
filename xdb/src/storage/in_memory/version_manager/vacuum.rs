use std::{collections::BTreeSet, sync::atomic::AtomicBool};

use tracing::debug;

use crate::{
    storage::{
        PageIndex, TransactionId,
        in_memory::{Bitmap, block::Block},
    },
    sync::{Arc, Mutex, atomic::Ordering},
    thread::{self, JoinHandle},
};

#[derive(Debug)]
pub struct Vacuum {
    running: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl Vacuum {
    pub fn start(
        running_transactions: Arc<Mutex<BTreeSet<TransactionId>>>,
        cow_copies: Arc<Block>,
        cow_copies_freemap: Arc<Bitmap>,
    ) -> Self {
        let running = Arc::new(AtomicBool::new(true));

        let handle = {
            let running = running.clone();
            thread::Builder::new()
                .name("vacuum".into())
                .spawn(move || {
                    while running.load(Ordering::Relaxed) {
                        let running_transactions = running_transactions.lock().unwrap();
                        let Some(min_txid) = running_transactions.first().copied() else {
                            // TODO we need a smarter way of scheduling vacuum (based on usage of the
                            // block)
                            thread::yield_now();
                            continue;
                        };
                        drop(running_transactions);

                        let mut index = PageIndex::from_value(1);

                        let mut returned = 0u64;

                        while let Some(page) = cow_copies.try_get(index) {
                            if let Ok(page) = page.lock_nowait()
                                && let Some(visible_until) = page.visible_until()
                                && visible_until < min_txid
                            {
                                cow_copies_freemap.set(index.0).unwrap();
                                returned += 1;
                            }

                            index = index.next();
                        }

                        debug!("vacuum scan finished, returned to freemap: {returned:?} pages");
                    }
                })
                .unwrap()
        };
        Self {
            running,
            handle: Some(handle),
        }
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
