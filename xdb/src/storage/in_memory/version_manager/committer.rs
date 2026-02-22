use std::collections::HashMap;
use std::fmt::Display;
use std::pin::Pin;

use tracing::{debug, info_span, instrument, record_all, trace};

use crate::platform::futex::Futex;
use crate::storage::in_memory::InMemoryPageId;
use crate::storage::in_memory::version_manager::transaction_log::{
    StartedTransaction, TransactionLog,
};
use crate::storage::in_memory::version_manager::{
    TransactionPage, TransactionPageAction, VersionedBlock,
};
use crate::storage::{PageIndex, StorageError};
use crate::sync::atomic::Ordering;
use crate::sync::mpsc::{self, Sender};
use crate::sync::{Arc, Mutex};
use crate::thread::{self, JoinHandle};

#[derive(Debug)]
pub struct CommitRequest {
    is_done: Pin<Arc<Futex>>,
    #[allow(clippy::type_complexity)]
    response: Arc<Mutex<Option<Result<(), StorageError<InMemoryPageId>>>>>,

    pages: HashMap<PageIndex, TransactionPage>,
    transaction: StartedTransaction,
}

impl CommitRequest {
    fn respond(self, response: Result<(), StorageError<InMemoryPageId>>) {
        *self.response.lock().unwrap() = Some(response);

        self.is_done.as_ref().atomic().store(1, Ordering::Release);
        self.is_done.as_ref().wake_one();
    }

    fn take_pages(&mut self) -> HashMap<PageIndex, TransactionPage> {
        self.pages.drain().collect()
    }
}

impl Display for CommitRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "commit request: {:?} ({} {}) [",
            self.transaction,
            if self.is_done.as_ref().atomic().load(Ordering::Relaxed) == 1 {
                "done "
            } else {
                "not done "
            },
            self.response.lock().map_or_else(
                |_| "[poisoned!]".to_string(),
                |x| x
                    .as_ref()
                    .map_or_else(|| "[none]".to_string(), |y| format!("{y:?}"))
            ),
        )?;

        write!(f, "{} pages]", self.pages.len())?;

        Ok(())
    }
}

#[derive(Debug)]
struct CommitterThread<'log, 'storage> {
    log: &'log TransactionLog,
    block: &'storage VersionedBlock,
}

impl CommitterThread<'_, '_> {
    fn rollback(
        &self,
        pages: HashMap<PageIndex, TransactionPage>,
        transaction: StartedTransaction,
    ) {
        for page in pages.into_values() {
            let to_free = match page.action {
                TransactionPageAction::Read | TransactionPageAction::Delete => None,
                TransactionPageAction::Insert => Some(self.block.get(page.logical_index).upgrade()),
                TransactionPageAction::Update(cow) => Some(self.block.get(cow).upgrade()),
            };

            if let Some(mut lock) = to_free {
                debug!(
                    physical_index = ?lock.physical_index(),
                    logical_index = ?page.logical_index,
                    "clearing page"
                );

                lock.mark_free();
            }
        }

        self.log.rollback(transaction);
    }

    #[instrument(skip(self, pages), fields(started, timestamp))]
    fn commit(
        &self,
        transaction: StartedTransaction,
        pages: HashMap<PageIndex, TransactionPage>,
    ) -> Result<(), StorageError<InMemoryPageId>> {
        let mut locks = HashMap::new();

        let mut rollback = None;

        for (index, page) in &pages {
            let lock = self
                .block
                .get_at(page.logical_index, transaction.started())
                .upgrade();

            if lock.next_version().is_some() {
                debug!(
                    physical_index = ?lock.physical_index(),
                    logical_index = ?page.logical_index,
                    next_version = ?lock.next_version(),
                    "rolling back, conflict"
                );

                rollback = Some(*index);

                break;
            }

            locks.insert(*index, lock);
        }

        if let Some(rollback_index) = rollback {
            drop(locks);

            self.rollback(pages, transaction);

            // TODO this is not a deadlock, but an optimistic concurrency race
            return Err(StorageError::Deadlock(InMemoryPageId(rollback_index)));
        }

        trace!(
            lock_count = locks.len(),
            page_count = pages.len(),
            "collected locks for pages"
        );

        // It is very important that we only start the commit in the log after we've taken the
        // locks. Otherwise, another commit could change the pages in the time between us taking
        // the lock and assigning a commit timestamp, which would cause inconsistencies.
        let commit_handle = self.log.start_commit(transaction);

        record_all!(tracing::Span::current(), started = ?commit_handle.started(), timestamp = ?commit_handle.timestamp());

        for (index, page) in pages {
            let _ = info_span!("committing page", logical_index=?index, ?page).entered();

            assert!(index == page.logical_index);

            // It's very tempting to change this `get_mut` to `remove`, but that would be
            // incorrect, as we'd be unlocking locks while still modifying the stored data.
            // We can only start unlocking after this loop is done.
            let lock = locks.get_mut(&index).unwrap();

            match page.action {
                TransactionPageAction::Read => {
                    trace!(
                        logical_index = ?page.logical_index,
                        "page not modified"
                    );
                }
                TransactionPageAction::Delete => {
                    debug!(
                        logical_index = ?page.logical_index,
                        "deleted"
                    );

                    lock.set_visible_until(Some(commit_handle.timestamp()));
                }
                TransactionPageAction::Update(cow) => {
                    let cow_page = self.block.get(cow);
                    debug!(
                        logical_index = ?page.logical_index,
                        cow.physical_index = ?cow_page.physical_index(),
                        logical_index=?index,
                        main.visible_until = ?commit_handle.timestamp(),
                        "updated"
                    );

                    let mut cow_lock = cow_page.upgrade();

                    lock.set_next_version(Some(cow_lock.physical_index()));
                    lock.set_visible_until(Some(commit_handle.timestamp()));

                    cow_lock.set_visible_from(Some(commit_handle.timestamp()));
                    cow_lock.set_visible_until(None);
                    cow_lock.set_previous_version(Some(lock.physical_index()));
                    cow_lock.set_next_version(None);
                }
                TransactionPageAction::Insert => {
                    debug!(
                        logical_index = ?page.logical_index,
                        "inserted"
                    );

                    lock.set_visible_from(Some(commit_handle.timestamp()));
                    lock.set_visible_until(None);
                }
            }
        }

        commit_handle.commit();

        drop(locks);

        debug!("commit completed");

        Ok(())
    }
}

#[derive(Debug)]
pub struct Committer {
    #[allow(unused)]
    handle: Option<JoinHandle<()>>,
    tx: Sender<CommitRequest>,
}

impl Committer {
    pub(crate) fn new(block: Arc<VersionedBlock>, log: Arc<TransactionLog>) -> Self {
        let (tx, rx) = mpsc::channel::<CommitRequest>();
        let handle = {
            thread::Builder::new()
                .name("committer".into())
                .spawn(move || {
                    let thread = CommitterThread {
                        log: &log,
                        block: &block,
                    };
                    while let Ok(mut request) = rx.recv() {
                        info_span!("transaction commit", transaction = ?request.transaction, %request).in_scope(
                            || {
                                let commit_result = thread.commit(request.transaction, request.take_pages());

                                request.respond(commit_result);
                            },
                        );
                    }
                })
                .unwrap()
        };

        Self {
            handle: Some(handle),
            tx,
        }
    }

    pub fn request(
        &self,
        transaction: StartedTransaction,
        pages: HashMap<PageIndex, TransactionPage>,
    ) -> Result<(), StorageError<InMemoryPageId>> {
        let is_done = Arc::pin(Futex::new(0));
        let response = Arc::new(Mutex::new(None));
        self.tx
            .send(CommitRequest {
                is_done: is_done.clone(),
                response: response.clone(),
                transaction,
                pages,
            })
            .unwrap();

        is_done.as_ref().wait(0, None);

        response.lock().unwrap().as_ref().unwrap().clone()
    }
}
