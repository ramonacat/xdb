use crate::storage::in_memory::version_manager::{
    TransactionPage, TransactionPageAction, get_matching_version,
};
use tracing::{debug, info_span, instrument, trace};

use crate::storage::in_memory::block::Block;
use crate::storage::in_memory::version_manager::transaction_log::{CommitHandle, TransactionLog};
use std::collections::HashMap;
use std::fmt::Display;
use std::pin::Pin;

use crate::platform::futex::Futex;
use crate::storage::{PageIndex, StorageError, TransactionId};
use crate::{
    sync::{
        Arc, Mutex,
        atomic::Ordering,
        mpsc::{self, Sender},
    },
    thread::{self, JoinHandle},
};

#[derive(Debug)]
pub struct CommitRequest {
    is_done: Pin<Arc<Futex>>,
    response: Arc<Mutex<Option<Result<(), StorageError>>>>,
    id: TransactionId,
    pages: HashMap<PageIndex, TransactionPage>,
}

impl CommitRequest {
    fn respond(self, response: Result<(), StorageError>) {
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
            self.id,
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

        if self.pages.len() < 100 {
            for page in self.pages.values() {
                write!(f, "{page:?}",)?;
            }
        } else {
            write!(f, "{} pages", self.pages.len()).unwrap();
        }

        write!(f, "]")?;

        Ok(())
    }
}

#[derive(Debug)]
struct CommitterThread<'log, 'storage> {
    log: &'log TransactionLog,
    block: &'storage Block,
}

impl CommitterThread<'_, '_> {
    fn rollback(&self, pages: HashMap<PageIndex, TransactionPage>, commit_handle: CommitHandle) {
        for page in pages.into_values() {
            let to_free = match page.action {
                TransactionPageAction::Read | TransactionPageAction::Delete => None,
                TransactionPageAction::Insert => Some(
                    self.block
                        .get(Some(page.logical_index), page.logical_index)
                        .lock(),
                ),
                TransactionPageAction::Update(cow) => {
                    Some(self.block.get(Some(page.logical_index), cow).lock())
                }
            };

            if let Some(mut lock) = to_free {
                debug!(
                    physical_index = ?lock.physical_index(),
                    logical_index = ?lock.logical_index(),
                    "clearing page"
                );

                lock.mark_free();
            }
        }

        commit_handle.rollback();
    }

    #[instrument(skip(pages))]
    fn commit(
        &self,
        id: TransactionId,
        pages: HashMap<PageIndex, TransactionPage>,
    ) -> Result<(), StorageError> {
        let commit_handle = self.log.start_commit(id).unwrap();
        let mut locks = HashMap::new();

        let mut rollback = None;

        for (index, page) in &pages {
            let lock =
                get_matching_version(self.block, page.logical_index, commit_handle.started())
                    .lock();

            if lock.next_version().is_some() {
                debug!(
                    physical_index = ?lock.physical_index(),
                    logical_index = ?lock.logical_index(),
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

            self.rollback(pages, commit_handle);

            // TODO this is not a deadlock, but an optimistic concurrency race
            return Err(StorageError::Deadlock(rollback_index));
        }

        trace!(
            lock_count = locks.len(),
            page_count = pages.len(),
            "collected locks for pages"
        );

        for (index, page) in pages {
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
                    let cow_page = self.block.get(Some(page.logical_index), cow);
                    debug!(
                        logical_index = ?page.logical_index,
                        cow.physical_index = ?cow_page.physical_index(),
                        logical_index=?index, "updated"
                    );

                    let mut cow_lock = cow_page.lock();

                    lock.set_next_version(Some(cow_page.physical_index()));
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
    pub(crate) fn new(block: Arc<Block>, log: Arc<TransactionLog>) -> Self {
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
                        info_span!("transaction commit", id = ?request.id, %request).in_scope(
                            || {
                                let commit_result = thread.commit(
                                    // TODO just pass the whole request???
                                    request.id,
                                    request.take_pages(),
                                );

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
        id: TransactionId,
        pages: HashMap<PageIndex, TransactionPage>,
    ) -> Result<(), StorageError> {
        let is_done = Arc::pin(Futex::new(0));
        let response = Arc::new(Mutex::new(None));
        self.tx
            .send(CommitRequest {
                is_done: is_done.clone(),
                response: response.clone(),
                id,
                pages,
            })
            .unwrap();

        is_done.as_ref().wait(0, None);

        response.lock().unwrap().as_ref().unwrap().clone()
    }
}
