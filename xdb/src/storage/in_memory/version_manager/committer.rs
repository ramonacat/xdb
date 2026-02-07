use tracing::{debug, info_span, instrument};

use crate::storage::in_memory::block::Block;
use crate::storage::in_memory::version_manager::transaction_log::TransactionLog;
use std::collections::HashMap;
use std::fmt::Display;
use std::pin::Pin;

use crate::platform::futex::Futex;
use crate::storage::in_memory::version_manager::{CowPage, RawCowPage};
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
    pages: HashMap<PageIndex, RawCowPage>,
}

impl CommitRequest {
    fn respond(self, response: Result<(), StorageError>) {
        *self.response.lock().unwrap() = Some(response);

        self.is_done.as_ref().atomic().store(1, Ordering::Release);
        self.is_done.as_ref().wake_one();
    }

    fn take_pages(&mut self) -> HashMap<PageIndex, RawCowPage> {
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
                "done"
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
                write!(
                    f,
                    "(main: {:?}, cow: {:?}) ",
                    page.main,
                    page.cow.map(|x| x.1),
                )?;
            }
        } else {
            write!(f, "{} pages", self.pages.len()).unwrap();
        }

        write!(f, "]")?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct Committer {
    #[allow(unused)]
    handle: Option<JoinHandle<()>>,
    tx: Sender<CommitRequest>,
}

#[instrument(skip(pages, log))]
fn do_commit(
    log: &TransactionLog,
    id: TransactionId,
    pages: HashMap<PageIndex, CowPage>,
) -> Result<(), StorageError> {
    let commit_handle = log.start_commit(id).unwrap();
    debug!("starting commit: {commit_handle:?}");

    let mut locks = HashMap::new();

    for (index, page) in &pages {
        let lock = page.main.lock();

        if lock.next_version().is_some() {
            debug!("rolling back, conflict");
            commit_handle.rollback();
            // TODO this is not a deadlock, but an optimistic concurrency race
            return Err(StorageError::Deadlock(*index));
        }

        locks.insert(*index, lock);
    }

    debug!("collected {} locks for {} pages", locks.len(), pages.len());

    for (index, page) in pages {
        // It's very tempting to change this `get_mut` to `remove`, but that would be
        // incorrect, as we'd be unlocking locks while still modifying the stored data.
        // We can only start unlocking after this loop is done.
        let lock = locks.get_mut(&index).unwrap();

        if page.deleted {
            debug!("page {index:?} deleted");

            lock.set_visible_until(Some(commit_handle.timestamp()));

            if let Some(cow) = page.cow {
                let mut cow = cow.lock();

                cow.set_visible_until(Some(commit_handle.timestamp()));
                cow.set_next_version(None);
                cow.set_previous_version(None);
            }
        } else if let Some(cow) = page.cow {
            debug!("page {index:?} was modified, updating...");

            let mut cow_lock = cow.lock();

            assert!(cow.logical_index() == Some(index));
            debug!(
                "setting next version for {:?} to {:?}",
                index,
                cow.physical_index()
            );

            lock.set_next_version(Some(cow.physical_index()));
            lock.set_visible_until(Some(commit_handle.timestamp()));

            cow_lock.set_visible_from(Some(commit_handle.timestamp()));
            cow_lock.set_visible_until(None);
            cow_lock.set_previous_version(Some(page.main.physical_index()));
            cow_lock.set_next_version(None);

            debug!(
                "page {index:?} updated to point at: {:?}/{:?}",
                cow.logical_index(),
                cow.physical_index()
            );
        } else if page.inserted {
            lock.set_visible_from(Some(commit_handle.timestamp()));
            lock.set_visible_until(None);
        } else {
            debug!("page {index:?} was not modified, leaving as is");
        }
    }

    commit_handle.commit();

    debug!("commit succesful");

    Ok(())
}

impl Committer {
    pub(crate) fn new(block: Arc<Block>, log: Arc<TransactionLog>) -> Self {
        let (tx, rx) = mpsc::channel::<CommitRequest>();
        let handle = {
            thread::Builder::new()
                .name("committer".into())
                .spawn(move || {
                    while let Ok(mut request) = rx.recv() {
                        info_span!("transaction commit", %request).in_scope(|| {
                            let commit_result = do_commit(
                                &log,
                                request.id,
                                request
                                    .take_pages()
                                    .into_iter()
                                    .map(|(k, v)| (k, unsafe { v.reconstruct(&block) }))
                                    .collect(),
                            );

                            request.respond(commit_result);
                        });
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
        pages: HashMap<PageIndex, CowPage>,
    ) -> Result<(), StorageError> {
        let is_done = Arc::pin(Futex::new(0));
        let response = Arc::new(Mutex::new(None));
        self.tx
            .send(CommitRequest {
                is_done: is_done.clone(),
                response: response.clone(),
                id,
                pages: pages.into_iter().map(|(k, v)| (k, v.into_raw())).collect(),
            })
            .unwrap();

        is_done.as_ref().wait(0, None);

        response.lock().unwrap().as_ref().unwrap().clone()
    }
}
