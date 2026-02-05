use tracing::{debug, info_span, instrument};

use crate::page::PAGE_DATA_SIZE;
use crate::storage::in_memory::block::Block;
use std::collections::HashMap;
use std::fmt::Display;
use std::pin::Pin;

use crate::platform::futex::{Futex, FutexError};
use crate::storage::in_memory::version_manager::{CowPage, MainPageRef, RawCowPage};
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
        self.is_done.as_ref().wake(1);
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
                    "(main: {}, cow: {:?}, verstion: {})",
                    match page.main {
                        crate::storage::in_memory::version_manager::RawMainPage::Initialized(
                            _,
                            index,
                        ) => format!("init({})", index.value()),
                        crate::storage::in_memory::version_manager::RawMainPage::Uninitialized(
                            _,
                            index,
                        ) => format!("uninit({})", index.value()),
                    },
                    page.cow.1.value(),
                    page.version
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

#[instrument(skip(pages))]
fn do_commit(id: TransactionId, pages: HashMap<PageIndex, CowPage>) -> Result<(), StorageError> {
    debug!("starting commit");

    let mut locks = HashMap::new();

    for (index, page) in &pages {
        match &page.main {
            MainPageRef::Initialized(page_ref) => {
                let lock = page_ref.lock();

                if lock.version() != page.version {
                    // TODO this is not a deadlock, but an optimistic concurrency race
                    return Err(StorageError::Deadlock(*index));
                }

                assert!(*index == page_ref.index());

                locks.insert(*index, lock);
            }
            MainPageRef::Uninitialized(_) => {}
        }
    }

    debug!("collcted {} locks for {} pages", locks.len(), pages.len());

    for (index, page) in pages {
        match page.main {
            MainPageRef::Initialized(_) => {
                // It's very tempting to change this `get_mut` to `remove`, but that would be
                // incorrect, as we'd be unlocking locks while still modifying the stored data.
                // We can only start unlocking after this loop is done.
                let lock = locks.get_mut(&index).unwrap();

                let mut modfied_copy = page.cow.lock();

                if index == PageIndex::zero() {
                    debug!(
                        "touching page zero. cow index: {:?}, index: {index:?}, page: {:?}, cow page: {:?}",
                        page.cow.index(),
                        &**lock,
                        &*modfied_copy
                    );
                }

                if (modfied_copy.data::<[u8; PAGE_DATA_SIZE.as_bytes()]>()
                    != lock.data::<[u8; PAGE_DATA_SIZE.as_bytes()]>())
                    // TODO do a full header comparison
                    || (modfied_copy.visible_until() != lock.visible_until())
                {
                    debug!("page {index:?} was modified, incrementing version");

                    modfied_copy.set_visible_from(id);
                    modfied_copy.increment_version();

                    **lock = *modfied_copy;

                    debug!("page {index:?} updated to: {:?}", &**lock);
                } else {
                    debug!("page {index:?} was not modified, leaving as is");
                    modfied_copy.set_visible_until(id);
                }
            }
            MainPageRef::Uninitialized(guard) => {
                assert!(guard.index() == index);

                let mut page = page.cow.lock();

                page.set_visible_from(id);

                debug!("initializing new page {index:?}");

                guard.initialize(*page);
            }
        }
    }

    debug!("commit succesful");

    Ok(())
}

impl Committer {
    pub(crate) fn new(block: Arc<Block>) -> Self {
        let (tx, rx) = mpsc::channel::<CommitRequest>();
        let handle = {
            thread::Builder::new()
                .name("committer".into())
                .spawn(move || {
                    while let Ok(mut request) = rx.recv() {
                        info_span!("transaction commit", %request).in_scope(|| {
                            let commit_result = do_commit(
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

        match is_done.as_ref().wait(0, None) {
            Ok(()) | Err(FutexError::Race) => {}
        }

        response.lock().unwrap().as_ref().unwrap().clone()
    }
}
