use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Mutex;

use tracing::debug;

use crate::storage::TransactionalTimestamp;
use crate::sync::atomic::AtomicU64;
use crate::sync::atomic::Ordering;

use crate::storage::TransactionId;

#[derive(Debug)]
// TODO the data structures are very wacky here in general, we need to store the log in a Storage +
// figure out how to best keep an in-memory state
pub struct TransactionLog {
    next_timestamp: AtomicU64,
    // TODO we'll need an on-disk format for this
    // TODO can we get rid of the Mutex?
    transactions: Mutex<HashMap<TransactionId, TransactionLogEntry>>,
    running_transactions: Mutex<BTreeMap<TransactionalTimestamp, TransactionId>>,
}

impl TransactionLog {
    pub fn new() -> Self {
        Self {
            next_timestamp: AtomicU64::new(1),
            transactions: Mutex::new(HashMap::new()),
            running_transactions: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn start_transaction(&'_ self, id: TransactionId) -> TransactionLogEntryHandle<'_> {
        let started = self.next_timestamp();

        self.running_transactions
            .lock()
            .unwrap()
            .insert(started, id);

        let previous = self.transactions.lock().unwrap().insert(
            id,
            TransactionLogEntry {
                id,
                started,
                state: TransactionState::Started,
            },
        );
        assert!(previous.is_none());

        TransactionLogEntryHandle {
            id,
            log: self,
            start_timestamp: started,
        }
    }

    pub fn get_handle(&self, id: TransactionId) -> Option<TransactionLogEntryHandle<'_>> {
        let start_timestamp = self.transactions.lock().unwrap().get(&id)?.started;

        Some(TransactionLogEntryHandle {
            id,
            start_timestamp,
            log: self,
        })
    }

    pub fn start_commit(&'_ self, id: TransactionId) -> Option<CommitHandle<'_>> {
        let mut transactions = self.transactions.lock().unwrap();
        let transaction = transactions.get_mut(&id)?;
        let timestamp = self.next_timestamp();

        transaction.start_commit(timestamp);

        Some(CommitHandle {
            id,
            started: transaction.started,
            timestamp,
            log: self,
        })
    }

    pub fn minimum_active_timestamp(&self) -> Option<TransactionalTimestamp> {
        let running_transactions = self.running_transactions.lock().unwrap();

        debug!("running transactions: {}", running_transactions.len());
        running_transactions.first_key_value().map(|(k, _)| *k)
    }

    fn next_timestamp(&self) -> TransactionalTimestamp {
        TransactionalTimestamp(self.next_timestamp.fetch_add(1, Ordering::AcqRel))
    }

    pub fn rollback(&self, id: TransactionId) {
        let mut transactions = self.transactions.lock().unwrap();
        let transaction = transactions.remove(&id).unwrap();
        drop(transactions);

        self.running_transactions
            .lock()
            .unwrap()
            .remove(&transaction.started);
    }
}

pub struct CommitHandle<'log> {
    log: &'log TransactionLog,

    id: TransactionId,
    timestamp: TransactionalTimestamp,
    started: TransactionalTimestamp,
}

impl Debug for CommitHandle<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitHandle")
            .field("id", &self.id)
            .field("timestamp", &self.timestamp)
            .field("started", &self.started)
            .finish_non_exhaustive()
    }
}

impl CommitHandle<'_> {
    pub const fn timestamp(&self) -> TransactionalTimestamp {
        self.timestamp
    }

    pub const fn started(&self) -> TransactionalTimestamp {
        self.started
    }

    pub fn commit(self) {
        {
            let mut transactions = self.log.transactions.lock().unwrap();
            transactions.get_mut(&self.id).unwrap().commit();

            // TODO we really should not do this in reality, but the log is in memory, so we can't
            // really keep all of it
            transactions.remove(&self.id);
        }

        self.log
            .running_transactions
            .lock()
            .unwrap()
            .remove(&self.started);
    }
}

#[derive(Debug)]
enum TransactionState {
    Started,
    CommitStarted {
        timestamp: TransactionalTimestamp,
    },
    #[allow(unused)]
    Committed {
        timestamp: TransactionalTimestamp,
    },
}

#[derive(Debug)]
#[allow(unused)]
struct TransactionLogEntry {
    id: TransactionId,
    started: TransactionalTimestamp,
    state: TransactionState,
}

// TODO there's surely a less wack way of implementing a state machine? At least return Result<>
// instead of panicking
impl TransactionLogEntry {
    fn start_commit(&mut self, timestamp: TransactionalTimestamp) {
        match self.state {
            TransactionState::Started => {
                self.state = TransactionState::CommitStarted { timestamp };
            }
            TransactionState::CommitStarted { .. } => panic!("commit already started"),
            TransactionState::Committed { .. } => panic!("already committed"),
        }
    }

    fn commit(&mut self) {
        match self.state {
            TransactionState::Started => panic!("commit not started yet"),
            TransactionState::CommitStarted { timestamp } => {
                self.state = TransactionState::Committed { timestamp }
            }
            TransactionState::Committed { .. } => panic!("already committed"),
        }
    }
}

pub struct TransactionLogEntryHandle<'log> {
    id: TransactionId,
    start_timestamp: TransactionalTimestamp,
    log: &'log TransactionLog,
}

impl Debug for TransactionLogEntryHandle<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionLogEntryHandle")
            .field("id", &self.id)
            .field("start_timestamp", &self.start_timestamp)
            .finish()
    }
}

impl TransactionLogEntryHandle<'_> {
    pub const fn start_timestamp(&self) -> TransactionalTimestamp {
        self.start_timestamp
    }

    // TODO this is not how rollbacks should be... we need to save them in the log anyway
    pub fn rollback(&self) {
        self.log.transactions.lock().unwrap().remove(&self.id);
        self.log
            .running_transactions
            .lock()
            .unwrap()
            .remove(&self.start_timestamp());
    }
}
