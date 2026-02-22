use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Mutex;

use tracing::debug;

use crate::storage::{TransactionId, TransactionalTimestamp};
use crate::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
// TODO the data structures are very wacky here in general, we need to store the log in a Storage +
// figure out how to best keep an in-memory state
pub struct TransactionLog {
    next_timestamp: AtomicU64,
    running_transactions: Mutex<BTreeMap<TransactionalTimestamp, TransactionId>>,
}

impl TransactionLog {
    pub const fn new() -> Self {
        Self {
            next_timestamp: AtomicU64::new(1),
            running_transactions: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn start_transaction(&'_ self, id: TransactionId) -> StartedTransaction {
        let started = self.next_timestamp();

        StartedTransaction { id, started }
    }

    pub fn start_commit(&'_ self, transaction: StartedTransaction) -> CommitHandle<'_> {
        let timestamp = self.next_timestamp();

        CommitHandle {
            transaction,
            timestamp,
            log: self,
        }
    }

    pub fn minimum_active_timestamp(&self) -> Option<TransactionalTimestamp> {
        let running_transactions = self.running_transactions.lock().unwrap();

        debug!("running transactions: {}", running_transactions.len());
        running_transactions.first_key_value().map(|(k, _)| *k)
    }

    fn next_timestamp(&self) -> TransactionalTimestamp {
        TransactionalTimestamp(self.next_timestamp.fetch_add(1, Ordering::AcqRel))
    }

    pub fn rollback(&self, transaction: StartedTransaction) {
        self.running_transactions
            .lock()
            .unwrap()
            .remove(&transaction.started());
    }
}

pub struct CommitHandle<'log> {
    log: &'log TransactionLog,

    transaction: StartedTransaction,
    timestamp: TransactionalTimestamp,
}

impl Debug for CommitHandle<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitHandle")
            .field("transaction", &self.transaction)
            .field("timestamp", &self.timestamp)
            .finish_non_exhaustive()
    }
}

impl CommitHandle<'_> {
    pub const fn timestamp(&self) -> TransactionalTimestamp {
        self.timestamp
    }

    pub const fn started(&self) -> TransactionalTimestamp {
        self.transaction.started()
    }

    pub fn commit(self) {
        self.log
            .running_transactions
            .lock()
            .unwrap()
            .remove(&self.transaction.started());
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StartedTransaction {
    #[allow(unused)]
    id: TransactionId,
    started: TransactionalTimestamp,
}

impl StartedTransaction {
    pub const fn started(&self) -> TransactionalTimestamp {
        self.started
    }

    #[allow(unused)]
    pub const fn id(&self) -> TransactionId {
        self.id
    }
}
