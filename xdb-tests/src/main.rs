mod random_threaded;

use std::{
    sync::Arc,
    thread::{self},
    time::Duration,
};
use tracing::{debug, error};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

use arbitrary::{Arbitrary, Unstructured};
use xdb::{
    bplustree::{Tree, TreeError, TreeTransaction},
    debug::BigKey,
    storage::{StorageError, in_memory::InMemoryStorage},
};

type KeyType = BigKey<u16, 1024>;

#[derive(Debug)]
struct Value(Vec<u8>);

impl<'a> Arbitrary<'a> for Value {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let mut buffer = vec![0u8; u.int_in_range(1..=128)?];

        u.fill_buffer(&mut buffer)?;

        Ok(Value(buffer))
    }
}

#[derive(Debug, Arbitrary)]
enum Command {
    Insert(KeyType, Value),
    Delete(KeyType),
    Read(KeyType),
}

#[derive(Debug, Arbitrary)]
struct TransactionCommands {
    commands: Vec<Command>,
    commit: bool,
}

// TODO this should stop being neccessary once transaction commits are single-threaded
fn retry_on_deadlock(
    tree: Arc<Tree<InMemoryStorage, KeyType>>,
    callable: impl Fn(TreeTransaction<InMemoryStorage, KeyType>) -> Result<(), TreeError>,
) -> Result<(), TreeError> {
    for i in 0..10 {
        let transaction = tree.transaction().unwrap();

        match callable(transaction) {
            Ok(ok) => return Ok(ok),
            Err(TreeError::StorageError(StorageError::Deadlock(_))) => {}
            error @ Err(_) => return error,
        };
        thread::sleep(Duration::from_millis(5));

        debug!("retrying: {i}");
    }

    // TODO we really should not ignore this
    error!("10 retries not succesful, giving up");

    Ok(())
}

// Add a separate "mod X" testing mode, where every thread operates only on keys that are
// (n%THREAD_COUNT)+thread_id, and verifies that it does not see anything from other threads.
fn main() {
    FmtSubscriber::builder()
        .with_thread_names(true)
        .with_env_filter(EnvFilter::from_default_env())
        .pretty()
        .with_writer(std::fs::File::create("log.txt").unwrap())
        .init();

    random_threaded::run();
}
