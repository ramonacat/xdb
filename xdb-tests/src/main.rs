use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender},
    },
    thread::{self, JoinHandle},
    time::{self, Duration},
};
use tracing::{debug, error};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

use arbitrary::{Arbitrary, Unstructured};
use rand::{Rng, rng};
use tracing::info;
use xdb::{
    bplustree::{
        Tree, TreeError, TreeTransaction,
        algorithms::{delete::delete, find, insert::insert},
        debug::assert_properties,
    },
    debug::BigKey,
    storage::{StorageError, in_memory::InMemoryStorage},
};

const THREAD_COUNT: usize = 16;
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

struct ServerThread {
    id: usize,
    tx: SyncSender<TransactionCommands>,
    handle: JoinHandle<()>,
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

fn server_thread(
    _id: usize,
    rx: Receiver<TransactionCommands>,
    tree: Arc<Tree<InMemoryStorage, KeyType>>,
) {
    while let Ok(TransactionCommands { commands, commit }) = rx.recv() {
        retry_on_deadlock(tree.clone(), |mut transaction| {
            for command in &commands {
                match command {
                    Command::Insert(key, value) => {
                        insert(&mut transaction, *key, &value.0).map(|_| ())?
                    }
                    Command::Delete(key) => {
                        delete(&mut transaction, *key).map(|_| ())?;
                    }
                    Command::Read(key) => find(&mut transaction, *key).map(|_| ())?,
                };
            }

            if commit {
                transaction.commit()?;
            } else {
                transaction.rollback()?;
            }

            Ok(())
        })
        .unwrap();
    }
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

    let storage = InMemoryStorage::new();
    let tree = Arc::new(Tree::new(storage).unwrap());

    let server_threads: Vec<_> = (0..THREAD_COUNT)
        .map(|id| {
            let (tx, rx) = mpsc::sync_channel(128);
            let tree = tree.clone();

            let handle = thread::Builder::new()
                .name(format!("server-{id:02}"))
                .spawn(move || {
                    server_thread(id, rx, tree);
                })
                .unwrap();

            ServerThread { id, tx, handle }
        })
        .collect();

    let stop = Arc::new(AtomicBool::new(false));

    let mut client_threads = vec![];

    for thread in server_threads {
        let stop = stop.clone();

        let handle = thread::Builder::new()
            .name(format!("client-{:02}", thread.id))
            .spawn(move || {
                let mut rng = rng();
                while !stop.load(Ordering::Relaxed) {
                    let mut buffer = [0u8; 1024];
                    rng.fill(&mut buffer);
                    let mut unstructured = Unstructured::new(&buffer);

                    let command = TransactionCommands::arbitrary(&mut unstructured).unwrap();
                    thread.tx.send(command).unwrap();
                }

                drop(thread.tx);

                thread.handle.join().unwrap();
            })
            .unwrap();

        client_threads.push(handle);
    }

    info!("threads started up, going to sleep");

    // TODO change this to a longer time, once we can handle running out of memory without
    // panicking
    let run_length = Duration::from_secs(60);
    let start = time::Instant::now();

    'outer: while time::Instant::now() - start < run_length {
        thread::sleep(Duration::from_secs(1));

        for thread in &client_threads {
            if thread.is_finished() {
                error!("thread finished prematurely, exiting...");

                stop.store(true, Ordering::Relaxed);
                break 'outer;
            }
        }
    }

    info!("wrapping up");

    stop.store(true, Ordering::Relaxed);

    for thread in client_threads {
        thread.join().unwrap();
    }

    info!("all threads stopped, checking tree properties...");

    let mut trx = tree.transaction().unwrap();
    assert_properties(&mut trx);
    trx.rollback().unwrap();

    info!("all done");
}
