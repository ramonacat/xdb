use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender},
    },
    thread::{self, JoinHandle},
    time::{self, Duration},
};
use xdb::bplustree::debug::assert_properties;

use arbitrary::{Arbitrary as _, Unstructured};
use rand::{Rng as _, rng};
use tracing::{error, info};
use xdb::{
    bplustree::{
        Tree,
        algorithms::{delete::delete, find, insert::insert},
    },
    storage::in_memory::InMemoryStorage,
};

use crate::{Command, KeyType, TransactionCommands, retry_on_deadlock};

const THREAD_COUNT: usize = 16;

struct ServerThread {
    id: usize,
    tx: SyncSender<TransactionCommands>,
    handle: JoinHandle<()>,
}

pub fn run() {
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
