use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender},
    },
    thread::{self, JoinHandle},
    time::{self, Duration},
};

use rand::rng;
use tracing::{error, info, info_span};
use xdb::{bplustree::Tree, storage::in_memory::InMemoryStorage};

use crate::{
    KeyType, RUN_LENGTH, THREAD_COUNT, TransactionCommands, final_checks, retry_on_deadlock,
};

struct ServerThread {
    id: usize,
    tx: SyncSender<TransactionCommands<KeyType>>,
    handle: JoinHandle<()>,
}

// TODO do we really need separate client/server threads?
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
                    let command = TransactionCommands::new_random(&mut rng);
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
    let start = time::Instant::now();

    'outer: while time::Instant::now() - start < RUN_LENGTH {
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

    final_checks(&tree);

    info!("all done");
}

fn server_thread(
    _id: usize,
    rx: Receiver<TransactionCommands<KeyType>>,
    tree: Arc<Tree<InMemoryStorage, KeyType>>,
) {
    while let Ok(commands) = rx.recv() {
        info_span!("transaction").in_scope(|| {
            retry_on_deadlock(&tree, |transaction| {
                commands.run(transaction)?;

                Ok(())
            })
            .unwrap();
        });
    }
}
