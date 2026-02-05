use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, Sender},
    },
    thread,
    time::{Duration, Instant},
};

use tracing::{info, instrument};
use xdb::{
    bplustree::{Tree, algorithms::find},
    storage::in_memory::InMemoryStorage,
};

use crate::{
    RUN_LENGTH, THREAD_COUNT, final_checks,
    predictable::{commands_for_iteration, expected_value_for_key},
    retry_on_deadlock,
};

const MILESTONE_EACH: u64 = 500;

#[instrument(skip(tree, tx))]
fn test_thread(
    thread_id: u64,
    stop: Arc<AtomicBool>,
    tree: Arc<Tree<InMemoryStorage, u64>>,
    tx: Sender<u64>,
) {
    let mut i: u64 = thread_id;

    while !stop.load(Ordering::Relaxed) {
        let commands = commands_for_iteration(i);
        retry_on_deadlock(&tree, |transaction| commands.run(transaction)).unwrap();

        if (i - thread_id).is_multiple_of(MILESTONE_EACH) {
            tx.send(i).unwrap();
        }

        i += THREAD_COUNT as u64;
    }
}

#[instrument(skip(tree, rx))]
fn checker_thread(tree: Arc<Tree<InMemoryStorage, u64>>, rx: Receiver<u64>) {
    while let Ok(milestone) = rx.recv() {
        info!("checking milestone {milestone}");

        let mut transaction = tree.transaction().unwrap();

        let key_start = milestone - (MILESTONE_EACH);
        let key_end = milestone;

        for j in (key_start..key_end).step_by(THREAD_COUNT) {
            let found = find(&mut transaction, j).unwrap();

            // TODO we should also check for deletes here
            if let expected @ Some(_) = expected_value_for_key(j)
                && found.is_some()
            {
                assert_eq!(found, expected, "at key {j}, milestone {milestone}");
            }
        }

        if (milestone / MILESTONE_EACH).is_multiple_of(2) {
            transaction.rollback().unwrap();
        } else {
            transaction.commit().unwrap();
        }

        info!("milestone {milestone} checked correctly!");
    }
}

pub fn run() {
    let storage = InMemoryStorage::new();
    let tree = Arc::new(Tree::<_, u64>::new(storage).unwrap());
    let stop = Arc::new(AtomicBool::new(false));
    let (tx, rx) = mpsc::channel();

    info!("initialization completed, starting up...");

    let threads = (0..THREAD_COUNT)
        .map(|thread_id| {
            let tree = tree.clone();
            let stop = stop.clone();
            let tx = tx.clone();

            thread::Builder::new()
                .name(format!("worker-{thread_id:02}"))
                .spawn(move || {
                    test_thread(thread_id as u64, stop, tree, tx);
                })
                .unwrap()
        })
        .collect::<Vec<_>>();

    let checker_thread = {
        let tree = tree.clone();
        thread::Builder::new()
            .name("checker".into())
            .spawn(move || {
                checker_thread(tree, rx);
            })
            .unwrap()
    };

    let start = Instant::now();

    while start.elapsed() < RUN_LENGTH {
        thread::sleep(Duration::from_secs(10));
    }

    info!("time's up, wrapping up");
    stop.store(true, Ordering::Relaxed);

    for thread in threads {
        thread.join().unwrap();
    }

    drop(tx);

    checker_thread.join().unwrap();

    final_checks(&tree);

    info!("test completed");
}
