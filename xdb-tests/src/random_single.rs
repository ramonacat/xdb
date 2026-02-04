use std::time::Instant;

use rand::rng;
use tracing::info;
use xdb::{bplustree::Tree, storage::in_memory::InMemoryStorage};

use crate::{KeyType, RUN_LENGTH, TransactionCommands, final_checks, retry_on_deadlock};

pub fn run() {
    let storage = InMemoryStorage::new();
    let tree = Tree::<_, KeyType>::new(storage).unwrap();
    let mut rng = rng();

    let start = Instant::now();

    info!("initialization completed, starting up...");

    while start.elapsed() < RUN_LENGTH {
        let commands = TransactionCommands::new_random(&mut rng);

        retry_on_deadlock(&tree, |transaction| commands.run(transaction)).unwrap();
    }

    info!("time's up, wrapping up");

    final_checks(&tree);

    info!("test completed");
}
