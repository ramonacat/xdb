use std::time::Instant;

use tracing::info;
use xdb::{
    bplustree::{Tree, algorithms::find},
    storage::in_memory::InMemoryStorage,
};

use crate::{
    RUN_LENGTH, final_checks,
    predictable::{KEYS_PER_ITERATION, commands_for_iteration, expected_value_for_key},
    retry_on_deadlock,
};

pub fn run() {
    let storage = InMemoryStorage::new();
    let tree = Tree::<_, u64>::new(storage).unwrap();

    let start = Instant::now();

    info!("initialization completed, starting up...");

    let mut i = 0u64;
    while start.elapsed() < RUN_LENGTH {
        let transaction_commands = commands_for_iteration(i);

        retry_on_deadlock(&tree, |transaction| transaction_commands.run(transaction)).unwrap();

        if i.is_multiple_of(10000) && i > 0 {
            retry_on_deadlock(&tree, |mut transaction| {
                for j in
                    ((i.saturating_sub(5000)) * KEYS_PER_ITERATION)..((i - 1) * KEYS_PER_ITERATION)
                {
                    let found = find(&mut transaction, j)?;

                    assert_eq!(found, expected_value_for_key(j), "at key {j}, i: {i}");
                }

                transaction.commit().unwrap();
                Ok(())
            })
            .unwrap();
        }

        i += 1;
    }

    info!("time's up, wrapping up");

    final_checks(&tree);

    info!("test completed");
}
