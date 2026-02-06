use crate::{Command, TransactionCommands, Value};

pub mod single;
pub mod threaded;

const KEYS_PER_ITERATION: u64 = 64;

// TODO sprinkle some deletes as well
fn commands_for_iteration(i: u64) -> TransactionCommands<u64> {
    let mut commands = vec![];

    for j in (i * KEYS_PER_ITERATION)..((i + 1) * KEYS_PER_ITERATION) {
        commands.push(Command::Insert(j, Value(vec![11; (j % 32) as usize])));
    }

    commands.push(Command::Delete((i - 1) * KEYS_PER_ITERATION));

    TransactionCommands {
        commands,
        commit: !i.is_multiple_of(10),
    }
}

fn expected_value_for_key(key: u64) -> Option<Vec<u8>> {
    let iteration = key / KEYS_PER_ITERATION;

    let insert_committed = !iteration.is_multiple_of(10);
    let delete_committed = !(iteration + 1).is_multiple_of(10);

    let deleted = key.is_multiple_of(KEYS_PER_ITERATION) && delete_committed;

    if !deleted && insert_committed {
        Some(vec![11; (key % 32) as usize])
    } else {
        None
    }
}
