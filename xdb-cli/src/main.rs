#![deny(clippy::all, clippy::pedantic, clippy::nursery, warnings)]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use xdb::storage::instrumented::InstrumentedStorage;

use xdb::{
    bplustree::{
        Tree,
        algorithms::{delete::delete, insert::insert},
    },
    debug::BigKey,
    storage::in_memory::InMemoryStorage,
};

fn main() {
    env_logger::init();

    let page_count = Arc::new(AtomicUsize::new(0));

    let storage = InstrumentedStorage::new(InMemoryStorage::new(), page_count.clone());
    let tree = Tree::new(storage).unwrap();
    let mut transaction = tree.transaction().unwrap();

    let mut i: usize = 0;
    while page_count.load(Ordering::Relaxed) < 5000 {
        insert(
            &mut transaction,
            BigKey::<u64, 512>::new(u64::try_from(i).unwrap()),
            &(u16::MAX - u16::try_from(i).unwrap())
                .to_be_bytes()
                .repeat(64),
        )
        .unwrap();

        i += 1;
    }

    for j in i / 2..0 {
        delete(&mut transaction, BigKey::new(u64::try_from(j).unwrap())).unwrap();
    }

    transaction.commit().unwrap();

    println!("{}", tree.to_dot(|x| format!("({})", x.len())).unwrap());
}
