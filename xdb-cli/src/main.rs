// TODO enable clippy pedantic
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use xdb::{
    bplustree::{Tree, algorithms::insert::insert},
    debug::BigKey,
    storage::in_memory::{InMemoryStorage, test::TestStorage},
};

fn main() {
    let page_count = Arc::new(AtomicUsize::new(0));

    let storage = TestStorage::new(InMemoryStorage::new(), page_count.clone());
    let tree = Tree::new(storage).unwrap();
    let transaction = tree.transaction().unwrap();

    let mut i: usize = 0;
    while page_count.load(Ordering::Relaxed) < 1024 {
        insert(
            &transaction,
            BigKey::new(i as u64),
            &(u16::MAX - (i as u16)).to_be_bytes().repeat(64),
        )
        .unwrap();

        i += 1;
    }

    let dot = tree
        .into_dot(|v| u16::from_be_bytes(v[..v.len() / 64].try_into().unwrap()).to_string())
        .unwrap();
    println!("{dot}");
}
