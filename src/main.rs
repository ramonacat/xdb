// TODO enable clippy pedantic
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crate::{
    bplustree::{Tree, algorithms::insert},
    storage::in_memory::{InMemoryStorage, test::TestStorage},
};

mod bplustree;
mod checksum;
mod page;
mod storage;

fn main() {
    let page_count = Arc::new(AtomicUsize::new(0));

    let storage = TestStorage::new(InMemoryStorage::new(), page_count.clone());
    let tree = Tree::new(storage).unwrap();

    let transaction = tree.transaction().unwrap();

    // 3 pages mean there's been a node split
    // TODO: find a more explicit way of counting nodes
    let mut i = 0;
    while page_count.load(Ordering::Relaxed) < 1024 {
        insert(&transaction, i, &(u16::MAX - i).to_be_bytes().repeat(128)).unwrap();

        i += 1;
    }

    let dot = tree
        .into_dot(|v| u16::from_be_bytes(v[..v.len() / 128].try_into().unwrap()).to_string())
        .unwrap();
    println!("{dot}");
}
