use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crate::{
    bplustree::Tree,
    storage::in_memory::{InMemoryStorage, test::TestStorage},
};

mod bplustree;
mod checksum;
mod page;
mod storage;

fn main() {
    let page_count = Arc::new(AtomicUsize::new(0));

    let storage = TestStorage::new(InMemoryStorage::new(), page_count.clone());
    let tree = Tree::new(storage, size_of::<usize>(), size_of::<usize>()).unwrap();

    // 3 pages mean there's been a node split
    // TODO: find a more explicit way of counting nodes
    let mut i = 0usize;
    while page_count.load(Ordering::Relaxed) < 10 {
        tree.insert(&i.to_be_bytes(), &(usize::MAX - i).to_be_bytes())
            .unwrap();

        i += 1;
    }

    let dot = tree
        .into_dot(
            |k| usize::from_be_bytes(k.try_into().unwrap()).to_string(),
            |v| usize::from_be_bytes(v.try_into().unwrap()).to_string(),
        )
        .unwrap();
    println!("{dot}");
}
