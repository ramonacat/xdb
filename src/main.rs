// TODO enable clippy pedantic
use std::{
    fmt::Display,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use bytemuck::{Pod, Zeroable};

use crate::{
    bplustree::{Tree, algorithms::insert},
    storage::in_memory::{InMemoryStorage, test::TestStorage},
};

mod bplustree;
mod checksum;
mod page;
mod storage;

#[derive(Debug, Clone, Copy, Pod, Zeroable, Ord, PartialOrd, PartialEq, Eq)]
#[repr(transparent)]
// TODO add validation that the data did not get corrupted
struct BigKey([u64; 8]);

impl Display for BigKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0[0])
    }
}

impl BigKey {
    pub fn new(value: u64) -> Self {
        Self(vec![value; 8].try_into().unwrap())
    }
}

fn main() {
    let page_count = Arc::new(AtomicUsize::new(0));

    let storage = TestStorage::new(InMemoryStorage::new(), page_count.clone());
    let tree = Tree::new(storage).unwrap();

    let transaction = tree.transaction().unwrap();

    // 3 pages mean there's been a node split
    // TODO: find a more explicit way of counting nodes
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
