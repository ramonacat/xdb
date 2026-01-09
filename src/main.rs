use crate::{bplustree::Tree, storage::InMemoryStorage};

mod bplustree;
mod checksum;
mod page;
mod storage;

fn main() {
    let storage = InMemoryStorage::new();
    let mut tree = Tree::new(storage, 32, 32).unwrap();
    tree.insert(&[1; 32], &[2; 32]).unwrap();
}
