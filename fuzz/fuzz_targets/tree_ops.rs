#![no_main]

use std::collections::BTreeMap;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use xdb::{
    bplustree::{Tree, algorithms::insert},
    storage::in_memory::InMemoryStorage,
};

#[derive(Debug, PartialEq, Eq)]
struct Value(Vec<u8>);

impl<'a> Arbitrary<'a> for Value {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let len = u.int_in_range(1..=1024)?;

        let mut result = Vec::with_capacity(len);
        for _ in 0..len {
            result.push(u.arbitrary()?);
        }

        Ok(Self(result))
    }
}

#[derive(Debug, Arbitrary)]
enum TreeAction {
    // TODO use some big type for key to encourage more splits, etc.
    Insert { key: u64, value: Value },
}

fuzz_target!(|actions: Vec<TreeAction>| {
    let storage = InMemoryStorage::new();
    let tree = Tree::new(storage).unwrap();
    let transaction = tree.transaction().unwrap();

    let mut rust_btree = BTreeMap::new();

    for action in actions {
        match action {
            TreeAction::Insert { key, value } => {
                insert(&transaction, key, &value.0).unwrap();
                rust_btree.insert(key, value);
            }
        };
    }

    assert!(
        rust_btree
            .iter()
            .map(|x| (*x.0, x.1.0.clone()))
            .collect::<Vec<_>>()
            == tree.iter().unwrap().map(|x| x.unwrap()).collect::<Vec<_>>()
    );
    assert!(
        rust_btree
            .iter()
            .rev()
            .map(|x| (*x.0, x.1.0.clone()))
            .collect::<Vec<_>>()
            == tree
                .iter_reverse()
                .unwrap()
                .map(|x| x.unwrap())
                .collect::<Vec<_>>()
    );
});
