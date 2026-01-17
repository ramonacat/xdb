#![no_main]

use std::collections::BTreeMap;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use xdb::{
    bplustree::{
        Tree,
        algorithms::{delete::delete, insert::insert},
    },
    debug::BigKey,
    storage::in_memory::InMemoryStorage,
};

const MAX_KEYS: u32 = 4096;

#[derive(Debug)]
struct KeyToDelete(u32);

impl<'a> Arbitrary<'a> for KeyToDelete {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self(u.int_in_range(0..=MAX_KEYS - 1)?))
    }
}

fuzz_target!(|keys_to_delete: Vec<KeyToDelete>| {
    let storage = InMemoryStorage::new();
    let tree = Tree::new(storage).unwrap();
    let transaction = tree.transaction().unwrap();

    let mut rust_btree = BTreeMap::new();

    for i in 0..MAX_KEYS {
        let key = BigKey::new(i);
        let value = vec![0; (i % 8) as usize];

        insert(&transaction, key, &value).unwrap();
        rust_btree.insert(key, value);
    }

    for key in keys_to_delete {
        let key = BigKey::new(key.0);
        rust_btree.remove(&key);
        delete(&transaction, key).unwrap();
    }

    // TODO extract those comparisons into an fn, as those are repeated all over the place
    assert_eq!(
        rust_btree
            .iter()
            .map(|(x, y)| (*x, y.clone()))
            .collect::<Vec<_>>(),
        tree.iter().unwrap().map(|x| x.unwrap()).collect::<Vec<_>>()
    );
    assert_eq!(
        rust_btree
            .iter()
            .rev()
            .map(|(x, y)| (*x, y.clone()))
            .collect::<Vec<_>>(),
        tree.iter()
            .unwrap()
            .rev()
            .map(|x| x.unwrap())
            .collect::<Vec<_>>()
    );
});
