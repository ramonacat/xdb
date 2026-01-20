#![no_main]

use std::collections::BTreeMap;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use xdb::{
    bplustree::{
        Tree,
        algorithms::{delete::delete, insert::insert},
        debug::assert_tree_equal,
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
        let key = BigKey::<u32, 512>::new(i);
        let value = vec![0; (i % 8) as usize];

        insert(&transaction, key, &value).unwrap();
        rust_btree.insert(i, value);
    }

    for key in keys_to_delete {
        rust_btree.remove(&key.0);

        let key = BigKey::new(key.0);
        delete(&transaction, key).unwrap();
    }

    assert_tree_equal(&tree, &rust_btree, |k| k.value());
});
