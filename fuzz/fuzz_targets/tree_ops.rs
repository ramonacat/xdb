#![no_main]

use std::{collections::BTreeMap, fmt::Debug};

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use pretty_assertions::assert_eq;
use xdb::{
    bplustree::{Tree, algorithms::insert},
    debug::BigKey,
    storage::in_memory::InMemoryStorage,
};

#[derive(PartialEq, Eq)]
struct Value(Vec<u8>);

impl Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Value({})", self.0.len())
    }
}

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
    Insert { key: BigKey, value: Value },
}

fuzz_target!(|actions: Vec<TreeAction>| {
    #[cfg(true)]
    {
        let mut result = "vec![\n".to_string();

        for action in &actions {
            match action {
                TreeAction::Insert { key, value } => {
                    result += &format!(
                        "(BigKey::new({}), vec![0u8; {}]),\n",
                        key.value(),
                        value.0.len()
                    )
                }
            }
        }

        result += "];\n";
        std::fs::write("/tmp/actions", result).unwrap();
    }

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

    assert_eq!(
        rust_btree
            .iter()
            .map(|x| (*x.0, x.1.0.clone()))
            .collect::<Vec<_>>(),
        tree.iter().unwrap().map(|x| x.unwrap()).collect::<Vec<_>>()
    );

    assert_eq!(
        rust_btree
            .iter()
            .rev()
            .map(|x| (*x.0, x.1.0.clone()))
            .map(|x| (x.0, Value(x.1)))
            .collect::<Vec<_>>(),
        tree.iter_reverse()
            .unwrap()
            .map(|x| x.unwrap())
            .map(|x| (x.0, Value(x.1)))
            .collect::<Vec<_>>()
    );
});
