use arbitrary::Arbitrary;
use std::collections::BTreeMap;
use std::fmt::Debug;
use xdb::bplustree::algorithms::delete::delete;
use xdb::bplustree::algorithms::insert::insert;
use xdb::bplustree::{Tree, TreeKey};
use xdb::debug::BigKey;
use xdb::storage::in_memory::InMemoryStorage;

#[derive(PartialEq, Eq)]
pub struct Value(pub Vec<u8>);

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
pub enum TreeAction<T: TreeKey> {
    Insert { key: BigKey<T>, value: Value },
    Delete { key: BigKey<T> },
}

pub fn run_ops<T: TreeKey>(actions: &[TreeAction<T>]) {
    #[cfg(true)]
    {
        let mut result = "vec![\n".to_string();

        for action in actions {
            match action {
                TreeAction::Insert { key, value } => {
                    result += &format!(
                        "TestAction::Insert(BigKey::new({:?}), vec![0u8; {}]),\n",
                        key.value(),
                        value.0.len()
                    )
                }
                TreeAction::Delete { key } => {
                    result += &format!("TestAction::Delete(BigKey::new({:?})),\n", key.value());
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
                insert(&transaction, *key, &value.0).unwrap();
                rust_btree.insert(key, value);
            }
            TreeAction::Delete { key } => {
                let deleted = delete(&transaction, *key).unwrap();
                let deleted2 = rust_btree.remove(key);

                assert!(deleted.map(Value).as_ref() == deleted2)
            }
        };
    }

    assert_eq!(
        rust_btree
            .iter()
            .map(|x| (**x.0, x.1.0.clone()))
            .collect::<Vec<_>>(),
        tree.iter().unwrap().map(|x| x.unwrap()).collect::<Vec<_>>()
    );

    assert_eq!(
        rust_btree
            .iter()
            .rev()
            .map(|x| (**x.0, x.1.0.clone()))
            .map(|x| (x.0, Value(x.1)))
            .collect::<Vec<_>>(),
        tree.iter()
            .unwrap()
            .rev()
            .map(|x| x.unwrap())
            .map(|x| (x.0, Value(x.1)))
            .collect::<Vec<_>>()
    );
}
