use arbitrary::Arbitrary;
use std::collections::BTreeMap;
use std::fmt::Debug;
use xdb::bplustree::algorithms::delete::delete;
use xdb::bplustree::algorithms::insert::insert;
use xdb::bplustree::debug::TransactionAction;
use xdb::bplustree::debug::{assert_properties, assert_tree_equal};
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
        let len = u.int_in_range(1..=64)?;

        let mut result = Vec::with_capacity(len);
        for _ in 0..len {
            result.push(u.arbitrary()?);
        }

        Ok(Self(result))
    }
}

#[derive(Debug, Arbitrary)]
pub enum TreeAction<T: TreeKey, const KEY_SIZE: usize> {
    Insert {
        key: BigKey<T, KEY_SIZE>,
        value: Value,
    },
    Delete {
        key: BigKey<T, KEY_SIZE>,
    },
    Commit,
    Rollback,
}

pub fn run_ops<T: TreeKey, const KEY_SIZE: usize>(actions: &[TreeAction<T, KEY_SIZE>]) {
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
                TreeAction::Commit => {
                    result += "TestAction::Commit,\n";
                }
                TreeAction::Rollback => {
                    result += "TestAction::Rollback,\n";
                }
            }
        }

        result += "];\n";
        std::fs::write("/tmp/actions", result).unwrap();
    }

    let storage = InMemoryStorage::new();
    let tree = Tree::new(storage).unwrap();
    let mut transaction = tree.transaction().unwrap();

    let mut rust_btree = BTreeMap::new();
    let mut current_transaction_actions = vec![];

    for action in actions {
        match action {
            TreeAction::Insert { key, value } => {
                insert(&mut transaction, *key, &value.0).unwrap();
                current_transaction_actions
                    .push(TransactionAction::Insert(key.value(), value.0.to_vec()));
            }
            TreeAction::Delete { key } => {
                let _ = delete(&mut transaction, *key).unwrap();
                current_transaction_actions.push(TransactionAction::Delete(key.value()));
            }
            TreeAction::Commit => {
                for action in current_transaction_actions.drain(..) {
                    action.execute_on(&mut rust_btree);
                }

                transaction.commit().unwrap();
                transaction = tree.transaction().unwrap();
            }
            TreeAction::Rollback => {
                current_transaction_actions.clear();
                transaction.rollback().unwrap();
                transaction = tree.transaction().unwrap();
            }
        };
    }

    for action in current_transaction_actions.drain(..) {
        action.execute_on(&mut rust_btree);
    }

    transaction.commit().unwrap();
    transaction = tree.transaction().unwrap();

    assert_properties(&mut transaction);

    transaction.commit().unwrap();

    assert_tree_equal(&tree, &rust_btree, |k| k.value());
}
