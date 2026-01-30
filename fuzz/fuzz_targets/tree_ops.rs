use arbitrary::Arbitrary;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::mem;
use std::sync::{Arc, Mutex};
use xdb::bplustree::algorithms::delete::delete;
use xdb::bplustree::algorithms::find;
use xdb::bplustree::algorithms::insert::insert;
use xdb::bplustree::debug::TransactionAction;
use xdb::bplustree::debug::{assert_properties, assert_tree_equal};
use xdb::bplustree::{Tree, TreeError, TreeKey};
use xdb::debug::BigKey;
use xdb::storage::Storage;
use xdb::storage::in_memory::InMemoryStorage;

#[derive(PartialEq, Eq, Clone)]
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

#[derive(Debug, Arbitrary, Clone)]
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
    Read {
        key: BigKey<T, KEY_SIZE>,
    },
}

fn execute_actions<TStorage: Storage, TKey: TreeKey, const KEY_SIZE: usize>(
    tree: &Tree<TStorage, BigKey<TKey, KEY_SIZE>>,
    actions: impl Iterator<Item = TreeAction<TKey, KEY_SIZE>>,
    after_action: impl Fn(),
    transaction_commit: impl Fn(Vec<TransactionAction<TKey>>),
) -> Result<(), TreeError> {
    let mut transaction = tree.transaction().unwrap();

    let mut current_transaction_actions = vec![];

    for action in actions {
        match action {
            TreeAction::Insert { key, value } => {
                insert(&mut transaction, key, &value.0)?;
                current_transaction_actions
                    .push(TransactionAction::Insert(key.value(), value.0.to_vec()));
            }
            TreeAction::Delete { key } => {
                let _ = delete(&mut transaction, key)?;
                current_transaction_actions.push(TransactionAction::Delete(key.value()));
            }
            TreeAction::Commit => {
                transaction_commit(mem::take(&mut current_transaction_actions));

                transaction.commit()?;
                transaction = tree.transaction().unwrap();
            }
            TreeAction::Rollback => {
                current_transaction_actions.clear();
                transaction.rollback()?;
                transaction = tree.transaction().unwrap();
            }
            TreeAction::Read { key } => {
                find(&mut transaction, key)?;
            }
        };

        after_action();
    }

    transaction_commit(mem::take(&mut current_transaction_actions));

    transaction.commit().unwrap();

    Ok(())
}

#[allow(unused)]
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
                TreeAction::Read { key } => {
                    result += &format!("TestAction::Read(BigKey::new({:?})),\n", key.value());
                }
            }
        }

        result += "];\n";
        std::fs::write("/tmp/actions", result).unwrap();
    }

    let storage = InMemoryStorage::new();
    let tree = Tree::new(storage).unwrap();

    let rust_btree = Arc::new(Mutex::new(BTreeMap::new()));

    let rust_btree_ = rust_btree.clone();
    execute_actions(
        &tree,
        actions.iter().cloned(),
        || {},
        |actions| {
            for action in actions {
                action.execute_on(&mut rust_btree_.lock().unwrap());
            }
        },
    );

    let mut transaction = tree.transaction().unwrap();

    assert_properties(&mut transaction);

    transaction.commit().unwrap();

    assert_tree_equal(&tree, &rust_btree.lock().unwrap(), |k| k.value());
}

// TODO a lot of the code is duplicated with run_ops and also the btree tests run on very similar
// concepts, clean this up
