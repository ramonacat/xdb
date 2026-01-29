use arbitrary::Arbitrary;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::mem;
use std::ops::DerefMut;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use xdb::bplustree::algorithms::delete::delete;
use xdb::bplustree::algorithms::insert::insert;
use xdb::bplustree::debug::TransactionAction;
use xdb::bplustree::debug::{assert_properties, assert_tree_equal};
use xdb::bplustree::{Tree, TreeKey};
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
}

pub const THREAD_COUNT: usize = 4;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct FuzzThreadId(pub usize);

impl<'a> Arbitrary<'a> for FuzzThreadId {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self(u.int_in_range(0..=THREAD_COUNT - 1)?))
    }
}

#[derive(Debug, Arbitrary)]
pub struct InThreadAction<T: TreeKey, const KEY_SIZE: usize> {
    pub action: TreeAction<T, KEY_SIZE>,
    pub thread_id: FuzzThreadId,
}

pub struct FuzzThread<TKey: TreeKey + Send + Sync, const KEY_SIZE: usize> {
    handle: JoinHandle<()>,
    done: Arc<AtomicBool>,
    tx: mpsc::Sender<TreeAction<TKey, KEY_SIZE>>,
}

impl<TKey: xdb::bplustree::TreeKey + std::marker::Send + std::marker::Sync, const KEY_SIZE: usize>
    FuzzThread<TKey, KEY_SIZE>
{
    pub fn spawn<TStorage: Storage + 'static>(
        tree: Arc<Tree<TStorage, BigKey<TKey, KEY_SIZE>>>,
        rust_btree: Arc<Mutex<BTreeMap<TKey, Vec<u8>>>>,
    ) -> Self {
        let done = Arc::new(AtomicBool::new(true));
        let (tx, rx) = mpsc::channel::<TreeAction<TKey, KEY_SIZE>>();

        let done_ = done.clone();
        let handle = thread::spawn(move || {
            execute_actions(
                tree.as_ref(),
                rx.into_iter(),
                || done_.store(true, Ordering::Relaxed),
                |actions| {
                    let mut tree = rust_btree.lock().unwrap();

                    for action in actions {
                        action.execute_on(tree.deref_mut());
                    }
                },
            )
        });

        Self {
            handle,
            done: done.clone(),
            tx,
        }
    }

    pub fn send_action(&mut self, action: TreeAction<TKey, KEY_SIZE>) {
        self.done.store(false, Ordering::Relaxed);
        self.tx.send(action).unwrap();

        while !self.done.load(Ordering::Relaxed) {
            std::hint::spin_loop();
        }
    }

    pub fn finalize(self) {
        let Self {
            handle,
            done: _,
            tx,
        } = self;
        drop(tx);

        handle.join().unwrap()
    }
}

fn execute_actions<TStorage: Storage, TKey: TreeKey, const KEY_SIZE: usize>(
    tree: &Tree<TStorage, BigKey<TKey, KEY_SIZE>>,
    actions: impl Iterator<Item = TreeAction<TKey, KEY_SIZE>>,
    after_action: impl Fn(),
    transaction_commit: impl Fn(Vec<TransactionAction<TKey, Vec<u8>>>),
) {
    let mut transaction = tree.transaction().unwrap();

    let mut current_transaction_actions = vec![];

    for action in actions {
        match action {
            TreeAction::Insert { key, value } => {
                insert(&mut transaction, key, &value.0).unwrap();
                current_transaction_actions
                    .push(TransactionAction::Insert(key.value(), value.0.to_vec()));
            }
            TreeAction::Delete { key } => {
                let _ = delete(&mut transaction, key).unwrap();
                current_transaction_actions.push(TransactionAction::Delete(key.value()));
            }
            TreeAction::Commit => {
                transaction_commit(mem::take(&mut current_transaction_actions));

                transaction.commit().unwrap();
                transaction = tree.transaction().unwrap();
            }
            TreeAction::Rollback => {
                current_transaction_actions.clear();
                transaction.rollback().unwrap();
                transaction = tree.transaction().unwrap();
            }
        };

        after_action();
    }

    transaction_commit(mem::take(&mut current_transaction_actions));

    transaction.commit().unwrap();
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
#[allow(unused)]
pub fn run_ops_threaded<T: TreeKey + Send + Sync, const KEY_SIZE: usize>(
    actions: &[InThreadAction<T, KEY_SIZE>],
) {
    #[cfg(true)]
    {
        let mut result = "vec![\n".to_string();

        for action in actions {
            let formatted_action = match &action.action {
                TreeAction::Insert { key, value } => &format!(
                    "TestAction::Insert(BigKey::new({:?}), vec![0u8; {}]),",
                    key.value(),
                    value.0.len()
                ),
                TreeAction::Delete { key } => {
                    &format!("TestAction::Delete(BigKey::new({:?})),", key.value())
                }
                TreeAction::Commit => "TestAction::Commit,",
                TreeAction::Rollback => "TestAction::Rollback,",
            };

            result += &format!(
                "InThreadTestAction {{ thread: {}, action: {} }},\n",
                action.thread_id.0, formatted_action
            );
        }

        result += "];\n";
        std::fs::write("/tmp/actions", result).unwrap();
    }

    let storage = InMemoryStorage::new();
    let tree = Arc::new(Tree::new(storage).unwrap());

    let mut threads: HashMap<FuzzThreadId, FuzzThread<T, KEY_SIZE>> = HashMap::new();
    let rust_btree = Arc::new(Mutex::new(BTreeMap::new()));
    for i in 0..THREAD_COUNT {
        let thread = FuzzThread::spawn(tree.clone(), rust_btree.clone());
        threads.insert(FuzzThreadId(i), thread);
    }

    for action in actions {
        threads
            .get_mut(&action.thread_id)
            .unwrap()
            .send_action(action.action.clone());
    }

    for thread in threads.into_values() {
        thread.finalize();
    }

    let mut transaction = tree.transaction().unwrap();

    assert_properties(&mut transaction);
    assert_tree_equal(&tree, &rust_btree.lock().unwrap(), |k| k.value());
}
