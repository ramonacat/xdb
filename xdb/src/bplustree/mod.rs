pub mod algorithms;
pub mod debug;
pub mod dot;
mod iterator;
mod node;
mod tuples;

use crate::Size;
use crate::bplustree::iterator::TreeIterator;
use crate::bplustree::tuples::NodeIds;
use std::fmt::Debug;
use std::marker::PhantomData;

use crate::bplustree::iterator::TreeIteratorItem;
use crate::bplustree::node::AnyNodeId;
use crate::bplustree::node::InteriorNodeId;
use crate::bplustree::node::LeafNodeId;
use crate::bplustree::node::Node;
use crate::bplustree::node::NodeId;
use crate::bplustree::node::interior::InteriorNode;
use crate::bplustree::node::leaf::LeafNode;
use crate::page::Page;
use crate::storage::PageIndex;
use crate::storage::PageReservation;
use crate::storage::Storage;
use crate::storage::StorageError;
use crate::storage::Transaction;
use bytemuck::{Pod, Zeroable};
use thiserror::Error;

use crate::page::PAGE_DATA_SIZE;

pub trait TreeKey: Debug + Ord + Pod {}
impl TreeKey for u8 {}
impl TreeKey for u16 {}
impl TreeKey for u32 {}
impl TreeKey for u64 {}
impl TreeKey for usize {}
impl TreeKey for i8 {}
impl TreeKey for i16 {}
impl TreeKey for i32 {}
impl TreeKey for i64 {}
impl TreeKey for isize {}

const ROOT_NODE_TAIL_SIZE: Size = PAGE_DATA_SIZE
    .subtract(Size::of::<u64>())
    .subtract(Size::of::<PageIndex>());

#[derive(Debug)]
pub struct Tree<T: Storage, TKey: TreeKey> {
    storage: T,
    _key: PhantomData<TKey>,
}

pub struct TreeTransaction<'storage, TStorage: Storage + 'storage, TKey>
where
    Self: 'storage,
{
    transaction: TStorage::Transaction<'storage>,
    _key: PhantomData<&'storage TKey>,
}

impl<'storage, TStorage: Storage + 'storage, TKey: TreeKey>
    TreeTransaction<'storage, TStorage, TKey>
{
    fn get_root(&mut self) -> Result<AnyNodeId, TreeError> {
        Ok(AnyNodeId::new(self.read_header(|x| x.root)?))
    }

    fn read_header<TReturn>(
        &mut self,
        read: impl FnOnce(&TreeHeader) -> TReturn,
    ) -> Result<TReturn, TreeError> {
        Ok(self
            .transaction
            .read(PageIndex::zero(), |[page]| read(page.data()))?)
    }

    fn write_header<TReturn>(
        &mut self,
        write: impl FnOnce(&mut TreeHeader) -> TReturn,
    ) -> Result<TReturn, TreeError> {
        Ok(self
            .transaction
            .write(PageIndex::zero(), |[page]| write(page.data_mut()))?)
    }

    fn read_nodes<TReturn, TIndices: NodeIds<N>, const N: usize>(
        &mut self,
        indices: TIndices,
        read: impl for<'node> FnOnce(TIndices::Nodes<'node, TKey>) -> TReturn,
    ) -> Result<TReturn, TreeError> {
        Ok(self.transaction.read(indices.to_page_indices(), |pages| {
            read(TIndices::pages_to_nodes(pages.map(|x| x)))
        })?)
    }

    fn write_nodes<TReturn, TIndices: NodeIds<N>, const N: usize>(
        &mut self,
        indices: TIndices,
        write: impl for<'node> FnOnce(TIndices::NodesMut<'node, TKey>) -> TReturn,
    ) -> Result<TReturn, TreeError> {
        Ok(self.transaction.write(indices.to_page_indices(), |pages| {
            write(TIndices::pages_to_nodes_mut(pages.map(|x| x)))
        })?)
    }

    fn reserve_node(&self) -> Result<TStorage::PageReservation<'storage>, TreeError> {
        Ok(self.transaction.reserve()?)
    }

    #[allow(clippy::large_types_passed_by_value)] // TODO perhaps we should do something to avoid
    // passing whole nodes here?
    fn insert_reserved(
        &mut self,
        reservation: TStorage::PageReservation<'storage>,
        page: impl Node<TKey>,
    ) -> Result<(), TreeError> {
        self.transaction
            .insert_reserved(reservation, Page::from_data(page))?;

        Ok(())
    }

    fn delete_node(&mut self, node_id: AnyNodeId) -> Result<(), TreeError> {
        self.transaction.delete(node_id.page())?;

        Ok(())
    }

    pub fn commit(self) -> Result<(), TreeError> {
        let Self { transaction, _key } = self;

        transaction.commit()?;

        Ok(())
    }

    pub fn rollback(self) -> Result<(), TreeError> {
        let Self { transaction, _key } = self;

        transaction.rollback()?;

        Ok(())
    }
}

impl<T: Storage, TKey: TreeKey> Tree<T, TKey> {
    // TODO also create a "new_read" method, or something like that (that reads a tree that already
    // exists from storage)
    pub fn new(storage: T) -> Result<Self, TreeError> {
        // TODO assert that the storage is empty, and that the header get's the 0th page, as we
        // depend on that invariant (i.e. PageIndex=0 must always refer to the TreeData and not to
        // a node)!

        TreeHeader::new_in::<_, TKey>(&storage)?;

        Ok(Self {
            storage,
            _key: PhantomData,
        })
    }

    #[allow(clippy::iter_not_returning_iterator)]
    pub fn iter(
        &self,
    ) -> Result<impl DoubleEndedIterator<Item = TreeIteratorItem<TKey>>, TreeError> {
        TreeIterator::<_, _>::new(self.transaction()?)
    }

    pub fn transaction(&self) -> Result<TreeTransaction<'_, T, TKey>, TreeError> {
        Ok(TreeTransaction::<T, TKey> {
            transaction: self.storage.transaction()?,
            _key: PhantomData,
        })
    }
}

#[derive(Debug, Pod, Zeroable, Clone, Copy)]
#[repr(C)]
struct TreeHeader {
    key_size: u64,
    root: PageIndex,
    _unused: [u8; ROOT_NODE_TAIL_SIZE.as_bytes()],
}

const _: () = assert!(
    Size::of::<TreeHeader>().is_equal(PAGE_DATA_SIZE),
    "The Tree descriptor must have size of exactly one page"
);

#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum TreeError {
    #[error("Storage error: {0}")]
    StorageError(#[from] StorageError),
}

impl TreeHeader {
    pub fn new_in<T: Storage, TKey: TreeKey>(storage: &T) -> Result<(), TreeError> {
        let mut transaction = storage.transaction()?;

        let header_page = transaction.reserve()?;
        assert!(header_page.index() == PageIndex::zero());

        // TODO replace usize with actual TKey!
        let root_index = transaction.insert(Page::from_data(LeafNode::<TKey>::new(None)))?;

        let page = Page::from_data(Self {
            key_size: size_of::<TKey>() as u64,
            root: root_index,
            _unused: [0; _],
        });

        transaction.insert_reserved(header_page, page)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{bplustree::debug::TransactionAction, storage::instrumented::InstrumentedStorage};
    use std::{
        collections::BTreeMap,
        hint,
        io::Write,
        mem,
        panic::{RefUnwindSafe, UnwindSafe, catch_unwind},
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
            mpsc,
        },
        thread,
    };

    use crate::{
        bplustree::{
            algorithms::{delete::delete, insert::insert},
            debug::{assert_properties, assert_tree_equal},
        },
        debug::BigKey,
        storage::in_memory::InMemoryStorage,
    };
    use log::info;
    use pretty_assertions::assert_eq;
    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn node_accessor_entries() {
        let mut node = LeafNode::zeroed();

        assert!(matches!(node.entries().next(), None));

        node.insert(1usize, &[2; 16]);

        let mut iter = node.entries();
        let first = iter.next().unwrap();
        assert!(first.key() == 1);
        assert!(first.value() == &[2; 16]);

        assert!(matches!(iter.next(), None));

        drop(iter);

        node.insert(2usize, &[1; 16]);

        let mut iter = node.entries();

        let first = iter.next().unwrap();
        assert!(first.key() == 1);
        assert!(first.value() == &[2; 16]);

        let second = iter.next().unwrap();
        assert!(second.key() == 2);
        assert!(second.value() == &[1; 16]);

        assert!(matches!(iter.next(), None));
    }

    #[test]
    #[cfg_attr(miri, ignore = "too slow in miri")]
    fn insert_multiple_nodes() {
        let mut data = vec![];

        for i in 0..1024 {
            // make the value bigger with repeat so fewer inserts are needed and the test runs faster
            data.push(TestAction::Insert(
                BigKey::<usize, 256>::new(i),
                (u16::max_value() - i as u16).to_be_bytes().repeat(8),
            ));
        }

        test_from_data(data);
    }

    #[test]
    #[cfg_attr(miri, ignore = "too slow in miri")]
    fn delete_first_from_big_tree() {
        let mut data = vec![];

        for i in 0..2048 {
            // make the value bigger with repeat so fewer inserts are needed and the test runs faster
            data.push(TestAction::Insert(
                BigKey::<u32, 1200>::new(i),
                vec![0; usize::try_from(i % 8).unwrap()],
            ));
        }

        data.push(TestAction::Delete(BigKey::new(0)));

        test_from_data(data);
    }

    #[test]
    #[cfg_attr(miri, ignore = "too slow in miri")]
    fn delete_with_interior_node_merge() {
        let mut data = vec![];

        for i in 0..8192 {
            // make the value bigger with repeat so fewer inserts are needed and the test runs faster
            data.push(TestAction::Insert(
                BigKey::<u32, 1024>::new(i),
                vec![0xff; 1],
            ));
        }

        data.push(TestAction::Delete(BigKey::new(8188)));

        test_from_data(data);
    }

    #[test]
    #[cfg_attr(miri, ignore = "too slow in miri")]
    fn variable_sized_keys() {
        let mut data = vec![];

        for i in 0..5000 {
            let value: &[u8] = match i % 8 {
                0 | 7 | 6 | 5 => &(i as u64).to_be_bytes(),
                4 | 3 => &(i as u32).to_be_bytes(),
                2 => &(i as u16).to_be_bytes(),
                1 => &(i as u8).to_be_bytes(),
                _ => unreachable!(),
            };

            data.push(TestAction::Insert(
                BigKey::<_, 256>::new(i),
                value.repeat(8),
            ));
        }

        test_from_data(data);
    }

    #[test]
    fn insert_reverse() {
        let storage = InMemoryStorage::new();
        let tree = Tree::new(storage).unwrap();
        let mut transaction = tree.transaction().unwrap();

        insert(&mut transaction, 1, &[0]).unwrap();
        insert(&mut transaction, 0, &[0]).unwrap();

        transaction.commit().unwrap();

        let result = tree.iter().unwrap().map(|x| x.unwrap()).collect::<Vec<_>>();

        assert!(result == &[(0, vec![0]), (1, vec![0])]);
    }

    #[test]
    fn same_key_overrides() {
        let storage = InMemoryStorage::new();
        let tree = Tree::new(storage).unwrap();
        let mut transaction = tree.transaction().unwrap();

        insert(&mut transaction, 1, &0u8.to_ne_bytes()).unwrap();
        insert(&mut transaction, 1, &1u8.to_ne_bytes()).unwrap();

        transaction.commit().unwrap();

        // TODO iter() should be on transaction, so it's created explicitly
        let result = tree.iter().unwrap().map(|x| x.unwrap()).collect::<Vec<_>>();

        assert_eq!(result, vec![(1, 1u8.to_ne_bytes().to_vec())]);
    }

    enum TestAction<TKey> {
        Insert(TKey, Vec<u8>),
        Delete(TKey),
        Commit,
        Rollback,
    }

    struct InThreadTestAction<TKey> {
        thread: usize,
        action: TestAction<TKey>,
    }

    fn execute_test_actions<TKey: TreeKey, const SIZE: usize>(
        tree: &Tree<InMemoryStorage, BigKey<TKey, SIZE>>,
        actions: impl Iterator<Item = TestAction<BigKey<TKey, SIZE>>>,
        commit: impl Fn(Vec<TransactionAction<TKey>>),
        after_action: impl Fn(Result<(), TreeError>),
    ) {
        let mut transaction = tree.transaction().unwrap();
        let mut uncommitted_actions = vec![];

        for action in actions {
            let result = match action {
                TestAction::Insert(key, value) => {
                    uncommitted_actions.push(TransactionAction::Insert(key.value(), value.clone()));

                    insert(&mut transaction, key, &value)
                }
                TestAction::Delete(key) => {
                    uncommitted_actions.push(TransactionAction::Delete(key.value()));

                    delete(&mut transaction, key).map(|_| ())
                }
                TestAction::Commit => {
                    commit(mem::take(&mut uncommitted_actions));

                    let result = transaction.commit();
                    transaction = tree.transaction().unwrap();

                    result
                }
                TestAction::Rollback => {
                    uncommitted_actions.clear();
                    let result = transaction.rollback();
                    transaction = tree.transaction().unwrap();

                    result
                }
            };

            after_action(result);
        }

        commit(mem::take(&mut uncommitted_actions));
        transaction.commit().unwrap();
    }

    fn threaded_test_from_data<TKey: TreeKey + Send + Sync, const SIZE: usize>(
        data: Vec<InThreadTestAction<BigKey<TKey, SIZE>>>,
        expected_error: impl FnOnce(&TreeError) -> bool,
    ) {
        if !cfg!(miri) {
            let _ = env_logger::builder().is_test(true).try_init();
        }

        let rust_tree = Arc::new(Mutex::new(BTreeMap::new()));
        let storage = InMemoryStorage::new();
        let tree = Arc::new(Tree::new(storage).unwrap());
        let mut threads = vec![];
        let error = Arc::new(Mutex::new(None));

        for _ in 0..4 {
            let (tx, rx) = mpsc::channel();
            let rust_tree = rust_tree.clone();
            let tree = tree.clone();
            let done = Arc::new(AtomicBool::new(true));
            let done_ = done.clone();
            let error = error.clone();

            let handle = thread::spawn(move || {
                execute_test_actions(
                    &tree,
                    rx.into_iter(),
                    |uncomitted| {
                        for action in uncomitted {
                            action.execute_on(&mut rust_tree.lock().unwrap());
                        }
                    },
                    |result| {
                        done_.store(true, Ordering::Release);
                        if let Err(err) = result {
                            *error.lock().unwrap() = Some(err);
                        }
                    },
                );
            });

            threads.push((tx, handle, done));
        }

        for datum in data {
            threads[datum.thread].2.store(false, Ordering::Release);

            threads[datum.thread].0.send(datum.action).unwrap();

            while !threads[datum.thread].2.load(Ordering::Acquire) {
                if threads[datum.thread].1.is_finished() {
                    panic!("thread exited unexpectedly");
                }

                hint::spin_loop();
            }

            if let Some(error) = error.lock().unwrap().clone() {
                if expected_error(&error) {
                    return;
                }

                panic!("Unexpected error: {error}");
            }
        }

        for (tx, handle, _) in threads {
            drop(tx);
            handle.join().unwrap();
        }

        assert_tree_equal(&tree, &rust_tree.lock().unwrap(), |k| k.value());
        assert_properties(&mut tree.transaction().unwrap());
    }

    fn test_from_data<TKey: TreeKey + UnwindSafe + RefUnwindSafe, const SIZE: usize>(
        data: Vec<TestAction<BigKey<TKey, SIZE>>>,
    ) {
        if !cfg!(miri) {
            let _ = env_logger::builder().is_test(true).try_init();
        }

        let storage = InMemoryStorage::new();
        let tree = Arc::new(Mutex::new(Tree::new(storage).unwrap()));

        let result = catch_unwind(|| {
            let rust_tree = Arc::new(Mutex::new(BTreeMap::new()));

            execute_test_actions(
                &tree.lock().unwrap(),
                data.into_iter(),
                |uncomitted| {
                    for action in uncomitted {
                        action.execute_on(&mut rust_tree.lock().unwrap());
                    }
                },
                |_| {},
            );

            assert_tree_equal(&tree.lock().unwrap(), &rust_tree.lock().unwrap(), |k| {
                k.value()
            });
            assert_properties(&mut tree.lock().unwrap().transaction().unwrap());
        });

        if let Err(_) = result {
            let dot_data = tree
                .lock()
                .unwrap()
                .to_dot(|value| {
                    let mut last_value_state: Option<(u8, usize)> = None;

                    // TODO extract this formatter into bplustree::debug probably
                    let mut formatted_value = value.iter().fold(String::new(), |acc, x| {
                        let mut result = "".to_string();
                        if let Some((last_value, repeats)) = last_value_state {
                            if last_value == *x {
                                last_value_state = Some((last_value, repeats + 1));

                                return acc;
                            } else {
                                last_value_state = Some((*x, 1));

                                result += &format!("({repeats})");
                            }
                        } else {
                            last_value_state = Some((*x, 1));
                        }

                        if !acc.is_empty() {
                            result += ",";
                        }

                        format!("{result}{x:#x}")
                    });

                    if let Some((_, repeats)) = last_value_state
                        && repeats > 1
                    {
                        formatted_value += &format!("({repeats})");
                    }

                    formatted_value
                })
                .unwrap();

            let mut output = NamedTempFile::new().unwrap();
            output.write_all(dot_data.as_bytes()).unwrap();
            let output_path = output.keep().unwrap().1;

            info!("dot data written to: {}", output_path.to_string_lossy());
        }

        result.unwrap();
    }

    #[test]
    fn reverse_with_splits() {
        // this case came from fuzzing, hence the slightly unhinged input
        let to_insert = vec![
            TestAction::Insert(BigKey::<u64, 256>::new(1095228325891), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(23552), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(749004913038733311), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(11730937), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(18446735329151090432), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(128434), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(4160773120), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(7277816997842399231), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(18446744069414780850), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(280375565746354), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(45568), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(8808972877568), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(196530), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(272678883712000), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(28428972659453951), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(18446735294791352064), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(193970), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(1096776417280), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(28428972659453944), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(18386508424398700466), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(280375565746354), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(270479860478464), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(227629727488), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(2986409983), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(866673871104), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(749004913038733311), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(11730937), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(18446735329151090432), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(128434), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(4160773120), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(759169024), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(41944653103338), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(400308568064), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(41956837944524949), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(17593749602304), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(1563623424), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(1560281088), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(12813251448442880), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(10740950511298543765), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(855638016), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(17955007290084764969), vec![0u8; 17]),
            TestAction::Insert(BigKey::new(327869), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(281471419940864), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(53198770610748672), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(661184721051266345), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(8796093034496), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(257449567191040), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(4194816), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(257449567200806), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(519695237120), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(3255307760466471209), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(2522068567888101421), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(17955007289400229888), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(32768), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(70650219154374656), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(9884556757906042153), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(12288), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(1383349474033664), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(70136747227152896), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(0), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(275977418571776), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(255), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(905955839), vec![0u8; 458]),
        ];

        test_from_data(to_insert);
    }

    #[test]
    fn fuzzer_a() {
        // this case came from fuzzing, hence the slightly unhinged input
        let to_insert = vec![
            TestAction::Insert(BigKey::<u64, 256>::new(1095228325891), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(3096224743840768), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(749004913038733311), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(18230289816630788089), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(18446735329151090432), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(128434), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(4294967258), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(7277816997842399231), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(18446744069414780850), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(280375565746354), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(45568), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(8808972877568), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(196530), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(272678900451785), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(28428972659453951), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(18446735294791352064), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(193970), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(1096776417280), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(28428972659453944), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(18386508424398700466), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(280375565877426), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(270479860478464), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(219039792896), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(2986409983), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(866673871104), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(749004913038733311), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(11730937), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(18446735329151090432), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(128434), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(4160773120), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(759169024), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(41944653103338), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(3773172062810537984), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(41956837944524949), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(17593749733376), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(1563623424), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(1560281088), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(12813251448442880), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(10740950511298543765), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(838860800), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(491736783624786638), vec![0u8; 17]),
            TestAction::Insert(BigKey::new(327869), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(281471419940864), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(53198770610748672), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(661184721051266345), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(8796093034496), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(268444683468800), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(2199027450368), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(257449567200806), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(519695237120), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(3255307760466471209), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(2522068567888101421), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(1125056745783855), vec![0u8; 5]),
            TestAction::Insert(BigKey::new(4863), vec![0u8; 11]),
            TestAction::Insert(BigKey::new(848840156512003), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(142284501207154471), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(15204011600974444839), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(217298682054180864), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(277076930199551), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(17432379), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(4863), vec![0u8; 11]),
            TestAction::Insert(BigKey::new(47855161267191555), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(142284501207154471), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(15204011600974444839), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(217298682054184960), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(4398046511103), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(9223372036854775801), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(3298534883194), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(9223372036854774055), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(576460752286590842), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(251638629179457535), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(30), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(3206556144328376103), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(4398046511104), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(3819055799724934143), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(576460752303367975), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(289079216299769639), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(142284501106491175), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(15204011463535491367), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(217298686248484864), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(1244967), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(288231475663273984), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(577309575280328703), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(18446743523953737721), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(3298534883327), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(9223372036854774271), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(576460752303368058), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(251638629179457319), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(142284501106360103), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(15204010544412490023), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(288230376151711744), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(3458767829535294463), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(576367293815007015), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(848840148057895), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(142284501106469415), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(15204011600974444839), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(1095233372169), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(9), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(5), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(15663113), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(23817), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(262383), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(399599728127), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(1095216660489), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(18374686879271089407), vec![0u8; 61]),
            TestAction::Insert(BigKey::new(23817), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(262153), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(399599728127), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(1095216660489), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(399599465727), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(3989292031), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(237), vec![0u8; 10]),
            TestAction::Insert(BigKey::new(647714935328997376), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(18374961357578502399), vec![0u8; 61]),
            TestAction::Insert(BigKey::new(262381), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(537038681599), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(71776119067901961), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(576461151902889215), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(3979885823), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(237), vec![0u8; 10]),
            TestAction::Insert(BigKey::new(71254183025573888), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(71106559), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(332009393485), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(524293), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(399447621641), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(41956837944524949), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(17593749602304), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(1563623424), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(1560281088), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(12813251448442880), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(10740950511298543765), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(855638016), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(12667444087565609), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(0), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(189), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(71776115504447488), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(17955007290153970985), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(70650219137597440), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(53198770610748672), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(661184721051266345), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(8796093296640), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(257449567191040), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(4194816), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(257449567200806), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(1792), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(3255307760466471209), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(2522068567888101421), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(17955007289400229888), vec![0u8; 256]),
            TestAction::Insert(BigKey::new(8863083360943013888), vec![0u8; 1024]),
        ];
        test_from_data(to_insert);
    }

    #[test]
    fn fuzzer_b() {
        let data = vec![
            TestAction::Insert(BigKey::<u64, 256>::new(1095228325891), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(3096224743840768), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(749004913038733311), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(18230289816630788089), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(18446735329151090432), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(128434), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(4294967258), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(7277816997842399231), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(18385945474445279154), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(280375565746354), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(45568), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(8808972877568), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(8590131122), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(272678883712000), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(28428972659453951), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(18446735294791352064), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(193970), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(1096776417280), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(28428972659453944), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(18386508424398700466), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(280375565877426), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(270479860478464), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(227629727488), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(2986409983), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(866673871104), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(749075281782910975), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(11730937), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(18446735329151090432), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(128434), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(4160773120), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(759169024), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(41944653103338), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(3773172062810537984), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(41956837944524949), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(17593749733376), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(1563623424), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(1560281344), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(12813251448442880), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(10740950511298543765), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(838860800), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(491736783624786638), vec![0u8; 17]),
            TestAction::Insert(BigKey::new(327869), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(18446462598732840960), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(53198770610748672), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(661184721051266345), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(8796093034496), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(268444683468800), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(2199027450368), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(257449567200806), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(519695237120), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(3255307760466471209), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(2522068567888101421), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(1125056745783855), vec![0u8; 5]),
            TestAction::Insert(BigKey::new(4863), vec![0u8; 11]),
            TestAction::Insert(BigKey::new(848840156512003), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(142284501207154471), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(18410708676077879079), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(217298682054180864), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(277076930199551), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(17432379), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(4863), vec![0u8; 11]),
            TestAction::Insert(BigKey::new(47855161267191555), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(142284501207154471), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(15204011600974444839), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(217298682054381568), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(4398046511103), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(9223372036854775801), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(12388197510152058), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(9223372036854774055), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(576460752286590842), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(251638629179457535), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(30), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(3206556144328376103), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(4398046511104), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(3819055799724934143), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(576460752303367975), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(289079216299769639), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(142284501106491175), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(15204011463535491367), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(217298686248484864), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(1244967), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(288231475663273984), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(577309575280328703), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(18446743523953737721), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(3298534883194), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(9223372036854774271), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(576460752303368058), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(251638629179457319), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(142284501106360103), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(15204010544412490023), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(288230376151711744), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(3458767829535294463), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(576367293815007015), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(848840148057895), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(142284501106469415), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(15132094747964866560), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(1095233372169), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(9), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(5), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(15663113), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(23817), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(262383), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(399599728127), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(1095216660489), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(18374686879271089407), vec![0u8; 61]),
            TestAction::Insert(BigKey::new(23817), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(262153), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(399599728127), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(1095216660489), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(399599465727), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(3989292031), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(237), vec![0u8; 10]),
            TestAction::Insert(BigKey::new(647714935328997376), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(18374961357578502399), vec![0u8; 61]),
            TestAction::Insert(BigKey::new(262381), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(537038681599), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(71776119067901961), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(4611686418026853631), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(3979885823), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(237), vec![0u8; 10]),
            TestAction::Insert(BigKey::new(71254183025573888), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(71106559), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(332009393485), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(524293), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(399447621641), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(41956837944524949), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(17593749602304), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(1563623424), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(1560281088), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(12813251448442880), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(10740950511298543765), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(855638016), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(12667444087565609), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(0), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(189), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(71776115504447488), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(17955007290153970985), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(70650219137597440), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(53198770610748672), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(661184721051266345), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(8796093062912), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(257449567191040), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(4194816), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(257449569681446), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(1792), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(3255307760466471209), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(2522068567888101421), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(17955007289400229888), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(36028797018996736), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(12840605863068565248), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(12839761439816155136), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(13509701064982528), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(3206556144328376103), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(4398046511104), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(18446736545355923455), vec![0u8; 1024]),
        ];
        test_from_data(data);
    }

    #[test]
    fn simple_delete() {
        let storage = InMemoryStorage::new();
        let tree = Tree::new(storage).unwrap();
        let mut transaction = tree.transaction().unwrap();

        insert(&mut transaction, BigKey::<_, 256>::new(1), &[1, 2, 3]).unwrap();
        insert(&mut transaction, BigKey::new(2), &[4, 5, 6]).unwrap();

        let deleted_value = delete(&mut transaction, BigKey::new(2)).unwrap();

        transaction.commit().unwrap();

        assert_eq!(deleted_value, Some(vec![4, 5, 6]));

        assert_eq!(
            tree.iter().unwrap().map(|x| x.unwrap()).collect::<Vec<_>>(),
            &[(BigKey::new(1), vec![1, 2, 3])]
        );
        assert_properties(&mut tree.transaction().unwrap());
    }

    #[test]
    fn delete_with_merge() {
        let page_count = Arc::new(AtomicUsize::new(0));
        let storage = InMemoryStorage::new();
        let storage = InstrumentedStorage::new(storage, page_count.clone());
        let tree = Tree::new(storage).unwrap();
        let mut transaction = tree.transaction().unwrap();

        let mut i: u64 = 0;
        let mut data = vec![];

        while page_count.load(Ordering::Relaxed) < 3 {
            let key = BigKey::<_, 256>::new(i);
            let value = vec![1, 2, 3];
            insert(&mut transaction, key, &value).unwrap();
            data.push((key, value));

            i += 1;
        }

        delete(&mut transaction, BigKey::new(i - 1)).unwrap();
        data.pop();

        transaction.commit().unwrap();

        assert_eq!(
            tree.iter().unwrap().map(|x| x.unwrap()).collect::<Vec<_>>(),
            data
        );
        assert_properties(&mut tree.transaction().unwrap());
    }

    #[test]
    fn transaction_rollback() {
        let storage = InMemoryStorage::new();
        let tree = Tree::<_, u64>::new(storage).unwrap();

        let mut transaction = tree.transaction().unwrap();

        insert(&mut transaction, 1, &1u8.to_ne_bytes()).unwrap();
        insert(&mut transaction, 2, &2u8.to_ne_bytes()).unwrap();
        insert(&mut transaction, 3, &3u8.to_ne_bytes()).unwrap();

        transaction.rollback().unwrap();

        let mut transaction = tree.transaction().unwrap();

        insert(&mut transaction, 4, &1u8.to_ne_bytes()).unwrap();
        insert(&mut transaction, 5, &2u8.to_ne_bytes()).unwrap();

        transaction.commit().unwrap();

        let result = tree.iter().unwrap().map(|x| x.unwrap()).collect::<Vec<_>>();

        assert_eq!(
            &result,
            &[
                (4, 1u8.to_ne_bytes().to_vec()),
                (5, 2u8.to_ne_bytes().to_vec())
            ]
        )
    }

    #[test]
    fn fuzzer_c() {
        let data = vec![
            TestAction::Insert(BigKey::<u64, 256>::new(18446744030759919616), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(4251399053270056960), vec![0u8; 5]),
            TestAction::Insert(BigKey::new(18201297895007745287), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(42784197198217216), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(281474834956288), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(72056495516217088), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(72056494526300160), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(67553994393780737), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(42785295954870527), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(42785146620870656), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(42788595496386560), vec![0u8; 145]),
            TestAction::Delete(BigKey::new(1152921504623624191)),
            TestAction::Insert(BigKey::new(72057594021150720), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(67552895888787712), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(72056494549172224), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(42788595496386794), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(42784197449905415), vec![0u8; 1]),
            TestAction::Delete(BigKey::new(42784197198217216)),
        ];
        test_from_data(data);
    }

    #[test]
    fn fuzzer_d() {
        let data = vec![
            TestAction::Insert(BigKey::<u64, 256>::new(72057594021191680), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(4251399053270056960), vec![0u8; 5]),
            TestAction::Insert(BigKey::new(18201297895007745287), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(42784197198217216), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(281474969174016), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(72056495516217088), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(72056494526300160), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(67553994393780737), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(42785295954870527), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(42785146620870656), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(42788595496386560), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(67553995400443143), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(1499004928), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(383751094272), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(20174954561536), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(383745261568), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(18302628501888434173), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(563333698682880), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(563333698682886), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(562949953421312), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(196608), vec![0u8; 3]),
            TestAction::Insert(BigKey::new(72057594021183488), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(67553994393780224), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(42785295954870272), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(42785146637647872), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(42788595496386560), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(42788595496386794), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(42784197449905415), vec![0u8; 1]),
            TestAction::Delete(BigKey::new(42784197198217216)),
        ];
        test_from_data(data);
    }

    #[test]
    fn fuzzer_e() {
        let data = vec![
            TestAction::Insert(BigKey::<u64, 256>::new(291326600879931392), vec![0u8; 1]),
            TestAction::Delete(BigKey::new(3170534137752715140)),
            TestAction::Insert(BigKey::new(18324302742308257536), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(21673582219952384), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(0), vec![0u8; 9]),
            TestAction::Insert(BigKey::new(22799473540530176), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(70931692064604160), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(288230378218192896), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(66428094489755648), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(1125901973388800), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(17301504), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(21673741138084352), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(598134325937111040), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(72056704979697474), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(18446744073709551615), vec![0u8; 1]),
            TestAction::Delete(BigKey::new(18446744073709551611)),
            TestAction::Insert(BigKey::new(260723), vec![0u8; 28]),
            TestAction::Insert(BigKey::new(18374822817046724608), vec![0u8; 45]),
            TestAction::Delete(BigKey::new(18446744073709551615)),
            TestAction::Delete(BigKey::new(18374755856278355967)),
        ];
        test_from_data(data);
    }

    #[test]
    fn fuzzer_f() {
        let data = vec![
            TestAction::Insert(BigKey::<u64, 1024>::new(72056679496225041), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(255), vec![0u8; 1]),
        ];

        test_from_data(data);
    }

    #[test]
    fn fuzzer_g() {
        let data = vec![
            TestAction::Insert(BigKey::<u16, 1024>::new(256), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(15616), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(573), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(16426), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(16705), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(16705), vec![0u8; 2]),
            TestAction::Delete(BigKey::new(16705)),
            TestAction::Delete(BigKey::new(16895)),
            TestAction::Insert(BigKey::new(16705), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(16705), vec![0u8; 26]),
        ];

        test_from_data(data);
    }

    #[test]
    fn fuzzer_h() {
        let data = vec![
            TestAction::Insert(BigKey::<u32, 1024>::new(4009754624), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(31275), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(42244), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(788529219), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(1191247872), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(2114339119), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(1128481545), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(2030633027), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(1126956806), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(1124810751), vec![0u8; 7]),
            TestAction::Insert(BigKey::new(822362947), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(1124665616), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(4408131), vec![0u8; 10]),
            TestAction::Insert(BigKey::new(1128481741), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(3442212614), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(155386238), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(17573185), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(0), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(1124675393), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(4294967295), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(1094795585), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(592137), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(1105806147), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(1124204543), vec![0u8; 64]),
            TestAction::Insert(BigKey::new(4027055105), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(1061241153), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(2), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(125), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(1094797633), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(1228357953), vec![0u8; 2]),
            TestAction::Delete(BigKey::new(1161905217)),
            TestAction::Insert(BigKey::new(16772721), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(1094796609), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(1093215045), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(50660099), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(1128350017), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(1128997641), vec![0u8; 4]),
            TestAction::Insert(BigKey::new(4294918986), vec![0u8; 64]),
        ];
        test_from_data(data);
    }

    #[test]
    fn fuzzer_i() {
        let data = vec![
            TestAction::Insert(BigKey::<u32, 1024>::new(4538293), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(84215045), vec![0u8; 6]),
            TestAction::Insert(BigKey::new(8388608), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(84215045), vec![0u8; 6]),
            TestAction::Insert(BigKey::new(18944257), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(33488897), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(16843009), vec![0u8; 2]),
            TestAction::Delete(BigKey::new(16843009)),
            TestAction::Insert(BigKey::new(16852993), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(21037313), vec![0u8; 2]),
            TestAction::Delete(BigKey::new(17891839)),
            TestAction::Insert(BigKey::new(16843011), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(17105153), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(4278255873), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(0), vec![0u8; 1]),
        ];
        test_from_data(data);
    }

    #[test]
    fn fuzzer_j() {
        let data = vec![
            TestAction::Insert(BigKey::<u32, 1024>::new(44056501), vec![0u8; 1]),
            TestAction::Insert(BigKey::new(16844037), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(33551105), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(16842752), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(16843009), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(16843263), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(16843009), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(1090641921), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(16888321), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(16843009), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(16843009), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(16843009), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(0), vec![0u8; 2]),
            TestAction::Commit,
            TestAction::Commit,
            TestAction::Commit,
            TestAction::Commit,
            TestAction::Insert(BigKey::new(16843009), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(16843073), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(16843009), vec![0u8; 2]),
            TestAction::Insert(BigKey::new(285278465), vec![0u8; 2]),
            TestAction::Delete(BigKey::new(16843009)),
            TestAction::Insert(BigKey::new(4294902017), vec![0u8; 64]),
            TestAction::Rollback,
            TestAction::Rollback,
            TestAction::Insert(BigKey::new(1), vec![0u8; 1]),
        ];
        test_from_data(data);
    }

    #[test]
    fn simple_deadlock() {
        let data = vec![
            InThreadTestAction {
                thread: 3,
                action: TestAction::Delete(BigKey::<u64, 1024>::new(18446462598749748992)),
            },
            InThreadTestAction {
                thread: 0,
                action: TestAction::Insert(BigKey::new(0), vec![0u8; 1]),
            },
        ];

        threaded_test_from_data(data, |error| {
            matches!(error, TreeError::StorageError(StorageError::Deadlock(_)))
        });
    }
}
