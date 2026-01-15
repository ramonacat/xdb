pub mod algorithms;
pub mod dot;
mod node;

use std::marker::PhantomData;

use crate::bplustree::algorithms::first_leaf;
use crate::bplustree::algorithms::last_leaf;
use crate::bplustree::node::AnyNodeId;
use crate::bplustree::node::InteriorNodeId;
use crate::bplustree::node::LeafNodeId;
use crate::bplustree::node::Node;
use crate::bplustree::node::NodeId;
use crate::bplustree::node::interior::InteriorNode;
use crate::bplustree::node::leaf::LeafInsertResult;
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

const ROOT_NODE_TAIL_SIZE: usize = PAGE_DATA_SIZE - size_of::<u64>() - size_of::<PageIndex>();

struct TreeIterator<'tree, T: Storage, TKey, const REVERSE: bool> {
    transaction: TreeTransaction<'tree, T, TKey>,
    current_leaf: LeafNodeId,
    index: usize,
}

impl<'tree, T: Storage, TKey: Pod + Ord, const REVERSE: bool>
    TreeIterator<'tree, T, TKey, REVERSE>
{
    fn new(transaction: TreeTransaction<'tree, T, TKey>) -> Result<Self, TreeError> {
        // TODO introduce some better/more abstract API for reading the header?
        let root = transaction.read_header(|h| AnyNodeId::new(h.root))?;
        let starting_leaf = if !REVERSE {
            first_leaf(&transaction, root)?
        } else {
            last_leaf(&transaction, root)?
        };

        Ok(Self {
            transaction,
            current_leaf: starting_leaf,
            index: 0,
        })
    }
}

enum IteratorResult<TKey> {
    Value(TreeIteratorItem<TKey>),
    Next,
    None,
}

impl<'tree, T: Storage, TKey: Pod + Ord, const REVERSE: bool> Iterator
    for TreeIterator<'tree, T, TKey, REVERSE>
{
    type Item = Result<(TKey, Vec<u8>), TreeError>;

    // TODO get rid of all the unwraps!
    fn next(&mut self) -> Option<Self::Item> {
        let read_result = self
            .transaction
            .read_node(self.current_leaf, |node| {
                // TODO expose an API on node that will allow us to avoid collecting all the
                // entries here
                let entries = node.entries().collect::<Vec<_>>();

                let entry = if !REVERSE {
                    entries.get(self.index)
                } else if entries.is_empty() || self.index >= entries.len() {
                    None
                } else {
                    Some(&entries[entries.len() - self.index - 1])
                };

                match entry {
                    Some(entry) => {
                        self.index += 1;

                        IteratorResult::Value(Ok((entry.key(), entry.value().to_vec())))
                    }
                    None => {
                        if let Some(next_leaf) = if !REVERSE {
                            node.next()
                        } else {
                            node.previous()
                        } {
                            self.current_leaf = next_leaf;
                            self.index = 0;

                            IteratorResult::Next
                        } else {
                            IteratorResult::None
                        }
                    }
                }
            })
            .unwrap();

        match read_result {
            IteratorResult::Value(x) => Some(x),
            IteratorResult::Next => self.next(),
            IteratorResult::None => None,
        }
    }
}

#[derive(Debug)]
pub struct Tree<T: Storage, TKey> {
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

impl<'storage, TStorage: Storage + 'storage, TKey: Pod + Ord>
    TreeTransaction<'storage, TStorage, TKey>
{
    fn read_header<TReturn>(
        &self,
        read: impl FnOnce(&TreeHeader) -> TReturn,
    ) -> Result<TReturn, TreeError> {
        Ok(self
            .transaction
            .read(PageIndex::zero(), |page| read(page.data()))?)
    }

    fn write_header<TReturn>(
        &self,
        write: impl FnOnce(&mut TreeHeader) -> TReturn,
    ) -> Result<TReturn, TreeError> {
        Ok(self
            .transaction
            .write(PageIndex::zero(), |page| write(page.data_mut()))?)
    }

    fn read_node<TReturn, TNodeId: NodeId>(
        &self,
        index: TNodeId,
        read: impl for<'node> FnOnce(&TNodeId::Node<TKey>) -> TReturn,
    ) -> Result<TReturn, TreeError> {
        Ok(self
            .transaction
            .read(index.page(), |page| read(page.data()))?)
    }

    fn write_node<TReturn, TNodeId: NodeId>(
        &self,
        index: TNodeId,
        write: impl for<'node> FnOnce(&mut TNodeId::Node<TKey>) -> TReturn,
    ) -> Result<TReturn, TreeError> {
        Ok(self
            .transaction
            .write(index.page(), |page| write(page.data_mut()))?)
    }

    fn reserve_node(&self) -> Result<TStorage::PageReservation<'storage>, TreeError> {
        Ok(self.transaction.reserve()?)
    }

    fn insert_reserved(
        &self,
        reservation: TStorage::PageReservation<'storage>,
        page: Page,
    ) -> Result<(), TreeError> {
        self.transaction.insert_reserved(reservation, page)?;

        Ok(())
    }
}

type TreeIteratorItem<TKey> = Result<(TKey, Vec<u8>), TreeError>;

impl<T: Storage, TKey: Pod + Ord> Tree<T, TKey> {
    // TODO also create a "new_read" method, or something like that (that reads a tree that already
    // exists from storage)
    pub fn new(mut storage: T) -> Result<Self, TreeError> {
        // TODO assert that the storage is empty, and that the header get's the 0th page, as we
        // depend on that invariant (i.e. PageIndex=0 must always refer to the TreeData and not to
        // a node)!

        TreeHeader::new_in(&mut storage, size_of::<TKey>())?;

        Ok(Self {
            storage,
            _key: PhantomData,
        })
    }

    pub fn iter(&self) -> Result<impl Iterator<Item = TreeIteratorItem<TKey>>, TreeError> {
        TreeIterator::<_, _, false>::new(self.transaction()?)
    }

    // TODO probably should just use iter with DoubleEndedIterator
    pub fn iter_reverse(&self) -> Result<impl Iterator<Item = TreeIteratorItem<TKey>>, TreeError> {
        TreeIterator::<_, _, true>::new(self.transaction()?)
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
    _unused: [u8; ROOT_NODE_TAIL_SIZE],
}

const _: () = assert!(
    size_of::<TreeHeader>() == PAGE_DATA_SIZE,
    "The Tree descriptor must have size of exactly one page"
);

#[derive(Debug, Error)]
pub enum TreeError {
    #[error("Storage error: {0}")]
    StorageError(#[from] StorageError),
}

impl TreeHeader {
    pub fn new_in<T: Storage>(storage: &mut T, key_size: usize) -> Result<(), TreeError> {
        let transaction = storage.transaction()?;

        let header_page = transaction.reserve()?;
        assert!(header_page.index() == PageIndex::zero());

        let root_index = transaction.insert(Page::from_data(LeafNode::<usize>::new()))?;

        let page = Page::from_data(Self {
            key_size: key_size as u64,
            root: root_index,
            _unused: [0; _],
        });

        transaction.insert_reserved(header_page, page)?;

        Ok(())
    }
}

// TODO: add quickcheck tests: https://rust-fuzz.github.io/book/cargo-fuzz/structure-aware-fuzzing.html
#[cfg(test)]
mod test {
    use std::{
        collections::BTreeMap,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use quickcheck::{Arbitrary, TestResult};
    use quickcheck_macros::quickcheck;

    use crate::{
        bplustree::algorithms::insert,
        storage::in_memory::{InMemoryStorage, test::TestStorage},
    };

    use super::*;

    // TODO assert on properties of the tree (balanced, etc.) where it makes sense

    #[test]
    fn node_accessor_entries() {
        let mut node = LeafNode::zeroed();

        assert!(matches!(node.entries().next(), None));

        let insert_result = node.insert(1usize, &[2; 16]).unwrap();

        assert!(matches!(insert_result, LeafInsertResult::Done));

        let mut iter = node.entries();
        let first = iter.next().unwrap();
        assert!(first.key() == 1);
        assert!(first.value() == &[2; 16]);

        assert!(matches!(iter.next(), None));

        drop(iter);

        let insert_result = node.insert(2usize, &[1; 16]).unwrap();

        assert!(matches!(insert_result, LeafInsertResult::Done));

        let mut iter = node.entries();

        let first = iter.next().unwrap();
        assert!(first.key() == 1);
        assert!(first.value() == &[2; 16]);

        let second = iter.next().unwrap();
        assert!(second.key() == 2);
        assert!(second.value() == &[1; 16]);

        assert!(matches!(iter.next(), None));
    }

    // TODO the below two tests are mostly copy-paste, refactor some sorta abstraction over them
    #[test]
    // TODO optimize this test
    fn insert_multiple_nodes() {
        let page_count = Arc::new(AtomicUsize::new(0));

        let storage = TestStorage::new(InMemoryStorage::new(), page_count.clone());
        let tree = Tree::<_, usize>::new(storage).unwrap();

        // TODO: find a more explicit way of counting nodes
        let mut i = 0usize;

        let tree_transaction = tree.transaction().unwrap();

        while page_count.load(Ordering::Relaxed) < 1024 {
            // make the value bigger with repeat so fewer inserts are needed and the test runs faster
            insert(
                &tree_transaction,
                i,
                &(u16::max_value() - i as u16).to_be_bytes().repeat(128),
            )
            .unwrap();

            i += 1;
        }

        let entry_count = i;

        let mut final_i = 0;

        for (i, item) in tree.iter().unwrap().enumerate() {
            assert!(i < entry_count);
            let (key, value) = item.unwrap();

            assert!(value == value[..size_of::<u16>()].repeat(128));

            let value: u16 = u16::from_be_bytes(value[0..size_of::<u16>()].try_into().unwrap());

            assert!(key == i);
            assert!(value == u16::max_value() - i as u16);

            final_i = i;
        }

        assert!(final_i == entry_count - 1);

        for (i, item) in tree.iter_reverse().unwrap().enumerate() {
            let i = entry_count - i - 1;

            assert!(i < entry_count);
            let (key, value) = item.unwrap();

            assert!(value == value[..size_of::<u16>()].repeat(128));

            let value = u16::from_be_bytes(value[0..size_of::<u16>()].try_into().unwrap());

            assert!(key == i);
            assert!(value == u16::max_value() - i as u16);
        }

        // TODO iterate the tree and ensure that all the keys are there, in correct order with
        // correct values
    }

    #[test]
    // TODO optimize this test
    fn variable_sized_keys() {
        let page_count = Arc::new(AtomicUsize::new(0));

        let storage = TestStorage::new(InMemoryStorage::new(), page_count.clone());
        let tree = Tree::<_, usize>::new(storage).unwrap();

        // TODO: find a more explicit way of counting nodes
        let mut i = 0usize;

        let transaction = tree.transaction().unwrap();

        // 10 pages should give us a resonable number of node splits to assume that the basic logic
        //    works
        while page_count.load(Ordering::Relaxed) < 10 {
            let value: &[u8] = match i % 8 {
                0 | 7 | 6 | 5 => &(i as u64).to_be_bytes(),
                4 | 3 => &(i as u32).to_be_bytes(),
                2 => &(i as u16).to_be_bytes(),
                1 => &(i as u8).to_be_bytes(),
                _ => unreachable!(),
            };

            // make the value bigger with repeat so fewer inserts are needed and the test runs faster
            insert(&transaction, i, &value.repeat(128)).unwrap();

            i += 1;
        }

        let entry_count = i;

        for (i, item) in tree.iter().unwrap().enumerate() {
            assert!(i < entry_count);
            let (key, value) = item.unwrap();

            let value_matches_expected = match i % 8 {
                0 | 7 | 6 | 5 => {
                    i as u64 == u64::from_be_bytes(value[..size_of::<u64>()].try_into().unwrap())
                }
                4 | 3 => {
                    i as u32 == u32::from_be_bytes(value[..size_of::<u32>()].try_into().unwrap())
                }
                2 => i as u16 == u16::from_be_bytes(value[..size_of::<u16>()].try_into().unwrap()),
                1 => i as u8 == u8::from_be_bytes(value[..size_of::<u8>()].try_into().unwrap()),
                _ => unreachable!(),
            };

            assert!(value[..value.len() / 128].repeat(128) == value);
            assert!(key == i);
            assert!(value_matches_expected);
        }
    }

    #[test]
    fn insert_reverse() {
        let storage = InMemoryStorage::new();
        let tree = Tree::new(storage).unwrap();
        let transaction = tree.transaction().unwrap();

        insert(&transaction, 1, &[0]).unwrap();
        insert(&transaction, 0, &[0]).unwrap();

        let result = tree.iter().unwrap().map(|x| x.unwrap()).collect::<Vec<_>>();

        assert!(result == &[(0, vec![0]), (1, vec![0])]);
    }

    #[test]
    fn same_key_overrides() {
        let storage = InMemoryStorage::new();
        let tree = Tree::new(storage).unwrap();
        let transaction = tree.transaction().unwrap();

        insert(&transaction, 1, &0u8.to_ne_bytes()).unwrap();
        insert(&transaction, 1, &1u8.to_ne_bytes()).unwrap();

        let result = tree.iter().unwrap().map(|x| x.unwrap()).collect::<Vec<_>>();
        dbg!(&result);
        assert!(result == vec![(1, 1u8.to_ne_bytes().to_vec())]);
    }

    #[derive(Debug, Clone)]
    struct Value(Vec<u8>);

    impl quickcheck::Arbitrary for Value {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            let mut result = vec![];

            let count = *g.choose(&(1..512).collect::<Vec<usize>>()).unwrap();
            for _ in 0..count {
                result.push(Arbitrary::arbitrary(g));
            }

            Value(result)
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
            Box::new(self.0.shrink().filter(|x| !x.is_empty()).map(|x| Value(x)))
        }
    }

    #[quickcheck]
    fn always_sorted(values: Vec<(u64, Value)>) -> TestResult {
        let storage = InMemoryStorage::new();
        let tree = Tree::new(storage).unwrap();
        let transaction = tree.transaction().unwrap();

        for (key, value) in &values {
            insert(&transaction, *key, &value.0).unwrap();
        }

        let mut sorted_values = values
            .iter()
            .map(|x| (x.0, x.1.0.clone()))
            .collect::<Vec<_>>()
            .clone();
        sorted_values.sort_by_key(|x| x.0);
        let sorted_values = sorted_values
            .into_iter()
            .collect::<BTreeMap<u64, Vec<u8>>>()
            .into_iter()
            .collect::<Vec<_>>();

        let result = tree.iter().unwrap().map(|x| x.unwrap()).collect::<Vec<_>>();

        // TODO also test iter_reverse
        TestResult::from_bool(result == sorted_values)
    }
}
