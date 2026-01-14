mod algorithms;
pub mod dot;
mod node;

use std::marker::PhantomData;

use crate::bplustree::algorithms::first_leaf;
use crate::bplustree::algorithms::leaf_search;
use crate::bplustree::node::AnyNodeId;
use crate::bplustree::node::InteriorNodeId;
use crate::bplustree::node::LeafNodeId;
use crate::bplustree::node::NodeId;
use crate::bplustree::node::NodeTrait;
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

struct TreeIterator<'tree, T: Storage, TKey> {
    transaction: TreeTransaction<'tree, T, TKey>,
    current_leaf: LeafNodeId,
    index: usize,
    _key: PhantomData<TKey>,
}

impl<'tree, T: Storage, TKey: Pod + Ord> TreeIterator<'tree, T, TKey> {
    fn new(transaction: TreeTransaction<'tree, T, TKey>) -> Result<Self, TreeError> {
        // TODO introduce some better/more abstract API for reading the header?
        let root = transaction.read_header(|h| AnyNodeId::new(h.root))?;
        let first_leaf = first_leaf(&transaction, root)?;

        Ok(Self {
            transaction,
            current_leaf: first_leaf,
            index: 0,
            _key: PhantomData,
        })
    }
}

enum IteratorResult<TKey> {
    Value(TreeIteratorItem<TKey>),
    Next,
    None,
}

impl<'tree, T: Storage, TKey: Pod + Ord> Iterator for TreeIterator<'tree, T, TKey> {
    type Item = Result<(TKey, Vec<u8>), TreeError>;

    // TODO get rid of all the unwraps!
    fn next(&mut self) -> Option<Self::Item> {
        let read_result = self
            .transaction
            .read_node(self.current_leaf, |node| {
                match node.entries().nth(self.index) {
                    Some(entry) => {
                        self.index += 1;
                        IteratorResult::Value(Ok((entry.key(), entry.value().to_vec())))
                    }
                    None => {
                        if let Some(next_leaf) = node.next() {
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

struct TreeTransaction<'storage, TStorage: Storage + 'storage, TKey>
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

    fn insert(&self, page: Page) -> Result<PageIndex, TreeError> {
        Ok(self.transaction.insert(page)?)
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

    // TODO move out into algorithms?
    pub fn insert(&self, key: TKey, value: &[u8]) -> Result<(), TreeError> {
        let transaction = TreeTransaction::<'_, T, TKey> {
            transaction: self.storage.transaction()?,
            _key: PhantomData,
        };

        let root_index = AnyNodeId::new(transaction.read_header(|h| h.root)?);
        let target_node_id = leaf_search(&transaction, root_index, &key)?;

        let (parent_index, insert_result) = transaction.write_node(target_node_id, |node| {
            // TODO parent_index should be a part of insert_result
            let parent_index = node.parent();
            let insert_result = node.insert(key, value);
            (parent_index, insert_result)
        })?;
        let insert_result = insert_result?;

        match insert_result {
            LeafInsertResult::Done => Ok(()),
            LeafInsertResult::Split {
                mut new_node,
                split_key,
            } => {
                let new_node_reservation = transaction.reserve_node()?;
                let new_node_id = LeafNodeId::new(new_node_reservation.index());

                let next = transaction.read_node(target_node_id, |node| node.next())?;

                if let Some(parent_id) = parent_index {
                    let new_leaf_reservation = transaction.reserve_node().unwrap();
                    let new_leaf_id = LeafNodeId::new(new_leaf_reservation.index());

                    let grandparent_id =
                        transaction.read_node(parent_id, |reader| reader.parent())?;
                    let new_grandparent_reservation = transaction.reserve_node()?;

                    let split_node = transaction.write_node(parent_id, |node| {
                        match node.insert_node(&split_key, new_node_id.into()) {
                            node::interior::InteriorInsertResult::Ok => None,
                            node::interior::InteriorInsertResult::Split(mut new_node) => {
                                // TODO get rid of the direct writes to node.data here, and make
                                // the node expose some API
                                let new_leaf_id = LeafNodeId::new(new_leaf_reservation.index());
                                let mut new_leaf = LeafNode::<TKey>::new();

                                new_leaf.set_links(Some(parent_id), Some(new_leaf_id), next);

                                new_node.set_first_pointer(new_leaf_id.into());
                                new_node.set_parent(grandparent_id);

                                Some((new_leaf, new_node))
                            }
                        }
                    })?;

                    match split_node {
                        Some((new_leaf, mut new_interior)) => {
                            let new_interior_reservation = transaction.reserve_node()?;
                            let new_interior_id =
                                InteriorNodeId::new(new_interior_reservation.index());

                            let grandparent_id = match grandparent_id {
                                None => {
                                    let mut new_grandparent = InteriorNode::<TKey>::new();
                                    let new_grandparent_id =
                                        InteriorNodeId::new(new_grandparent_reservation.index());
                                    transaction
                                        .write_header(|h| h.root = new_grandparent_id.page())?;

                                    new_grandparent.set_first_pointer(parent_id.into());

                                    transaction.insert_reserved(
                                        new_grandparent_reservation,
                                        Page::from_data(new_grandparent),
                                    )?;

                                    new_grandparent_id
                                }
                                Some(x) => x,
                            };

                            transaction.write_node(grandparent_id, |node| {
                                let grandparent_insert_result = node.insert_node(
                                    &new_interior.first_key().unwrap(),
                                    new_interior_id.into(),
                                );
                                match grandparent_insert_result {
                                    node::interior::InteriorInsertResult::Ok => {}
                                    node::interior::InteriorInsertResult::Split(_) => todo!(),
                                }
                            })?;

                            new_interior.set_first_pointer(new_leaf_id.into());

                            transaction
                                .insert_reserved(new_leaf_reservation, Page::from_data(new_leaf))?;
                            transaction.insert_reserved(
                                new_interior_reservation,
                                Page::from_data(*new_interior),
                            )?;
                        }
                        None => {
                            transaction.write_node(target_node_id, |target_node| {
                                target_node.set_links(
                                    target_node.parent(),
                                    target_node.previous(),
                                    Some(new_node_id),
                                );
                            })?;

                            new_node.set_links(Some(parent_id), Some(target_node_id), next);
                        }
                    }
                } else {
                    let new_root_page = Page::from_data(InteriorNode::create_root(
                        &[&split_key],
                        &[root_index, new_node_id.into()],
                    ));
                    let new_root_page_index = transaction.insert(new_root_page)?;
                    let new_root_page_id = InteriorNodeId::new(new_root_page_index);

                    new_node.set_links(Some(new_root_page_id), Some(target_node_id), next);

                    transaction.write_node(target_node_id, |target_node| {
                        target_node.set_links(
                            Some(new_root_page_id),
                            target_node.previous(),
                            Some(new_node_id),
                        );
                    })?;

                    transaction.write_header(|header| header.root = new_root_page_index)?;
                }

                transaction.insert_reserved(new_node_reservation, Page::from_data(*new_node))?;

                Ok(())
            }
        }
    }

    #[allow(unused)]
    fn iter(&self) -> Result<impl Iterator<Item = TreeIteratorItem<TKey>>, TreeError> {
        TreeIterator::new(self.transaction()?)
    }

    fn transaction(&self) -> Result<TreeTransaction<'_, T, TKey>, TreeError> {
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
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use crate::storage::in_memory::{InMemoryStorage, test::TestStorage};

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

    #[test]
    // TODO optimize this test
    fn insert_multiple_nodes() {
        let page_count = Arc::new(AtomicUsize::new(0));

        let storage = TestStorage::new(InMemoryStorage::new(), page_count.clone());
        let tree = Tree::<_, usize>::new(storage).unwrap();

        // TODO: find a more explicit way of counting nodes
        let mut i = 0usize;

        // 10 pages should give us a resonable number of node splits to assume that the basic logic
        //    works
        while page_count.load(Ordering::Relaxed) < 1024 {
            // make the value bigger with repeat so fewer inserts are needed and the test runs faster
            tree.insert(i, &(usize::max_value() - i).to_be_bytes().repeat(128))
                .unwrap();

            i += 1;
        }

        let entry_count = i;

        for (i, item) in tree.iter().unwrap().enumerate() {
            assert!(i < entry_count);
            let (key, value) = item.unwrap();

            assert!(value == value[..size_of::<usize>()].repeat(128));

            let value: usize =
                usize::from_be_bytes(value[0..size_of::<usize>()].try_into().unwrap());

            assert!(key == i);
            assert!(value == usize::max_value() - i);
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
            tree.insert(i, &value.repeat(128)).unwrap();

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
}
