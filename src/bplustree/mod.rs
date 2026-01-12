mod algorithms;
pub mod dot;
mod node;

use std::marker::PhantomData;

use crate::bplustree::algorithms::first_leaf;
use crate::bplustree::algorithms::leaf_search;
use crate::bplustree::node::AnyNodeId;
use crate::bplustree::node::AnyNodeReader;
use crate::bplustree::node::InteriorNodeId;
use crate::bplustree::node::LeafNodeId;
use crate::bplustree::node::Node;
use crate::bplustree::node::NodeId;
use crate::bplustree::node::NodeReader;
use crate::bplustree::node::NodeWriter;
use crate::bplustree::node::interior::InteriorNodeReader;
use crate::bplustree::node::interior::InteriorNodeWriter;
use crate::bplustree::node::leaf::LeafInsertResult;
use crate::bplustree::node::leaf::LeafNodeReader;
use crate::bplustree::node::leaf::LeafNodeWriter;
use crate::page::Page;
use crate::storage::PageIndex;
use crate::storage::PageReservation;
use crate::storage::Storage;
use crate::storage::StorageError;
use crate::storage::Transaction;
use bytemuck::{Pod, Zeroable};
use thiserror::Error;

use crate::page::PAGE_DATA_SIZE;

const ROOT_NODE_TAIL_SIZE: usize = PAGE_DATA_SIZE - size_of::<u64>() * 2 - size_of::<PageIndex>();

struct TreeIterator<'tree, T: Storage, TKey> {
    transaction: TreeTransaction<'tree, T, TKey>,
    current_leaf: LeafNodeId,
    index: usize,
    _key: PhantomData<TKey>,
}

impl<'tree, T: Storage, TKey: Pod + PartialOrd> TreeIterator<'tree, T, TKey> {
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

impl<'tree, T: Storage, TKey: Pod + PartialOrd> Iterator for TreeIterator<'tree, T, TKey> {
    // TODO we should use references here instead of copying into Vecs
    type Item = Result<(TKey, Vec<u8>), TreeError>;

    // TODO get rid of all the unwraps!
    fn next(&mut self) -> Option<Self::Item> {
        let read_result = self
            .transaction
            .read_node(self.current_leaf, |reader| {
                match reader.entries().nth(self.index) {
                    Some(entry) => {
                        self.index += 1;
                        IteratorResult::Value(Ok((*entry.key(), entry.value().to_vec())))
                    }
                    None => {
                        if let Some(next_leaf) = reader.next() {
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
    value_size: usize,
    _key: PhantomData<TKey>,
}

struct TreeTransaction<'storage, TStorage: Storage + 'storage, TKey>
where
    Self: 'storage,
{
    transaction: TStorage::Transaction<'storage>,

    // TODO figure something out so we don't have to pass those around everywhere
    value_size: usize,

    _key: PhantomData<&'storage TKey>,
}

impl<'storage, TStorage: Storage + 'storage, TKey: Pod + PartialOrd>
    TreeTransaction<'storage, TStorage, TKey>
{
    fn read_header<TReturn>(
        &self,
        read: impl FnOnce(&TreeData) -> TReturn,
    ) -> Result<TReturn, TreeError> {
        Ok(self
            .transaction
            .read(PageIndex::zeroed(), |page| read(page.data()))?)
    }

    fn write_header<TReturn>(
        &self,
        write: impl FnOnce(&mut TreeData) -> TReturn,
    ) -> Result<TReturn, TreeError> {
        Ok(self
            .transaction
            .write(PageIndex::zeroed(), |page| write(page.data_mut()))?)
    }

    fn read_node<TReturn, TNodeId: NodeId>(
        &self,
        index: TNodeId,
        read: impl for<'node> FnOnce(TNodeId::Reader<'node, TKey>) -> TReturn,
    ) -> Result<TReturn, TreeError> {
        Ok(self.transaction.read(index.page(), |page| {
            let reader =
                <TNodeId::Reader<'_, TKey> as NodeReader<TKey>>::new(page.data(), self.value_size);

            read(reader)
        })?)
    }

    fn write_node<TReturn, TNodeId: NodeId>(
        &self,
        index: TNodeId,
        write: impl for<'node> FnOnce(TNodeId::Writer<'node, TKey>) -> TReturn,
    ) -> Result<TReturn, TreeError> {
        Ok(self.transaction.write(index.page(), |page| {
            let writer = <TNodeId::Writer<'_, TKey> as NodeWriter<TKey>>::new(
                page.data_mut(),
                self.value_size,
            );
            write(writer)
        })?)
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

// TODO this should be by reference not by copy perhaps?
type TreeIteratorItem<TKey> = Result<(TKey, Vec<u8>), TreeError>;

impl<T: Storage, TKey: Pod + PartialOrd> Tree<T, TKey> {
    // TODO also create a "new_read" method, or something like that (that reads a tree that already
    // exists from storage)
    pub fn new(mut storage: T, value_size: usize) -> Result<Self, TreeError> {
        // TODO assert that the storage is empty, and that the header get's the 0th page, as we
        // depend on that invariant (i.e. PageIndex=0 must always refer to the TreeData and not to
        // a node)!

        TreeData::new_in(&mut storage, size_of::<TKey>(), value_size)?;

        Ok(Self {
            storage,
            value_size,
            _key: PhantomData,
        })
    }

    // TODO move out into algorithms?
    pub fn insert(&self, key: TKey, value: &[u8]) -> Result<(), TreeError> {
        let transaction = TreeTransaction::<'_, T, TKey> {
            transaction: self.storage.transaction()?,
            value_size: self.value_size,
            _key: PhantomData,
        };

        let root_index = AnyNodeId::new(transaction.read_header(|h| h.root)?);
        let target_node_id = leaf_search(&transaction, root_index, &key)?;

        let (parent_index, insert_result) =
            transaction.write_node(target_node_id, |mut writer| {
                // TODO parent_index should be a part of insert_result
                let parent_index = writer.reader().parent();
                let insert_result = writer.insert(key, value);
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

                let mut new_node_writer: LeafNodeWriter<'_, TKey> =
                    LeafNodeWriter::new(&mut new_node, self.value_size);

                let next = transaction.read_node(target_node_id, |reader| reader.next())?;

                if let Some(parent_id) = parent_index {
                    transaction.write_node(parent_id, |mut writer| {
                        match writer.insert_node(&split_key, new_node_id.into()) {
                            node::interior::InteriorInsertResult::Ok => {}
                            node::interior::InteriorInsertResult::Split => todo!(),
                        }
                    })?;

                    transaction.write_node(target_node_id, |mut target_node| {
                        target_node.set_links(
                            target_node.reader().parent(),
                            target_node.reader().previous(),
                            Some(new_node_id),
                        );
                    })?;

                    new_node_writer.set_links(Some(parent_id), Some(target_node_id), next);
                } else {
                    let new_root_page = Page::from_data(InteriorNodeWriter::create_root(
                        &[&split_key],
                        &[root_index, new_node_id.into()],
                    ));
                    let new_root_page_index = transaction.insert(new_root_page)?;
                    let new_root_page_id = InteriorNodeId::new(new_root_page_index);

                    new_node_writer.set_links(Some(new_root_page_id), Some(target_node_id), next);

                    transaction.write_node(target_node_id, |mut target_node| {
                        target_node.set_links(
                            Some(new_root_page_id),
                            target_node.reader().previous(),
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
            value_size: self.value_size,
            _key: PhantomData,
        })
    }
}

#[derive(Debug, Pod, Zeroable, Clone, Copy)]
#[repr(C)]
struct TreeData {
    key_size: u64,
    value_size: u64,
    root: PageIndex,
    _unused: [u8; ROOT_NODE_TAIL_SIZE],
}

const _: () = assert!(
    size_of::<TreeData>() == PAGE_DATA_SIZE,
    "The Tree descriptor must have size of exactly one page"
);

#[derive(Debug, Error)]
pub enum TreeError {
    #[error("The provided value's length does not match the one defined in the tree")]
    InvalidValueLength,
    #[error("Storage error: {0}")]
    StorageError(#[from] StorageError),
}

impl TreeData {
    pub fn new_in<T: Storage>(
        storage: &mut T,
        key_size: usize,
        value_size: usize,
    ) -> Result<(), TreeError> {
        let transaction = storage.transaction()?;

        let header_page = transaction.reserve()?;
        assert!(header_page.index() == PageIndex::zeroed());

        let root_index = transaction.insert(Page::from_data(Node::new_leaf()))?;

        let page = Page::from_data(Self {
            key_size: key_size as u64,
            value_size: value_size as u64,
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

    #[test]
    fn insert() {
        let mut node = Node::zeroed();
        let mut accessor = LeafNodeWriter::new(&mut node, 8);

        assert!(matches!(
            accessor.insert(0u64, &[0; 9]),
            Err(TreeError::InvalidValueLength)
        ));
    }

    #[test]
    fn node_accessor_entries() {
        let mut node = Node::zeroed();

        assert!(matches!(
            LeafNodeReader::<'_, u64>::new(&node, 16).entries().next(),
            None
        ));

        let insert_result = LeafNodeWriter::new(&mut node, 16)
            .insert(1usize, &[2; 16])
            .unwrap();

        assert!(matches!(insert_result, LeafInsertResult::Done));

        let reader = LeafNodeReader::<'_, usize>::new(&node, 16);
        let mut iter = reader.entries();
        let first = iter.next().unwrap();
        assert!(*first.key() == 1);
        assert!(first.value() == &[2; 16]);

        assert!(matches!(iter.next(), None));

        drop(iter);

        let insert_result = LeafNodeWriter::new(&mut node, 16)
            .insert(2usize, &[1; 16])
            .unwrap();

        assert!(matches!(insert_result, LeafInsertResult::Done));

        let reader = LeafNodeReader::<'_, usize>::new(&node, 16);
        let mut iter = reader.entries();

        let first = iter.next().unwrap();
        assert!(*first.key() == 1);
        assert!(dbg!(first.value()) == &[2; 16]);

        let second = iter.next().unwrap();
        assert!(*second.key() == 2);
        assert!(second.value() == &[1; 16]);

        assert!(matches!(iter.next(), None));
    }

    #[test]
    fn insert_multiple_nodes() {
        let page_count = Arc::new(AtomicUsize::new(0));

        let storage = TestStorage::new(InMemoryStorage::new(), page_count.clone());
        let tree = Tree::<_, usize>::new(storage, size_of::<usize>()).unwrap();

        // TODO: find a more explicit way of counting nodes
        let mut i = 0usize;

        // 10 pages should give us a resonable number of node splits to assume that the basic logic
        //    works
        while page_count.load(Ordering::Relaxed) < 10 {
            tree.insert(i, &(usize::max_value() - i).to_be_bytes())
                .unwrap();

            i += 1;
        }

        let entry_count = i;

        for (i, item) in tree.iter().unwrap().enumerate() {
            assert!(i < entry_count);
            let (key, value) = item.unwrap();

            let value: usize = usize::from_be_bytes(value.try_into().unwrap());

            assert!(key == i);
            assert!(value == usize::max_value() - i);
        }

        // TODO iterate the tree and ensure that all the keys are there, in correct order with
        // correct values
    }
}
