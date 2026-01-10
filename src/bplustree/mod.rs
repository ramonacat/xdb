pub mod dot;
mod node;

use crate::bplustree::node::Node;
use crate::bplustree::node::interior::InteriorNodeReader;
use crate::bplustree::node::interior::InteriorNodeWriter;
use crate::bplustree::node::leaf::LeafInsertResult;
use crate::bplustree::node::leaf::LeafNodeReader;
use crate::bplustree::node::leaf::LeafNodeWriter;
use crate::page::Page;
use crate::storage::PageIndex;
use crate::storage::Storage;
use crate::storage::StorageError;
use crate::storage::Transaction;
use bytemuck::{Pod, Zeroable};
use std::collections::VecDeque;
use thiserror::Error;

use crate::page::PAGE_DATA_SIZE;

const ROOT_NODE_TAIL_SIZE: usize = PAGE_DATA_SIZE - size_of::<u64>() * 2 - size_of::<PageIndex>();

// TODO this is an abomination, we won't need it once we actually have the next/previous keys
// in leaf nodes!
struct TreeIterator<'tree, T: Storage> {
    transaction: TreeTransaction<'tree, T>,
    nodes_to_visit: VecDeque<PageIndex>,
    index: usize,

    // TODO these should really not be needed here, we should just depend on the TreeTransaction to
    // read whatever data it needs to read
    key_size: usize,
    value_size: usize,
}

impl<'tree, T: Storage> TreeIterator<'tree, T> {
    fn new(
        transaction: TreeTransaction<'tree, T>,
        key_size: usize,
        value_size: usize,
    ) -> Result<Self, TreeError> {
        let root = transaction.read_header(|h| h.root)?;

        Ok(Self {
            transaction,
            nodes_to_visit: VecDeque::from([root]),
            index: 0,
            key_size,
            value_size,
        })
    }
}

impl<'tree, T: Storage> Iterator for TreeIterator<'tree, T> {
    // TODO we should use references here instead of copying into Vecs
    type Item = Result<(Vec<u8>, Vec<u8>), TreeError>;

    // TODO get rid of all the unwraps!
    fn next(&mut self) -> Option<Self::Item> {
        let last = self.nodes_to_visit.front()?;

        let result = self.transaction.read_node(*last, |last| {
            if last.is_leaf() {
                match LeafNodeReader::new(last, self.key_size, self.value_size)
                    .entries()
                    .nth(self.index)
                {
                    Some(entry) => {
                        self.index += 1;

                        return Some(Ok((entry.key().to_vec(), entry.value().to_vec())));
                    }
                    None => {
                        self.nodes_to_visit.pop_front();

                        self.index = 0;
                    }
                }
            } else {
                self.nodes_to_visit.pop_front().unwrap();

                let mut interior_nodes = InteriorNodeReader::new(last, self.key_size)
                    .values()
                    .collect::<Vec<_>>();
                interior_nodes.reverse();

                for next_node in interior_nodes {
                    self.nodes_to_visit.push_front(next_node);
                }
            }

            None
        });

        match result.unwrap() {
            Some(r) => Some(r),
            None => self.next(),
        }
    }
}

#[derive(Debug)]
pub struct Tree<T: Storage> {
    storage: T,
    key_size: usize,
    value_size: usize,
}

struct TreeTransaction<'storage, TStorage: Storage + 'storage> {
    transaction: TStorage::Transaction<'storage>,

    // TODO figure something out so we don't have to pass those around everywhere
    key_size: usize,
    value_size: usize,
}

impl<'storage, TStorage: Storage + 'storage> TreeTransaction<'storage, TStorage> {
    fn read_header<TReturn>(
        &self,
        read: impl FnOnce(&TreeData) -> TReturn,
    ) -> Result<TReturn, TreeError> {
        Ok(self
            .transaction
            .read(PageIndex::zeroed(), |page| read(page.data()))?)
    }

    // TODO make this take a non-mut reference, and do per-page locking
    fn write_header<TReturn>(
        &mut self,
        write: impl FnOnce(&mut TreeData) -> TReturn,
    ) -> Result<TReturn, TreeError> {
        Ok(self
            .transaction
            .write(PageIndex::zeroed(), |page| write(page.data_mut()))?)
    }

    fn read_node<TReturn>(
        &self,
        index: PageIndex,
        read: impl FnOnce(&Node) -> TReturn,
    ) -> Result<TReturn, TreeError> {
        assert!(index != PageIndex::zeroed());

        Ok(self.transaction.read(index, |page| read(page.data()))?)
    }

    // TODO make this take a non-mut reference
    fn write_node<TReturn>(
        &mut self,
        index: PageIndex,
        write: impl FnOnce(&mut Node) -> TReturn,
    ) -> Result<TReturn, TreeError> {
        assert!(index != PageIndex::zeroed());

        Ok(self
            .transaction
            .write(index, |page| write(page.data_mut()))?)
    }

    // TODO this really should not exist, instead the transaction should be able to execute more
    // specific actions and not allow page-level access
    fn insert(&mut self, page: Page) -> Result<PageIndex, TreeError> {
        Ok(self.transaction.insert(page)?)
    }

    // TODO this should return a Result
    fn leaf_search(&self, key: &[u8], node_index: PageIndex) -> PageIndex {
        assert!(node_index != PageIndex::zeroed());

        let result = self.transaction.read(node_index, |node_page| {
            let node = node_page.data::<Node>();

            if node.is_leaf() {
                return node_index;
            }

            let interior_node_reader = InteriorNodeReader::new(node, self.key_size);

            let mut found_page_index = None;

            for (key_index, node_key) in interior_node_reader.keys().enumerate() {
                if node_key > key {
                    let child_page: PageIndex = interior_node_reader.value_at(key_index).unwrap();

                    found_page_index = Some(child_page);
                }
            }

            match found_page_index {
                Some(child_page_index) => self.leaf_search(key, child_page_index),
                None => interior_node_reader.last_value(),
            }
        });

        result.unwrap()
    }
}

// TODO this should have &[u8] instead of vecs!
type TreeIteratorItem = Result<(Vec<u8>, Vec<u8>), TreeError>;

impl<T: Storage> Tree<T> {
    // TODO also create a "new_read" method, or something like that (that reads a tree that already
    // exists from storage)
    pub fn new(mut storage: T, key_size: usize, value_size: usize) -> Result<Self, TreeError> {
        // TODO assert that the storage is empty, and that the header get's the 0th page, as we
        // depend on that invariant (i.e. PageIndex=0 must always refer to the TreeData and not to
        // a node)!

        TreeData::new_in(&mut storage, key_size, value_size)?;

        Ok(Self {
            storage,
            key_size,
            value_size,
        })
    }

    // TODO make this take a non-mut reference
    // TODO this whole method must happen in transaction, so the tree is never accessible in an
    // inconsistent state
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), TreeError> {
        let key_size = self.key_size;
        let value_size = self.value_size;

        let mut transaction = TreeTransaction::<'_, T> {
            transaction: self.storage.transaction()?,
            key_size: self.key_size,
            value_size: self.value_size,
        };

        let root_index = transaction.read_header(|h| h.root)?;

        let target_node_index = transaction.leaf_search(key, root_index);
        let (parent_index, insert_result) =
            transaction.write_node(target_node_index, |target_node| {
                // TODO parent_index should be a part of insert_result
                let parent_index = target_node.parent();
                let insert_result =
                    LeafNodeWriter::new(target_node, key_size, value_size).insert(key, value);
                (parent_index, insert_result)
            })?;
        let insert_result = insert_result?;

        match insert_result {
            LeafInsertResult::Done => Ok(()),
            LeafInsertResult::Split {
                new_node,
                split_key,
            } => {
                if let Some(_parent_index) = parent_index {
                    todo!();
                } else {
                    let mut new_node_page = Page::new();
                    *new_node_page.data_mut() = *new_node;

                    // TODO create a `reserve_page` method, so that the storage can give us an ID,
                    // but the write can be deferred
                    let new_node_index = transaction.insert(new_node_page)?;

                    let mut new_root_page = Page::new();
                    *new_root_page.data_mut() = Node::new_internal_root();

                    let new_root_page_index = transaction.insert(new_root_page)?;

                    transaction.write_node(new_root_page_index, |new_root| {
                        let mut new_root_writer = InteriorNodeWriter::new(new_root, key_size);

                        new_root_writer.set_first_pointer(root_index);
                        new_root_writer.insert_node(&split_key, new_node_index);
                    })?;

                    transaction.write_node(new_node_index, |new_node| {
                        new_node.set_parent(new_root_page_index);
                    })?;

                    transaction.write_node(target_node_index, |target_node| {
                        target_node.set_parent(new_root_page_index);
                    })?;

                    transaction.write_header(|header| header.root = new_root_page_index)?;

                    Ok(())
                }
            }
        }
    }

    #[allow(unused)]
    // TODO make it take non-mut reference
    fn iter(&mut self) -> Result<impl Iterator<Item = TreeIteratorItem>, TreeError> {
        let key_size = self.key_size;
        let value_size = self.value_size;

        TreeIterator::new(self.transaction()?, key_size, value_size)
    }

    fn transaction(&mut self) -> Result<TreeTransaction<'_, T>, TreeError> {
        Ok(TreeTransaction::<T> {
            transaction: self.storage.transaction()?,
            key_size: self.key_size,
            value_size: self.value_size,
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
    #[error("The provided key's length does not match the one defined in the tree")]
    InvalidKeyLength,
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
        let mut transaction = storage.transaction()?;
        let mut header_page = Page::new();
        *header_page.data_mut() = Self {
            key_size: key_size as u64,
            value_size: value_size as u64,
            root: PageIndex::zeroed(),
            _unused: [0; _],
        };

        let header_index = transaction.insert(header_page)?;

        assert!(header_index == PageIndex::zeroed());

        let mut root_page = Page::new();
        *root_page.data_mut() = Node::new_leaf_root();

        let root_index = transaction.insert(root_page).unwrap();

        transaction.write(header_index, |page| {
            page.data_mut::<TreeData>().root = root_index;
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use bytemuck::from_bytes;

    use crate::storage::in_memory::{InMemoryStorage, test::TestStorage};

    use super::*;

    #[test]
    fn insert() {
        let mut node = Node::zeroed();
        let mut accessor = LeafNodeWriter::new(&mut node, 16, 8);

        assert!(matches!(
            accessor.insert(&[0; 15], &[0; 8]),
            Err(TreeError::InvalidKeyLength)
        ));

        assert!(matches!(
            accessor.insert(&[0; 16], &[0; 9]),
            Err(TreeError::InvalidValueLength)
        ));
    }

    #[test]
    fn node_accessor_entries() {
        let mut node = Node::zeroed();

        assert!(matches!(
            LeafNodeReader::new(&node, 8, 16).entries().next(),
            None
        ));

        let insert_result = LeafNodeWriter::new(&mut node, 8, 16)
            .insert(&[1; 8], &[2; 16])
            .unwrap();

        assert!(matches!(insert_result, LeafInsertResult::Done));

        let reader = LeafNodeReader::new(&node, 8, 16);
        let mut iter = reader.entries();
        let first = iter.next().unwrap();
        assert!(first.key() == &[1; 8]);
        assert!(first.value() == &[2; 16]);

        assert!(matches!(iter.next(), None));

        drop(iter);

        let key_first = [1, 1, 1, 1, 1, 1, 1, 0];
        let insert_result = LeafNodeWriter::new(&mut node, 8, 16)
            .insert(&key_first, &[1; 16])
            .unwrap();

        assert!(matches!(insert_result, LeafInsertResult::Done));

        let reader = LeafNodeReader::new(&node, 8, 16);
        let mut iter = reader.entries();

        let first = iter.next().unwrap();
        assert!(first.key() == &key_first);
        assert!(first.value() == &[1; 16]);

        let second = iter.next().unwrap();
        assert!(second.key() == &[1; 8]);
        assert!(second.value() == &[2; 16]);

        assert!(matches!(iter.next(), None));
    }

    #[test]
    fn insert_multiple_nodes() {
        let page_count = Arc::new(AtomicUsize::new(0));

        let storage = TestStorage::new(InMemoryStorage::new(), page_count.clone());
        let mut tree = Tree::new(storage, size_of::<usize>(), size_of::<usize>()).unwrap();

        // 3 pages mean there's been a node split
        // TODO: find a more explicit way of counting nodes
        let mut i = 0usize;
        while page_count.load(Ordering::Relaxed) < 3 {
            tree.insert(&i.to_le_bytes(), &(usize::max_value() - i).to_le_bytes())
                .unwrap();

            i += 1;
        }

        let entry_count = i;

        for (i, item) in tree.iter().unwrap().enumerate() {
            assert!(i < entry_count);
            let (key, value) = item.unwrap();

            let key: usize = *from_bytes(&key);
            let value: usize = *from_bytes(&value);

            assert!(key == i);
            assert!(value == usize::max_value() - i);
        }

        // TODO iterate the tree and ensure that all the keys are there, in correct order with
        // correct values
    }
}
