pub mod dot;
mod node;

use std::collections::VecDeque;

use crate::bplustree::node::InteriorNodeReader;
use crate::bplustree::node::InteriorNodeWriter;
use crate::bplustree::node::LeafNodeReader;
use crate::bplustree::node::LeafNodeWriter;
use crate::bplustree::node::Node;
use crate::page::Page;
use crate::storage::PageIndex;
use crate::storage::Storage;
use crate::storage::StorageError;
use bytemuck::{Pod, Zeroable};
use thiserror::Error;

use crate::page::PAGE_DATA_SIZE;

const ROOT_NODE_TAIL_SIZE: usize = PAGE_DATA_SIZE - size_of::<u64>() * 2 - size_of::<PageIndex>();

// TODO this is an abomination, we won't need it once we actually have the next/previous keys
// in leaf nodes!
struct TreeIterator<'tree, T: Storage> {
    tree: &'tree Tree<T>,
    nodes_to_visit: VecDeque<&'tree Node>,
    index: usize,
}

impl<'tree, T: Storage> TreeIterator<'tree, T> {
    fn new(tree: &'tree Tree<T>) -> Result<Self, TreeError> {
        let header = tree.header()?;

        Ok(Self {
            tree,
            nodes_to_visit: VecDeque::from([tree.node(header.root)?]),
            index: 0,
        })
    }
}

impl<'tree, T: Storage> Iterator for TreeIterator<'tree, T> {
    // TODO we should use references here instead of copying into Vecs
    type Item = Result<(Vec<u8>, Vec<u8>), TreeError>;

    fn next(&mut self) -> Option<Self::Item> {
        let last = self.nodes_to_visit.front()?;

        if last.is_leaf() {
            match LeafNodeReader::new(last, self.tree.key_size, self.tree.value_size)
                .entries()
                .nth(self.index)
            {
                Some(entry) => {
                    self.index += 1;

                    Some(Ok((entry.key().to_vec(), entry.value().to_vec())))
                }
                None => {
                    self.nodes_to_visit.pop_front();

                    self.index = 0;

                    self.next()
                }
            }
        } else {
            let last = self.nodes_to_visit.pop_front().unwrap();
            let mut interior_nodes = InteriorNodeReader::new(last, self.tree.key_size)
                .values()
                .collect::<Vec<_>>();
            interior_nodes.reverse();

            for next_node in interior_nodes {
                let value = self.tree.node(next_node);

                match value {
                    Ok(value) => self.nodes_to_visit.push_front(value),
                    Err(error) => return Some(Err(error)),
                }
            }

            self.next()
        }
    }
}

#[derive(Debug)]
pub struct Tree<T: Storage> {
    storage: T,
    key_size: usize,
    value_size: usize,
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

    fn header(&self) -> Result<&TreeData, TreeError> {
        let header_page = self.storage.get(PageIndex::zeroed())?;

        Ok(header_page.data())
    }

    fn header_mut(&mut self) -> Result<&mut TreeData, TreeError> {
        let header_page = self.storage.get_mut(PageIndex::zeroed())?;

        Ok(header_page.data_mut())
    }

    fn node(&self, index: PageIndex) -> Result<&Node, TreeError> {
        assert!(index != PageIndex::zeroed());
        let page = self.storage.get(index)?;

        Ok(page.data())
    }

    fn node_mut(&mut self, index: PageIndex) -> Result<&mut Node, TreeError> {
        assert!(index != PageIndex::zeroed());
        let page = self.storage.get_mut(index)?;

        Ok(page.data_mut())
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), TreeError> {
        let key_size = self.key_size;
        let value_size = self.value_size;

        let header: &TreeData = self.header()?;

        let root_index = header.root;

        let target_node_index = self.leaf_search(key, header.root);
        let target_node = self.node_mut(target_node_index)?;

        let parent_index = target_node.parent();

        let insert_result =
            LeafNodeWriter::new(target_node, key_size, value_size).insert(key, value)?;

        match insert_result {
            node::LeafInsertResult::Done => Ok(()),
            node::LeafInsertResult::Split {
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
                    let new_node_index = self.storage.insert(new_node_page)?;

                    let mut new_root_page = Page::new();
                    *new_root_page.data_mut() = Node::new_internal_root();

                    let new_root_page_index = self.storage.insert(new_root_page)?;
                    let new_root = self.node_mut(new_root_page_index)?;

                    let mut new_root_writer = InteriorNodeWriter::new(new_root, key_size);

                    new_root_writer.set_first_pointer(root_index);
                    new_root_writer.insert_node(&split_key, new_node_index);

                    let new_node = self.node_mut(new_node_index)?;
                    new_node.set_parent(new_root_page_index);

                    let target_node = self.node_mut(target_node_index)?;
                    target_node.set_parent(new_root_page_index);

                    let header = self.header_mut()?;
                    header.root = new_root_page_index;

                    Ok(())
                }
            }
        }
    }

    // TODO we also need non-mut leaf_search!
    fn leaf_search(&self, key: &[u8], node_index: PageIndex) -> PageIndex {
        assert!(node_index != PageIndex::zeroed());

        let node_page = self.storage.get(node_index).unwrap();
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
    }

    #[allow(unused)]
    fn iter(&self) -> Result<impl Iterator<Item = TreeIteratorItem>, TreeError> {
        TreeIterator::new(self)
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
        let mut header_page = Page::new();
        *header_page.data_mut() = Self {
            key_size: key_size as u64,
            value_size: value_size as u64,
            root: PageIndex::zeroed(),
            _unused: [0; _],
        };

        let header_index = storage.insert(header_page)?;

        assert!(header_index == PageIndex::zeroed());

        let mut root_page = Page::new();
        *root_page.data_mut() = Node::new_leaf_root();

        let root_index = storage.insert(root_page).unwrap();

        storage
            .get_mut(header_index)
            .unwrap()
            .data_mut::<TreeData>()
            .root = root_index;

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

    use crate::{
        bplustree::node::{LeafInsertResult, LeafNodeReader},
        storage::in_memory::{InMemoryStorage, test::TestStorage},
    };

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
