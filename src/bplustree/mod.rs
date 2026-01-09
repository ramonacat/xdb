mod node;

use crate::bplustree::node::InteriorNodeReader;
use crate::bplustree::node::LeafNodeWriter;
use crate::bplustree::node::Node;
use crate::page::Page;
use crate::storage::PageIndex;
use crate::storage::Storage;
use crate::storage::StorageError;
use bytemuck::from_bytes;
use bytemuck::from_bytes_mut;
use bytemuck::{Pod, Zeroable};
use thiserror::Error;

use crate::page::PAGE_DATA_SIZE;

const ROOT_NODE_TAIL_SIZE: usize = PAGE_DATA_SIZE - size_of::<u64>() * 2 - size_of::<PageIndex>();

pub struct Tree<T: Storage> {
    storage: T,
    key_size: usize,
    value_size: usize,
}

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

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), TreeError> {
        let header_page = self.storage.get(PageIndex::zeroed())?;
        let header: &TreeData = from_bytes(header_page.data());

        let target_node_index = self.leaf_search(key, header.root);

        let target_page = self.storage.get_mut(target_node_index)?;
        let target_node: &mut Node = from_bytes_mut(target_page.data_mut());

        let insert_result =
            LeafNodeWriter::new(target_node, self.key_size, self.value_size).insert(key, value)?;
        match insert_result {
            node::LeafInsertResult::Done => Ok(()),
            node::LeafInsertResult::Split { count: _, data: _ } => todo!(),
        }
    }

    // TODO we also need non-mut leaf_search!
    fn leaf_search(&self, key: &[u8], node_index: PageIndex) -> PageIndex {
        let node_page = self.storage.get(node_index).unwrap();
        let node: &Node = from_bytes(node_page.data());

        if node.is_leaf() {
            return node_index;
        }

        let interior_node_reader = InteriorNodeReader::new(node, self.key_size);

        let mut found_page_index = None;

        for (key_index, node_key) in interior_node_reader.keys().enumerate() {
            if node_key > key {
                let child_page: PageIndex =
                    *from_bytes(interior_node_reader.value_at(key_index).unwrap());
                found_page_index = Some(child_page);
            }
        }

        match found_page_index {
            Some(child_page_index) => self.leaf_search(key, child_page_index),
            None => todo!(),
        }
    }
}

#[derive(Pod, Zeroable, Clone, Copy)]
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
    pub fn new_in(
        storage: &mut dyn Storage,
        key_size: usize,
        value_size: usize,
    ) -> Result<(), TreeError> {
        // TODO Self should be stored in storage!
        let mut header_page = Page::new();
        let treedata: &mut Self = from_bytes_mut(header_page.data_mut());
        *treedata = Self {
            key_size: key_size as u64,
            value_size: value_size as u64,
            root: PageIndex::zeroed(),
            _unused: [0; _],
        };

        let header_index = storage.insert(header_page)?;

        assert!(header_index == PageIndex::zeroed());

        let mut root_page = Page::new();
        let root_node: &mut Node = from_bytes_mut(root_page.data_mut());
        // TODO create a constructor for node (Node::new_root())
        *root_node = Node::zeroed();

        let root_index = storage.insert(root_page).unwrap();

        // TODO make data_mut generic, so that the conversion to TreeData gets done inside of it
        let header_page: &mut TreeData =
            from_bytes_mut(storage.get_mut(header_index).unwrap().data_mut());
        header_page.root = root_index;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{
        bplustree::node::{LeafInsertResult, LeafNodeReader},
        storage::InMemoryStorage,
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
        let storage = InMemoryStorage::new();
        let mut tree = Tree::new(storage, size_of::<usize>(), size_of::<usize>()).unwrap();

        // PAGE_DATA_SIZE as the number of entries will always definitely be more entries than can
        // fit in a node, no matter the data layout, key size, value size, etc.
        for i in 0..PAGE_DATA_SIZE {
            tree.insert(&i.to_le_bytes(), &(PAGE_DATA_SIZE - i).to_le_bytes())
                .unwrap();
        }

        // TODO iterate the tree and ensure that all the keys are there, in correct order with
        // correct values
    }
}
