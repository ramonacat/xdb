mod node;

use crate::bplustree::node::InteriorNodeReader;
use crate::bplustree::node::LeafNodeWriter;
use crate::bplustree::node::Node;
use crate::bplustree::node::storage::NodeStorage;
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

#[derive(Pod, Zeroable, Clone, Copy)]
#[repr(C)]
pub struct Tree {
    key_size: u64,
    value_size: u64,
    root: PageIndex,
    _unused: [u8; ROOT_NODE_TAIL_SIZE],
}

const _: () = assert!(
    size_of::<Tree>() == PAGE_DATA_SIZE,
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

impl Tree {
    pub fn new(
        key_size: usize,
        value_size: usize,
        storage: &mut dyn Storage,
    ) -> Result<Self, TreeError> {
        // TODO Self should be stored in storage!
        let root_page = Page::new();
        let root_index = storage.insert(root_page)?;

        Ok(Self {
            key_size: key_size as u64,
            value_size: value_size as u64,
            root: root_index,
            _unused: [0; _],
        })
    }

    pub fn insert(
        &mut self,
        key: &[u8],
        value: &[u8],
        storage: &mut dyn Storage,
    ) -> Result<(), TreeError> {
        let target_node_index = self.leaf_search(key, self.root, NodeStorage::new(storage));

        LeafNodeWriter::new(
            from_bytes_mut(storage.get_mut(target_node_index).unwrap().data_mut()),
            self.key_size as usize,
            self.value_size as usize,
        )
        .insert(key, value)
    }

    // TODO wrap Tree into a TreeReader or something, and keep the Storage as field there
    // TODO we also need non-mut leaf_search!
    fn leaf_search<'node>(
        &self,
        key: &[u8],
        node_index: PageIndex,
        storage: NodeStorage<'node>,
    ) -> PageIndex {
        let node = storage.get(node_index).unwrap();

        if node.is_leaf() {
            return node_index;
        }

        let interior_node_reader = InteriorNodeReader::new(node, self.key_size as usize);

        let mut found_page_index = None;

        for (key_index, node_key) in interior_node_reader.keys().enumerate() {
            if node_key > key {
                let child_page: PageIndex =
                    *from_bytes(interior_node_reader.value_at(key_index).unwrap());
                found_page_index = Some(child_page);
            }
        }

        match found_page_index {
            Some(child_page_index) => self.leaf_search(key, child_page_index, storage),
            None => todo!(),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{bplustree::node::LeafNodeReader, storage::InMemoryStorage};

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

        LeafNodeWriter::new(&mut node, 8, 16)
            .insert(&[1; 8], &[2; 16])
            .unwrap();

        let reader = LeafNodeReader::new(&node, 8, 16);
        let mut iter = reader.entries();
        let first = iter.next().unwrap();
        assert!(first.key() == &[1; 8]);
        assert!(first.value() == &[2; 16]);

        assert!(matches!(iter.next(), None));

        drop(iter);

        let key_first = [1, 1, 1, 1, 1, 1, 1, 0];
        LeafNodeWriter::new(&mut node, 8, 16)
            .insert(&key_first, &[1; 16])
            .unwrap();

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
    #[ignore = "node splits aren't implemented yet"]
    fn insert_multiple_nodes() {
        let mut storage = InMemoryStorage::new();
        let mut tree = Tree::new(size_of::<usize>(), size_of::<usize>(), &mut storage).unwrap();

        // PAGE_DATA_SIZE as the number of entries will always definitely be more entries than can
        // fit in a node, no matter the data layout, key size, value size, etc.
        for i in 0..PAGE_DATA_SIZE {
            tree.insert(
                &i.to_le_bytes(),
                &(PAGE_DATA_SIZE - i).to_le_bytes(),
                &mut storage,
            )
            .unwrap();
        }

        // TODO iterate the tree and ensure that all the keys are there, in correct order with
        // correct values
    }
}
