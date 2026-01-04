use bytemuck::{Pod, Zeroable, checked::from_bytes, from_bytes_mut};
use thiserror::Error;

use crate::{page::PAGE_DATA_SIZE, storage::PageIndex};

struct NodeEntry<'node> {
    key: &'node [u8],
    #[allow(unused)] // TODO use this once we can read data
    value: &'node [u8],
}

struct NodeEntryIterator<'node, const SIZE: usize> {
    accessor: &'node NodeAccessor<'node, SIZE>,
    offset: usize,
}

impl<'node, const SIZE: usize> Iterator for NodeEntryIterator<'node, SIZE> {
    type Item = NodeEntry<'node>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.accessor.present_entries() {
            return None;
        }

        self.offset += 1;

        self.accessor.entry(self.offset - 1)
    }
}

struct NodeAccessor<'node, const SIZE: usize> {
    key_size: usize,
    value_size: usize,
    node: &'node mut Node<SIZE>,
}

impl<'node, const SIZE: usize> NodeAccessor<'node, SIZE> {
    fn new(node: &'node mut Node<SIZE>, key_size: usize, value_size: usize) -> Self {
        // TODO return a result with an error if we can't fit at least two entries
        Self {
            key_size,
            value_size,
            node,
        }
    }

    fn entry_size(&self) -> usize {
        self.key_size + self.value_size()
    }

    fn value_size(&self) -> usize {
        if self.node.header().flags.contains(NodeFlags::INTERNAL) {
            size_of::<PageIndex>()
        } else {
            self.value_size
        }
    }

    fn entries(&'node self) -> impl Iterator<Item = NodeEntry<'node>> {
        NodeEntryIterator {
            accessor: self,
            offset: 0,
        }
    }

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), TreeError> {
        if key.len() != self.key_size {
            return Err(TreeError::InvalidKeyLength);
        }

        if value.len() != self.value_size() {
            return Err(TreeError::InvalidValueLength);
        }

        let mut insert_index = 0;

        for (index, entry) in self.entries().enumerate() {
            if entry.key > key {
                insert_index = index;
            }
        }

        self.insert_at(insert_index, key, value)
    }

    fn capacity(&self) -> usize {
        SIZE / self.entry_size()
    }

    fn entry_offset(&self, index: usize) -> Option<usize> {
        if index >= usize::from(self.node.header().present_entries) {
            return None;
        }

        Some(index * self.entry_size())
    }

    fn entry(&'node self, index: usize) -> Option<NodeEntry<'node>> {
        if index >= usize::from(self.node.header().present_entries) {
            return None;
        }

        let entry_offset = self.entry_offset(index)?;

        Some(NodeEntry {
            key: &self.node.data()[entry_offset..entry_offset + self.key_size],
            value: &self.node.data()
                [entry_offset + self.key_size..entry_offset + self.key_size + self.value_size()],
        })
    }

    fn insert_at(&mut self, index: usize, key: &[u8], value: &[u8]) -> Result<(), TreeError> {
        if index >= self.capacity() {
            todo!("Split node");
        }

        if self.present_entries() >= self.capacity() {
            todo!("Split node");
        }

        self.node.header_mut().present_entries += 1;

        if let Some(move_start_offset) = self.entry_offset(index) {
            let move_end_offset =
                move_start_offset + (self.present_entries() - index) * self.entry_size();

            let data_to_move = self.node.data()[move_start_offset..move_end_offset].to_vec();

            let entry_size = self.entry_size();
            self.node.data_mut()[move_start_offset + entry_size..move_end_offset + entry_size]
                .copy_from_slice(&data_to_move);
        }

        let entry_offset = self.entry_offset(index).unwrap();

        let key_hole: &mut [u8] = &mut self.node.data_mut()[entry_offset..entry_offset + key.len()];
        key_hole.copy_from_slice(key);

        let value_hole = &mut self.node.data_mut()
            [entry_offset + key.len()..entry_offset + key.len() + value.len()];
        value_hole.copy_from_slice(value);

        Ok(())
    }

    fn present_entries(&self) -> usize {
        usize::from(self.node.header().present_entries)
    }
}

#[derive(Debug, Pod, Zeroable, Clone, Copy)]
#[repr(transparent)]
// The header isn't declared as a field and instead accessed with casts because bytemuck has a
// limitation, where it does not work with const-generic-sized arrays unless it's the only field
struct Node<const SIZE: usize>([u8; SIZE]);

impl<const SIZE: usize> Node<SIZE> {
    fn header(&self) -> &NodeHeader {
        from_bytes(&self.0[0..size_of::<NodeHeader>()])
    }

    fn header_mut(&mut self) -> &mut NodeHeader {
        from_bytes_mut(&mut self.0[0..size_of::<NodeHeader>()])
    }

    fn data(&self) -> &[u8] {
        &self.0[size_of::<NodeHeader>()..]
    }

    fn data_mut(&mut self) -> &mut [u8] {
        &mut self.0[size_of::<NodeHeader>()..]
    }
}

bitflags::bitflags! {
    #[derive(Debug, Pod, Zeroable, Clone, Copy)]
    #[repr(transparent)]
    struct NodeFlags: u16 {
        const INTERNAL = 1 << 0;
    }
}

#[derive(Debug, Pod, Zeroable, Clone, Copy)]
#[repr(C)]
struct NodeHeader {
    present_entries: u16,
    flags: NodeFlags,
    _unused2: u32,
}
const _: () = assert!(size_of::<NodeHeader>() == size_of::<u64>());

impl<const SIZE: usize> Node<SIZE> {}

const ROOT_NODE_DATA_SIZE: usize = PAGE_DATA_SIZE - size_of::<u64>();

#[derive(Pod, Zeroable, Clone, Copy)]
#[repr(C)]
pub struct Tree {
    key_size: u16,
    value_size: u16,

    _unused: u32,

    root: Node<ROOT_NODE_DATA_SIZE>,
}

const _: () = assert!(size_of::<Tree>() == PAGE_DATA_SIZE);

#[derive(Debug, Error)]
pub enum TreeError {
    #[error("The provided key's length does not match the one defined in the tree")]
    InvalidKeyLength,
    #[error("The provided value's length does not match the one defined in the tree")]
    InvalidValueLength,
}

impl Tree {
    pub fn new(key_size: u16, value_size: u16) -> Result<Self, TreeError> {
        let root = Node::zeroed();

        Ok(Self {
            key_size,
            value_size,
            root,
            _unused: 0,
        })
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), TreeError> {
        // TODO actually perform the B+ tree insertion algorithm...
        NodeAccessor::new(
            &mut self.root,
            usize::from(self.key_size),
            usize::from(self.value_size),
        )
        .insert(key, value)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn insert() {
        let mut node_1024 = Node::<1024>::zeroed();
        let mut accessor = NodeAccessor::new(&mut node_1024, 16, 8);

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
        let mut node_1024 = Node::<1024>::zeroed();
        let mut accessor = NodeAccessor::new(&mut node_1024, 8, 16);

        assert!(matches!(accessor.entries().next(), None));

        accessor.insert(&[1; 8], &[2; 16]).unwrap();

        let mut iter = accessor.entries();
        let first = iter.next().unwrap();
        assert!(first.key == &[1; 8]);
        assert!(first.value == &[2; 16]);

        assert!(matches!(iter.next(), None));

        drop(iter);

        let key_first = [1, 1, 1, 1, 1, 1, 1, 0];
        accessor.insert(&key_first, &[1; 16]).unwrap();

        let mut iter = accessor.entries();

        let first = iter.next().unwrap();
        assert!(first.key == &key_first);
        assert!(first.value == &[1; 16]);

        let second = iter.next().unwrap();
        assert!(second.key == &[1; 8]);
        assert!(second.value == &[2; 16]);

        assert!(matches!(iter.next(), None));
    }
}
