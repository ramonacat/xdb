pub(super) mod storage;

use crate::storage::PageIndex;
use crate::{bplustree::TreeError, page::PAGE_DATA_SIZE};
use bytemuck::{Pod, Zeroable};

// TODO Support variable-sized values
// TODO Support variable-sized keys?

pub(super) struct LeafNodeEntry<'node> {
    key: &'node [u8],
    value: &'node [u8],
}

impl<'node> LeafNodeEntry<'node> {
    #[allow(unused)] // TODO use this once we can read data
    pub(super) fn key(&self) -> &'node [u8] {
        self.key
    }
    #[allow(unused)] // TODO use this once we can read data
    pub(super) fn value(&self) -> &'node [u8] {
        self.value
    }
}

struct LeafNodeEntryIterator<'node> {
    reader: &'node LeafNodeReader<'node>,
    offset: usize,
}

impl<'node> Iterator for LeafNodeEntryIterator<'node> {
    type Item = LeafNodeEntry<'node>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.reader.len() {
            return None;
        }

        self.offset += 1;

        self.reader.entry(self.offset - 1)
    }
}

pub(super) struct LeafNodeReader<'node> {
    key_size: usize,
    value_size: usize,
    node: &'node Node,
}

impl<'node> LeafNodeReader<'node> {
    pub(super) fn new(node: &'node Node, key_size: usize, value_size: usize) -> Self {
        assert!(node.is_leaf());

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
        if self.node.header.flags.contains(NodeFlags::INTERNAL) {
            size_of::<PageIndex>()
        } else {
            self.value_size
        }
    }

    pub(super) fn entries(&'node self) -> impl Iterator<Item = LeafNodeEntry<'node>> {
        LeafNodeEntryIterator {
            reader: self,
            offset: 0,
        }
    }

    fn len(&self) -> usize {
        usize::from(self.node.header.key_len)
    }

    fn entry(&'node self, index: usize) -> Option<LeafNodeEntry<'node>> {
        if index >= usize::from(self.node.header.key_len) {
            return None;
        }

        let entry_offset = self.entry_offset(index)?;

        Some(LeafNodeEntry {
            key: &self.node.data[entry_offset..entry_offset + self.key_size],
            value: &self.node.data
                [entry_offset + self.key_size..entry_offset + self.key_size + self.value_size()],
        })
    }

    fn entry_offset(&self, index: usize) -> Option<usize> {
        if index >= usize::from(self.node.header.key_len) {
            return None;
        }

        Some(index * self.entry_size())
    }
}

pub(super) struct LeafNodeWriter<'node> {
    key_size: usize,
    value_size: usize,
    node: &'node mut Node,
}

impl<'node> LeafNodeWriter<'node> {
    pub(super) fn new(node: &'node mut Node, key_size: usize, value_size: usize) -> Self {
        assert!(node.is_leaf());
        // TODO return a result with an error if we can't fit at least two entries
        Self {
            key_size,
            value_size,
            node,
        }
    }

    fn reader(&'node self) -> LeafNodeReader<'node> {
        LeafNodeReader::new(self.node, self.key_size, self.value_size)
    }

    pub(super) fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), TreeError> {
        if key.len() != self.key_size {
            return Err(TreeError::InvalidKeyLength);
        }

        if value.len() != self.reader().value_size() {
            return Err(TreeError::InvalidValueLength);
        }

        let mut insert_index = 0;

        for (index, entry) in self.reader().entries().enumerate() {
            if entry.key > key {
                insert_index = index;
            }
        }

        if self.is_full() {
            todo!("Split node");
        }

        self.insert_at(insert_index, key, value)
    }

    fn capacity(&self) -> usize {
        self.node.data.len() / self.reader().entry_size()
    }

    fn is_full(&self) -> bool {
        self.reader().len() + 1 >= self.capacity()
    }

    fn insert_at(&mut self, index: usize, key: &[u8], value: &[u8]) -> Result<(), TreeError> {
        assert!(index < self.capacity());
        assert!(!self.is_full());

        self.node.header.key_len += 1;

        if let Some(move_start_offset) = self.reader().entry_offset(index) {
            let move_end_offset =
                move_start_offset + (self.reader().len() - index) * self.reader().entry_size();

            let data_to_move = self.node.data[move_start_offset..move_end_offset].to_vec();

            let entry_size = self.reader().entry_size();
            self.node.data[move_start_offset + entry_size..move_end_offset + entry_size]
                .copy_from_slice(&data_to_move);
        }

        let entry_offset = self.reader().entry_offset(index).unwrap();

        let key_hole: &mut [u8] = &mut self.node.data[entry_offset..entry_offset + key.len()];
        key_hole.copy_from_slice(key);

        let value_hole =
            &mut self.node.data[entry_offset + key.len()..entry_offset + key.len() + value.len()];
        value_hole.copy_from_slice(value);

        Ok(())
    }
}

struct InteriorNodeKeysIterator<'node> {
    node: &'node Node,
    key_size: usize,
    index: usize,
}

impl<'node> Iterator for InteriorNodeKeysIterator<'node> {
    type Item = &'node [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let reader = InteriorNodeReader::new(self.node, self.key_size);

        if self.index >= reader.key_len() {
            return None;
        }

        Some(&reader.node.data[self.index * self.key_size..(self.index + 1) * self.key_size])
    }
}

pub(super) struct InteriorNodeReader<'node> {
    node: &'node Node,
    key_size: usize,
}

impl<'node> InteriorNodeReader<'node> {
    pub(super) fn new(node: &'node Node, key_size: usize) -> Self {
        Self { node, key_size }
    }

    pub(super) fn keys(&self) -> impl Iterator<Item = &'node [u8]> {
        InteriorNodeKeysIterator {
            node: self.node,
            key_size: self.key_size,
            index: 0,
        }
    }

    fn key_len(&self) -> usize {
        self.node.header.key_len as usize
    }

    pub(super) fn value_at(&self, index: usize) -> Option<&[u8]> {
        // n - max number of keys
        //
        // size = key_size*n + value_size*(n+1)
        // size = key_size*n + value_size*n + value_size
        // size - value_size = key_size*n + value_size*n
        // (size - value_size)/(key_size + value_size) = n

        let key_capacity = (self.node.data.len() - size_of::<PageIndex>())
            / (self.key_size + size_of::<PageIndex>());
        if index > key_capacity {
            return None;
        }

        let values_offset = key_capacity * self.key_size;

        let value_start = values_offset + (index * size_of::<PageIndex>());

        Some(&self.node.data[value_start..value_start + size_of::<PageIndex>()])
    }
}

const NODE_DATA_SIZE: usize = PAGE_DATA_SIZE - size_of::<NodeHeader>();

#[derive(Debug, Pod, Zeroable, Clone, Copy)]
#[repr(C)]
pub(super) struct Node {
    header: NodeHeader,
    data: [u8; NODE_DATA_SIZE],
}

const _: () = assert!(size_of::<Node>() == PAGE_DATA_SIZE);

impl Node {
    pub(super) fn is_leaf(&self) -> bool {
        !self.header.flags.contains(NodeFlags::INTERNAL)
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
pub(super) struct NodeHeader {
    key_len: u16,
    flags: NodeFlags,
    _unused2: u32,
}
const _: () = assert!(size_of::<NodeHeader>() == size_of::<u64>());
