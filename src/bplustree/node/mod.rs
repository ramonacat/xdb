use crate::storage::PageIndex;
use crate::{bplustree::TreeError, page::PAGE_DATA_SIZE};
use bytemuck::{Pod, Zeroable, bytes_of, from_bytes};

// TODO Support variable-sized values
// TODO Support variable-sized keys?

pub(super) struct LeafNodeEntry<'node> {
    key: &'node [u8],
    value: &'node [u8],
}

impl<'node> LeafNodeEntry<'node> {
    pub(super) fn key(&self) -> &'node [u8] {
        self.key
    }

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

#[must_use]
#[derive(Debug)]
pub(super) enum LeafInsertResult {
    Done,
    Split {
        new_node: Box<Node>,
        split_key: Vec<u8>,
    },
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

    pub(super) fn insert(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> Result<LeafInsertResult, TreeError> {
        if key.len() != self.key_size {
            return Err(TreeError::InvalidKeyLength);
        }

        if value.len() != self.reader().value_size() {
            return Err(TreeError::InvalidValueLength);
        }

        let mut insert_index = self.reader().len();

        for (index, entry) in self.reader().entries().enumerate() {
            if key < entry.key {
                insert_index = if index == 0 { 0 } else { index - 1 };
                break;
            }
        }

        if self.is_full() {
            let (split_index, split_key, mut new_node) = self.split();

            if insert_index > split_index {
                let result = LeafNodeWriter::new(&mut new_node, self.key_size, self.value_size)
                    .insert(key, value)
                    .unwrap();

                match result {
                    LeafInsertResult::Done => {}
                    LeafInsertResult::Split {
                        new_node: _,
                        split_key: _,
                    } => todo!(),
                }
            } else {
                let result = self.insert(key, value).unwrap();

                match result {
                    LeafInsertResult::Done => {}
                    LeafInsertResult::Split {
                        new_node: _,
                        split_key: _,
                    } => todo!(),
                }
            }

            Ok(LeafInsertResult::Split {
                new_node: Box::new(new_node),
                split_key,
            })
        } else {
            self.insert_at(insert_index, key, value)?;

            Ok(LeafInsertResult::Done)
        }
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

    fn split(&mut self) -> (usize, Vec<u8>, Node) {
        let initial_len = self.reader().len();
        let entries_to_leave = initial_len / 2 + initial_len % 2;
        let entries_to_move = initial_len - entries_to_leave;

        let move_start_offset = entries_to_leave * self.reader().entry_size();
        let moved_entries_size = entries_to_move * self.reader().entry_size();
        let new_node_entries =
            &self.node.data[move_start_offset..move_start_offset + moved_entries_size];

        self.node.header.key_len = entries_to_leave as u16;

        let mut new_node = Node {
            header: NodeHeader {
                key_len: entries_to_move as u16,
                flags: NodeFlags::empty(),
                _unused2: 0,
                parent: PageIndex::zeroed(),
            },
            data: [0; _],
        };
        new_node.data[..moved_entries_size].copy_from_slice(new_node_entries);

        (
            entries_to_move,
            new_node.data[..size_of::<PageIndex>()].to_vec(),
            new_node,
        )
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

        self.index += 1;

        Some(&reader.node.data[self.index * self.key_size..(self.index + 1) * self.key_size])
    }
}

#[derive(Debug)]
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

    fn key_capacity(&self) -> usize {
        // n - max number of keys
        //
        // size = key_size*n + value_size*(n+1)
        // size = key_size*n + value_size*n + value_size
        // size - value_size = key_size*n + value_size*n
        // (size - value_size)/(key_size + value_size) = n

        (self.node.data.len() - size_of::<PageIndex>()) / (self.key_size + size_of::<PageIndex>())
    }

    fn values_offset(&self) -> usize {
        self.key_capacity() * self.key_size
    }

    pub(super) fn value_at(&self, index: usize) -> Option<PageIndex> {
        if index > self.key_len() {
            return None;
        }

        let value_start = self.values_offset() + (index * size_of::<PageIndex>());

        let value = *from_bytes(&self.node.data[value_start..value_start + size_of::<PageIndex>()]);

        assert!(value != PageIndex::zeroed());

        Some(value)
    }

    pub(crate) fn last_value(&self) -> PageIndex {
        self.value_at(self.key_len()).unwrap()
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = PageIndex> {
        (0..(self.key_len() + 1)).map(|x| self.value_at(x).unwrap())
    }
}

pub(super) enum InteriorInsertResult {
    Ok,
    Split,
}

pub(super) struct InteriorNodeWriter<'node> {
    node: &'node mut Node,
    key_size: usize,
}

impl<'node> InteriorNodeWriter<'node> {
    pub(super) fn new(node: &'node mut Node, key_size: usize) -> Self {
        Self { node, key_size }
    }

    fn reader(&'node self) -> InteriorNodeReader<'node> {
        InteriorNodeReader::new(self.node, self.key_size)
    }

    pub(crate) fn set_first_pointer(&mut self, index: PageIndex) {
        let offset = self.reader().values_offset();

        self.node.data[offset..offset + size_of::<PageIndex>()].copy_from_slice(bytes_of(&index));
    }

    pub(crate) fn insert_node(&mut self, key: &[u8], value: PageIndex) -> InteriorInsertResult {
        assert!(value != PageIndex::zeroed());
        let mut insert_at = 0;

        for (index, current_key) in self.reader().keys().enumerate() {
            if current_key > key {
                insert_at = index;
                break;
            }
        }

        self.insert_at(insert_at, key, value)
    }

    fn insert_at(&mut self, index: usize, key: &[u8], value: PageIndex) -> InteriorInsertResult {
        let key_len = self.reader().key_len();

        if key_len + 1 == self.reader().key_capacity() {
            return InteriorInsertResult::Split;
        }

        if key_len < index {
            todo!();
        }

        self.node.header.key_len += 1;

        let key_offset = self.key_size * (index + 1);
        let value_offset = self.reader().values_offset() + size_of::<PageIndex>() * (index + 1);

        self.node.data[key_offset..key_offset + self.key_size].copy_from_slice(key);
        self.node.data[value_offset..value_offset + size_of::<PageIndex>()]
            .copy_from_slice(bytes_of(&value));

        InteriorInsertResult::Ok
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
    pub(super) fn new_internal_root() -> Self {
        Self {
            header: NodeHeader {
                key_len: 0,
                flags: NodeFlags::INTERNAL,
                _unused2: 0,
                parent: PageIndex::zeroed(),
            },
            data: [0; _],
        }
    }

    pub(super) fn new_leaf_root() -> Self {
        Self {
            header: NodeHeader {
                key_len: 0,
                flags: NodeFlags::empty(),
                _unused2: 0,
                parent: PageIndex::zeroed(),
            },
            data: [0; _],
        }
    }

    pub(super) fn is_leaf(&self) -> bool {
        !self.header.flags.contains(NodeFlags::INTERNAL)
    }

    pub(super) fn parent(&self) -> Option<PageIndex> {
        if self.header.parent == PageIndex::zeroed() {
            None
        } else {
            Some(self.header.parent)
        }
    }

    pub(crate) fn set_parent(&mut self, parent: PageIndex) {
        assert!(parent != PageIndex::zeroed());

        self.header.parent = parent;
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
    parent: PageIndex,
}
const _: () = assert!(size_of::<NodeHeader>() == size_of::<u64>() * 2);
