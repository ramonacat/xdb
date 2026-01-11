use bytemuck::{Pod, Zeroable, from_bytes, from_bytes_mut};

use crate::{
    bplustree::{
        LeafNodeId, Node, NodeId, TreeError,
        node::{NodeFlags, NodeHeader, NodeReader, NodeWriter},
    },
    storage::PageIndex,
};

#[derive(Zeroable, Pod, Debug, Clone, Copy)]
#[repr(C, align(8))]
struct LeafNodeHeader {
    previous: PageIndex,
    next: PageIndex,
}

pub(in crate::bplustree) struct LeafNodeEntry<'node> {
    key: &'node [u8],
    value: &'node [u8],
}

impl<'node> LeafNodeEntry<'node> {
    pub fn key(&self) -> &'node [u8] {
        self.key
    }

    pub fn value(&self) -> &'node [u8] {
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

pub(in crate::bplustree) struct LeafNodeReader<'node> {
    key_size: usize,
    value_size: usize,
    node: &'node Node,
}

impl<'node> NodeReader<'node> for LeafNodeReader<'node> {
    fn new(node: &'node Node, key_size: usize, value_size: usize) -> Self {
        Self {
            key_size,
            value_size,
            node,
        }
    }
}

impl<'node> LeafNodeReader<'node> {
    pub fn new(node: &'node Node, key_size: usize, value_size: usize) -> Self {
        assert!(node.is_leaf());

        // TODO return a result with an error if we can't fit at least two entries
        Self {
            key_size,
            value_size,
            node,
        }
    }

    fn entry_size(&self) -> usize {
        self.key_size + self.value_size
    }

    pub fn entries(&'node self) -> impl Iterator<Item = LeafNodeEntry<'node>> {
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
                [entry_offset + self.key_size..entry_offset + self.key_size + self.value_size],
        })
    }

    fn entry_offset(&self, index: usize) -> Option<usize> {
        if index >= usize::from(self.node.header.key_len) {
            return None;
        }

        Some(self.entries_offset() + index * self.entry_size())
    }

    // TODO this should return a NodeId!
    pub fn parent(&self) -> Option<PageIndex> {
        self.node.parent()
    }

    fn entries_offset(&self) -> usize {
        size_of::<LeafNodeHeader>()
    }

    fn header(&self) -> &LeafNodeHeader {
        from_bytes(&self.node.data[..size_of::<LeafNodeHeader>()])
    }

    pub fn previous(&self) -> Option<LeafNodeId> {
        let previous = self.header().previous;

        if previous == PageIndex::zeroed() {
            None
        } else {
            Some(LeafNodeId::new(previous))
        }
    }

    pub fn next(&self) -> Option<LeafNodeId> {
        let next = self.header().next;

        if next == PageIndex::zeroed() {
            None
        } else {
            Some(LeafNodeId::new(next))
        }
    }
}

pub(in crate::bplustree) struct LeafNodeWriter<'node> {
    key_size: usize,
    value_size: usize,
    node: &'node mut Node,
}

impl<'node> NodeWriter<'node> for LeafNodeWriter<'node> {
    fn new(node: &'node mut Node, key_size: usize, value_size: usize) -> Self {
        Self {
            key_size,
            value_size,
            node,
        }
    }
}

#[must_use]
#[derive(Debug)]
pub(in crate::bplustree) enum LeafInsertResult {
    Done,
    Split {
        new_node: Box<Node>,
        split_key: Vec<u8>,
    },
}

impl<'node> LeafNodeWriter<'node> {
    pub fn new(node: &'node mut Node, key_size: usize, value_size: usize) -> Self {
        assert!(node.is_leaf());

        // TODO return a result with an error if we can't fit at least two entries
        Self {
            key_size,
            value_size,
            node,
        }
    }

    pub fn reader(&'node self) -> LeafNodeReader<'node> {
        LeafNodeReader::new(self.node, self.key_size, self.value_size)
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<LeafInsertResult, TreeError> {
        if key.len() != self.key_size {
            return Err(TreeError::InvalidKeyLength);
        }

        if value.len() != self.reader().value_size {
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
        (self.node.data.len() - size_of::<LeafNodeHeader>()) / self.reader().entry_size()
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
        let entries_start = self.reader().entries_offset();

        let initial_len = self.reader().len();
        let entries_to_leave = initial_len / 2 + initial_len % 2;
        let entries_to_move = initial_len - entries_to_leave;

        let move_start_offset = entries_start + entries_to_leave * self.reader().entry_size();
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

        new_node.data[entries_start..entries_start + moved_entries_size]
            .copy_from_slice(new_node_entries);

        (
            entries_to_move,
            new_node.data[entries_start..entries_start + size_of::<PageIndex>()].to_vec(),
            new_node,
        )
    }

    pub fn set_parent(&mut self, new_parent: PageIndex) {
        self.node.set_parent(new_parent);
    }

    pub fn set_previous(&mut self, previous: Option<LeafNodeId>) {
        self.header_mut().previous = previous.map_or(PageIndex::zeroed(), |x| x.page())
    }

    pub fn set_next(&mut self, next: Option<LeafNodeId>) {
        self.header_mut().next = next.map_or(PageIndex::zeroed(), |x| x.page())
    }

    fn header_mut(&mut self) -> &mut LeafNodeHeader {
        from_bytes_mut(&mut self.node.data[0..size_of::<LeafNodeHeader>()])
    }
}
