use std::marker::PhantomData;

use bytemuck::{AnyBitPattern, NoUninit, Pod, Zeroable, bytes_of, from_bytes, from_bytes_mut};

use crate::{
    bplustree::{
        LeafNodeId, Node, NodeId, TreeError,
        node::{InteriorNodeId, NodeFlags, NodeHeader, NodeReader, NodeWriter},
    },
    storage::PageIndex,
};

#[derive(Zeroable, Pod, Debug, Clone, Copy)]
#[repr(C, align(8))]
struct LeafNodeHeader {
    previous: PageIndex,
    next: PageIndex,
}

pub(in crate::bplustree) struct LeafNodeEntry<'node, TKey> {
    key: &'node TKey,
    value: &'node [u8],
}

impl<'node, TKey> LeafNodeEntry<'node, TKey> {
    pub fn key(&self) -> &'node TKey {
        self.key
    }

    pub fn value(&self) -> &'node [u8] {
        self.value
    }
}

struct LeafNodeEntryIterator<'node, TKey> {
    reader: &'node LeafNodeReader<'node, TKey>,
    offset: usize,
    _key: PhantomData<TKey>,
}

impl<'node, TKey: AnyBitPattern + 'node> Iterator for LeafNodeEntryIterator<'node, TKey> {
    type Item = LeafNodeEntry<'node, TKey>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.reader.len() {
            return None;
        }

        self.offset += 1;

        self.reader.entry(self.offset - 1)
    }
}

pub(in crate::bplustree) struct LeafNodeReader<'node, TKey> {
    value_size: usize,
    node: &'node Node,
    _key: PhantomData<&'node TKey>,
}

impl<'node, TKey> NodeReader<'node, TKey> for LeafNodeReader<'node, TKey> {
    fn new(node: &'node Node, value_size: usize) -> Self {
        Self {
            value_size,
            node,
            _key: PhantomData,
        }
    }
}

impl<'node, TKey: AnyBitPattern> LeafNodeReader<'node, TKey> {
    pub fn new(node: &'node Node, value_size: usize) -> Self {
        assert!(node.is_leaf());

        // TODO return a result with an error if we can't fit at least two entries
        Self {
            value_size,
            node,
            _key: PhantomData,
        }
    }

    fn entry_size(&self) -> usize {
        size_of::<TKey>() + self.value_size
    }

    pub fn entries(&'node self) -> impl Iterator<Item = LeafNodeEntry<'node, TKey>> {
        LeafNodeEntryIterator {
            reader: self,
            offset: 0,
            _key: PhantomData,
        }
    }

    fn len(&self) -> usize {
        usize::from(self.node.header.key_len)
    }

    fn entry(&'node self, index: usize) -> Option<LeafNodeEntry<'node, TKey>> {
        if index >= usize::from(self.node.header.key_len) {
            return None;
        }

        let entry_offset = self.entry_offset(index)?;

        Some(LeafNodeEntry {
            key: from_bytes(&self.node.data[entry_offset..entry_offset + size_of::<TKey>()]),
            value: &self.node.data[entry_offset + size_of::<TKey>()
                ..entry_offset + size_of::<TKey>() + self.value_size],
        })
    }

    fn entry_offset(&self, index: usize) -> Option<usize> {
        if index >= usize::from(self.node.header.key_len) {
            return None;
        }

        Some(self.entries_offset() + index * self.entry_size())
    }

    pub fn parent(&self) -> Option<InteriorNodeId> {
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

pub(in crate::bplustree) struct LeafNodeWriter<'node, TKey> {
    value_size: usize,
    node: &'node mut Node,
    _key: PhantomData<&'node TKey>,
}

impl<'node, TKey> NodeWriter<'node, TKey> for LeafNodeWriter<'node, TKey> {
    fn new(node: &'node mut Node, value_size: usize) -> Self {
        Self {
            value_size,
            node,
            _key: PhantomData,
        }
    }
}

#[must_use]
#[derive(Debug)]
pub(in crate::bplustree) enum LeafInsertResult<TKey> {
    Done,
    Split {
        new_node: Box<Node>,
        split_key: TKey,
    },
}

impl<'node, TKey: AnyBitPattern + PartialOrd + NoUninit + Clone> LeafNodeWriter<'node, TKey> {
    pub fn new(node: &'node mut Node, value_size: usize) -> Self {
        assert!(node.is_leaf());

        // TODO return a result with an error if we can't fit at least two entries
        Self {
            value_size,
            node,
            _key: PhantomData,
        }
    }

    pub fn reader(&'node self) -> LeafNodeReader<'node, TKey> {
        LeafNodeReader::new(self.node, self.value_size)
    }

    pub fn insert(&mut self, key: TKey, value: &[u8]) -> Result<LeafInsertResult<TKey>, TreeError> {
        if value.len() != self.reader().value_size {
            return Err(TreeError::InvalidValueLength);
        }

        let mut insert_index = self.reader().len();

        for (index, entry) in self.reader().entries().enumerate() {
            if &key < entry.key {
                insert_index = index;
                break;
            }
        }

        if self.is_full() {
            let (split_index, split_key, mut new_node) = self.split();

            if insert_index > split_index {
                let result = LeafNodeWriter::new(&mut new_node, self.value_size)
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

    fn insert_at(&mut self, index: usize, key: TKey, value: &[u8]) -> Result<(), TreeError> {
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

        let key_hole: &mut [u8] =
            &mut self.node.data[entry_offset..entry_offset + size_of::<TKey>()];
        key_hole.copy_from_slice(bytes_of(&key));

        let value_hole = &mut self.node.data
            [entry_offset + size_of::<TKey>()..entry_offset + size_of::<TKey>() + value.len()];
        value_hole.copy_from_slice(value);

        Ok(())
    }

    fn split(&mut self) -> (usize, TKey, Node) {
        let entries_start = self.reader().entries_offset();

        let initial_len = self.reader().len();
        let entries_to_leave = initial_len / 2 + initial_len % 2;
        let entries_to_move = initial_len - entries_to_leave;

        let move_start_offset = entries_start + entries_to_leave * self.reader().entry_size();
        let moved_entries_size = entries_to_move * self.reader().entry_size();
        let new_node_entries =
            &self.node.data[move_start_offset..move_start_offset + moved_entries_size];

        self.node.header.key_len = entries_to_leave as u16;

        // TODO we should not create the node here at all, and instead just return the data, so the
        // user can construct the new node with the correct links
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
            from_bytes::<TKey>(
                &new_node.data[entries_start..entries_start + size_of::<PageIndex>()],
            )
            .to_owned(),
            new_node,
        )
    }

    pub fn set_links(
        &mut self,
        parent: Option<InteriorNodeId>,
        previous: Option<LeafNodeId>,
        next: Option<LeafNodeId>,
    ) {
        self.node
            // TODO replace this and all the other instances of PageIndex::zeroed() with an
            // explicit constructor
            .set_parent(parent.map_or_else(PageIndex::zeroed, |x| x.page()));
        let header = self.header_mut();
        header.previous = previous.map_or(PageIndex::zeroed(), |x| x.page());
        header.next = next.map_or(PageIndex::zeroed(), |x| x.page())
    }

    fn header_mut(&mut self) -> &mut LeafNodeHeader {
        from_bytes_mut(&mut self.node.data[0..size_of::<LeafNodeHeader>()])
    }
}
