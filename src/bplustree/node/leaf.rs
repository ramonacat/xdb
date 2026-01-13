use std::marker::PhantomData;

use bytemuck::{Pod, Zeroable, bytes_of, checked::pod_read_unaligned, from_bytes, from_bytes_mut};

use crate::{
    bplustree::{
        LeafNodeId, NodeId, TreeError,
        node::{
            InteriorNodeId, NODE_DATA_SIZE, NodeFlags, NodeHeader, NodeReader, NodeTrait,
            NodeWriter,
        },
    },
    storage::PageIndex,
};

#[derive(Debug, Zeroable, Clone, Copy)]
#[repr(C, align(8))]
pub(in crate::bplustree) struct LeafNode<TKey>
where
    TKey: Pod,
{
    header: NodeHeader,
    data: [u8; NODE_DATA_SIZE],
    _key: PhantomData<TKey>,
}

impl<TKey: Pod + PartialOrd> LeafNode<TKey> {
    pub(crate) fn new() -> Self {
        Self {
            header: NodeHeader {
                key_len: 0,
                flags: NodeFlags::empty(),
                _unused2: 0,
                parent: PageIndex::zeroed(),
            },
            data: [0; _],
            _key: PhantomData,
        }
    }

    fn len(&self) -> usize {
        usize::from(self.header.key_len)
    }

    fn entry(&self, index: usize) -> Option<LeafNodeEntry<'_, TKey>> {
        if index >= usize::from(self.header.key_len) {
            return None;
        }

        let entry_offset = self.entry_offset(index)?;
        let value_size: u64 = pod_read_unaligned(
            &self.data[entry_offset + size_of::<TKey>()
                ..entry_offset + size_of::<TKey>() + size_of::<u64>()],
        );

        Some(LeafNodeEntry {
            key: pod_read_unaligned(&self.data[entry_offset..entry_offset + size_of::<TKey>()]),
            value: &self.data[entry_offset + size_of::<TKey>() + size_of::<u64>()
                ..entry_offset + size_of::<TKey>() + size_of::<u64>() + value_size as usize],
        })
    }

    // TODO don't allow one-past access and instead have a `entries_size` method for better
    // clarity?
    fn entry_offset(&self, index: usize) -> Option<usize> {
        if index > self.len() {
            return None;
        }

        let mut offset = self.entries_offset();

        for i in 0..index {
            offset += self.entry_size(i).unwrap();
            debug_assert!(offset < self.data.len());
        }

        Some(offset)
    }

    fn entries_offset(&self) -> usize {
        size_of::<LeafNodeHeader>()
    }

    fn entry_size(&self, index: usize) -> Option<usize> {
        if index >= self.len() {
            return None;
        }

        let mut offset = self.entries_offset();
        for _i in 0..index {
            let value_start = offset + size_of::<TKey>();

            // TODO use some sort of a varint encoding for the entry size
            let value_size = u64::from_le_bytes(
                self.data[value_start..value_start + size_of::<u64>()]
                    .try_into()
                    .unwrap(),
            ) as usize;
            debug_assert!(value_size > 0);

            offset += self.entry_size_for_value_size(value_size);
        }

        let entry_size_start = offset + size_of::<TKey>();
        let value_size: u64 =
            pod_read_unaligned(&self.data[entry_size_start..entry_size_start + size_of::<u64>()]);
        debug_assert!(value_size > 0);

        Some(self.entry_size_for_value_size(value_size as usize))
    }

    fn entry_size_for_value_size(&self, value_size: usize) -> usize {
        size_of::<TKey>() + size_of::<u64>() + value_size
    }

    pub fn entries(&'_ self) -> impl Iterator<Item = LeafNodeEntry<'_, TKey>> {
        LeafNodeEntryIterator {
            node: self,
            offset: 0,
        }
    }

    // TODO make this a field instead
    fn header(&self) -> &LeafNodeHeader {
        from_bytes(&self.data[..size_of::<LeafNodeHeader>()])
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

    pub fn insert(&mut self, key: TKey, value: &[u8]) -> Result<LeafInsertResult<TKey>, TreeError> {
        let mut insert_index = self.len();

        for (index, entry) in self.entries().enumerate() {
            if key < entry.key {
                insert_index = index;
                break;
            }
        }

        if !self.can_fit(value.len()) {
            let (split_index, split_key, mut new_node) = self.split();

            if insert_index > split_index {
                let result = new_node.insert(key, value).unwrap();

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

    fn insert_at(&mut self, index: usize, key: TKey, value: &[u8]) -> Result<(), TreeError> {
        assert!(self.can_fit(value.len()));

        if let Some(move_start_offset) = self.entry_offset(index + 1) {
            let move_end_offset = self.entry_offset(self.len()).unwrap();

            let data_to_move = self.data[move_start_offset..move_end_offset].to_vec();

            let entry_size = self.entry_size_for_value_size(value.len());
            self.data[move_start_offset + entry_size..move_end_offset + entry_size]
                .copy_from_slice(&data_to_move);
        }

        let entry_offset = self.entry_offset(index).unwrap();

        let key_hole: &mut [u8] = &mut self.data[entry_offset..entry_offset + size_of::<TKey>()];

        key_hole.copy_from_slice(bytes_of(&key));

        let value_size_hole = &mut self.data
            [entry_offset + size_of::<TKey>()..entry_offset + size_of::<TKey>() + size_of::<u64>()];

        value_size_hole.copy_from_slice(bytes_of(&(value.len() as u64)));

        let value_hole = &mut self.data[entry_offset + size_of::<TKey>() + size_of::<u64>()
            ..entry_offset + size_of::<TKey>() + size_of::<u64>() + value.len()];

        value_hole.copy_from_slice(value);

        self.header.key_len += 1;

        Ok(())
    }

    fn can_fit(&self, value_size: usize) -> bool {
        self.entry_offset(self.len()).unwrap() + self.entry_size_for_value_size(value_size)
            < (self.data.len() - self.entries_offset())
    }

    fn split(&mut self) -> (usize, TKey, LeafNode<TKey>) {
        let initial_len = self.len();
        assert!(initial_len > 0, "Trying to split an empty node");

        let entries_start = self.entries_offset();

        let entries_to_leave = initial_len / 2 + initial_len % 2;
        let entries_to_move = initial_len - entries_to_leave;

        let move_start_offset = self.entry_offset(entries_to_leave).unwrap();
        let moved_entries_end = self.entry_offset(initial_len).unwrap();

        let new_node_entries = &self.data[move_start_offset..moved_entries_end];

        self.header.key_len = entries_to_leave as u16;

        // TODO we should not create the node here at all, and instead just return the data, so the
        // user can construct the new node with the correct links
        let mut new_node = LeafNode {
            header: NodeHeader {
                key_len: entries_to_move as u16,
                flags: NodeFlags::empty(),
                _unused2: 0,
                parent: PageIndex::zeroed(),
            },
            data: [0; _],
            _key: PhantomData,
        };

        new_node.data[entries_start..entries_start + new_node_entries.len()]
            .copy_from_slice(new_node_entries);

        (
            entries_to_move,
            from_bytes::<TKey>(&new_node.data[entries_start..entries_start + size_of::<TKey>()])
                .to_owned(),
            new_node,
        )
    }
}

// SAFETY: this is sound, because the struct has no padding and would be able to derive Pod
// automatically if not for the PhantomData
unsafe impl<TKey: Pod> Pod for LeafNode<TKey> {}

impl<TKey: Pod> NodeTrait<TKey> for LeafNode<TKey> {
    fn parent(&self) -> Option<InteriorNodeId> {
        if self.header.parent == PageIndex::zeroed() {
            None
        } else {
            Some(InteriorNodeId::new(self.header.parent))
        }
    }

    fn set_parent(&mut self, parent: Option<InteriorNodeId>) {
        self.header.set_parent(parent);
    }
}

#[derive(Zeroable, Pod, Debug, Clone, Copy)]
#[repr(C, align(8))]
struct LeafNodeHeader {
    previous: PageIndex,
    next: PageIndex,
}

pub(in crate::bplustree) struct LeafNodeEntry<'node, TKey> {
    key: TKey,
    value: &'node [u8],
}

impl<'node, TKey: Copy> LeafNodeEntry<'node, TKey> {
    pub fn key(&self) -> TKey {
        self.key
    }

    pub fn value(&self) -> &'node [u8] {
        self.value
    }
}

struct LeafNodeEntryIterator<'node, TKey: Pod> {
    node: &'node LeafNode<TKey>,
    offset: usize,
}

impl<'node, TKey: Pod + PartialOrd + 'node> Iterator for LeafNodeEntryIterator<'node, TKey> {
    type Item = LeafNodeEntry<'node, TKey>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.node.len() {
            return None;
        }

        self.offset += 1;

        self.node.entry(self.offset - 1)
    }
}

pub(in crate::bplustree) struct LeafNodeReader<'node, TKey: Pod> {
    // TODO make private
    pub node: &'node LeafNode<TKey>,
}

impl<'node, TKey: Pod> NodeReader<'node, LeafNode<TKey>, TKey> for LeafNodeReader<'node, TKey> {
    fn new(node: &'node LeafNode<TKey>) -> Self {
        Self { node }
    }
}

impl<'node, TKey: Pod> LeafNodeReader<'node, TKey> {
    pub fn new(node: &'node LeafNode<TKey>) -> Self {
        // TODO return a result with an error if we can't fit at least two entries
        Self { node }
    }
}

pub(in crate::bplustree) struct LeafNodeWriter<'node, TKey: Pod> {
    // TODO make private
    pub node: &'node mut LeafNode<TKey>,
}

impl<'node, TKey: Pod> NodeWriter<'node, LeafNode<TKey>, TKey> for LeafNodeWriter<'node, TKey> {
    fn new(node: &'node mut LeafNode<TKey>) -> Self {
        Self { node }
    }
}

#[must_use]
#[derive(Debug)]
pub(in crate::bplustree) enum LeafInsertResult<TKey: Pod> {
    Done,
    Split {
        new_node: Box<LeafNode<TKey>>,
        split_key: TKey,
    },
}

impl<'node, TKey: Pod + PartialOrd> LeafNodeWriter<'node, TKey> {
    pub fn new(node: &'node mut LeafNode<TKey>) -> Self {
        // TODO return a result with an error if we can't fit at least two entries
        Self { node }
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
            .set_parent(parent);
        let header = self.header_mut();
        header.previous = previous.map_or(PageIndex::zeroed(), |x| x.page());
        header.next = next.map_or(PageIndex::zeroed(), |x| x.page())
    }

    fn header_mut(&mut self) -> &mut LeafNodeHeader {
        from_bytes_mut(&mut self.node.data[0..size_of::<LeafNodeHeader>()])
    }
}
