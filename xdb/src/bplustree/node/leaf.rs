use std::marker::PhantomData;

use bytemuck::{Pod, Zeroable, bytes_of, checked::pod_read_unaligned};

use crate::{
    bplustree::{
        LeafNodeId, NodeId, TreeError,
        node::{InteriorNodeId, NODE_DATA_SIZE, Node, NodeFlags, NodeHeader},
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
    leaf_header: LeafNodeHeader,
    data: [u8; NODE_DATA_SIZE - size_of::<LeafNodeHeader>()],
    _key: PhantomData<TKey>,
}

impl<TKey: Pod + Ord> LeafNode<TKey> {
    pub fn new() -> Self {
        Self {
            header: NodeHeader {
                key_count: 0,
                flags: NodeFlags::empty(),
                _unused2: 0,
                parent: PageIndex::zero(),
            },
            leaf_header: LeafNodeHeader {
                previous: PageIndex::zero(),
                next: PageIndex::zero(),
            },
            data: [0; _],
            _key: PhantomData,
        }
    }

    pub fn from_raw_entries(entry_count: usize, entries: &[u8]) -> Self {
        let mut node = Self::new();
        node.data[..entries.len()].copy_from_slice(entries);
        node.header.key_count = entry_count as u16;

        node
    }

    fn len(&self) -> usize {
        usize::from(self.header.key_count)
    }

    fn entry(&self, index: usize) -> Option<LeafNodeEntry<'_, TKey>> {
        if index >= usize::from(self.header.key_count) {
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

        let mut offset = 0;

        for i in 0..index {
            offset += self.entry_size(i).unwrap();
            debug_assert!(offset < self.data.len());
        }

        Some(offset)
    }

    fn entry_size(&self, index: usize) -> Option<usize> {
        if index >= self.len() {
            return None;
        }

        let mut offset = 0;
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

    pub fn previous(&self) -> Option<LeafNodeId> {
        let previous = self.leaf_header.previous;

        if previous == PageIndex::zero() {
            None
        } else {
            Some(LeafNodeId::new(previous))
        }
    }

    pub fn next(&self) -> Option<LeafNodeId> {
        let next = self.leaf_header.next;

        if next == PageIndex::zero() {
            None
        } else {
            Some(LeafNodeId::new(next))
        }
    }

    pub fn insert(&mut self, key: TKey, value: &[u8]) -> Result<LeafInsertResult, TreeError> {
        let mut insert_index = self.len();

        let mut delete_index = None;

        for (index, entry) in self.entries().enumerate() {
            if key == entry.key {
                // TODO return a different result type for replaced?
                delete_index = Some(index);

                insert_index = index;
                break;
            }

            if key < entry.key {
                insert_index = index;
                break;
            }
        }

        if let Some(delete_index) = delete_index {
            self.delete_at(delete_index);
        }

        if !self.can_fit(value.len()) {
            Ok(LeafInsertResult::Split)
        } else {
            self.insert_at(insert_index, key, value)?;

            Ok(LeafInsertResult::Done)
        }
    }

    fn move_entries(&mut self, start_index: usize, end_index: usize, offset: isize) {
        let move_start_offset = self.entry_offset(start_index).unwrap();
        let move_end_offset = self.entry_offset(end_index).unwrap();
        let data_to_move = self.data[move_start_offset..move_end_offset].to_vec();

        self.data[move_start_offset.strict_add_signed(offset)
            ..move_end_offset.strict_add_signed(offset)]
            .copy_from_slice(&data_to_move);
    }

    fn insert_at(&mut self, index: usize, key: TKey, value: &[u8]) -> Result<(), TreeError> {
        assert!(self.can_fit(value.len()));

        if index < self.len() {
            let entry_size = self.entry_size_for_value_size(value.len());

            self.move_entries(index, self.len(), entry_size as isize);
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

        self.header.key_count += 1;

        Ok(())
    }

    fn delete_at(&mut self, index: usize) {
        let size = self.entry_size(index).unwrap();

        if index + 1 < self.len() {
            self.move_entries(index + 1, self.len(), -(size as isize));
        }

        self.header.key_count -= 1;
    }

    fn can_fit(&self, value_size: usize) -> bool {
        self.entry_offset(self.len()).unwrap() + self.entry_size_for_value_size(value_size)
            < (self.data.len())
    }

    pub fn split(&mut self) -> LeafNode<TKey> {
        let initial_len = self.len();
        assert!(initial_len > 0, "Trying to split an empty node");

        let entries_to_leave = initial_len / 2 + initial_len % 2;
        let entries_to_move = initial_len - entries_to_leave;

        let move_start_offset = self.entry_offset(entries_to_leave).unwrap();
        let moved_entries_end = self.entry_offset(initial_len).unwrap();

        let new_node_entries = &self.data[move_start_offset..moved_entries_end];

        self.header.key_count = entries_to_leave as u16;

        // TODO introduce some sort of "NodeMissingTopology" type that we can return here instead
        // of a LeafNode in an invalid state
        LeafNode::from_raw_entries(entries_to_move, new_node_entries)
    }

    pub fn set_links(
        &mut self,
        parent: Option<InteriorNodeId>,
        previous: Option<LeafNodeId>,
        next: Option<LeafNodeId>,
    ) {
        self.set_parent(parent);
        self.leaf_header.previous = previous.map_or(PageIndex::zero(), |x| x.page());
        self.leaf_header.next = next.map_or(PageIndex::zero(), |x| x.page())
    }

    pub(in crate::bplustree) fn first_key(&self) -> Option<TKey> {
        self.entry(0).map(|x| x.key)
    }
}

// SAFETY: this is sound, because the struct has no padding and would be able to derive Pod
// automatically if not for the PhantomData
unsafe impl<TKey: Pod> Pod for LeafNode<TKey> {}

impl<TKey: Pod> Node<TKey> for LeafNode<TKey> {
    fn parent(&self) -> Option<InteriorNodeId> {
        if self.header.parent == PageIndex::zero() {
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

impl<'node, TKey: Pod + Ord + 'node> Iterator for LeafNodeEntryIterator<'node, TKey> {
    type Item = LeafNodeEntry<'node, TKey>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.node.len() {
            return None;
        }

        self.offset += 1;

        self.node.entry(self.offset - 1)
    }
}

#[must_use]
#[derive(Debug)]
pub(in crate::bplustree) enum LeafInsertResult {
    Done,
    Split,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn insert_reverse() {
        let mut node = LeafNode::new();
        let _ = node.insert(1, &[0]).unwrap();
        let _ = node.insert(0, &[0]).unwrap();

        // TODO this is repeated, extract an fn for collecting
        let result = node
            .entries()
            .map(|x| (x.key, x.value.to_vec()))
            .collect::<Vec<_>>();

        assert!(&result == &[(0, vec![0]), (1, vec![0])]);
    }

    #[test]
    fn same_key_overrides() {
        let mut node = LeafNode::new();
        let _ = node.insert(0, &[0]);
        let _ = node.insert(0, &[1]);

        let result = node
            .entries()
            .map(|x| (x.key, x.value.to_vec()))
            .collect::<Vec<_>>();

        dbg!(&result);
        assert!(&result == &[(0, vec![1])]);
    }

    #[test]
    fn same_key_same_overrides_with_intermediate() {
        let mut node = LeafNode::new();
        let _ = node.insert(1, &[0]);
        let _ = node.insert(2, &[0]);
        let _ = node.insert(1, &[0]);

        let result = node
            .entries()
            .map(|x| (x.key, x.value.to_vec()))
            .collect::<Vec<_>>();

        assert!(&result == &[(1, vec![0]), (2, vec![0])]);
    }
}
