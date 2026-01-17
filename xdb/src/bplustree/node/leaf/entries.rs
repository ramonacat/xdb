use std::marker::PhantomData;

use bytemuck::{Pod, Zeroable, bytes_of, pod_read_unaligned};

use crate::bplustree::{TreeError, node::leaf::LEAF_NODE_DATA_SIZE};

pub(in crate::bplustree) struct LeafNodeEntry<'node, TKey> {
    key: TKey,
    value: &'node [u8],
    size: usize,
}

impl<'node, TKey: Copy> LeafNodeEntry<'node, TKey> {
    pub fn key(&self) -> TKey {
        self.key
    }

    pub fn value(&self) -> &'node [u8] {
        self.value
    }

    pub fn total_size(&self) -> usize {
        self.size + size_of::<u64>() + size_of::<TKey>()
    }

    pub(crate) fn value_size(&self) -> usize {
        self.size
    }
}

pub(super) struct LeafNodeEntryIterator<'node, TKey: Pod> {
    data: &'node LeafNodeEntries<TKey>,
    offset: usize,
    index: usize,
}

impl<'node, TKey: Pod> LeafNodeEntryIterator<'node, TKey> {
    pub(crate) fn new(data: &'node LeafNodeEntries<TKey>) -> Self {
        Self {
            data,
            offset: 0,
            index: 0,
        }
    }
}

impl<'node, TKey: Pod + Ord + 'node> Iterator for LeafNodeEntryIterator<'node, TKey> {
    type Item = LeafNodeEntry<'node, TKey>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.data.len() {
            return None;
        }

        let entry = self.data.entry_at(self.offset);

        self.offset += entry.total_size();
        self.index += 1;

        Some(entry)
    }
}

#[derive(Debug, Zeroable, Clone, Copy)]
#[repr(C, align(8))]
pub struct LeafNodeEntries<TKey> {
    data: [u8; LEAF_NODE_DATA_SIZE - size_of::<u16>()],
    len: u16,
    _key: PhantomData<TKey>,
}

impl<TKey: Pod + Ord> LeafNodeEntries<TKey> {
    const _ASSERT_SIZE: () = assert!(size_of::<LeafNodeEntries<TKey>>() == LEAF_NODE_DATA_SIZE);

    pub fn new() -> Self {
        Self {
            len: 0,
            data: [0; _],
            _key: PhantomData,
        }
    }

    pub fn entries(&self) -> impl Iterator<Item = LeafNodeEntry<'_, TKey>> {
        LeafNodeEntryIterator::new(self)
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn entry(&self, index: usize) -> Option<LeafNodeEntry<'_, TKey>> {
        if index >= self.len() {
            return None;
        }

        let entry_offset = self.entry_offset(index)?;

        Some(self.entry_at(entry_offset))
    }

    fn entry_offset(&self, index: usize) -> Option<usize> {
        if index >= self.len() {
            return None;
        }

        let mut offset = 0;

        for _ in 0..index {
            let entry = self.entry_at(offset);

            offset += entry.total_size();
            debug_assert!(offset < self.data.len());
        }

        Some(offset)
    }

    fn entry_at(&self, offset: usize) -> LeafNodeEntry<'_, TKey> {
        let value_size_offset = offset + size_of::<TKey>();
        let value_offset = value_size_offset + size_of::<u64>();
        let value_size = pod_read_unaligned::<u64>(
            &self.data[value_size_offset..value_size_offset + size_of::<u64>()],
        ) as usize;

        LeafNodeEntry {
            key: pod_read_unaligned(&self.data[offset..offset + size_of::<TKey>()]),
            value: &self.data[value_offset..value_offset + value_size],
            size: value_size,
        }
    }

    fn used_size(&self) -> usize {
        let mut size = 0;

        for entry in self.entries() {
            size += entry.total_size();
        }

        size
    }

    fn entry_size_for_value_size(&self, value_size: usize) -> usize {
        size_of::<TKey>() + size_of::<u64>() + value_size
    }

    pub fn can_fit(&self, value_size: usize) -> bool {
        self.used_size() + self.entry_size_for_value_size(value_size) <= self.data.len()
    }

    fn move_entries(&mut self, start_index: usize, offset: isize) {
        let move_start_offset = self.entry_offset(start_index).unwrap();
        let move_end_offset = self.used_size();
        let data_to_move = self.data[move_start_offset..move_end_offset].to_vec();

        self.data[move_start_offset.strict_add_signed(offset)
            ..move_end_offset.strict_add_signed(offset)]
            .copy_from_slice(&data_to_move);
    }

    pub fn insert_at(&mut self, index: usize, key: TKey, value: &[u8]) -> Result<(), TreeError> {
        assert!(self.can_fit(value.len()));

        let entry_offset = if index < self.len() {
            let entry_size = self.entry_size_for_value_size(value.len());

            self.move_entries(index, entry_size as isize);
            self.entry_offset(index).unwrap()
        } else {
            self.used_size()
        };

        let key_hole: &mut [u8] = &mut self.data[entry_offset..entry_offset + size_of::<TKey>()];

        key_hole.copy_from_slice(bytes_of(&key));

        let value_size_hole = &mut self.data
            [entry_offset + size_of::<TKey>()..entry_offset + size_of::<TKey>() + size_of::<u64>()];

        value_size_hole.copy_from_slice(bytes_of(&(value.len() as u64)));

        let value_hole = &mut self.data[entry_offset + size_of::<TKey>() + size_of::<u64>()
            ..entry_offset + size_of::<TKey>() + size_of::<u64>() + value.len()];

        value_hole.copy_from_slice(value);

        self.len += 1;

        Ok(())
    }

    pub fn delete_at(&mut self, index: usize) {
        let size = self.entry(index).unwrap().total_size();

        if index + 1 < self.len() {
            self.move_entries(index + 1, -(size as isize));
        }

        self.len -= 1;
    }

    pub(crate) fn split_at(&mut self, entries_to_leave: usize) -> &[u8] {
        let move_start_offset = self.entry_offset(entries_to_leave).unwrap();
        let moved_entries_end = self.used_size();

        let new_node_entries = &self.data[move_start_offset..moved_entries_end];

        self.len = entries_to_leave as u16;

        new_node_entries
    }

    pub(crate) fn from_data(entry_count: usize, data: &[u8]) -> LeafNodeEntries<TKey> {
        let mut entries = Self::new();
        entries.data[0..data.len()].copy_from_slice(data);
        entries.len = entry_count as u16;

        entries
    }

    pub(super) fn needs_merge(&self) -> bool {
        self.used_size() * 2 < self.data.len()
    }

    pub(crate) fn can_fit_merge(&self, other: LeafNodeEntries<TKey>) -> bool {
        self.used_size() + other.used_size() <= self.data.len()
    }
}
