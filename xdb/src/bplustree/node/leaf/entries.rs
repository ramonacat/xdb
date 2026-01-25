use std::marker::PhantomData;

use bytemuck::{Zeroable, bytes_of, pod_read_unaligned};

use crate::{
    Size,
    bplustree::{
        TreeKey,
        node::leaf::{LEAF_NODE_DATA_SIZE, builder::MaterializedData},
    },
};

pub(in crate::bplustree) struct LeafNodeEntry<'node, TKey> {
    key: TKey,
    value: &'node [u8],
    size: usize,
}

impl<'node, TKey: TreeKey> LeafNodeEntry<'node, TKey> {
    pub const fn key(&self) -> TKey {
        self.key
    }

    pub const fn value(&self) -> &'node [u8] {
        self.value
    }

    pub const fn total_size(&self) -> usize {
        self.size + size_of::<u64>() + size_of::<TKey>()
    }

    pub(crate) const fn value_size(&self) -> usize {
        self.size
    }
}

pub(super) struct LeafNodeEntryIterator<'node, TKey: TreeKey> {
    data: &'node LeafNodeEntries<TKey>,
    offset: usize,
    index: usize,
}

impl<'node, TKey: TreeKey> LeafNodeEntryIterator<'node, TKey> {
    pub(crate) const fn new(data: &'node LeafNodeEntries<TKey>) -> Self {
        Self {
            data,
            offset: 0,
            index: 0,
        }
    }
}

impl<'node, TKey: TreeKey + 'node> Iterator for LeafNodeEntryIterator<'node, TKey> {
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
    data: [u8; LEAF_NODE_DATA_SIZE.subtract(Size::of::<u16>()).as_bytes()],
    len: u16,
    _key: PhantomData<TKey>,
}

impl<TKey: TreeKey> LeafNodeEntries<TKey> {
    const _ASSERT_SIZE: () = assert!(Size::of::<Self>().is_equal(LEAF_NODE_DATA_SIZE));

    pub const fn new() -> Self {
        Self {
            len: 0,
            data: [0; _],
            _key: PhantomData,
        }
    }

    pub fn entries(&self) -> impl Iterator<Item = LeafNodeEntry<'_, TKey>> {
        LeafNodeEntryIterator::new(self)
    }

    pub const fn len(&self) -> usize {
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
        let value_size = usize::try_from(pod_read_unaligned::<u64>(
            &self.data[value_size_offset..value_size_offset + size_of::<u64>()],
        ))
        .unwrap();

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

    const fn entry_size_for_value_size(value_size: usize) -> usize {
        size_of::<TKey>() + size_of::<u64>() + value_size
    }

    pub fn can_fit(&self, value_size: usize) -> bool {
        self.used_size() + Self::entry_size_for_value_size(value_size) <= self.data.len()
    }

    fn move_entries(&mut self, start_index: usize, offset: isize) {
        let move_start_offset = self.entry_offset(start_index).unwrap();
        let move_end_offset = self.used_size();
        let data_to_move = self.data[move_start_offset..move_end_offset].to_vec();

        self.data[move_start_offset.strict_add_signed(offset)
            ..move_end_offset.strict_add_signed(offset)]
            .copy_from_slice(&data_to_move);
    }

    // TODO extract functions/struct/whatever for managing a value of any size
    pub fn insert_at(&mut self, index: usize, key: TKey, value: &[u8]) {
        assert!(self.can_fit(value.len()));

        let entry_offset = if index < self.len() {
            let entry_size = Self::entry_size_for_value_size(value.len());

            self.move_entries(index, isize::try_from(entry_size).unwrap());
            self.entry_offset(index).unwrap()
        } else {
            self.used_size()
        };

        let key_hole: &mut [u8] = &mut self.data[entry_offset..entry_offset + size_of::<TKey>()];

        key_hole.copy_from_slice(bytes_of(&key));

        let value_size_hole = &mut self.data
            [entry_offset + size_of::<TKey>()..entry_offset + size_of::<TKey>() + size_of::<u64>()];

        value_size_hole.copy_from_slice(bytes_of(&(value.len() as u64)));

        let entry_data_offset = Size::of::<TKey>() + Size::of::<u64>();

        let value_hole = &mut self.data[entry_offset + entry_data_offset.as_bytes()
            ..entry_offset + (entry_data_offset + Size::B(value.len())).as_bytes()];

        value_hole.copy_from_slice(value);

        self.len += 1;
    }

    pub fn delete_at(&mut self, index: usize) {
        let size = self.entry(index).unwrap().total_size();

        if index + 1 < self.len() {
            self.move_entries(index + 1, -isize::try_from(size).unwrap());
        }

        self.len -= 1;
    }

    pub fn split(&'_ mut self) -> MaterializedData<'_, TKey> {
        let initial_len = self.len();
        assert!(initial_len > 0, "Trying to split an empty node");

        let mut entries_to_leave = 0;
        let mut offset = 0;

        while offset <= self.used_size() / 2 {
            let entry = self.entry(entries_to_leave).unwrap();

            offset += entry.total_size();
            entries_to_leave += 1;
        }

        entries_to_leave -= 1;

        let entries_to_move = initial_len - entries_to_leave;

        MaterializedData::new(entries_to_move, self.split_at(entries_to_leave))
    }

    fn split_at(&mut self, entries_to_leave: usize) -> &[u8] {
        let move_start_offset = self.entry_offset(entries_to_leave).unwrap();
        let moved_entries_end = self.used_size();

        let new_node_entries = &self.data[move_start_offset..moved_entries_end];

        self.len = u16::try_from(entries_to_leave).unwrap();

        new_node_entries
    }

    pub(crate) fn from_data(entry_count: usize, data: &[u8]) -> Self {
        let mut entries = Self::new();
        entries.data[0..data.len()].copy_from_slice(data);
        entries.len = u16::try_from(entry_count).unwrap();

        entries
    }

    pub(super) fn needs_merge(&self) -> bool {
        self.used_size() * 2 < self.data.len()
    }

    pub(crate) fn can_fit_merge(&self, other: Self) -> bool {
        self.used_size() + other.used_size() <= self.data.len()
    }
}
