use std::marker::PhantomData;

use bytemuck::{Pod, Zeroable, bytes_of, cast_slice, cast_slice_mut};

use crate::{
    bplustree::{TreeKey, node::NODE_DATA_SIZE},
    storage::PageIndex,
};

const INTERIOR_NODE_DATA_SIZE: usize = NODE_DATA_SIZE - size_of::<u64>();
#[derive(Debug, Zeroable, Clone, Copy)]
#[repr(C, align(8))]
pub struct InteriorNodeEntries<TKey> {
    key_count: u16,
    _unused1: u16,
    _unused2: u32,

    data: InteriorNodeData<TKey>,
}

// SAFETY: this is sound, because the struct has no padding and would be able to derive Pod
// automatically if not for the PhantomData
unsafe impl<TKey: TreeKey> Pod for InteriorNodeEntries<TKey> {}

#[derive(Debug, Zeroable, Clone, Copy)]
#[repr(C, align(8))]
struct InteriorNodeData<TKey> {
    data: [u8; INTERIOR_NODE_DATA_SIZE],
    _key: PhantomData<TKey>,
}
// SAFETY: this is sound, because the struct has no padding and would be able to derive Pod
// automatically if not for the PhantomData
unsafe impl<TKey: TreeKey> Pod for InteriorNodeData<TKey> {}

impl<TKey: TreeKey> InteriorNodeData<TKey> {
    const VALUES_OFFSET: usize = Self::KEY_CAPACITY * size_of::<TKey>();
    // n - max number of keys
    //
    // size = key_size*n + value_size*(n+1)
    // size = key_size*n + value_size*n + value_size
    // size - value_size = key_size*n + value_size*n
    // (size - value_size)/(key_size + value_size) = n
    const KEY_CAPACITY: usize = (INTERIOR_NODE_DATA_SIZE - size_of::<PageIndex>())
        / (size_of::<TKey>() + size_of::<PageIndex>());

    fn from_raw_data(keys: &[TKey], values: &[PageIndex]) -> Self {
        assert!(size_of_val(keys) < Self::VALUES_OFFSET);

        let mut data = [0; _];

        data[..size_of_val(keys)].copy_from_slice(cast_slice(keys));
        data[Self::VALUES_OFFSET..Self::VALUES_OFFSET + size_of_val(values)]
            .copy_from_slice(cast_slice(values));

        Self {
            data,
            _key: PhantomData,
        }
    }

    fn keys(&self) -> &[TKey] {
        cast_slice(&self.data[..Self::VALUES_OFFSET])
    }

    fn values(&self) -> &[PageIndex] {
        cast_slice(&self.data[Self::VALUES_OFFSET..])
    }

    fn keys_mut(&mut self) -> &mut [TKey] {
        cast_slice_mut(&mut self.data[..Self::VALUES_OFFSET])
    }

    fn values_mut(&mut self) -> &mut [PageIndex] {
        cast_slice_mut(&mut self.data[Self::VALUES_OFFSET..])
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub(in crate::bplustree) struct KeyIndex(usize);

impl KeyIndex {
    pub(crate) const fn value_after(self) -> ValueIndex {
        ValueIndex(self.0.strict_add(1))
    }

    pub(crate) const fn value_before(self) -> ValueIndex {
        ValueIndex(self.0)
    }

    pub const fn new(index: usize) -> Self {
        Self(index)
    }

    pub(crate) fn key_before(self) -> Option<Self> {
        self.0.checked_sub(1).map(Self)
    }

    const fn key_after(self) -> Self {
        Self(self.0.strict_add(1))
    }

    const fn offset(self, offset: isize) -> Self {
        Self(self.0.strict_add_signed(offset))
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub(in crate::bplustree) struct ValueIndex(usize);

impl ValueIndex {
    pub const fn value_after(self) -> Self {
        Self(self.0 + 1)
    }

    pub(super) const fn new(x: usize) -> Self {
        Self(x)
    }

    pub fn key_before(self) -> Option<KeyIndex> {
        self.0.checked_sub(1).map(KeyIndex)
    }

    pub(crate) fn value_before(self) -> Option<Self> {
        self.0.checked_sub(1).map(Self)
    }

    const fn key_after(self) -> KeyIndex {
        KeyIndex(self.0)
    }

    const fn offset(self, offset: isize) -> Self {
        Self(self.0.strict_add_signed(offset))
    }
}

impl<TKey: TreeKey> InteriorNodeEntries<TKey> {
    pub fn new(left: PageIndex, key: TKey, right: PageIndex) -> Self {
        Self {
            key_count: 1,
            _unused1: 0,
            _unused2: 0,
            data: InteriorNodeData::from_raw_data(&[key], &[left, right]),
        }
    }

    pub fn key_count(&self) -> usize {
        usize::from(self.key_count)
    }

    pub fn has_spare_capacity(&self) -> bool {
        self.key_count() + 1 < InteriorNodeData::<TKey>::KEY_CAPACITY
    }

    pub fn split(&mut self) -> (TKey, Self) {
        let key_count = self.key_count();
        let keys_to_leave = key_count.div_ceil(2);
        let keys_to_move = key_count - keys_to_leave - 1;

        let values_to_leave = keys_to_leave + 1;
        let values_to_move = (key_count + 1) - values_to_leave;

        let split_key = self.data.keys()[keys_to_leave];

        let key_data_to_move =
            self.data.keys()[(keys_to_leave + 1)..(keys_to_leave + 1) + keys_to_move].to_vec();
        let value_data_to_move =
            self.data.values()[values_to_leave..values_to_leave + values_to_move].to_vec();

        self.key_count = u16::try_from(keys_to_leave).unwrap();

        let new_node_data = InteriorNodeData::from_raw_data(&key_data_to_move, &value_data_to_move);

        (
            split_key,
            Self {
                key_count: u16::try_from(keys_to_move).unwrap(),
                _unused1: 0,
                _unused2: 0,
                data: new_node_data,
            },
        )
    }

    pub(crate) fn merge_from(&mut self, entries: &Self, merge_key: TKey) {
        let merge_key_offset = self.key_after_last();

        self.data.keys_mut()[merge_key_offset.0] = merge_key;

        let new_keys_offset = merge_key_offset.key_after();

        self.data.keys_mut()[new_keys_offset.0..new_keys_offset.0 + entries.key_count()]
            .copy_from_slice(&entries.data.keys()[..entries.key_count()]);

        let new_values_offset = self.value_after_last();
        let values_size = entries.key_count() + 1;

        self.data.values_mut()[new_values_offset.0..new_values_offset.0 + values_size]
            .copy_from_slice(&entries.data.values()[..values_size]);

        self.key_count += entries.key_count + 1;
    }

    pub fn insert_at(&mut self, index: KeyIndex, key: TKey, value: PageIndex) {
        assert!(self.key_count() < InteriorNodeData::<TKey>::KEY_CAPACITY);

        debug_assert!(bytes_of(&key) != vec![0; size_of::<TKey>()]);

        self.move_keys(index, 1);
        self.move_values(index.value_after(), 1);

        self.data.keys_mut()[index.0] = key;
        self.data.values_mut()[index.value_after().0] = value;

        self.key_count += 1;
    }

    fn move_keys(&mut self, start_index: KeyIndex, offset: isize) {
        let keys_to_move = self.data.keys()[start_index.0..self.key_after_last().0].to_vec();

        let target_end_index = self.key_after_last().offset(offset).0;

        self.data.keys_mut()[start_index.0.strict_add_signed(offset)..target_end_index]
            .copy_from_slice(&keys_to_move);
    }

    fn move_values(&mut self, start_index: ValueIndex, offset: isize) {
        let end_index = self.value_after_last();

        let values_to_move = self.data.values()[start_index.0..end_index.0].to_vec();

        self.data.values_mut()[start_index.offset(offset).0..end_index.0.strict_add_signed(offset)]
            .copy_from_slice(&values_to_move);
    }

    pub fn value_at(&self, index: ValueIndex) -> Option<PageIndex> {
        if index.0 > self.key_count() {
            return None;
        }

        let value = self.data.values()[index.0];

        assert!(value != PageIndex::zero());

        Some(value)
    }

    pub fn delete_at(&mut self, index: ValueIndex) {
        assert!(index.0 <= self.key_count());

        self.move_keys(index.key_after(), -1);
        self.move_values(index.value_after(), -1);

        self.key_count -= 1;
    }

    pub fn needs_merge(&self) -> bool {
        2 * self.key_count() <= InteriorNodeData::<TKey>::KEY_CAPACITY
    }

    pub fn can_fit_merge(&self, right: &Self) -> bool {
        self.key_count() + right.key_count() < InteriorNodeData::<TKey>::KEY_CAPACITY
    }

    pub fn key_at(&self, index: KeyIndex) -> Option<TKey> {
        self.data.keys().get(index.0).copied()
    }

    pub(crate) fn key_after_last(&self) -> KeyIndex {
        KeyIndex(self.key_count())
    }

    pub(crate) fn last_value(&self) -> ValueIndex {
        ValueIndex(self.key_count())
    }

    fn value_after_last(&self) -> ValueIndex {
        ValueIndex(self.key_count() + 1)
    }
}
