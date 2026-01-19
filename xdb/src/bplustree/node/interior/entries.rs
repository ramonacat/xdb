use std::marker::PhantomData;

use bytemuck::{Pod, Zeroable, bytes_of, from_bytes, pod_read_unaligned};

use crate::{
    bplustree::{TreeKey, node::NODE_DATA_SIZE},
    storage::PageIndex,
};

#[derive(Debug, Zeroable, Clone, Copy)]
#[repr(C, align(8))]
pub struct InteriorNodeEntries<TKey> {
    key_count: u16,
    _unused1: u16,
    _unused2: u32,

    data: [u8; NODE_DATA_SIZE - size_of::<u64>()],
    _key: PhantomData<TKey>,
}

// SAFETY: this is sound, because the struct has no padding and would be able to derive Pod
// automatically if not for the PhantomData
unsafe impl<TKey: TreeKey> Pod for InteriorNodeEntries<TKey> {}

impl<TKey: TreeKey> InteriorNodeEntries<TKey> {
    pub fn new() -> Self {
        Self {
            key_count: 0,
            _unused1: 0,
            _unused2: 0,
            data: [0; _],
            _key: PhantomData,
        }
    }

    pub fn key_count(&self) -> usize {
        usize::from(self.key_count)
    }

    pub fn set_first_pointer(&mut self, value: PageIndex) {
        let offset = self.values_offset();

        self.data[offset..offset + size_of::<PageIndex>()].copy_from_slice(bytes_of(&value));
    }

    pub fn has_spare_capacity(&self) -> bool {
        self.key_count() + 1 < self.key_capacity()
    }

    pub fn split(&mut self) -> (TKey, InteriorNodeEntries<TKey>) {
        let key_count = self.key_count();
        let keys_to_leave = key_count.div_ceil(2);
        let keys_to_move = key_count - keys_to_leave - 1;

        let values_to_leave = keys_to_leave + 1;
        let values_to_move = (key_count + 1) - values_to_leave;

        let key_data_to_move_start = (keys_to_leave + 1) * size_of::<TKey>();
        let value_data_to_move_start =
            self.values_offset() + values_to_leave * size_of::<PageIndex>();

        let key_data_to_move = self.data
            [key_data_to_move_start..key_data_to_move_start + keys_to_move * size_of::<TKey>()]
            .to_vec();
        let value_data_to_move = self.data[value_data_to_move_start
            ..value_data_to_move_start + values_to_move * size_of::<PageIndex>()]
            .to_vec();

        self.key_count = keys_to_leave as u16;

        let mut split_node = InteriorNodeEntries::new();
        let split_node_values_offset = split_node.values_offset();

        // TODO The first key here is not set, as that child must be created, enforce this via the
        // type system!
        split_node.data[..key_data_to_move.len()].copy_from_slice(&key_data_to_move);
        split_node.data
            [split_node_values_offset..split_node_values_offset + value_data_to_move.len()]
            .copy_from_slice(&value_data_to_move);
        split_node.key_count = keys_to_move as u16;

        let split_key_offset = (keys_to_leave) * size_of::<TKey>();
        (
            pod_read_unaligned(&self.data[split_key_offset..split_key_offset + size_of::<TKey>()]),
            split_node,
        )
    }

    pub fn insert_at(&mut self, index: usize, key: &TKey, value: PageIndex) {
        let key_len = self.key_count();
        assert!(key_len < self.key_capacity());

        debug_assert!(bytes_of(key) != vec![0; size_of::<TKey>()]);

        let key_offset = size_of::<TKey>() * (index);
        let value_offset = self.values_offset() + size_of::<PageIndex>() * (index + 1);

        self.move_keys(index, size_of::<TKey>() as isize);
        self.move_values(index + 1, size_of::<PageIndex>() as isize);

        self.data[key_offset..key_offset + size_of::<TKey>()].copy_from_slice(bytes_of(key));
        self.data[value_offset..value_offset + size_of::<PageIndex>()]
            .copy_from_slice(bytes_of(&value));

        self.key_count += 1;
    }

    fn move_keys(&mut self, start_index: usize, offset: isize) {
        let start_offset = start_index * size_of::<TKey>();
        let end_offset = self.key_count() * size_of::<TKey>();

        assert!(end_offset < self.values_offset());

        let keys_to_move = self.data[start_offset..end_offset].to_vec();

        let target_end_offset = end_offset.strict_add_signed(offset);
        assert!(target_end_offset < self.values_offset());
        self.data[start_offset.strict_add_signed(offset)..target_end_offset]
            .copy_from_slice(&keys_to_move);
    }

    fn values_offset(&self) -> usize {
        self.key_capacity() * size_of::<TKey>()
    }

    fn key_capacity(&self) -> usize {
        // n - max number of keys
        //
        // size = key_size*n + value_size*(n+1)
        // size = key_size*n + value_size*n + value_size
        // size - value_size = key_size*n + value_size*n
        // (size - value_size)/(key_size + value_size) = n

        (self.data.len() - size_of::<PageIndex>()) / (size_of::<TKey>() + size_of::<PageIndex>())
    }

    fn move_values(&mut self, start_index: usize, offset: isize) {
        let start_offset = self.values_offset() + size_of::<PageIndex>() * start_index;
        let end_offset = self.values_offset() + size_of::<PageIndex>() * (self.key_count() + 1);

        let values_to_move = self.data[start_offset..end_offset].to_vec();

        let target_start_offset = start_offset.strict_add_signed(offset);

        assert!(target_start_offset >= self.values_offset());

        self.data[target_start_offset..end_offset.strict_add_signed(offset)]
            .copy_from_slice(&values_to_move);
    }

    pub fn value_at(&self, index: usize) -> Option<PageIndex> {
        if index > self.key_count() {
            return None;
        }

        let value_start = self.values_offset() + (index * size_of::<PageIndex>());

        let value: PageIndex =
            pod_read_unaligned(&self.data[value_start..value_start + size_of::<PageIndex>()]);

        assert!(value != PageIndex::zero());

        Some(value)
    }

    pub fn delete_at(&mut self, index: usize) {
        assert!(index > 0); // TODO we should handle this situation as well

        if index < self.key_count() {
            self.move_keys(index, -(size_of::<TKey>() as isize));
            self.move_values(index + 1, -(size_of::<PageIndex>() as isize));
        }

        self.key_count -= 1;
    }

    pub fn needs_merge(&self) -> bool {
        2 * (self.key_count() * size_of::<TKey>() + (self.key_count() + 1) * size_of::<PageIndex>())
            < self.data.len()
    }

    pub fn can_fit_merge(&self, right: &InteriorNodeEntries<TKey>) -> bool {
        self.key_count() + right.key_count() > self.key_capacity()
    }

    pub fn key_at(&self, index: usize) -> Option<&TKey> {
        if index >= self.key_count() {
            return None;
        }

        // TODO is the alignment here guaranteed? could this cause trouble with funny-sized keys?
        Some(from_bytes(
            &self.data[(index) * size_of::<TKey>()..(index + 1) * size_of::<TKey>()],
        ))
    }
}
