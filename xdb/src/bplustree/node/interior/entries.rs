use std::marker::PhantomData;

use bytemuck::{Pod, Zeroable, bytes_of, from_bytes, pod_read_unaligned};

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

    // TODO divide into keys: [u8; ...] and values: [u8; ...], as the sizes can be computed
    // statically
    data: [u8; INTERIOR_NODE_DATA_SIZE],
    _key: PhantomData<TKey>,
}

// SAFETY: this is sound, because the struct has no padding and would be able to derive Pod
// automatically if not for the PhantomData
unsafe impl<TKey: TreeKey> Pod for InteriorNodeEntries<TKey> {}

impl<TKey: TreeKey> InteriorNodeEntries<TKey> {
    const VALUES_OFFSET: usize = Self::KEY_CAPACITY * size_of::<TKey>();
    // n - max number of keys
    //
    // size = key_size*n + value_size*(n+1)
    // size = key_size*n + value_size*n + value_size
    // size - value_size = key_size*n + value_size*n
    // (size - value_size)/(key_size + value_size) = n
    const KEY_CAPACITY: usize = (INTERIOR_NODE_DATA_SIZE - size_of::<PageIndex>())
        / (size_of::<TKey>() + size_of::<PageIndex>());

    // TODO get rid of this, don't allow creating an invalid node
    pub const fn new() -> Self {
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
        let offset = Self::VALUES_OFFSET;

        self.data[offset..offset + size_of::<PageIndex>()].copy_from_slice(bytes_of(&value));
    }

    pub fn has_spare_capacity(&self) -> bool {
        self.key_count() + 1 < Self::KEY_CAPACITY
    }

    pub fn split(&mut self) -> (TKey, Self) {
        let key_count = self.key_count();
        let keys_to_leave = key_count.div_ceil(2);
        let keys_to_move = key_count - keys_to_leave - 1;

        let values_to_leave = keys_to_leave + 1;
        let values_to_move = (key_count + 1) - values_to_leave;

        let key_data_to_move_start = (keys_to_leave + 1) * size_of::<TKey>();
        let value_data_to_move_start =
            Self::VALUES_OFFSET + values_to_leave * size_of::<PageIndex>();

        let key_data_to_move = self.data
            [key_data_to_move_start..key_data_to_move_start + keys_to_move * size_of::<TKey>()]
            .to_vec();
        let value_data_to_move = self.data[value_data_to_move_start
            ..value_data_to_move_start + values_to_move * size_of::<PageIndex>()]
            .to_vec();

        self.key_count = u16::try_from(keys_to_leave).unwrap();

        let mut new_node_data = [0; _];

        new_node_data[..key_data_to_move.len()].copy_from_slice(&key_data_to_move);
        let values_offset = Self::VALUES_OFFSET;
        new_node_data[values_offset..values_offset + value_data_to_move.len()]
            .copy_from_slice(&value_data_to_move);

        let split_key_offset = (keys_to_leave) * size_of::<TKey>();
        (
            pod_read_unaligned(&self.data[split_key_offset..split_key_offset + size_of::<TKey>()]),
            Self {
                key_count: u16::try_from(keys_to_move).unwrap(),
                _unused1: 0,
                _unused2: 0,
                data: new_node_data,
                _key: PhantomData,
            },
        )
    }

    pub fn insert_at(&mut self, index: usize, key: &TKey, value: PageIndex) {
        let key_len = self.key_count();
        assert!(key_len < Self::KEY_CAPACITY);

        debug_assert!(bytes_of(key) != vec![0; size_of::<TKey>()]);

        let key_offset = size_of::<TKey>() * (index);
        let value_offset = Self::VALUES_OFFSET + size_of::<PageIndex>() * (index + 1);

        self.move_keys(index, isize::try_from(size_of::<TKey>()).unwrap());
        self.move_values(index + 1, isize::try_from(size_of::<PageIndex>()).unwrap());

        self.data[key_offset..key_offset + size_of::<TKey>()].copy_from_slice(bytes_of(key));
        self.data[value_offset..value_offset + size_of::<PageIndex>()]
            .copy_from_slice(bytes_of(&value));

        self.key_count += 1;
    }

    fn move_keys(&mut self, start_index: usize, offset: isize) {
        let start_offset = start_index * size_of::<TKey>();
        let end_offset = self.key_count() * size_of::<TKey>();

        assert!(end_offset < Self::VALUES_OFFSET);

        let keys_to_move = self.data[start_offset..end_offset].to_vec();

        let target_end_offset = end_offset.strict_add_signed(offset);
        assert!(target_end_offset < Self::VALUES_OFFSET);
        self.data[start_offset.strict_add_signed(offset)..target_end_offset]
            .copy_from_slice(&keys_to_move);
    }

    fn move_values(&mut self, start_index: usize, offset: isize) {
        let start_offset = Self::VALUES_OFFSET + size_of::<PageIndex>() * start_index;
        let end_offset = Self::VALUES_OFFSET + size_of::<PageIndex>() * (self.key_count() + 1);

        let values_to_move = self.data[start_offset..end_offset].to_vec();

        let target_start_offset = start_offset.strict_add_signed(offset);

        assert!(target_start_offset >= Self::VALUES_OFFSET);

        self.data[target_start_offset..end_offset.strict_add_signed(offset)]
            .copy_from_slice(&values_to_move);
    }

    pub fn value_at(&self, index: usize) -> Option<PageIndex> {
        if index > self.key_count() {
            return None;
        }

        let value_start = Self::VALUES_OFFSET + (index * size_of::<PageIndex>());

        let value: PageIndex =
            pod_read_unaligned(&self.data[value_start..value_start + size_of::<PageIndex>()]);

        assert!(value != PageIndex::zero());

        Some(value)
    }

    pub fn delete_at(&mut self, index: usize) {
        if index < self.key_count() {
            self.move_keys(index, -isize::try_from(size_of::<TKey>()).unwrap());
            self.move_values(index + 1, -isize::try_from(size_of::<PageIndex>()).unwrap());
        }

        self.key_count -= 1;
    }

    pub fn needs_merge(&self) -> bool {
        2 * (self.key_count() * size_of::<TKey>() + (self.key_count() + 1) * size_of::<PageIndex>())
            < INTERIOR_NODE_DATA_SIZE
    }

    pub fn can_fit_merge(&self, right: &Self) -> bool {
        self.key_count() + right.key_count() > Self::KEY_CAPACITY
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
