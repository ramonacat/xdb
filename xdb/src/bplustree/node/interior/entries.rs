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
    const VALUES_CAPACITY: usize = Self::KEY_CAPACITY + 1;

    fn from_raw_data(keys: &[u8], values: &[u8]) -> Self {
        assert!(keys.len() < Self::VALUES_OFFSET);

        let mut data = [0; _];

        data[..keys.len()].copy_from_slice(keys);
        data[Self::VALUES_OFFSET..Self::VALUES_OFFSET + values.len()].copy_from_slice(values);

        Self {
            data,
            _key: PhantomData,
        }
    }

    fn keys(&self) -> &[u8] {
        &self.data[..Self::VALUES_OFFSET]
    }

    fn values(&self) -> &[u8] {
        &self.data[Self::VALUES_OFFSET..]
    }

    fn keys_mut(&mut self) -> &mut [u8] {
        &mut self.data[..Self::VALUES_OFFSET]
    }

    fn values_mut(&mut self) -> &mut [u8] {
        &mut self.data[Self::VALUES_OFFSET..]
    }
}

impl<TKey: TreeKey> InteriorNodeEntries<TKey> {
    pub fn new(left: PageIndex, key: TKey, right: PageIndex) -> Self {
        let values = bytes_of(&left)
            .iter()
            .chain(bytes_of(&right))
            .copied()
            .collect::<Vec<_>>();

        Self {
            key_count: 1,
            _unused1: 0,
            _unused2: 0,
            data: InteriorNodeData::from_raw_data(bytes_of(&key), &values),
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

        let key_data_to_move_start = (keys_to_leave + 1) * size_of::<TKey>();
        let value_data_to_move_start = values_to_leave * size_of::<PageIndex>();

        let key_data_to_move = self.data.keys()
            [key_data_to_move_start..key_data_to_move_start + keys_to_move * size_of::<TKey>()]
            .to_vec();
        let value_data_to_move = self.data.values()[value_data_to_move_start
            ..value_data_to_move_start + values_to_move * size_of::<PageIndex>()]
            .to_vec();

        self.key_count = u16::try_from(keys_to_leave).unwrap();

        let new_node_data = InteriorNodeData::from_raw_data(&key_data_to_move, &value_data_to_move);

        let split_key_offset = (keys_to_leave) * size_of::<TKey>();
        (
            pod_read_unaligned(
                &self.data.keys()[split_key_offset..split_key_offset + size_of::<TKey>()],
            ),
            Self {
                key_count: u16::try_from(keys_to_move).unwrap(),
                _unused1: 0,
                _unused2: 0,
                data: new_node_data,
            },
        )
    }

    // TODO methods like `write_key_at` and `write_value_at` to contain the index arithmetic?
    pub(crate) fn merge_from(&mut self, entries: &Self, at_key: TKey) {
        let new_keys_offset = self.key_count() * size_of::<TKey>();
        let keys_size = entries.key_count() * size_of::<TKey>();

        self.data.keys_mut()[new_keys_offset..new_keys_offset + size_of::<TKey>()]
            .copy_from_slice(bytes_of(&at_key));

        let new_keys_offset = new_keys_offset + size_of::<TKey>();
        self.data.keys_mut()[new_keys_offset..new_keys_offset + keys_size]
            .copy_from_slice(&entries.data.keys()[..keys_size]);

        let new_values_offset = (self.key_count() + 1) * size_of::<PageIndex>();
        let values_size = (entries.key_count() + 1) * size_of::<PageIndex>();
        self.data.values_mut()[new_values_offset..new_values_offset + values_size]
            .copy_from_slice(&entries.data.values()[..values_size]);

        self.key_count += entries.key_count + 1;
    }

    pub fn insert_at(&mut self, index: usize, key: &TKey, value: PageIndex) {
        let key_len = self.key_count();
        assert!(key_len < InteriorNodeData::<TKey>::KEY_CAPACITY);

        debug_assert!(bytes_of(key) != vec![0; size_of::<TKey>()]);

        let key_offset = size_of::<TKey>() * (index);
        let value_offset = size_of::<PageIndex>() * (index + 1);

        self.move_keys(index, isize::try_from(size_of::<TKey>()).unwrap());
        self.move_values(index + 1, isize::try_from(size_of::<PageIndex>()).unwrap());

        self.data.keys_mut()[key_offset..key_offset + size_of::<TKey>()]
            .copy_from_slice(bytes_of(key));
        self.data.values_mut()[value_offset..value_offset + size_of::<PageIndex>()]
            .copy_from_slice(bytes_of(&value));

        self.key_count += 1;
    }

    fn move_keys(&mut self, start_index: usize, offset: isize) {
        let start_offset = start_index * size_of::<TKey>();
        let end_offset = self.key_count() * size_of::<TKey>();

        let keys_to_move = self.data.keys()[start_offset..end_offset].to_vec();

        let target_end_offset = end_offset.strict_add_signed(offset);
        self.data.keys_mut()[start_offset.strict_add_signed(offset)..target_end_offset]
            .copy_from_slice(&keys_to_move);
    }

    fn move_values(&mut self, start_index: usize, offset: isize) {
        let start_offset = size_of::<PageIndex>() * start_index;
        let end_offset = size_of::<PageIndex>() * (self.key_count() + 1);

        let values_to_move = self.data.values()[start_offset..end_offset].to_vec();

        let target_start_offset = start_offset.strict_add_signed(offset);

        self.data.values_mut()[target_start_offset..end_offset.strict_add_signed(offset)]
            .copy_from_slice(&values_to_move);
    }

    pub fn value_at(&self, index: usize) -> Option<PageIndex> {
        if index > self.key_count() {
            return None;
        }

        let value_start = index * size_of::<PageIndex>();

        let value: PageIndex = pod_read_unaligned(
            &self.data.values()[value_start..value_start + size_of::<PageIndex>()],
        );

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
        2 * self.key_count() < InteriorNodeData::<TKey>::KEY_CAPACITY
    }

    pub fn can_fit_merge(&self, right: &Self) -> bool {
        self.key_count() + right.key_count() <= InteriorNodeData::<TKey>::KEY_CAPACITY
            && (self.key_count() + 1) + (right.key_count() + 1)
                <= InteriorNodeData::<TKey>::VALUES_CAPACITY
    }

    pub fn key_at(&self, index: usize) -> Option<&TKey> {
        if index >= self.key_count() {
            return None;
        }

        // TODO is the alignment here guaranteed? could this cause trouble with funny-sized keys?
        Some(from_bytes(
            &self.data.keys()[(index) * size_of::<TKey>()..(index + 1) * size_of::<TKey>()],
        ))
    }
}
