use crate::bplustree::{TreeKey, node::NodeFlags};
use std::marker::PhantomData;

use bytemuck::{AnyBitPattern, NoUninit, bytes_of, checked::pod_read_unaligned, from_bytes};

use crate::{
    bplustree::{
        InteriorNodeId, NodeId,
        node::{AnyNodeId, NODE_DATA_SIZE, Node, NodeHeader},
    },
    storage::PageIndex,
};

impl From<Option<InteriorNodeId>> for PageIndex {
    fn from(value: Option<InteriorNodeId>) -> Self {
        value.map_or_else(PageIndex::zero, |x| x.0)
    }
}

#[derive(Debug, AnyBitPattern, Clone, Copy)]
#[repr(C, align(8))]
pub(in crate::bplustree) struct InteriorNode<TKey>
where
    TKey: TreeKey,
{
    header: NodeHeader,

    key_count: u16,
    _unused1: u16,
    _unused2: u32,

    data: [u8; NODE_DATA_SIZE - size_of::<u64>()],
    _key: PhantomData<TKey>,
}

// SAFETY: this is sound, because the struct has no padding and would be able to derive Pod
// automatically if not for the PhantomData
unsafe impl<TKey: TreeKey + 'static> NoUninit for InteriorNode<TKey> {}

impl<TKey: TreeKey> InteriorNode<TKey> {
    pub fn new() -> Self {
        Self {
            header: NodeHeader {
                flags: NodeFlags::INTERNAL,
                _unused1: 0,
                _unused2: 0,
                parent: PageIndex::zero(),
            },
            key_count: 0,
            _unused1: 0,
            _unused2: 0,
            data: [0; _],
            _key: PhantomData,
        }
    }

    pub(in crate::bplustree) fn set_parent(&mut self, parent: Option<InteriorNodeId>) {
        self.header.parent = parent.map_or_else(PageIndex::zero, |x| x.page());
    }

    fn key_count(&self) -> usize {
        self.key_count as usize
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

    pub(in crate::bplustree) fn keys(&self) -> impl Iterator<Item = &TKey> {
        InteriorNodeKeysIterator {
            node: self,
            index: 0,
        }
    }

    pub(crate) fn set_first_pointer(&mut self, index: AnyNodeId) {
        let offset = self.values_offset();

        self.data[offset..offset + size_of::<PageIndex>()].copy_from_slice(bytes_of(&index.page()));
    }

    pub fn has_spare_capacity(&self) -> bool {
        self.key_count() + 1 < self.key_capacity()
    }

    pub(crate) fn insert_node(&mut self, key: &TKey, value: AnyNodeId) {
        let mut insert_at = self.key_count();

        if !self.has_spare_capacity() {
            panic!("no capacity for insert, split the node first");
        }

        for (index, current_key) in self.keys().enumerate() {
            if key < current_key {
                insert_at = index;
                break;
            }
        }

        self.insert_at(insert_at, key, value);
    }

    pub fn split(&mut self) -> (TKey, InteriorNode<TKey>) {
        let key_len = self.key_count();
        assert!(
            key_len > 1,
            "A node must have more than one key to be split."
        );

        let keys_to_leave = key_len.div_ceil(2);
        let keys_to_move = key_len - keys_to_leave - 1;

        let values_to_leave = keys_to_leave + 1;
        let values_to_move = (key_len + 1) - values_to_leave;

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

        let mut split_node = InteriorNode::new();
        let split_node_values_offset = split_node.values_offset();

        // TODO The first key here is not set, as that child must be created, enforce this via the
        // type system!
        split_node.set_parent(self.parent());
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

    fn insert_at(&mut self, index: usize, key: &TKey, value: AnyNodeId) {
        let key_len = self.key_count();
        assert!(key_len < self.key_capacity());

        debug_assert!(bytes_of(key) != vec![0; size_of::<TKey>()]);

        let key_offset = size_of::<TKey>() * (index);
        let value_offset = self.values_offset() + size_of::<PageIndex>() * (index + 1);

        self.move_keys(index, size_of::<TKey>() as isize);
        self.move_values(index + 1, size_of::<PageIndex>() as isize);

        self.data[key_offset..key_offset + size_of::<TKey>()].copy_from_slice(bytes_of(key));
        self.data[value_offset..value_offset + size_of::<PageIndex>()]
            .copy_from_slice(bytes_of(&value.page()));

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

    fn move_values(&mut self, start_index: usize, offset: isize) {
        let start_offset = self.values_offset() + size_of::<PageIndex>() * start_index;
        let end_offset = self.values_offset() + size_of::<PageIndex>() * (self.key_count() + 1);

        let values_to_move = self.data[start_offset..end_offset].to_vec();

        let target_start_offset = start_offset.strict_add_signed(offset);

        assert!(target_start_offset >= self.values_offset());

        self.data[target_start_offset..end_offset.strict_add_signed(offset)]
            .copy_from_slice(&values_to_move);
    }

    pub(in crate::bplustree) fn value_at(&self, index: usize) -> Option<AnyNodeId> {
        if index > self.key_count() {
            return None;
        }

        let value_start = self.values_offset() + (index * size_of::<PageIndex>());

        let value: PageIndex =
            pod_read_unaligned(&self.data[value_start..value_start + size_of::<PageIndex>()]);

        assert!(value != PageIndex::zero());

        Some(AnyNodeId::new(value))
    }

    pub(crate) fn first_value(&self) -> Option<AnyNodeId> {
        self.value_at(0)
    }

    pub(crate) fn last_value(&self) -> Option<AnyNodeId> {
        self.value_at(self.key_count())
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = AnyNodeId> {
        (0..(self.key_count() + 1)).map(|x| self.value_at(x).unwrap())
    }

    fn delete_at(&mut self, index: usize) {
        assert!(index > 0); // TODO we should handle this situation as well

        if index < self.key_count() {
            self.move_keys(index, -(size_of::<TKey>() as isize));
            self.move_values(index + 1, -(size_of::<PageIndex>() as isize));
        }

        self.key_count -= 1;
    }

    pub(crate) fn delete(&mut self, child: AnyNodeId) {
        let mut delete_index = None;
        for (index, value) in self.values().enumerate() {
            if value == child {
                delete_index = Some(index);
                break;
            }
        }

        if let Some(delete_index) = delete_index {
            self.delete_at(delete_index);
        }
    }

    pub(crate) fn needs_merge(&self) -> bool {
        // TODO extract the raw byte operations into InteriorNodeData<TKey>
        2 * (self.key_count() * size_of::<TKey>() + (self.key_count() + 1) * size_of::<PageIndex>())
            < self.data.len()
    }

    pub(crate) fn find_value_index(&self, node_id: AnyNodeId) -> Option<usize> {
        for (index, value) in self.values().enumerate() {
            if value == node_id {
                return Some(index);
            }
        }

        None
    }

    pub(crate) fn can_fit_merge(&self, right: &InteriorNode<TKey>) -> bool {
        self.key_count() + right.key_count() > self.key_capacity()
    }

    pub(crate) fn merge_from(&self, right: &mut InteriorNode<TKey>) {
        assert!(self.can_fit_merge(right));
    }

    // TODO rename -> delete_value for consistency
    pub(crate) fn remove_value(&mut self, value: AnyNodeId) {
        let index = self.find_value_index(value).unwrap();

        self.delete_at(index);
    }
}

impl<TKey: TreeKey> Node<TKey> for InteriorNode<TKey> {
    fn parent(&self) -> Option<InteriorNodeId> {
        self.header.parent()
    }

    fn set_parent(&mut self, parent: Option<InteriorNodeId>) {
        self.header.set_parent(parent);
    }
}

struct InteriorNodeKeysIterator<'node, TKey: TreeKey> {
    node: &'node InteriorNode<TKey>,
    index: usize,
}

impl<'node, TKey: TreeKey> Iterator for InteriorNodeKeysIterator<'node, TKey> {
    type Item = &'node TKey;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.node.key_count() {
            return None;
        }

        self.index += 1;

        let key_bytes =
            &self.node.data[(self.index - 1) * size_of::<TKey>()..self.index * size_of::<TKey>()];

        Some(from_bytes(key_bytes))
    }
}
