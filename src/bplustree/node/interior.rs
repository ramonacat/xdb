use crate::bplustree::node::NodeFlags;
use std::marker::PhantomData;

use bytemuck::{Pod, Zeroable, bytes_of, checked::pod_read_unaligned, from_bytes};

use crate::{
    bplustree::{
        InteriorNodeId, NodeId,
        node::{AnyNodeId, NODE_DATA_SIZE, NodeHeader, NodeReader, NodeTrait, NodeWriter},
    },
    storage::PageIndex,
};

#[derive(Debug, Zeroable, Clone, Copy)]
#[repr(C, align(8))]
pub(in crate::bplustree) struct InteriorNode<TKey> where TKey:Pod {
    header: NodeHeader,
    data: [u8; NODE_DATA_SIZE],
    _key: PhantomData<TKey>
}
impl<TKey: Pod> InteriorNode<TKey> {
    pub fn new() -> Self {
        Self { 
            header: NodeHeader {
                key_len: 0,
                flags: NodeFlags::INTERNAL,
                _unused2: 0,
                parent: PageIndex::zeroed(),
            },
            data: [0; _],
            _key: PhantomData
        }
    }

    fn set_parent(&mut self, parent: Option<InteriorNodeId>) {
        self.header.parent = parent.map_or_else(PageIndex::zeroed, |x| x.page());
    }
}

// SAFETY: this is sound, because the struct has no padding and would be able to derive Pod
// automatically if not for the PhantomData
unsafe impl<TKey: Pod> Pod for InteriorNode<TKey> {}

impl<TKey: Pod> NodeTrait<TKey> for InteriorNode<TKey> {
    fn parent(&self) -> Option<InteriorNodeId> {
        self.header.parent()
    }

    fn set_parent(&mut self, parent: Option<InteriorNodeId>) {
        self.header.set_parent(parent);
    }
}

struct InteriorNodeKeysIterator<'node, TKey: Pod> {
    node: &'node InteriorNode<TKey>,
    index: usize,
}

impl<'node, TKey: Pod> Iterator for InteriorNodeKeysIterator<'node, TKey> {
    type Item = &'node TKey;

    fn next(&mut self) -> Option<Self::Item> {
        let reader: InteriorNodeReader<'_, TKey> = InteriorNodeReader::new(self.node);

        if self.index >= reader.key_len() {
            return None;
        }

        self.index += 1;

        let key_bytes =
            &reader.node.data[(self.index - 1) * size_of::<TKey>()..self.index * size_of::<TKey>()];

        Some(from_bytes(key_bytes))
    }
}

#[derive(Debug)]
pub(in crate::bplustree) struct InteriorNodeReader<'node, TKey: Pod> {
    node: &'node InteriorNode<TKey>,
}

impl<'node, TKey: Pod> NodeReader<'node, InteriorNode<TKey>, TKey> for InteriorNodeReader<'node, TKey> {
    fn new(node: &'node InteriorNode<TKey>) -> Self {
        Self {
            node,
        }
    }
}

impl<'node, TKey: Pod> InteriorNodeReader<'node, TKey> {
    pub(in crate::bplustree) fn new(node: &'node InteriorNode<TKey>) -> Self {
        Self {
            node,
        }
    }

    pub(in crate::bplustree) fn keys(&self) -> impl Iterator<Item = &'node TKey> {
        InteriorNodeKeysIterator {
            node: self.node,
            index: 0,
        }
    }

    fn key_len(&self) -> usize {
        self.node.header.key_len as usize
    }

    fn key_capacity(&self) -> usize {
        // n - max number of keys
        //
        // size = key_size*n + value_size*(n+1)
        // size = key_size*n + value_size*n + value_size
        // size - value_size = key_size*n + value_size*n
        // (size - value_size)/(key_size + value_size) = n

        (self.node.data.len() - size_of::<PageIndex>())
            / (size_of::<TKey>() + size_of::<PageIndex>())
    }

    // TODO make this private
    pub(in crate::bplustree) fn values_offset(&self) -> usize {
        self.key_capacity() * size_of::<TKey>()
    }

    pub(in crate::bplustree) fn value_at(&self, index: usize) -> Option<AnyNodeId> {
        if index > self.key_len() {
            return None;
        }

        let value_start = self.values_offset() + (index * size_of::<PageIndex>());

        let value: PageIndex =
            pod_read_unaligned(&self.node.data[value_start..value_start + size_of::<PageIndex>()]);

        assert!(value != PageIndex::zeroed());

        Some(AnyNodeId::new(value))
    }

    pub(crate) fn first_value(&self) -> Option<AnyNodeId> {
        self.value_at(0)
    }

    pub(crate) fn last_value(&self) -> Option<AnyNodeId> {
        self.value_at(self.key_len())
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = AnyNodeId> {
        (0..(self.key_len() + 1)).map(|x| self.value_at(x).unwrap())
    }

    pub(crate) fn parent(&self) -> Option<InteriorNodeId> {
        self.node.parent()
    }

    pub(crate) fn first_key(&self) -> Option<TKey> {
        if self.key_len() == 0 {
            return None;
        }

        Some(pod_read_unaligned(&self.node.data[..size_of::<TKey>()]))
    }
}

#[must_use]
pub(in crate::bplustree) enum InteriorInsertResult<TKey: Pod> {
    Ok,
    Split(Box<InteriorNode<TKey>>),
}

pub(in crate::bplustree) struct InteriorNodeWriter<'node, TKey: Pod> {
    node: &'node mut InteriorNode<TKey>,
}

impl<'node, TKey: Pod> NodeWriter<'node, InteriorNode<TKey>, TKey> for InteriorNodeWriter<'node, TKey> {
    fn new(node: &'node mut InteriorNode<TKey>) -> Self {
        Self {
            node,
        }
    }
}

impl<'node, TKey: Pod + PartialOrd> InteriorNodeWriter<'node, TKey> {
    pub(in crate::bplustree) fn new(node: &'node mut InteriorNode<TKey>) -> Self {
        Self {
            node,
        }
    }

    pub fn create_root(keys: &[&TKey], values: &[AnyNodeId]) -> InteriorNode<TKey> {
        assert!(values.len() == keys.len() + 1);

        if values.len() != 2 || keys.len() != 1 {
            todo!();
        }

        let mut node = InteriorNode::new();

        let mut writer: InteriorNodeWriter<'_, TKey> = InteriorNodeWriter::new(&mut node);

        writer.set_first_pointer(values[0]);
        match writer.insert_node(keys[0], values[1]) {
            InteriorInsertResult::Ok => {}
            InteriorInsertResult::Split(_) => todo!(),
        }

        node
    }

    pub(in crate::bplustree) fn reader(&'node self) -> InteriorNodeReader<'node, TKey> {
        InteriorNodeReader::new(self.node)
    }

    pub(crate) fn set_first_pointer(&mut self, index: AnyNodeId) {
        let offset = self.reader().values_offset();

        self.node.data[offset..offset + size_of::<PageIndex>()]
            .copy_from_slice(bytes_of(&index.page()));
    }

    // TODO should this whole struct be also generic over TValue? (it will have to do some magic
    // around node IDs to type them right though)
    // TODO create a struct for the return type
    fn split(&mut self) -> (Vec<u8>, Vec<u8>, TKey) {
        let key_len = self.reader().key_len();
        assert!(
            key_len > 1,
            "A node must have more than one key to be split."
        );

        let keys_to_leave = key_len.div_ceil(2);
        let keys_to_move = key_len - keys_to_leave;

        let values_to_leave = keys_to_leave + 1;
        let values_to_move = keys_to_move;

        let key_data_to_move_start = keys_to_leave * size_of::<TKey>();
        let value_data_to_move_start =
            self.reader().values_offset() + values_to_leave * size_of::<PageIndex>();

        let key_data_to_move = self.node.data
            [key_data_to_move_start..key_data_to_move_start + keys_to_move * size_of::<TKey>()]
            .to_vec();
        let value_data_to_move = self.node.data[value_data_to_move_start
            ..value_data_to_move_start + values_to_move * size_of::<PageIndex>()]
            .to_vec();

        let first_key = pod_read_unaligned(&key_data_to_move[..size_of::<TKey>()]);

        (key_data_to_move, value_data_to_move, first_key)
    }

    pub(crate) fn insert_node(&mut self, key: &TKey, value: AnyNodeId) -> InteriorInsertResult<TKey> {
        let mut insert_at = self.reader().key_len();

        let key_len = self.reader().key_len();

        if key_len + 1 == self.reader().key_capacity() {
            let (new_node_keys, new_node_values, split_key) = self.split();
            let mut new_node = InteriorNode::new();

            if key < &split_key {
                self.insert_at(insert_at, key, value);
            } else {
                new_node.data[..new_node_keys.len()].copy_from_slice(&new_node_keys);

                let values_offset = InteriorNodeReader::<'_, TKey>::new(&new_node).values_offset()
                    + size_of::<PageIndex>();
                new_node.data[values_offset..values_offset + new_node_values.len()]
                    .copy_from_slice(&new_node_values);

                new_node.header.key_len = (new_node_keys.len() / size_of::<TKey>()) as u16;

                match InteriorNodeWriter::new(&mut new_node).insert_node(key, value) {
                    InteriorInsertResult::Ok => {}
                    InteriorInsertResult::Split(_) => todo!(),
                }
            }

            return InteriorInsertResult::Split(Box::new(new_node));
        }

        for (index, current_key) in self.reader().keys().enumerate() {
            if current_key > key {
                insert_at = index;
                break;
            }
        }

        self.insert_at(insert_at, key, value);

        InteriorInsertResult::Ok
    }

    fn insert_at(&mut self, index: usize, key: &TKey, value: AnyNodeId) {
        let key_len = self.reader().key_len();
        assert!(key_len < self.reader().key_capacity());

        debug_assert!(bytes_of(key) != vec![0; size_of::<TKey>()]);

        self.node.header.key_len += 1;

        let key_offset = size_of::<TKey>() * (index);
        let value_offset = self.reader().values_offset() + size_of::<PageIndex>() * (index + 1);

        let keys_to_move = &self.node.data
            [key_offset..key_offset + size_of::<TKey>() * (key_len - index)]
            .to_vec();
        self.node.data[key_offset + size_of::<TKey>()
            ..key_offset + size_of::<TKey>() * (key_len - index + 1)]
            .copy_from_slice(keys_to_move);

        let values_to_move = &self.node.data
            [value_offset..value_offset + (key_len - index) * size_of::<PageIndex>()]
            .to_vec();
        self.node.data[value_offset + size_of::<PageIndex>()
            ..value_offset + (key_len - index + 1) * size_of::<PageIndex>()]
            .copy_from_slice(values_to_move);

        self.node.data[key_offset..key_offset + size_of::<TKey>()].copy_from_slice(bytes_of(key));
        self.node.data[value_offset..value_offset + size_of::<PageIndex>()]
            .copy_from_slice(bytes_of(&value.page()));
    }

    pub(crate) fn set_parent_id(&mut self, parent: Option<InteriorNodeId>) {
        self.node
            .set_parent(parent)
    }
}
