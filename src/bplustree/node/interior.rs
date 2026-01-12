use std::marker::PhantomData;

use bytemuck::{Pod, Zeroable as _, bytes_of, from_bytes};

use crate::{
    bplustree::{
        Node, NodeId,
        node::{AnyNodeId, NodeReader, NodeWriter},
    },
    storage::PageIndex,
};

struct InteriorNodeKeysIterator<'node, TKey> {
    node: &'node Node,
    index: usize,
    _key: PhantomData<&'node TKey>,
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
pub(in crate::bplustree) struct InteriorNodeReader<'node, TKey> {
    node: &'node Node,
    _key: PhantomData<&'node TKey>,
}

impl<'node, TKey> NodeReader<'node, TKey> for InteriorNodeReader<'node, TKey> {
    fn new(node: &'node Node, _value_size: usize) -> Self {
        Self {
            node,
            _key: PhantomData,
        }
    }
}

impl<'node, TKey: Pod> InteriorNodeReader<'node, TKey> {
    pub(in crate::bplustree) fn new(node: &'node Node) -> Self {
        Self {
            node,
            _key: PhantomData,
        }
    }

    pub(in crate::bplustree) fn keys(&self) -> impl Iterator<Item = &'node TKey> {
        InteriorNodeKeysIterator {
            node: self.node,
            index: 0,
            _key: PhantomData::<&'node TKey>,
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

    fn values_offset(&self) -> usize {
        self.key_capacity() * size_of::<TKey>()
    }

    pub(in crate::bplustree) fn value_at(&self, index: usize) -> Option<AnyNodeId> {
        if index > self.key_len() {
            return None;
        }

        let value_start = self.values_offset() + (index * size_of::<PageIndex>());

        let value: PageIndex =
            *from_bytes(&self.node.data[value_start..value_start + size_of::<PageIndex>()]);

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
}

#[must_use]
pub(in crate::bplustree) enum InteriorInsertResult {
    Ok,
    Split,
}

pub(in crate::bplustree) struct InteriorNodeWriter<'node, TKey> {
    node: &'node mut Node,
    _key: PhantomData<&'node TKey>,
}

impl<'node, TKey> NodeWriter<'node, TKey> for InteriorNodeWriter<'node, TKey> {
    fn new(node: &'node mut Node, _value_size: usize) -> Self {
        Self {
            node,
            _key: PhantomData,
        }
    }
}

impl<'node, TKey: Pod + PartialOrd> InteriorNodeWriter<'node, TKey> {
    pub(in crate::bplustree) fn new(node: &'node mut Node) -> Self {
        Self {
            node,
            _key: PhantomData,
        }
    }

    pub fn create_root(keys: &[&TKey], values: &[AnyNodeId]) -> Node {
        assert!(values.len() == keys.len() + 1);

        if values.len() != 2 || keys.len() != 1 {
            todo!();
        }

        let mut node = Node::new_interior();

        let mut writer: InteriorNodeWriter<'_, TKey> = InteriorNodeWriter::new(&mut node);

        writer.set_first_pointer(values[0]);
        match writer.insert_node(keys[0], values[1]) {
            InteriorInsertResult::Ok => {}
            InteriorInsertResult::Split => todo!(),
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

    pub(crate) fn insert_node(&mut self, key: &TKey, value: AnyNodeId) -> InteriorInsertResult {
        let mut insert_at = self.reader().key_len();

        for (index, current_key) in self.reader().keys().enumerate() {
            if current_key > key {
                insert_at = index;
                break;
            }
        }

        self.insert_at(insert_at, key, value)
    }

    fn insert_at(&mut self, index: usize, key: &TKey, value: AnyNodeId) -> InteriorInsertResult {
        let key_len = self.reader().key_len();

        if key_len + 1 == self.reader().key_capacity() {
            return InteriorInsertResult::Split;
        }

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

        InteriorInsertResult::Ok
    }
}
