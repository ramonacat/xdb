use bytemuck::{Zeroable as _, bytes_of, from_bytes};

use crate::{bplustree::Node, storage::PageIndex};

struct InteriorNodeKeysIterator<'node> {
    node: &'node Node,
    key_size: usize,
    index: usize,
}

impl<'node> Iterator for InteriorNodeKeysIterator<'node> {
    type Item = &'node [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let reader = InteriorNodeReader::new(self.node, self.key_size);

        if self.index >= reader.key_len() {
            return None;
        }

        self.index += 1;

        Some(&reader.node.data[self.index * self.key_size..(self.index + 1) * self.key_size])
    }
}

#[derive(Debug)]
pub(in crate::bplustree) struct InteriorNodeReader<'node> {
    node: &'node Node,
    key_size: usize,
}

impl<'node> InteriorNodeReader<'node> {
    pub(in crate::bplustree) fn new(node: &'node Node, key_size: usize) -> Self {
        Self { node, key_size }
    }

    pub(in crate::bplustree) fn keys(&self) -> impl Iterator<Item = &'node [u8]> {
        InteriorNodeKeysIterator {
            node: self.node,
            key_size: self.key_size,
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

        (self.node.data.len() - size_of::<PageIndex>()) / (self.key_size + size_of::<PageIndex>())
    }

    fn values_offset(&self) -> usize {
        self.key_capacity() * self.key_size
    }

    pub(in crate::bplustree) fn value_at(&self, index: usize) -> Option<PageIndex> {
        if index > self.key_len() {
            return None;
        }

        let value_start = self.values_offset() + (index * size_of::<PageIndex>());

        let value = *from_bytes(&self.node.data[value_start..value_start + size_of::<PageIndex>()]);

        assert!(value != PageIndex::zeroed());

        Some(value)
    }

    pub(crate) fn last_value(&self) -> PageIndex {
        self.value_at(self.key_len()).unwrap()
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = PageIndex> {
        (0..(self.key_len() + 1)).map(|x| self.value_at(x).unwrap())
    }
}

pub(in crate::bplustree) enum InteriorInsertResult {
    Ok,
    Split,
}

pub(in crate::bplustree) struct InteriorNodeWriter<'node> {
    node: &'node mut Node,
    key_size: usize,
}

impl<'node> InteriorNodeWriter<'node> {
    pub(in crate::bplustree) fn new(node: &'node mut Node, key_size: usize) -> Self {
        Self { node, key_size }
    }

    pub(in crate::bplustree) fn reader(&'node self) -> InteriorNodeReader<'node> {
        InteriorNodeReader::new(self.node, self.key_size)
    }

    pub(crate) fn set_first_pointer(&mut self, index: PageIndex) {
        let offset = self.reader().values_offset();

        self.node.data[offset..offset + size_of::<PageIndex>()].copy_from_slice(bytes_of(&index));
    }

    pub(crate) fn insert_node(&mut self, key: &[u8], value: PageIndex) -> InteriorInsertResult {
        assert!(value != PageIndex::zeroed());
        let mut insert_at = 0;

        for (index, current_key) in self.reader().keys().enumerate() {
            if current_key > key {
                insert_at = index;
                break;
            }
        }

        self.insert_at(insert_at, key, value)
    }

    fn insert_at(&mut self, index: usize, key: &[u8], value: PageIndex) -> InteriorInsertResult {
        let key_len = self.reader().key_len();

        if key_len + 1 == self.reader().key_capacity() {
            return InteriorInsertResult::Split;
        }

        if key_len < index {
            todo!();
        }

        self.node.header.key_len += 1;

        let key_offset = self.key_size * (index + 1);
        let value_offset = self.reader().values_offset() + size_of::<PageIndex>() * (index + 1);

        self.node.data[key_offset..key_offset + self.key_size].copy_from_slice(key);
        self.node.data[value_offset..value_offset + size_of::<PageIndex>()]
            .copy_from_slice(bytes_of(&value));

        InteriorInsertResult::Ok
    }

    // TODO do we really need it
    pub(crate) fn replace_with(self, new_node: Node) {
        *self.node = new_node;
    }

    pub(crate) fn set_parent(&mut self, new_parent: PageIndex) {
        self.node.set_parent(new_parent);
    }
}
