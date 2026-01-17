pub(in crate::bplustree) mod builder;
mod entries;

use std::fmt::Debug;

use bytemuck::{Pod, Zeroable};

use crate::{
    bplustree::{
        LeafNodeId, NodeId, TreeError,
        node::{
            InteriorNodeId, NODE_DATA_SIZE, Node, NodeFlags, NodeHeader,
            leaf::{
                builder::{LeafNodeBuilder, MaterializedData},
                entries::{LeafNodeEntries, LeafNodeEntry},
            },
        },
    },
    storage::PageIndex,
};

impl From<Option<LeafNodeId>> for PageIndex {
    fn from(value: Option<LeafNodeId>) -> Self {
        value.map_or_else(PageIndex::zero, |x| x.0)
    }
}

const LEAF_NODE_DATA_SIZE: usize = NODE_DATA_SIZE - size_of::<LeafNodeHeader>();

#[derive(Debug, Zeroable, Clone, Copy)]
#[repr(C, align(8))]
pub(in crate::bplustree) struct LeafNode<TKey>
where
    TKey: Pod,
{
    header: NodeHeader,
    leaf_header: LeafNodeHeader,
    data: LeafNodeEntries<TKey>,
}

// SAFETY: this is sound, because the struct has no padding and would be able to derive Pod
// automatically if not for the PhantomData
unsafe impl<TKey: Pod> Pod for LeafNode<TKey> {}

impl<TKey: Pod + Ord + Debug> LeafNode<TKey> {
    pub fn new() -> Self {
        Self {
            header: NodeHeader {
                flags: NodeFlags::empty(),
                _unused1: 0,
                _unused2: 0,
                parent: PageIndex::zero(),
            },
            leaf_header: LeafNodeHeader {
                previous: PageIndex::zero(),
                next: PageIndex::zero(),
            },
            data: LeafNodeEntries::new(),
        }
    }

    pub fn entries(&'_ self) -> impl Iterator<Item = LeafNodeEntry<'_, TKey>> {
        self.data.entries()
    }

    pub fn entry(&'_ self, index: usize) -> Option<LeafNodeEntry<'_, TKey>> {
        self.data.entry(index)
    }

    pub fn previous(&self) -> Option<LeafNodeId> {
        let previous = self.leaf_header.previous;

        if previous == PageIndex::zero() {
            None
        } else {
            Some(LeafNodeId::new(previous))
        }
    }

    pub fn next(&self) -> Option<LeafNodeId> {
        let next = self.leaf_header.next;

        if next == PageIndex::zero() {
            None
        } else {
            Some(LeafNodeId::new(next))
        }
    }

    pub fn delete(&mut self, key: TKey) -> Option<(Vec<u8>, bool)> {
        let index = self.find(key)?;

        let entry = self.entry(index).unwrap();
        let result = entry.value().to_vec();

        self.data.delete_at(index);

        Some((result, self.data.needs_merge()))
    }

    pub fn find(&self, key: TKey) -> Option<usize> {
        for (index, entry) in self.entries().enumerate() {
            if entry.key() == key {
                return Some(index);
            }
        }

        None
    }

    pub fn insert(&mut self, key: TKey, value: &[u8]) -> Result<(), TreeError> {
        let mut insert_index = self.data.len();

        let mut delete_index = None;

        for (index, entry) in self.entries().enumerate() {
            if key == entry.key() {
                // TODO return a different result type for replaced?
                delete_index = Some(index);

                insert_index = index;
                break;
            }

            if key < entry.key() {
                insert_index = index;
                break;
            }
        }

        let size_increase = value.len().saturating_sub(
            delete_index
                .and_then(|x| self.data.entry(x))
                .map(|x| x.value_size())
                .unwrap_or(0),
        );

        assert!(
            self.data.can_fit(size_increase),
            "not enough capacity for the value, split node before inserting"
        );

        if let Some(delete_index) = delete_index {
            self.data.delete_at(delete_index);
        }

        self.data.insert_at(insert_index, key, value)?;

        Ok(())
    }

    pub fn split(&'_ mut self) -> LeafNodeBuilder<TKey, (), MaterializedData<'_, TKey>> {
        let initial_len = self.data.len();
        assert!(initial_len > 0, "Trying to split an empty node");

        // TODO this should be based on size and not indices to keep balance in case of unbalanced
        // values
        let entries_to_leave = initial_len.div_ceil(2);
        let entries_to_move = initial_len - entries_to_leave;

        let new_node_entries = self.data.split_at(entries_to_leave);

        LeafNodeBuilder::new().with_data(MaterializedData::new(entries_to_move, new_node_entries))
    }

    pub fn set_links(
        &mut self,
        parent: Option<InteriorNodeId>,
        previous: Option<LeafNodeId>,
        next: Option<LeafNodeId>,
    ) {
        self.set_parent(parent);
        self.leaf_header.previous = previous.map_or(PageIndex::zero(), |x| x.page());
        self.leaf_header.next = next.map_or(PageIndex::zero(), |x| x.page())
    }

    pub(in crate::bplustree) fn first_key(&self) -> Option<TKey> {
        self.entry(0).map(|x| x.key())
    }

    pub(crate) fn can_fit(&self, value_size: usize) -> bool {
        self.data.can_fit(value_size)
    }

    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }

    pub(crate) fn merge_from(&mut self, right: &mut LeafNode<TKey>) {
        // TODO we can optimize this merge as it can be a straight data copy, as the entries on the
        // right are already sorted and greater than ours
        for entry in right.entries() {
            self.insert(entry.key(), entry.value()).unwrap();
        }
        self.set_links(self.parent(), self.previous(), right.next());
    }

    pub(crate) fn can_fit_merge(&self, right: &LeafNode<TKey>) -> bool {
        self.data.can_fit_merge(right.data)
    }
}

impl<TKey: Pod> Node<TKey> for LeafNode<TKey> {
    fn parent(&self) -> Option<InteriorNodeId> {
        if self.header.parent == PageIndex::zero() {
            None
        } else {
            Some(InteriorNodeId::new(self.header.parent))
        }
    }

    fn set_parent(&mut self, parent: Option<InteriorNodeId>) {
        self.header.set_parent(parent);
    }
}

#[derive(Zeroable, Pod, Debug, Clone, Copy)]
#[repr(C, align(8))]
struct LeafNodeHeader {
    previous: PageIndex,
    next: PageIndex,
}

const _: () = assert!(size_of::<LeafNodeHeader>() == size_of::<u64>() * 2);

#[cfg(test)]
mod test {
    use super::*;

    fn collect_entries<TKey: Pod + Ord + Debug>(node: &LeafNode<TKey>) -> Vec<(TKey, Vec<u8>)> {
        node.entries()
            .map(|x| (x.key(), x.value().to_vec()))
            .collect::<Vec<_>>()
    }

    #[test]
    fn insert_reverse() {
        let mut node = LeafNode::new();
        let _ = node.insert(1, &[0]).unwrap();
        let _ = node.insert(0, &[0]).unwrap();

        assert_eq!(collect_entries(&node), &[(0, vec![0]), (1, vec![0])]);
    }

    #[test]
    fn same_key_overrides() {
        let mut node = LeafNode::new();
        let _ = node.insert(0, &[0]);
        let _ = node.insert(0, &[1]);

        assert_eq!(collect_entries(&node), &[(0, vec![1])]);
    }

    #[test]
    fn same_key_same_overrides_with_intermediate() {
        let mut node = LeafNode::new();
        let _ = node.insert(1, &[0]);
        let _ = node.insert(2, &[0]);
        let _ = node.insert(1, &[0]);

        assert_eq!(collect_entries(&node), &[(1, vec![0]), (2, vec![0])]);
    }
}
