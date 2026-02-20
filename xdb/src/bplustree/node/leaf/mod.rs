pub(in crate::bplustree) mod builder;
mod entries;

use std::fmt::Debug;

use bytemuck::{Pod, Zeroable};

use crate::{
    Size,
    bplustree::{
        LeafNodeId, NodeId, TreeKey,
        node::{
            InteriorNodeId, Node, NodeHeader,
            leaf::{
                builder::{LeafNodeBuilder, MaterializedData, MaterializedTopology, Topology},
                entries::{LeafNodeEntries, LeafNodeEntry},
            },
        },
    },
    storage::{SENTINEL_PAGE_ID, SerializedPageId, page::PAGE_DATA_SIZE},
};

// TODO magic numbers depending on size of PageId!
const LEAF_NODE_DATA_SIZE: Size = PAGE_DATA_SIZE.subtract(
    Size::of::<u64>()
        .multiply(2)
        .add(Size::of::<u64>().multiply(2)),
);

#[derive(Debug, Zeroable, Clone, Copy)]
#[repr(C, align(8))]
pub(in crate::bplustree) struct LeafNode<TKey>
where
    TKey: TreeKey,
{
    header: NodeHeader,
    leaf_header: LeafNodeHeader,
    data: LeafNodeEntries<TKey>,
}

// SAFETY: this is sound, because the struct has no padding and would be able to derive Pod
// automatically if not for the PhantomData
unsafe impl<TKey: TreeKey> Pod for LeafNode<TKey> {}

impl<TKey: TreeKey> LeafNode<TKey> {
    pub fn new(parent: Option<InteriorNodeId>) -> Self {
        Self {
            header: NodeHeader::new_leaf(parent.map_or(SENTINEL_PAGE_ID, |x| x.page())),
            leaf_header: LeafNodeHeader {
                previous: SENTINEL_PAGE_ID,
                next: SENTINEL_PAGE_ID,
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

        if previous == SENTINEL_PAGE_ID {
            None
        } else {
            Some(LeafNodeId::new(previous))
        }
    }

    pub fn next(&self) -> Option<LeafNodeId> {
        let next = self.leaf_header.next;

        if next == SENTINEL_PAGE_ID {
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

        // TODO the caller should call needs_merge themselves
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

    pub fn insert(&mut self, key: TKey, value: &[u8]) -> Option<Vec<u8>> {
        let mut insert_index = self.data.len();

        let mut delete_index = None;

        for (index, entry) in self.entries().enumerate() {
            if key == entry.key() {
                delete_index = Some(index);

                insert_index = index;
                break;
            }

            if key < entry.key() {
                insert_index = index;
                break;
            }
        }

        let deleted_entry = delete_index.and_then(|x| self.data.entry(x));
        let result = deleted_entry.as_ref().map(|x| x.value().to_vec());

        let size_increase = value
            .len()
            .saturating_sub(deleted_entry.map_or(0, |x| x.value_size()));

        assert!(
            self.data.can_fit(size_increase),
            "not enough capacity for the value, split node before inserting: {:?}",
            tracing::Span::current()
        );

        if let Some(delete_index) = delete_index {
            self.data.delete_at(delete_index);
        }

        self.data.insert_at(insert_index, key, value);

        result
    }

    pub fn split(
        &'_ mut self,
        new_topology: &MaterializedTopology,
    ) -> LeafNodeBuilder<TKey, (), MaterializedData<'_, TKey>> {
        self.set_parent(new_topology.parent());
        self.set_previous(new_topology.previous());
        self.set_next(new_topology.next());

        let new_node_entries = self.data.split();

        LeafNodeBuilder::new().with_data(new_node_entries)
    }

    pub fn set_previous(&mut self, previous: Option<LeafNodeId>) {
        self.leaf_header.previous = previous.map_or(SENTINEL_PAGE_ID, |x| x.page());
    }

    pub fn set_next(&mut self, next: Option<LeafNodeId>) {
        self.leaf_header.next = next.map_or(SENTINEL_PAGE_ID, |x| x.page());
    }

    pub(in crate::bplustree) fn first_key(&self) -> Option<TKey> {
        self.entry(0).map(|x| x.key())
    }

    pub(crate) fn can_fit(&self, value_size: usize) -> bool {
        self.data.can_fit(value_size)
    }

    pub(crate) const fn len(&self) -> usize {
        self.data.len()
    }

    pub(crate) fn merge_from(&mut self, right: &Self) {
        for entry in right.entries() {
            self.insert(entry.key(), entry.value());
        }

        self.set_next(right.next());
    }

    pub(crate) fn can_fit_merge(&self, right: &Self) -> bool {
        self.data.can_fit_merge(right.data)
    }
}

impl<TKey: TreeKey> Node<TKey> for LeafNode<TKey> {
    fn parent(&self) -> Option<InteriorNodeId> {
        if self.header.parent == SENTINEL_PAGE_ID {
            None
        } else {
            Some(InteriorNodeId::new(self.header.parent))
        }
    }

    fn set_parent(&mut self, parent: Option<InteriorNodeId>) {
        self.header.set_parent(parent);
    }
}

#[derive(Zeroable, Debug, Clone, Copy)]
#[repr(C, align(8))]
struct LeafNodeHeader {
    previous: SerializedPageId,
    next: SerializedPageId,
}

unsafe impl Pod for LeafNodeHeader {}

const _: () = assert!(Size::of::<LeafNodeHeader>().is_equal(Size::of::<u64>().multiply(2)));

#[cfg(test)]
mod test {
    use super::*;

    fn collect_entries<TKey: TreeKey>(node: &LeafNode<TKey>) -> Vec<(TKey, Vec<u8>)> {
        node.entries()
            .map(|x| (x.key(), x.value().to_vec()))
            .collect::<Vec<_>>()
    }

    #[test]
    fn insert_reverse() {
        let mut node = LeafNode::new(None);
        let _ = node.insert(1, &[0]);
        let _ = node.insert(0, &[0]);

        assert_eq!(collect_entries(&node), &[(0, vec![0]), (1, vec![0])]);
    }

    #[test]
    fn same_key_overrides() {
        let mut node = LeafNode::new(None);
        let _ = node.insert(0, &[0]);
        let _ = node.insert(0, &[1]);

        assert_eq!(collect_entries(&node), &[(0, vec![1])]);
    }

    #[test]
    fn same_key_same_overrides_with_intermediate() {
        let mut node = LeafNode::new(None);
        let _ = node.insert(1, &[0]);
        let _ = node.insert(2, &[0]);
        let _ = node.insert(1, &[0]);

        assert_eq!(collect_entries(&node), &[(1, vec![0]), (2, vec![0])]);
    }
}
