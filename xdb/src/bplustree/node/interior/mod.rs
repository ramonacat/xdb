use std::fmt::Write;
mod entries;

use crate::{
    bplustree::{
        TreeKey,
        node::interior::entries::{InteriorNodeEntries, KeyIndex, ValueIndex},
    },
    storage::SENTINEL_PAGE_ID,
};

use bytemuck::{AnyBitPattern, NoUninit};

use crate::bplustree::{
    InteriorNodeId, NodeId,
    node::{AnyNodeId, Node, NodeHeader},
};

#[derive(Debug, AnyBitPattern, Clone, Copy)]
#[repr(C, align(8))]
pub(in crate::bplustree) struct InteriorNode<TKey>
where
    TKey: TreeKey,
{
    header: NodeHeader,
    entries: InteriorNodeEntries<TKey>,
}

unsafe impl<TKey: TreeKey + 'static> NoUninit for InteriorNode<TKey> {}

impl<TKey: TreeKey> InteriorNode<TKey> {
    pub fn new(
        parent: Option<InteriorNodeId>,
        left: AnyNodeId,
        key: TKey,
        right: AnyNodeId,
    ) -> Self {
        Self {
            header: NodeHeader::new_interior(parent.map_or(SENTINEL_PAGE_ID, |x| x.page())),
            entries: InteriorNodeEntries::new(left.page(), key, right.page()),
        }
    }

    // TODO construct the entries in pre-allocated memory?
    #[allow(clippy::large_types_passed_by_value)]
    fn from_entries(parent: Option<InteriorNodeId>, entries: InteriorNodeEntries<TKey>) -> Self {
        Self {
            header: NodeHeader::new_interior(parent.map_or(SENTINEL_PAGE_ID, |x| x.page())),
            entries,
        }
    }

    pub(in crate::bplustree) fn set_parent(&mut self, parent: Option<InteriorNodeId>) {
        // TODO implement From<> InteriorNodeId/AnyNodeId/LeafNodeId for SerializedPageId
        self.header.parent = parent.map_or(SENTINEL_PAGE_ID, |x| x.page());
    }

    pub(in crate::bplustree) fn keys(&self) -> impl Iterator<Item = (KeyIndex, TKey)> {
        InteriorNodeKeysIterator {
            node: self,
            index: 0,
        }
    }

    pub fn has_spare_capacity(&self) -> bool {
        self.entries.has_spare_capacity()
    }

    pub(crate) fn insert_node(&mut self, key: TKey, value: AnyNodeId) {
        let mut insert_at = self.entries.key_after_last();

        assert!(
            self.has_spare_capacity(),
            "no capacity for insert, split the node first: {:?}",
            tracing::Span::current()
        );

        for (index, current_key) in self.keys() {
            if key < current_key {
                insert_at = index;
                break;
            }
        }

        self.entries.insert_at(insert_at, key, value.page());
    }

    pub fn split(&mut self) -> (TKey, Self) {
        let (split_key, new_node_entries) = self.entries.split();

        (
            split_key,
            Self::from_entries(self.parent(), new_node_entries),
        )
    }

    pub(in crate::bplustree) fn value_at(&self, index: ValueIndex) -> Option<AnyNodeId> {
        self.entries.value_at(index).map(AnyNodeId::new)
    }

    pub(crate) fn first_value(&self) -> Option<AnyNodeId> {
        self.value_at(ValueIndex::new(0))
    }

    pub(crate) fn last_value(&self) -> Option<AnyNodeId> {
        self.value_at(self.entries.last_value())
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = (ValueIndex, AnyNodeId)> {
        (0..=self.entries.key_count())
            .map(ValueIndex::new)
            .map(|x| (x, self.value_at(x).unwrap()))
    }

    pub(crate) fn delete(&mut self, child: AnyNodeId) {
        let mut delete_index = None;
        for (index, value) in self.values() {
            if value == child {
                delete_index = Some(index);
                break;
            }
        }

        if let Some(delete_index) = delete_index {
            self.entries.delete_at(delete_index);
        }
    }

    pub(crate) fn needs_merge(&self) -> bool {
        self.entries.needs_merge()
    }

    pub(crate) fn find_value_index(&self, node_id: AnyNodeId) -> Option<ValueIndex> {
        for (index, value) in self.values() {
            if value == node_id {
                return Some(index);
            }
        }

        None
    }

    pub(crate) fn can_fit_merge(&self, right: &Self) -> bool {
        self.entries.can_fit_merge(&right.entries)
    }

    pub(crate) fn merge_from(&mut self, right: &Self, at_key: TKey) {
        assert!(self.can_fit_merge(right));

        self.entries.merge_from(&right.entries, at_key);
    }

    pub(crate) fn delete_at(&mut self, index: ValueIndex) {
        self.entries.delete_at(index);
    }

    pub(crate) fn key_at(&self, index: KeyIndex) -> Option<TKey> {
        self.entries.key_at(index)
    }

    pub(crate) fn debug(&self) -> String {
        let mut output = String::new();

        writeln!(output, "header: {:?}", self.header).unwrap();
        writeln!(output, "entries: {:?}", self.entries.debug()).unwrap();

        output
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

impl<TKey: TreeKey> Iterator for InteriorNodeKeysIterator<'_, TKey> {
    type Item = (KeyIndex, TKey);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.node.entries.key_count() {
            return None;
        }

        let index = KeyIndex::new(self.index);

        self.index += 1;

        self.node.entries.key_at(index).map(|x| (index, x))
    }
}

#[cfg(test)]
mod test {
    use crate::{
        bplustree::{AnyNodeId, InteriorNode},
        storage::SerializedPageId,
    };

    #[test]
    fn merge_with() {
        let mut node_a = InteriorNode::new(
            None,
            AnyNodeId::new(SerializedPageId::new(1u64.to_le_bytes())),
            1usize,
            AnyNodeId::new(SerializedPageId::new(2u64.to_le_bytes())),
        );
        let node_b = InteriorNode::new(
            None,
            AnyNodeId::new(SerializedPageId::new(3u64.to_le_bytes())),
            3usize,
            AnyNodeId::new(SerializedPageId::new(4u64.to_le_bytes())),
        );

        node_a.merge_from(&node_b, 2usize);

        let keys = node_a.keys().map(|x| x.1).collect::<Vec<_>>();
        let values = node_a.values().collect::<Vec<_>>();

        assert_eq!(keys, vec![1usize, 2usize, 3usize]);
        assert_eq!(
            values.iter().map(|x| x.1).collect::<Vec<_>>(),
            vec![
                AnyNodeId::new(SerializedPageId::new(1u64.to_le_bytes())),
                AnyNodeId::new(SerializedPageId::new(2u64.to_le_bytes())),
                AnyNodeId::new(SerializedPageId::new(3u64.to_le_bytes())),
                AnyNodeId::new(SerializedPageId::new(4u64.to_le_bytes())),
            ]
        );
    }
}
