use std::marker::PhantomData;

use crate::{
    bplustree::{
        InteriorNodeId, LeafNode, LeafNodeId, NodeId, TreeKey,
        node::{
            NodeHeader,
            leaf::{LeafNodeHeader, entries::LeafNodeEntries},
        },
    },
    storage::PageId,
};

pub(in crate::bplustree) trait Topology<TPageId: PageId> {
    fn parent(&self) -> Option<InteriorNodeId<TPageId>>;
    fn previous(&self) -> Option<LeafNodeId<TPageId>>;
    fn next(&self) -> Option<LeafNodeId<TPageId>>;
}

pub(in crate::bplustree) struct MaterializedTopology<TPageId: PageId> {
    parent: Option<InteriorNodeId<TPageId>>,
    previous: Option<LeafNodeId<TPageId>>,
    next: Option<LeafNodeId<TPageId>>,
}
impl<TPageId: PageId> MaterializedTopology<TPageId> {
    pub(crate) const fn new(
        parent: Option<InteriorNodeId<TPageId>>,
        previous: Option<LeafNodeId<TPageId>>,
        next: Option<LeafNodeId<TPageId>>,
    ) -> Self {
        Self {
            parent,
            previous,
            next,
        }
    }
}

impl<TPageId: PageId> Topology<TPageId> for MaterializedTopology<TPageId> {
    fn parent(&self) -> Option<InteriorNodeId<TPageId>> {
        self.parent
    }

    fn previous(&self) -> Option<LeafNodeId<TPageId>> {
        self.previous
    }

    fn next(&self) -> Option<LeafNodeId<TPageId>> {
        self.next
    }
}

pub(in crate::bplustree) struct LeafNodeBuilder<TKey, TPageId: PageId, TTopology, TData> {
    topology: TTopology,
    data: TData,
    _key: PhantomData<TKey>,
    _page_id: PhantomData<TPageId>,
}

pub(in crate::bplustree) trait Data<'data, TKey> {
    fn data(&self) -> &'data [u8];
    fn entry_count(&self) -> usize;
}

pub(in crate::bplustree) struct MaterializedData<'data, TKey> {
    data: &'data [u8],
    entry_count: usize,
    _key: PhantomData<&'data TKey>,
}

impl<'data, TKey> MaterializedData<'data, TKey> {
    pub(crate) const fn new(entry_count: usize, data: &'data [u8]) -> Self {
        Self {
            data,
            entry_count,
            _key: PhantomData,
        }
    }
}

impl<'data, TKey> Data<'data, TKey> for MaterializedData<'data, TKey> {
    fn data(&self) -> &'data [u8] {
        self.data
    }

    // TODO make this an Option<usize>? technically it's just an optimization, we can calculate the
    // number of entries, as the data has a known format
    fn entry_count(&self) -> usize {
        self.entry_count
    }
}

impl<TKey, TPageId: PageId> LeafNodeBuilder<TKey, TPageId, (), ()> {
    pub const fn new() -> Self {
        Self {
            topology: (),
            data: (),
            _key: PhantomData,
            _page_id: PhantomData,
        }
    }
}

impl<TKey, TPageId: PageId, TTopology, TData> LeafNodeBuilder<TKey, TPageId, TTopology, TData> {
    pub fn with_topology(
        self,
        parent: Option<InteriorNodeId<TPageId>>,
        previous: Option<LeafNodeId<TPageId>>,
        next: Option<LeafNodeId<TPageId>>,
    ) -> LeafNodeBuilder<TKey, TPageId, MaterializedTopology<TPageId>, TData> {
        LeafNodeBuilder {
            topology: MaterializedTopology {
                parent,
                previous,
                next,
            },
            data: self.data,
            _key: PhantomData,
            _page_id: PhantomData,
        }
    }
}

impl<TKey, TPageId: PageId, TTopology, TData> LeafNodeBuilder<TKey, TPageId, TTopology, TData> {
    pub fn with_data<'data, TNewData: Data<'data, TKey>>(
        self,
        data: TNewData,
    ) -> LeafNodeBuilder<TKey, TPageId, TTopology, TNewData> {
        LeafNodeBuilder {
            topology: self.topology,
            data,
            _key: PhantomData,
            _page_id: PhantomData,
        }
    }
}

impl<'data, TKey: TreeKey, TPageId: PageId, TTopology: Topology<TPageId>, TData: Data<'data, TKey>>
    LeafNodeBuilder<TKey, TPageId, TTopology, TData>
{
    pub fn build(self) -> LeafNode<TKey, TPageId> {
        LeafNode {
            header: NodeHeader::new_leaf(
                self.topology
                    .parent()
                    .map_or_else(TPageId::sentinel, |x| x.page()),
            ),
            leaf_header: LeafNodeHeader {
                previous: self
                    .topology
                    .previous()
                    .map_or_else(TPageId::sentinel, |x| x.page()),
                next: self
                    .topology
                    .next()
                    .map_or_else(TPageId::sentinel, |x| x.page()),
            },
            data: LeafNodeEntries::from_data(self.data.entry_count(), self.data.data()),
        }
    }
}
