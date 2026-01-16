use std::marker::PhantomData;

use bytemuck::Pod;

use crate::bplustree::{
    InteriorNodeId, LeafNode, LeafNodeId,
    node::{NodeFlags, NodeHeader, leaf::LeafNodeHeader},
};

pub(in crate::bplustree) trait Topology {
    fn parent(&self) -> Option<InteriorNodeId>;
    fn previous(&self) -> Option<LeafNodeId>;
    fn next(&self) -> Option<LeafNodeId>;
}

pub(in crate::bplustree) struct MaterializedTopology {
    parent: Option<InteriorNodeId>,
    previous: Option<LeafNodeId>,
    next: Option<LeafNodeId>,
}

impl Topology for MaterializedTopology {
    fn parent(&self) -> Option<InteriorNodeId> {
        self.parent
    }

    fn previous(&self) -> Option<LeafNodeId> {
        self.previous
    }

    fn next(&self) -> Option<LeafNodeId> {
        self.next
    }
}

pub(in crate::bplustree) struct LeafNodeBuilder<TKey, TTopology, TData> {
    topology: TTopology,
    data: TData,
    _key: PhantomData<TKey>,
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
    pub(crate) fn new(entry_count: usize, data: &'data [u8]) -> Self {
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

impl<TKey> LeafNodeBuilder<TKey, (), ()> {
    pub fn new() -> Self {
        Self {
            topology: (),
            data: (),
            _key: PhantomData,
        }
    }
}

impl<TKey, TTopology, TData> LeafNodeBuilder<TKey, TTopology, TData> {
    pub fn with_topology(
        self,
        parent: Option<InteriorNodeId>,
        previous: Option<LeafNodeId>,
        next: Option<LeafNodeId>,
    ) -> LeafNodeBuilder<TKey, MaterializedTopology, TData> {
        LeafNodeBuilder {
            topology: MaterializedTopology {
                parent,
                previous,
                next,
            },
            data: self.data,
            _key: PhantomData,
        }
    }
}

impl<TKey, TTopology, TData> LeafNodeBuilder<TKey, TTopology, TData> {
    pub fn with_data<'data, TNewData: Data<'data, TKey>>(
        self,
        data: TNewData,
    ) -> LeafNodeBuilder<TKey, TTopology, TNewData> {
        LeafNodeBuilder {
            topology: self.topology,
            data,
            _key: PhantomData,
        }
    }
}

impl<'data, TKey: Pod, TTopology: Topology, TData: Data<'data, TKey>>
    LeafNodeBuilder<TKey, TTopology, TData>
{
    pub fn build(self) -> LeafNode<TKey> {
        let mut node = LeafNode {
            header: NodeHeader {
                key_count: self.data.entry_count() as u16,
                flags: NodeFlags::empty(),
                _unused2: 0,
                parent: self.topology.parent().into(),
            },
            leaf_header: LeafNodeHeader {
                previous: self.topology.previous().into(),
                next: self.topology.next().into(),
            },
            data: [0; _],
            _key: PhantomData,
        };
        node.data[0..self.data.data().len()].copy_from_slice(self.data.data());

        node
    }
}
