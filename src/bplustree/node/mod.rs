pub(super) mod interior;
pub(super) mod leaf;

use std::{fmt::Display, marker::PhantomData};

use crate::bplustree::node::interior::InteriorNode;
use crate::bplustree::node::leaf::LeafNode;
use crate::page::PAGE_DATA_SIZE;
use crate::storage::PageIndex;
use bytemuck::{Pod, Zeroable, must_cast_ref};

pub(super) trait NodeId: Copy + PartialEq {
    type Node<TKey>: Node<TKey>
    where
        TKey: Pod;

    fn page(&self) -> PageIndex;
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(super) struct AnyNodeId(PageIndex);

impl From<LeafNodeId> for AnyNodeId {
    fn from(value: LeafNodeId) -> Self {
        Self(value.page())
    }
}

impl From<InteriorNodeId> for AnyNodeId {
    fn from(value: InteriorNodeId) -> Self {
        Self(value.page())
    }
}

impl Display for AnyNodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AnyNodeId {
    pub fn new(index: PageIndex) -> Self {
        assert!(index != PageIndex::zero());

        Self(index)
    }
}

impl NodeId for AnyNodeId {
    type Node<TKey>
        = AnyNode<TKey>
    where
        TKey: Pod;

    fn page(&self) -> PageIndex {
        self.0
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(super) struct LeafNodeId(PageIndex);

impl Display for LeafNodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl LeafNodeId {
    // TODO is there a way to enforce validity in this API?
    pub fn from_any(unknown: AnyNodeId) -> LeafNodeId {
        Self(unknown.0)
    }

    pub fn new(index: PageIndex) -> Self {
        Self(index)
    }
}

impl NodeId for LeafNodeId {
    type Node<TKey>
        = LeafNode<TKey>
    where
        TKey: Pod;

    fn page(&self) -> PageIndex {
        self.0
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(super) struct InteriorNodeId(PageIndex);

impl InteriorNodeId {
    pub(crate) fn new(index: PageIndex) -> Self {
        assert!(index != PageIndex::zero());

        Self(index)
    }
}

impl NodeId for InteriorNodeId {
    type Node<TKey>
        = InteriorNode<TKey>
    where
        TKey: Pod;

    fn page(&self) -> PageIndex {
        self.0
    }
}

bitflags::bitflags! {
    #[derive(Debug, Pod, Zeroable, Clone, Copy)]
    #[repr(transparent)]
    struct NodeFlags: u16 {
        const INTERNAL = 1 << 0;
    }
}

#[derive(Debug, Pod, Zeroable, Clone, Copy)]
#[repr(C, align(8))]
pub(super) struct NodeHeader {
    key_count: u16,
    flags: NodeFlags,
    _unused2: u32,
    parent: PageIndex,
}

impl NodeHeader {
    fn parent(&self) -> Option<InteriorNodeId> {
        if self.parent == PageIndex::zero() {
            None
        } else {
            Some(InteriorNodeId::new(self.parent))
        }
    }

    fn set_parent(&mut self, parent: Option<InteriorNodeId>) {
        self.parent = parent.map_or_else(PageIndex::zero, |x| x.page());
    }
}
const _: () = assert!(size_of::<NodeHeader>() == size_of::<u64>() * 2);

const NODE_DATA_SIZE: usize = PAGE_DATA_SIZE - size_of::<NodeHeader>();

#[derive(Debug, Zeroable, Clone, Copy)]
#[repr(C, align(8))]
pub(super) struct AnyNode<TKey> {
    header: NodeHeader,
    data: [u8; NODE_DATA_SIZE],
    _key: PhantomData<TKey>,
}

// SAFETY: this struct does not have padding and can be initialized to zero, but can't
// automatically derive Pod since it contains a PhantomData (which does not actually affect the
// layout)
unsafe impl<TKey: Pod> Pod for AnyNode<TKey> {}

pub(super) trait Node<TKey>: Pod {
    const _ASSERT_SIZE: () = assert!(size_of::<Self>() == PAGE_DATA_SIZE);

    fn parent(&self) -> Option<InteriorNodeId>;
    fn set_parent(&mut self, parent: Option<InteriorNodeId>);
}

impl<TKey: Pod> Node<TKey> for AnyNode<TKey> {
    fn parent(&self) -> Option<InteriorNodeId> {
        self.header.parent()
    }

    fn set_parent(&mut self, parent: Option<InteriorNodeId>) {
        self.header.set_parent(parent);
    }
}

pub(super) enum AnyNodeKind<'node, TKey: Pod> {
    Interior(&'node InteriorNode<TKey>),
    Leaf(&'node LeafNode<TKey>),
}

impl<TKey: Pod> AnyNode<TKey> {
    fn is_leaf(&self) -> bool {
        !self.header.flags.contains(NodeFlags::INTERNAL)
    }

    pub(crate) fn as_any(&self) -> AnyNodeKind<'_, TKey> {
        if self.is_leaf() {
            AnyNodeKind::Leaf(must_cast_ref(self))
        } else {
            AnyNodeKind::Interior(must_cast_ref(self))
        }
    }
}
