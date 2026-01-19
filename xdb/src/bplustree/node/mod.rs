pub(super) mod interior;
pub(super) mod leaf;

use std::{fmt::Display, marker::PhantomData};

use crate::bplustree::node::leaf::LeafNode;
use crate::bplustree::{TreeKey, node::interior::InteriorNode};
use crate::page::PAGE_DATA_SIZE;
use crate::storage::PageIndex;
use bytemuck::{AnyBitPattern, NoUninit, Pod, Zeroable, must_cast_ref};

pub(super) trait NodeId: Copy + PartialEq {
    type Node<TKey>: Node<TKey>
    where
        TKey: TreeKey;

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
        TKey: TreeKey;

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
    pub const fn from_any(unknown: AnyNodeId) -> Self {
        Self(unknown.0)
    }

    pub const fn new(index: PageIndex) -> Self {
        Self(index)
    }
}

impl NodeId for LeafNodeId {
    type Node<TKey>
        = LeafNode<TKey>
    where
        TKey: TreeKey;

    fn page(&self) -> PageIndex {
        self.0
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(super) struct InteriorNodeId(PageIndex);

impl Display for InteriorNodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl InteriorNodeId {
    pub(crate) fn new(index: PageIndex) -> Self {
        assert!(index != PageIndex::zero());

        Self(index)
    }

    pub(crate) fn from_any(other: AnyNodeId) -> Self {
        Self::new(other.0)
    }
}

impl NodeId for InteriorNodeId {
    type Node<TKey>
        = InteriorNode<TKey>
    where
        TKey: TreeKey;

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
    flags: NodeFlags,
    _unused1: u16,
    _unused2: u32,
    parent: PageIndex,
}
const _: () = assert!(size_of::<NodeHeader>() == size_of::<u64>() * 2);

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
unsafe impl<TKey: TreeKey> Pod for AnyNode<TKey> {}

pub(super) trait Node<TKey>: AnyBitPattern + NoUninit {
    const _ASSERT_SIZE: () = assert!(size_of::<Self>() == PAGE_DATA_SIZE);

    fn parent(&self) -> Option<InteriorNodeId>;
    fn set_parent(&mut self, parent: Option<InteriorNodeId>);
}

impl<TKey: TreeKey> Node<TKey> for AnyNode<TKey> {
    fn parent(&self) -> Option<InteriorNodeId> {
        self.header.parent()
    }

    fn set_parent(&mut self, parent: Option<InteriorNodeId>) {
        self.header.set_parent(parent);
    }
}

pub(super) enum AnyNodeKind<'node, TKey: TreeKey> {
    Interior(&'node InteriorNode<TKey>),
    Leaf(&'node LeafNode<TKey>),
}

impl<TKey: TreeKey> AnyNode<TKey> {
    pub const fn is_leaf(&self) -> bool {
        !self.header.flags.contains(NodeFlags::INTERNAL)
    }

    pub(crate) const fn as_any(&self) -> AnyNodeKind<'_, TKey> {
        if self.is_leaf() {
            AnyNodeKind::Leaf(must_cast_ref(self))
        } else {
            AnyNodeKind::Interior(must_cast_ref(self))
        }
    }
}
