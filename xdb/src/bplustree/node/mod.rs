// TODO nodes should no know about PageId at all and just use a simple representation (u64 or
// somethings) that can be deserialized as needed
pub(super) mod interior;
pub(super) mod leaf;

use std::marker::PhantomData;

use crate::Size;
use crate::bplustree::node::leaf::LeafNode;
use crate::bplustree::{TreeKey, node::interior::InteriorNode};
use crate::page::PAGE_DATA_SIZE;
use crate::storage::{SENTINEL_PAGE_ID, SerializedPageId};
use bytemuck::{AnyBitPattern, NoUninit, Pod, Zeroable, must_cast_ref};

pub(super) trait NodeId: Copy + PartialEq {
    type Node<TKey>: Node<TKey>
    where
        TKey: TreeKey;

    fn page(&self) -> SerializedPageId;
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub(super) struct AnyNodeId(SerializedPageId);

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

impl AnyNodeId {
    pub fn new(index: SerializedPageId) -> Self {
        assert!(index != SENTINEL_PAGE_ID);

        Self(index)
    }
}

impl NodeId for AnyNodeId {
    type Node<TKey>
        = AnyNode<TKey>
    where
        TKey: TreeKey;

    fn page(&self) -> SerializedPageId {
        self.0
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(super) struct LeafNodeId(SerializedPageId);

impl LeafNodeId {
    // TODO is there a way to enforce validity in this API?
    pub const fn from_any(unknown: AnyNodeId) -> Self {
        Self(unknown.0)
    }

    pub fn new(index: SerializedPageId) -> Self {
        assert!(index != SENTINEL_PAGE_ID);

        Self(index)
    }
}

impl NodeId for LeafNodeId {
    type Node<TKey>
        = LeafNode<TKey>
    where
        TKey: TreeKey;

    fn page(&self) -> SerializedPageId {
        self.0
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(super) struct InteriorNodeId(SerializedPageId);

impl InteriorNodeId {
    pub(crate) fn new(index: SerializedPageId) -> Self {
        assert!(index != SENTINEL_PAGE_ID);

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

    fn page(&self) -> SerializedPageId {
        self.0
    }
}

bitflags::bitflags! {
    #[derive(Debug, Pod, Zeroable, Clone, Copy)]
    #[repr(transparent)]
    struct NodeFlags: u16 {
        const INTERIOR = 1 << 0;
    }
}

#[derive(Debug, Zeroable, Clone, Copy)]
#[repr(C, align(8))]
pub(super) struct NodeHeader {
    flags: NodeFlags,
    _unused1: u16,
    _unused2: u32,
    parent: SerializedPageId,
}

// TODO we should add a size check here probably, so that padding doesn't get messed up?
unsafe impl Pod for NodeHeader {}

impl NodeHeader {
    const _SIZE_AS_EXPECTED: () =
        assert!(Size::of::<Self>().is_equal(Size::of::<u64>().multiply(2)));

    const fn new_interior(parent: SerializedPageId) -> Self {
        Self {
            flags: NodeFlags::INTERIOR,
            _unused1: 0,
            _unused2: 0,
            parent,
        }
    }

    const fn new_leaf(parent: SerializedPageId) -> Self {
        Self {
            flags: NodeFlags::empty(),
            _unused1: 0,
            _unused2: 0,
            parent,
        }
    }

    fn parent(&self) -> Option<InteriorNodeId> {
        if self.parent == SENTINEL_PAGE_ID {
            None
        } else {
            Some(InteriorNodeId::new(self.parent))
        }
    }

    fn set_parent(&mut self, parent: Option<InteriorNodeId>) {
        self.parent = parent.map_or(SENTINEL_PAGE_ID, |x| x.page());
    }
}

#[derive(Debug, Zeroable, Clone, Copy)]
#[repr(C, align(8))]
pub(super) struct AnyNode<TKey> {
    header: NodeHeader,
    // TODO the size here is hardcoded with the assumption that TPageId's size is u64
    data: [u8; PAGE_DATA_SIZE
        .subtract(Size::of::<u64>().multiply(2))
        .as_bytes()],
    _key: PhantomData<TKey>,
}

// SAFETY: this struct does not have padding and can be initialized to zero, but can't
// automatically derive Pod since it contains a PhantomData (which does not actually affect the
// layout)
unsafe impl<TKey: TreeKey> Pod for AnyNode<TKey> {}

pub(super) trait Node<TKey>: AnyBitPattern + NoUninit {
    const _ASSERT_SIZE: () = assert!(Size::of::<Self>().is_equal(PAGE_DATA_SIZE));

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
        !self.header.flags.contains(NodeFlags::INTERIOR)
    }

    pub(crate) const fn as_any(&self) -> AnyNodeKind<'_, TKey> {
        if self.is_leaf() {
            AnyNodeKind::Leaf(must_cast_ref(self))
        } else {
            AnyNodeKind::Interior(must_cast_ref(self))
        }
    }
}
