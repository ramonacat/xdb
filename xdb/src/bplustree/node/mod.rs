// TODO nodes should no know about PageId at all and just use a simple representation (u64 or
// somethings) that can be deserialized as needed
pub(super) mod interior;
pub(super) mod leaf;

use std::marker::PhantomData;

use crate::Size;
use crate::bplustree::node::leaf::LeafNode;
use crate::bplustree::{TreeKey, node::interior::InteriorNode};
use crate::page::PAGE_DATA_SIZE;
use crate::storage::PageId;
use bytemuck::{AnyBitPattern, NoUninit, Pod, Zeroable, must_cast_ref};

pub(super) trait NodeId<TPageId: PageId>: Copy + PartialEq {
    type Node<TKey>: Node<TKey, TPageId>
    where
        TKey: TreeKey;

    fn page(&self) -> TPageId;
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub(super) struct AnyNodeId<T: PageId>(T);

impl<TPageId: PageId> From<LeafNodeId<TPageId>> for AnyNodeId<TPageId> {
    fn from(value: LeafNodeId<TPageId>) -> Self {
        Self(value.page())
    }
}

impl<T: PageId> From<InteriorNodeId<T>> for AnyNodeId<T> {
    fn from(value: InteriorNodeId<T>) -> Self {
        Self(value.page())
    }
}

impl<T: PageId> AnyNodeId<T> {
    pub fn new(index: T) -> Self {
        assert!(index != T::sentinel());

        Self(index)
    }
}

impl<T: PageId> NodeId<T> for AnyNodeId<T> {
    type Node<TKey>
        = AnyNode<TKey, T>
    where
        TKey: TreeKey;

    fn page(&self) -> T {
        self.0
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(super) struct LeafNodeId<T: PageId>(T);

impl<T: PageId> LeafNodeId<T> {
    // TODO is there a way to enforce validity in this API?
    pub const fn from_any(unknown: AnyNodeId<T>) -> Self {
        Self(unknown.0)
    }

    pub fn new(index: T) -> Self {
        assert!(index != T::sentinel());

        Self(index)
    }
}

impl<T: PageId> NodeId<T> for LeafNodeId<T> {
    type Node<TKey>
        = LeafNode<TKey, T>
    where
        TKey: TreeKey;

    fn page(&self) -> T {
        self.0
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(super) struct InteriorNodeId<T: PageId>(T);

impl<T: PageId> InteriorNodeId<T> {
    pub(crate) fn new(index: T) -> Self {
        assert!(index != T::sentinel());

        Self(index)
    }

    pub(crate) fn from_any(other: AnyNodeId<T>) -> Self {
        Self::new(other.0)
    }
}

impl<T: PageId> NodeId<T> for InteriorNodeId<T> {
    type Node<TKey>
        = InteriorNode<TKey, T>
    where
        TKey: TreeKey;

    fn page(&self) -> T {
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
pub(super) struct NodeHeader<T> {
    flags: NodeFlags,
    _unused1: u16,
    _unused2: u32,
    parent: T,
}

// TODO we should add a size check here probably, so that padding doesn't get messed up?
unsafe impl<T: Pod + Copy + Clone> Pod for NodeHeader<T> {}

impl<T: PageId> NodeHeader<T> {
    const _SIZE_AS_EXPECTED: () =
        assert!(Size::of::<Self>().is_equal(Size::of::<u64>().multiply(2)));

    const fn new_interior(parent: T) -> Self {
        Self {
            flags: NodeFlags::INTERIOR,
            _unused1: 0,
            _unused2: 0,
            parent,
        }
    }

    const fn new_leaf(parent: T) -> Self {
        Self {
            flags: NodeFlags::empty(),
            _unused1: 0,
            _unused2: 0,
            parent,
        }
    }

    fn parent(&self) -> Option<InteriorNodeId<T>> {
        if self.parent == T::sentinel() {
            None
        } else {
            Some(InteriorNodeId::new(self.parent))
        }
    }

    fn set_parent(&mut self, parent: Option<InteriorNodeId<T>>) {
        self.parent = parent.map_or_else(T::sentinel, |x| x.page());
    }
}

#[derive(Debug, Zeroable, Clone, Copy)]
#[repr(C, align(8))]
pub(super) struct AnyNode<TKey, TPageId> {
    header: NodeHeader<TPageId>,
    // TODO the size here is hardcoded with the assumption that TPageId's size is u64
    data: [u8; PAGE_DATA_SIZE
        .subtract(Size::of::<u64>().multiply(2))
        .as_bytes()],
    _key: PhantomData<TKey>,
}

// SAFETY: this struct does not have padding and can be initialized to zero, but can't
// automatically derive Pod since it contains a PhantomData (which does not actually affect the
// layout)
unsafe impl<TKey: TreeKey, TPageId: PageId> Pod for AnyNode<TKey, TPageId> {}

pub(super) trait Node<TKey, TPageId: PageId>: AnyBitPattern + NoUninit {
    const _ASSERT_SIZE: () = assert!(Size::of::<Self>().is_equal(PAGE_DATA_SIZE));

    fn parent(&self) -> Option<InteriorNodeId<TPageId>>;
    fn set_parent(&mut self, parent: Option<InteriorNodeId<TPageId>>);
}

impl<TKey: TreeKey, TPageId: PageId> Node<TKey, TPageId> for AnyNode<TKey, TPageId> {
    fn parent(&self) -> Option<InteriorNodeId<TPageId>> {
        self.header.parent()
    }

    fn set_parent(&mut self, parent: Option<InteriorNodeId<TPageId>>) {
        self.header.set_parent(parent);
    }
}

pub(super) enum AnyNodeKind<'node, TKey: TreeKey, TPageId: PageId> {
    Interior(&'node InteriorNode<TKey, TPageId>),
    Leaf(&'node LeafNode<TKey, TPageId>),
}

impl<TKey: TreeKey, TPageId: PageId> AnyNode<TKey, TPageId> {
    pub const fn is_leaf(&self) -> bool {
        !self.header.flags.contains(NodeFlags::INTERIOR)
    }

    pub(crate) const fn as_any(&self) -> AnyNodeKind<'_, TKey, TPageId> {
        if self.is_leaf() {
            AnyNodeKind::Leaf(must_cast_ref(self))
        } else {
            AnyNodeKind::Interior(must_cast_ref(self))
        }
    }
}
