pub(super) mod interior;
pub(super) mod leaf;

use std::fmt::Display;

use crate::bplustree::node::interior::InteriorNode;
use crate::bplustree::node::leaf::LeafNode;
use crate::bplustree::{InteriorNodeWriter, LeafNodeReader, LeafNodeWriter};
use crate::storage::PageIndex;
use crate::{bplustree::InteriorNodeReader, page::PAGE_DATA_SIZE};
use bytemuck::{Pod, Zeroable, must_cast_mut, must_cast_ref};

// TODO: should the TKey be Ord, instead of PartialOrd?
// TODO Integrate the node readers and writers into the Node struct
pub(super) trait NodeReader<'node, TNode: NodeTrait<TKey>, TKey> {
    fn new(node: &'node TNode) -> Self;
}

pub(super) trait NodeWriter<'node, TNode: NodeTrait<TKey>, TKey> {
    fn new(node: &'node mut TNode) -> Self;
}

pub(super) trait NodeId: Copy + PartialEq {
    type Reader<'node, TKey>: NodeReader<'node, Self::Node<TKey>, TKey>
    where
        TKey: Pod + 'node;
    type Writer<'node, TKey>: NodeWriter<'node, Self::Node<TKey>, TKey>
    where
        TKey: Pod + PartialOrd + 'node;

    type Node<TKey>: NodeTrait<TKey> where TKey: Pod;

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
        assert!(index != PageIndex::zeroed());

        Self(index)
    }
}

impl NodeId for AnyNodeId {
    type Reader<'node, TKey>
        = AnyNodeReader<'node, TKey>
    where
        TKey: Pod + 'node;
    type Writer<'node, TKey>
        = AnyNodeWriter<'node, TKey>
    where
        TKey: Pod + PartialOrd + 'node;

    type Node<TKey> = Node where TKey: Pod;

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
    type Reader<'node, TKey>
        = LeafNodeReader<'node, TKey>
    where
        TKey: Pod + 'node;
    type Writer<'node, TKey>
        = LeafNodeWriter<'node, TKey>
    where
        TKey: Pod + PartialOrd + 'node;

    type Node<TKey> = LeafNode<TKey> where TKey: Pod;

    fn page(&self) -> PageIndex {
        self.0
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[allow(unused)] // TODO remove if we really don't need it
pub(super) struct InteriorNodeId(PageIndex);

impl InteriorNodeId {
    pub(crate) fn new(index: PageIndex) -> Self {
        assert!(index != PageIndex::zeroed());

        Self(index)
    }
}

impl NodeId for InteriorNodeId {
    type Reader<'node, TKey>
        = InteriorNodeReader<'node, TKey>
    where
        TKey: Pod + 'node;

    type Writer<'node, TKey>
        = InteriorNodeWriter<'node, TKey>
    where
        TKey: Pod + PartialOrd + 'node;

    type Node<TKey> = InteriorNode<TKey> where TKey: Pod;

    fn page(&self) -> PageIndex {
        self.0
    }
}

// TODO Support variable-sized values
// TODO Support variable-sized keys?

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
    // TODO rename -> key_count
    // TODO make private once we have a reasonable API for it
    pub(super) key_len: u16,
    flags: NodeFlags,
    _unused2: u32,
    parent: PageIndex,
}
impl NodeHeader {
    fn parent(&self) -> Option<InteriorNodeId> {
        if self.parent == PageIndex::zeroed() {
            None
        } else {
            Some(InteriorNodeId::new(self.parent))
        }
    }

    fn set_parent(&mut self, parent: Option<InteriorNodeId>) {
        self.parent = parent.map_or_else(PageIndex::zeroed, |x| x.page());
    }
}
const _: () = assert!(size_of::<NodeHeader>() == size_of::<u64>() * 2);

const NODE_DATA_SIZE: usize = PAGE_DATA_SIZE - size_of::<NodeHeader>();

#[derive(Debug, Pod, Zeroable, Clone, Copy)]
#[repr(C, align(8))]
// TODO rename -> AnyNode
// TODO keep TKey as PhantomData?
pub(super) struct Node {
    // TODO make this private once we have a reasonable API for it
    pub(super) header: NodeHeader,
    // TODO make this private once we have a reasonable API for it
    pub(super) data: [u8; NODE_DATA_SIZE],
}

// TODO rename -> Node, once the struct with that name is gone
// TODO see if we can drop Zeroable from the node types & header
pub(super) trait NodeTrait<TKey> : Pod {
    const _ASSERT_SIZE: () = assert!(size_of::<Self>() == PAGE_DATA_SIZE);

    fn parent(&self) -> Option<InteriorNodeId>; 
    fn set_parent(&mut self, parent: Option<InteriorNodeId>);
}

const _: () = assert!(size_of::<Node>() == PAGE_DATA_SIZE);

impl<TKey> NodeTrait<TKey> for Node {
    fn parent(&self) -> Option<InteriorNodeId> {
        self.header.parent()
    }

    fn set_parent(&mut self, parent: Option<InteriorNodeId>) {
        self.header.set_parent(parent);
    }
}

impl Node {
    fn is_leaf(&self) -> bool {
        !self.header.flags.contains(NodeFlags::INTERNAL)
    }
}

pub(super) enum AnyNodeReader<'node, TKey: Pod> {
    Interior(InteriorNodeReader<'node, TKey>),
    Leaf(LeafNodeReader<'node, TKey>),
}

impl<'node, TKey: Pod> NodeReader<'node, Node, TKey> for AnyNodeReader<'node, TKey> {
    fn new(node: &'node Node) -> Self {
        if node.is_leaf() {
            Self::Leaf(LeafNodeReader::new(must_cast_ref(node)))
        } else {
            Self::Interior(InteriorNodeReader::new(must_cast_ref(node)))
        }
    }
}

#[allow(unused)] // TODO remove if we really don't need it
pub(super) enum AnyNodeWriter<'node, TKey: Pod> {
    Interior(InteriorNodeWriter<'node, TKey>),
    Leaf(LeafNodeWriter<'node, TKey>),
}

impl<'node, TKey: Pod + PartialOrd> NodeWriter<'node, Node, TKey> for AnyNodeWriter<'node, TKey> {
    fn new(node: &'node mut Node) -> Self {
        if node.is_leaf() {
            AnyNodeWriter::Leaf(LeafNodeWriter::new(must_cast_mut(node)))
        } else {
            AnyNodeWriter::Interior(InteriorNodeWriter::new(must_cast_mut(node)))
        }
    }
}
