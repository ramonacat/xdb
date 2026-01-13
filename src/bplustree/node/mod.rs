pub(super) mod interior;
pub(super) mod leaf;

use std::fmt::Display;

use crate::bplustree::{InteriorNodeWriter, LeafNodeReader, LeafNodeWriter};
use crate::storage::PageIndex;
use crate::{bplustree::InteriorNodeReader, page::PAGE_DATA_SIZE};
use bytemuck::{Pod, Zeroable};

// TODO: should the TKey be Ord, instead of PartialOrd?
// TODO Integrate the node readers and writers into the Node struct
pub(super) trait NodeReader<'node, TKey> {
    fn new(node: &'node Node) -> Self;
}

pub(super) trait NodeWriter<'node, TKey> {
    fn new(node: &'node mut Node) -> Self;
}

pub(super) trait NodeId: Copy + PartialEq {
    type Reader<'node, TKey>: NodeReader<'node, TKey>
    where
        TKey: Pod + 'node;
    type Writer<'node, TKey>: NodeWriter<'node, TKey>
    where
        TKey: Pod + PartialOrd + 'node;

    fn page(&self) -> PageIndex;
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(super) struct AnyNodeId(PageIndex);

impl From<LeafNodeId> for AnyNodeId {
    fn from(value: LeafNodeId) -> Self {
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
    key_len: u16,
    flags: NodeFlags,
    _unused2: u32,
    parent: PageIndex,
}
const _: () = assert!(size_of::<NodeHeader>() == size_of::<u64>() * 2);

const NODE_DATA_SIZE: usize = PAGE_DATA_SIZE - size_of::<NodeHeader>();

#[derive(Debug, Pod, Zeroable, Clone, Copy)]
#[repr(C, align(8))]
pub(super) struct Node {
    header: NodeHeader,
    data: [u8; NODE_DATA_SIZE],
}

const _: () = assert!(size_of::<Node>() == PAGE_DATA_SIZE);

impl Node {
    pub(super) fn new_interior() -> Self {
        Self {
            header: NodeHeader {
                key_len: 0,
                flags: NodeFlags::INTERNAL,
                _unused2: 0,
                parent: PageIndex::zeroed(),
            },
            data: [0; _],
        }
    }

    pub(super) fn new_leaf() -> Self {
        Self {
            header: NodeHeader {
                key_len: 0,
                flags: NodeFlags::empty(),
                _unused2: 0,
                parent: PageIndex::zeroed(),
            },
            data: [0; _],
        }
    }

    fn is_leaf(&self) -> bool {
        !self.header.flags.contains(NodeFlags::INTERNAL)
    }

    fn parent(&self) -> Option<InteriorNodeId> {
        if self.header.parent == PageIndex::zeroed() {
            None
        } else {
            Some(InteriorNodeId::new(self.header.parent))
        }
    }

    pub(crate) fn set_parent(&mut self, parent: PageIndex) {
        assert!(parent != PageIndex::zeroed());

        self.header.parent = parent;
    }
}

pub(super) enum AnyNodeReader<'node, TKey> {
    Interior(InteriorNodeReader<'node, TKey>),
    Leaf(LeafNodeReader<'node, TKey>),
}

impl<'node, TKey: Pod> NodeReader<'node, TKey> for AnyNodeReader<'node, TKey> {
    fn new(node: &'node Node) -> Self {
        if node.is_leaf() {
            Self::Leaf(LeafNodeReader::new(node))
        } else {
            Self::Interior(InteriorNodeReader::new(node))
        }
    }
}

#[allow(unused)] // TODO remove if we really don't need it
pub(super) enum AnyNodeWriter<'node, TKey> {
    Interior(InteriorNodeWriter<'node, TKey>),
    Leaf(LeafNodeWriter<'node, TKey>),
}

impl<'node, TKey: Pod + PartialOrd> NodeWriter<'node, TKey> for AnyNodeWriter<'node, TKey> {
    fn new(node: &'node mut Node) -> Self {
        if node.is_leaf() {
            AnyNodeWriter::Leaf(LeafNodeWriter::new(node))
        } else {
            AnyNodeWriter::Interior(InteriorNodeWriter::new(node))
        }
    }
}
