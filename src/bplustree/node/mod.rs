pub(super) mod interior;
pub(super) mod leaf;

use std::fmt::Display;

use crate::bplustree::{InteriorNodeWriter, LeafNodeReader, LeafNodeWriter};
use crate::storage::PageIndex;
use crate::{bplustree::InteriorNodeReader, page::PAGE_DATA_SIZE};
use bytemuck::{Pod, Zeroable};

pub(super) trait NodeReader<'node> {
    fn new(node: &'node Node, key_size: usize, value_size: usize) -> Self;
}

pub(super) trait NodeWriter<'node> {
    fn new(node: &'node mut Node, key_size: usize, value_size: usize) -> Self;
}

pub(super) trait NodeId: Copy + PartialEq {
    type Reader<'node>: NodeReader<'node>;
    type Writer<'node>: NodeWriter<'node>;

    fn page(&self) -> PageIndex;
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(super) struct AnyNodeId(PageIndex);

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
    type Reader<'node> = AnyNodeReader<'node>;
    type Writer<'node> = AnyNodeWriter<'node>;

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
    type Reader<'node> = LeafNodeReader<'node>;
    type Writer<'node> = LeafNodeWriter<'node>;

    fn page(&self) -> PageIndex {
        self.0
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[allow(unused)] // TODO remove if we really don't need it
pub(super) struct InteriorNodeId(PageIndex);

impl NodeId for InteriorNodeId {
    type Reader<'node> = InteriorNodeReader<'node>;
    type Writer<'node> = InteriorNodeWriter<'node>;

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
    pub(super) fn new_internal_root() -> Self {
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

    pub(super) fn new_leaf_root() -> Self {
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

    fn parent(&self) -> Option<PageIndex> {
        if self.header.parent == PageIndex::zeroed() {
            None
        } else {
            Some(self.header.parent)
        }
    }

    pub(crate) fn set_parent(&mut self, parent: PageIndex) {
        assert!(parent != PageIndex::zeroed());

        self.header.parent = parent;
    }
}

pub(super) enum AnyNodeReader<'node> {
    Interior(InteriorNodeReader<'node>),
    Leaf(LeafNodeReader<'node>),
}

impl<'node> NodeReader<'node> for AnyNodeReader<'node> {
    fn new(node: &'node Node, key_size: usize, value_size: usize) -> Self {
        if node.is_leaf() {
            Self::Leaf(LeafNodeReader::new(node, key_size, value_size))
        } else {
            Self::Interior(InteriorNodeReader::new(node, key_size))
        }
    }
}

#[allow(unused)] // TODO remove if we really don't need it
pub(super) enum AnyNodeWriter<'node> {
    Interior(InteriorNodeWriter<'node>),
    Leaf(LeafNodeWriter<'node>),
}

impl<'node> NodeWriter<'node> for AnyNodeWriter<'node> {
    fn new(node: &'node mut Node, key_size: usize, value_size: usize) -> Self {
        if node.is_leaf() {
            AnyNodeWriter::Leaf(LeafNodeWriter::new(node, key_size, value_size))
        } else {
            AnyNodeWriter::Interior(InteriorNodeWriter::new(node, key_size))
        }
    }
}
