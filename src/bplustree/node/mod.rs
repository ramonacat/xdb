pub(super) mod interior;
pub(super) mod leaf;

use crate::bplustree::LeafNodeReader;
use crate::storage::PageIndex;
use crate::{bplustree::InteriorNodeReader, page::PAGE_DATA_SIZE};
use bytemuck::{Pod, Zeroable};

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

    pub(super) fn is_leaf(&self) -> bool {
        !self.header.flags.contains(NodeFlags::INTERNAL)
    }

    pub(super) fn parent(&self) -> Option<PageIndex> {
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

    // TODO avoid passing key_size/value_size here?
    pub(super) fn reader(&'_ self, key_size: usize, value_size: usize) -> NodeReader<'_> {
        if self.is_leaf() {
            NodeReader::Leaf(LeafNodeReader::new(self, key_size, value_size))
        } else {
            NodeReader::Interior(InteriorNodeReader::new(self, key_size))
        }
    }
}

pub(super) enum NodeReader<'node> {
    Interior(InteriorNodeReader<'node>),
    Leaf(LeafNodeReader<'node>),
}
