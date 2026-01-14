use crate::bplustree::node::AnyNode;
use bytemuck::Pod;

use crate::{
    bplustree::{
        TreeError, TreeTransaction,
        node::{AnyNodeId, LeafNodeId},
    },
    storage::Storage,
};

pub(super) fn leaf_search<TStorage: Storage, TKey: Pod + Ord>(
    transaction: &TreeTransaction<TStorage, TKey>,
    node_index: AnyNodeId,
    key: &TKey,
) -> Result<LeafNodeId, TreeError> {
    transaction.read_node(node_index, |node| {
        let node = match node.as_any() {
            AnyNode::Interior(reader) => reader,
            // TODO this should maybe be held by the reader which would do the conversion so we
            // don't share the `From`???
            AnyNode::Leaf(_) => {
                return Ok(LeafNodeId::from_any(node_index));
            }
        };

        for (key_index, node_key) in node.keys().enumerate() {
            if key < node_key {
                let child_page = node.value_at(key_index).unwrap();

                return leaf_search(transaction, child_page, key);
            }
        }

        leaf_search(transaction, node.last_value().unwrap(), key)
    })?
}

pub(super) fn first_leaf<TStorage: Storage, TKey: Pod + Ord>(
    transaction: &TreeTransaction<TStorage, TKey>,
    root: AnyNodeId,
) -> Result<LeafNodeId, TreeError> {
    transaction.read_node(root, |node| match node.as_any::<TKey>() {
        AnyNode::Interior(interior_node_reader) => {
            first_leaf(transaction, interior_node_reader.first_value().unwrap())
        }
        AnyNode::Leaf(_) => Ok(LeafNodeId::from_any(root)),
    })?
}
