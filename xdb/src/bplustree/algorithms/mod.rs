pub mod delete;
pub mod insert;

use crate::bplustree::{TreeKey, node::AnyNodeKind};

use crate::{
    bplustree::{
        TreeError, TreeTransaction,
        node::{AnyNodeId, LeafNodeId},
    },
    storage::Storage,
};

pub(super) fn leaf_search<TStorage: Storage, TKey: TreeKey>(
    transaction: &TreeTransaction<TStorage, TKey>,
    node_index: AnyNodeId,
    key: &TKey,
) -> Result<LeafNodeId, TreeError> {
    transaction.read_nodes(node_index, |node| {
        let node = match node.as_any() {
            AnyNodeKind::Interior(reader) => reader,
            // TODO this should maybe be held by the reader which would do the conversion so we
            // don't share the `From`???
            AnyNodeKind::Leaf(_) => {
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

pub(super) fn first_leaf<TStorage: Storage, TKey: TreeKey>(
    transaction: &TreeTransaction<TStorage, TKey>,
    root: AnyNodeId,
) -> Result<LeafNodeId, TreeError> {
    transaction.read_nodes(root, |node| match node.as_any() {
        AnyNodeKind::Interior(interior_node_reader) => {
            first_leaf(transaction, interior_node_reader.first_value().unwrap())
        }
        AnyNodeKind::Leaf(_) => Ok(LeafNodeId::from_any(root)),
    })?
}

pub(super) fn last_leaf<TStorage: Storage, TKey: TreeKey>(
    transaction: &TreeTransaction<TStorage, TKey>,
    root: AnyNodeId,
) -> Result<LeafNodeId, TreeError> {
    transaction.read_nodes(root, |node| match node.as_any() {
        AnyNodeKind::Interior(interior_node_reader) => {
            last_leaf(transaction, interior_node_reader.last_value().unwrap())
        }
        AnyNodeKind::Leaf(_) => Ok(LeafNodeId::from_any(root)),
    })?
}
