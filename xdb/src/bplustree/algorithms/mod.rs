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

enum LeafSearchResult {
    Recurse(AnyNodeId),
    Done(LeafNodeId),
}

pub(super) fn leaf_search<TStorage: Storage, TKey: TreeKey>(
    transaction: &TreeTransaction<TStorage, TKey>,
    start_id: AnyNodeId,
    key: TKey,
) -> Result<LeafNodeId, TreeError> {
    let result = transaction.read_nodes(start_id, |node| {
        match node.as_any() {
            AnyNodeKind::Interior(node) => {
                for (key_index, node_key) in node.keys() {
                    if key < node_key {
                        return LeafSearchResult::Recurse(
                            node.value_at(key_index.value_before()).unwrap(),
                        );
                    }
                }

                LeafSearchResult::Recurse(node.last_value().unwrap())
            }
            AnyNodeKind::Leaf(_) => {
                // TODO can we avoid from_any here and instead make the conversion happen higher in
                // the transaction API?
                LeafSearchResult::Done(LeafNodeId::from_any(start_id))
            }
        }
    })?;

    match result {
        LeafSearchResult::Recurse(interior_node_id) => {
            leaf_search(transaction, interior_node_id, key)
        }
        LeafSearchResult::Done(leaf_node_id) => Ok(leaf_node_id),
    }
}

pub(super) fn first_leaf<TStorage: Storage, TKey: TreeKey>(
    transaction: &TreeTransaction<TStorage, TKey>,
    root: AnyNodeId,
) -> Result<LeafNodeId, TreeError> {
    let result = transaction.read_nodes(root, |node| match node.as_any() {
        AnyNodeKind::Interior(interior_node_reader) => {
            LeafSearchResult::Recurse(interior_node_reader.first_value().unwrap())
        }
        AnyNodeKind::Leaf(_) => LeafSearchResult::Done(LeafNodeId::from_any(root)),
    })?;

    match result {
        LeafSearchResult::Recurse(node_id) => first_leaf(transaction, node_id),
        LeafSearchResult::Done(leaf_id) => Ok(leaf_id),
    }
}

pub(super) fn last_leaf<TStorage: Storage, TKey: TreeKey>(
    transaction: &TreeTransaction<TStorage, TKey>,
    root: AnyNodeId,
) -> Result<LeafNodeId, TreeError> {
    let result = transaction.read_nodes(root, |node| match node.as_any() {
        AnyNodeKind::Interior(interior_node_reader) => {
            LeafSearchResult::Recurse(interior_node_reader.last_value().unwrap())
        }
        AnyNodeKind::Leaf(_) => LeafSearchResult::Done(LeafNodeId::from_any(root)),
    })?;

    match result {
        LeafSearchResult::Recurse(node_id) => last_leaf(transaction, node_id),
        LeafSearchResult::Done(leaf_node_id) => Ok(leaf_node_id),
    }
}
