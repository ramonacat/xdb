use std::fmt::Debug;

use log::debug;
use thiserror::Error;

use crate::{
    bplustree::{
        InteriorNodeId, LeafNodeId, TreeError, TreeKey, TreeTransaction,
        algorithms::{last_leaf, leaf_search},
        node::{Node, interior::InteriorNode, leaf::LeafNode},
    },
    storage::Storage,
};

#[must_use]
#[derive(Debug, Error)]
enum MergeError {
    #[error("nodes are not siblings")]
    NotSiblings,
    #[error("there is not enough capacity in the target node")]
    NotEnoughCapacity,
    #[error("tree error: {0:?}")]
    Tree(#[from] TreeError),
}

fn merge_leaf_with<TStorage: Storage, TKey: TreeKey>(
    transaction: &TreeTransaction<'_, TStorage, TKey>,
    left_id: LeafNodeId,
    right_id: LeafNodeId,
) -> Result<(), MergeError> {
    transaction.read_nodes((left_id, right_id), |(left, right)| {
        if left.parent() != right.parent() {
            Err(MergeError::NotSiblings)
        } else if !left.can_fit_merge(right) {
            Err(MergeError::NotEnoughCapacity)
        } else {
            Ok(())
        }
    })??;

    let next = transaction.write_nodes((left_id, right_id), |(left, right)| {
        left.merge_from(right);

        left.next()
    })?;

    if let Some(next) = next {
        transaction.write_nodes(next, |node| {
            node.set_previous(Some(left_id));
        })?;
    }

    let parent_id = transaction.read_nodes(left_id, Node::parent)?.unwrap();

    transaction.write_nodes(parent_id, |parent| parent.delete(right_id.into()))?;
    transaction.delete_node(right_id.into())?;

    debug!("merged leaf {left_id:?} with {right_id:?}");

    Ok(())
}

fn merge_interior_node_with<TStorage: Storage, TKey: TreeKey>(
    transaction: &TreeTransaction<TStorage, TKey>,
    left_id: InteriorNodeId,
    right_id: InteriorNodeId,
    parent_id: InteriorNodeId,
) -> Result<(), MergeError> {
    let last_right_leaf = last_leaf(transaction, right_id.into())?;
    let next = transaction.read_nodes(last_right_leaf, LeafNode::next)?;

    transaction.write_nodes((left_id, right_id, parent_id), |(left, right, parent)| {
        if left.parent() != right.parent() || left.parent() != Some(parent_id) {
            return Err(MergeError::NotSiblings);
        }

        if !left.can_fit_merge(right) {
            return Err(MergeError::NotEnoughCapacity);
        }

        let parent_key_index = parent.find_value_index(right_id.into()).unwrap() - 1;
        let parent_key = parent.key_at(parent_key_index).unwrap();

        left.merge_from(right, parent_key);
        parent.delete_at(parent_key_index+1);

        // TODO actually remove the Dispaly impls for NodeIds, as we don't print them outside of
        // debug contexts
        debug!("merged interior node {left_id:?} from {right_id:?} (parent: {parent_id}, key: {parent_key:?}, index: {parent_key_index})");
        dbg!(parent.keys().collect::<Vec<_>>(), parent.values().collect::<Vec<_>>());

        Ok(())
    })??;

    transaction.write_nodes(last_right_leaf, |leaf| {
        leaf.set_next(next);
    })?;

    let children = transaction.read_nodes(left_id, |node| node.values().collect::<Vec<_>>())?;

    for child in children {
        transaction.write_nodes(child, |child| {
            child.set_parent(Some(left_id));
        })?;
    }

    Ok(())
}

fn merge_interior_node<TStorage: Storage, TKey: TreeKey>(
    transaction: &TreeTransaction<TStorage, TKey>,
    node_id: InteriorNodeId,
) -> Result<(), TreeError> {
    if !transaction.read_nodes(node_id, InteriorNode::needs_merge)? {
        return Ok(());
    }

    let parent_id = transaction.read_nodes(node_id, Node::parent)?;
    let Some(parent_id) = parent_id else {
        return Ok(());
    };

    let index_in_parent =
        transaction.read_nodes(parent_id, |x| x.find_value_index(node_id.into()).unwrap())?;

    if index_in_parent > 0 {
        let left =
            transaction.read_nodes(parent_id, |x| x.value_at(index_in_parent - 1).unwrap())?;
        let left = InteriorNodeId::from_any(left);

        match merge_interior_node_with(transaction, left, node_id, parent_id) {
            Ok(()) => {
                merge_interior_node(transaction, parent_id)?;
                return Ok(());
            }
            Err(MergeError::NotEnoughCapacity) => {}
            Err(MergeError::NotSiblings) => todo!(), // this should probably just panic?
            Err(MergeError::Tree(err)) => return Err(err),
        }
    }

    let right_id =
        transaction.read_nodes(parent_id, |parent| parent.value_at(index_in_parent + 1))?;

    if let Some(right_id) = right_id {
        let right_id = InteriorNodeId::from_any(right_id);

        match merge_interior_node_with(transaction, node_id, right_id, parent_id) {
            Ok(()) => {
                merge_interior_node(transaction, parent_id)?;
                return Ok(());
            }
            Err(MergeError::NotEnoughCapacity) => {}
            Err(MergeError::NotSiblings) => todo!(), // this should probably just panic?
            Err(MergeError::Tree(err)) => return Err(err),
        }
    }

    Ok(())
}

fn merge_leaf<TStorage: Storage, TKey: TreeKey>(
    transaction: &TreeTransaction<TStorage, TKey>,
    leaf_id: LeafNodeId,
) -> Result<(), TreeError> {
    let (next, previous, parent) =
        transaction.read_nodes(leaf_id, |x| (x.next(), x.previous(), x.parent()))?;

    if let Some(next) = next {
        match merge_leaf_with(transaction, leaf_id, next) {
            Ok(()) => {
                if let Some(parent) = parent {
                    merge_interior_node(transaction, parent)?;
                }

                return Ok(());
            }
            Err(err) => match err {
                MergeError::NotSiblings | MergeError::NotEnoughCapacity => {}
                MergeError::Tree(tree_error) => return Err(tree_error),
            },
        }
    }

    if let Some(previous) = previous {
        match merge_leaf_with(transaction, previous, leaf_id) {
            Ok(()) => {
                if let Some(parent) = parent {
                    merge_interior_node(transaction, parent)?;
                }

                return Ok(());
            }
            Err(err) => match err {
                MergeError::NotSiblings | MergeError::NotEnoughCapacity => {}
                MergeError::Tree(tree_error) => return Err(tree_error),
            },
        }
    }

    Ok(())
}

pub fn delete<TStorage: Storage, TKey: TreeKey>(
    transaction: &TreeTransaction<TStorage, TKey>,
    key: TKey,
) -> Result<Option<Vec<u8>>, TreeError> {
    let starting_leaf = leaf_search(transaction, transaction.get_root()?, key)?;

    let result = transaction.write_nodes(starting_leaf, |node| node.delete(key))?;

    debug!("deleted {key:?} from {starting_leaf:?}");
    match result {
        Some((deleted, needs_merge)) => {
            if needs_merge {
                merge_leaf(transaction, starting_leaf)?;
            }

            Ok(Some(deleted))
        }
        None => Ok(None),
    }
}
