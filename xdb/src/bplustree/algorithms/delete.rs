use std::fmt::Debug;

use bytemuck::Pod;
use log::debug;
use thiserror::Error;

use crate::{
    bplustree::{LeafNodeId, Node, TreeError, TreeTransaction, algorithms::leaf_search},
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

fn merge_leaf_with<TStorage: Storage, TKey: Pod + Ord + Debug>(
    transaction: &TreeTransaction<'_, TStorage, TKey>,
    left_id: LeafNodeId,
    right_id: LeafNodeId,
) -> Result<(), MergeError> {
    // TODO create transaction.read_nodes, as we don't need mut refs here
    // TODO figure out a way to let the closure here return a Result<T, E> wihtout nesting
    transaction.write_nodes((left_id, right_id), |left, right| {
        if left.parent() != right.parent() {
            Err(MergeError::NotSiblings)
        } else if !left.can_fit_merge(right) {
            Err(MergeError::NotEnoughCapacity)
        } else {
            Ok(())
        }
    })??;

    let next = transaction.write_nodes((left_id, right_id), |left, right| {
        left.merge_from(right);

        left.next()
    })?;

    if let Some(next) = next {
        transaction.write_node(next, |node| {
            node.set_links(node.parent(), Some(left_id), node.next())
        })?;
    }

    let parent_id = transaction.read_node(left_id, |x| x.parent())?.unwrap();

    transaction.write_node(parent_id, |parent| parent.delete(right_id.into()))?;

    // TODO delete the sibling_id node

    debug!("merged leaf {left_id:?} with {right_id:?}");

    Ok(())
}

fn merge_leaf<TStorage: Storage, TKey: Pod + Ord + Debug>(
    transaction: &TreeTransaction<TStorage, TKey>,
    leaf_id: LeafNodeId,
) -> Result<(), TreeError> {
    let (next, previous) = transaction.read_node(leaf_id, |x| (x.next(), x.previous()))?;

    if let Some(next) = next {
        match merge_leaf_with(transaction, leaf_id, next) {
            // TODO check if the parent needs a merge as well
            Ok(_) => return Ok(()),
            Err(err) => match err {
                MergeError::NotSiblings => {}
                MergeError::NotEnoughCapacity => {}
                MergeError::Tree(tree_error) => return Err(tree_error),
            },
        }
    }

    if let Some(previous) = previous {
        match merge_leaf_with(transaction, previous, leaf_id) {
            // TODO check if the parent needs a merge as well
            Ok(_) => return Ok(()),
            Err(err) => match err {
                MergeError::NotSiblings => {}
                MergeError::NotEnoughCapacity => {}
                MergeError::Tree(tree_error) => return Err(tree_error),
            },
        }
    }

    Ok(())
}

pub fn delete<TStorage: Storage, TKey: Pod + Ord + Debug>(
    transaction: &TreeTransaction<TStorage, TKey>,
    key: TKey,
) -> Result<Option<Vec<u8>>, TreeError> {
    let starting_leaf = leaf_search(transaction, transaction.get_root()?, &key)?;

    let result = transaction.write_node(starting_leaf, |node| node.delete(key))?;

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
