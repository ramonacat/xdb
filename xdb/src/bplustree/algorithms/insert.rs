#[cfg(debug_assertions)]
use std::collections::HashSet;

use tracing::{error, instrument, trace};

use crate::{
    bplustree::{
        AnyNodeId, InteriorNode, InteriorNodeId, LeafNodeId, Node, NodeId as _, TreeError, TreeKey,
        TreeTransaction, algorithms::leaf_search, node::leaf::builder::MaterializedTopology,
    },
    storage::{PageId, PageReservation as _, Storage},
};

fn create_new_root<'storage, TStorage: Storage, TKey: TreeKey>(
    transaction: &mut TreeTransaction<'storage, TStorage, TKey>,
    reservation: <TStorage as Storage>::PageReservation<'storage>,
    left: AnyNodeId,
    key: TKey,
    right: AnyNodeId,
) -> Result<(), TreeError<TStorage::PageId>> {
    let new_root_id = InteriorNodeId::new(reservation.index().serialize());
    let new_root = InteriorNode::<TKey>::new(None, left, key, right);

    transaction.insert_reserved(reservation, new_root)?;
    transaction.write_header(|header| header.root = new_root_id.page())?;

    Ok(())
}

fn split_leaf_root<TStorage: Storage, TKey: TreeKey>(
    transaction: &mut TreeTransaction<TStorage, TKey>,
) -> Result<(), TreeError<TStorage::PageId>> {
    let root_id = transaction.get_root()?;

    if !(transaction.read_nodes(root_id, super::super::node::AnyNode::is_leaf)?) {
        error!(?root_id, "root is not a leaf");
        panic!("root is not a leaf");
    }

    let root_id = LeafNodeId::from_any(root_id);

    let new_root_reservation = transaction.reserve_node()?;
    let new_root_id = InteriorNodeId::new(new_root_reservation.index().serialize());

    let new_leaf_reservation = transaction.reserve_node()?;
    let new_leaf_id = LeafNodeId::new(new_leaf_reservation.index().serialize());

    let new_leaf = transaction.write_nodes(root_id, |root| {
        root.split(&MaterializedTopology::new(
            Some(new_root_id),
            None,
            Some(new_leaf_id),
        ))
        .with_topology(Some(new_root_id), Some(root_id), None)
        .build()
    })?;

    transaction.insert_reserved(new_leaf_reservation, new_leaf)?;
    create_new_root(
        transaction,
        new_root_reservation,
        root_id.into(),
        new_leaf.first_key().unwrap(),
        new_leaf_id.into(),
    )?;

    Ok(())
}

fn split_interior_node<TStorage: Storage, TKey: TreeKey>(
    transaction: &mut TreeTransaction<TStorage, TKey>,
    split_id: InteriorNodeId,
) -> Result<bool, TreeError<TStorage::PageId>> {
    let parent_id = transaction.read_nodes(split_id, Node::parent)?;

    if let Some(parent) = parent_id
        && !transaction.read_nodes(parent, InteriorNode::has_spare_capacity)?
    {
        let _ = split_interior_node(transaction, parent)?;

        return Ok(false);
    }

    let new_node_reservation = transaction.reserve_node()?;
    let new_node_id = InteriorNodeId::new(new_node_reservation.index().serialize());

    let (split_key, new_node) = transaction.write_nodes(split_id, InteriorNode::split)?;

    #[cfg(debug_assertions)]
    {
        let mut target_values = HashSet::new();

        transaction.read_nodes(split_id, |target| {
            for (_, child) in target.values() {
                target_values.insert(child);
            }
        })?;

        for (_, child) in new_node.values() {
            assert!(!target_values.contains(&child));
        }
    }

    for (_, child_id) in new_node.values() {
        transaction.write_nodes(child_id, |node| node.set_parent(Some(new_node_id)))?;
    }

    transaction.insert_reserved(new_node_reservation, new_node)?;

    if let Some(parent_id) = parent_id {
        trace!(node_id=?split_id, new_node_id=?new_node_id, parent_id=?parent_id, "split interior node");

        insert_child(transaction, parent_id, split_key, new_node_id.into())?;
    } else {
        let new_root_reservation = transaction.reserve_node()?;
        let new_root_id = InteriorNodeId::new(new_root_reservation.index().serialize());

        transaction.write_nodes(split_id, |node| node.set_parent(Some(new_root_id)))?;
        transaction.write_nodes(new_node_id, |node| node.set_parent(Some(new_root_id)))?;

        create_new_root(
            transaction,
            new_root_reservation,
            split_id.into(),
            split_key,
            new_node_id.into(),
        )?;

        trace!(
            parent_id=?new_root_id,
            left_id=?split_id,
            right_id=?new_node_id,
            ?split_key,
            "created new root"
        );
    }

    Ok(true)
}

#[instrument]
fn insert_child<TStorage: Storage, TKey: TreeKey>(
    transaction: &mut TreeTransaction<TStorage, TKey>,
    parent_id: InteriorNodeId,
    key: TKey,
    child_id: AnyNodeId,
) -> Result<(), TreeError<TStorage::PageId>> {
    transaction.write_nodes(parent_id, |node| node.insert_node(key, child_id))?;
    transaction.write_nodes(child_id, |x| x.set_parent(Some(parent_id)))?;

    Ok(())
}

#[instrument]
fn split_leaf<TStorage: Storage, TKey: TreeKey>(
    transaction: &mut TreeTransaction<TStorage, TKey>,
    leaf_id: LeafNodeId,
) -> Result<(), TreeError<TStorage::PageId>> {
    let parent_id = transaction.read_nodes(leaf_id, Node::parent)?.unwrap();

    let has_spare_capacity = transaction.read_nodes(parent_id, InteriorNode::has_spare_capacity)?;

    if !(has_spare_capacity) {
        error!("the node does not have spare capacity");
        panic!("the node does not have spare capacity",);
    }

    let new_leaf_reservation = transaction.reserve_node()?;
    let new_leaf_id = LeafNodeId::new(new_leaf_reservation.index().serialize());

    let new_leaf = transaction.write_nodes(leaf_id, |target_node| {
        let next = target_node.next();

        target_node
            .split(&MaterializedTopology::new(
                Some(parent_id),
                target_node.previous(),
                Some(new_leaf_id),
            ))
            .with_topology(Some(parent_id), Some(leaf_id), next)
            .build()
    })?;

    if let Some(next_leaf) = new_leaf.next() {
        transaction.write_nodes(next_leaf, |node| {
            node.set_previous(Some(new_leaf_id));
        })?;
    }

    let split_key = new_leaf.first_key().unwrap();
    trace!(
        node_id=?leaf_id,
        split_id=?new_leaf_id,
        ?split_key,
        "split leaf node"
    );

    transaction.insert_reserved(new_leaf_reservation, new_leaf)?;

    insert_child(transaction, parent_id, split_key, new_leaf_id.into())?;

    Ok(())
}

#[instrument(skip(value, transaction), fields(transaction_id=?transaction.id()))]
pub fn insert<TStorage: Storage, TKey: TreeKey>(
    transaction: &mut TreeTransaction<TStorage, TKey>,
    key: TKey,
    value: &[u8],
) -> Result<(), TreeError<TStorage::PageId>> {
    let root_index = transaction.get_root()?;
    let target_node_id = leaf_search(transaction, root_index, key)?;

    let (can_fit, parent) = transaction.read_nodes(target_node_id, |node| {
        (node.can_fit(value.len()), node.parent())
    })?;

    if !can_fit {
        if let Some(parent) = parent {
            if !transaction.read_nodes(parent, InteriorNode::has_spare_capacity)? {
                let _ = split_interior_node(transaction, parent)?;

                return insert(transaction, key, value);
            }
            split_leaf(transaction, target_node_id)?;

            return insert(transaction, key, value);
        }
        if !(root_index == target_node_id.into()) {
            error!(?root_index, ?target_node_id, "target node is not the root");
            panic!("target node is not the root");
        }

        split_leaf_root(transaction)?;

        return insert(transaction, key, value);
    }

    transaction.write_nodes(target_node_id, |node| node.insert(key, value))?;

    Ok(())
}
