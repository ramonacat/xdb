use log::debug;

use crate::{
    bplustree::{
        AnyNodeId, InteriorNode, InteriorNodeId, LeafNodeId, Node as _, NodeId as _, TreeError,
        TreeKey, TreeTransaction, algorithms::leaf_search,
        node::leaf::builder::MaterializedTopology,
    },
    storage::{PageReservation as _, Storage},
};

fn create_new_root<'storage, TStorage: Storage, TKey: TreeKey>(
    transaction: &mut TreeTransaction<'storage, TStorage, TKey>,
    reservation: <TStorage as Storage>::PageReservation<'storage>,
    left: AnyNodeId,
    key: TKey,
    right: AnyNodeId,
) -> Result<(), TreeError> {
    let new_root_id = InteriorNodeId::new(reservation.index());
    let new_root = InteriorNode::<TKey>::new(None, left, key, right);

    transaction.insert_reserved(reservation, new_root)?;
    transaction.write_header(|header| header.root = new_root_id.page())?;

    Ok(())
}

fn split_leaf_root<TStorage: Storage, TKey: TreeKey>(
    transaction: &mut TreeTransaction<TStorage, TKey>,
) -> Result<(), TreeError> {
    let root_id = transaction.get_root()?;

    assert!(transaction.read_nodes(root_id, super::super::node::AnyNode::is_leaf)?);

    let root_id = LeafNodeId::from_any(root_id);

    let new_root_reservation = transaction.reserve_node()?;
    let new_root_id = InteriorNodeId::new(new_root_reservation.index());

    let new_leaf_reservation = transaction.reserve_node()?;
    let new_leaf_id = LeafNodeId::new(new_leaf_reservation.index());

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
    target: InteriorNodeId,
) -> Result<bool, TreeError> {
    let parent = transaction.read_nodes(target, super::super::node::Node::parent)?;

    if let Some(parent) = parent
        && !transaction.read_nodes(
            parent,
            super::super::node::interior::InteriorNode::has_spare_capacity,
        )?
    {
        let _ = split_interior_node(transaction, parent)?;

        return Ok(false);
    }

    let parent = transaction.read_nodes(target, super::super::node::Node::parent)?;

    let new_node_reservation = transaction.reserve_node()?;
    let new_node_id = InteriorNodeId::new(new_node_reservation.index());

    let (split_key, new_node) =
        transaction.write_nodes(target, super::super::node::interior::InteriorNode::split)?;

    for (_, child) in new_node.values() {
        transaction.write_nodes(child, |node| node.set_parent(Some(new_node_id)))?;
    }

    transaction.insert_reserved(new_node_reservation, new_node)?;

    if let Some(parent) = parent {
        debug!("split interior node {target:?} into new node {new_node_id:?}");
        insert_child(transaction, parent, split_key, new_node_id.into())?;
    } else {
        let new_root_reservation = transaction.reserve_node()?;
        let new_root_id = InteriorNodeId::new(new_root_reservation.index());

        transaction.write_nodes(target, |node| node.set_parent(Some(new_root_id)))?;
        transaction.write_nodes(new_node_id, |node| node.set_parent(Some(new_root_id)))?;

        create_new_root(
            transaction,
            new_root_reservation,
            target.into(),
            split_key,
            new_node_id.into(),
        )?;
        debug!("created new root {new_root_id:?} at split key {split_key:?}");
    }

    Ok(true)
}

fn insert_child<TStorage: Storage, TKey: TreeKey>(
    transaction: &mut TreeTransaction<TStorage, TKey>,
    target: InteriorNodeId,
    key: TKey,
    child_id: AnyNodeId,
) -> Result<(), TreeError> {
    transaction.write_nodes(target, |node| node.insert_node(key, child_id))?;
    transaction.write_nodes(child_id, |x| x.set_parent(Some(target)))?;

    Ok(())
}

fn split_leaf<TStorage: Storage, TKey: TreeKey>(
    transaction: &mut TreeTransaction<TStorage, TKey>,
    target_node_id: LeafNodeId,
) -> Result<(), TreeError> {
    let parent = transaction
        .read_nodes(target_node_id, super::super::node::Node::parent)?
        .unwrap();

    let has_spare_capacity = transaction.read_nodes(
        parent,
        super::super::node::interior::InteriorNode::has_spare_capacity,
    )?;

    assert!(has_spare_capacity);

    let new_leaf_reservation = transaction.reserve_node()?;
    let new_leaf_id = LeafNodeId::new(new_leaf_reservation.index());

    let new_leaf = transaction.write_nodes(target_node_id, |target_node| {
        let next = target_node.next();

        target_node
            .split(&MaterializedTopology::new(
                Some(parent),
                target_node.previous(),
                Some(new_leaf_id),
            ))
            .with_topology(Some(parent), Some(target_node_id), next)
            .build()
    })?;

    if let Some(next_leaf) = new_leaf.next() {
        transaction.write_nodes(next_leaf, |node| {
            node.set_previous(Some(new_leaf_id));
        })?;
    }

    let split_key = new_leaf.first_key().unwrap();
    debug!("split {target_node_id:?} into {new_leaf_id:?} at key {split_key:?}");

    transaction.insert_reserved(new_leaf_reservation, new_leaf)?;

    insert_child(transaction, parent, split_key, new_leaf_id.into())?;

    Ok(())
}

pub fn insert<TStorage: Storage, TKey: TreeKey>(
    transaction: &mut TreeTransaction<TStorage, TKey>,
    key: TKey,
    value: &[u8],
) -> Result<(), TreeError> {
    let root_index = transaction.get_root()?;
    let target_node_id = leaf_search(transaction, root_index, key)?;

    let (can_fit, parent) = transaction.read_nodes(target_node_id, |node| {
        (node.can_fit(value.len()), node.parent())
    })?;

    if !can_fit {
        if let Some(parent) = parent {
            if !transaction.read_nodes(
                parent,
                super::super::node::interior::InteriorNode::has_spare_capacity,
            )? {
                let _ = split_interior_node(transaction, parent)?;

                return insert(transaction, key, value);
            }
            split_leaf(transaction, target_node_id)?;

            return insert(transaction, key, value);
        }
        assert!(root_index == target_node_id.into());

        split_leaf_root(transaction)?;

        return insert(transaction, key, value);
    }

    transaction.write_nodes(target_node_id, |node| node.insert(key, value))?;

    debug!("inserted {key:?} into {target_node_id:?}");

    Ok(())
}
