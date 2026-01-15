use crate::bplustree::node::AnyNodeKind;
use crate::bplustree::node::interior::InteriorInsertResult;
use crate::bplustree::{InteriorNode, InteriorNodeId, LeafInsertResult, NodeId};
use crate::storage::PageReservation;
use crate::{bplustree::Node as _, page::Page};
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

pub(super) fn first_leaf<TStorage: Storage, TKey: Pod + Ord>(
    transaction: &TreeTransaction<TStorage, TKey>,
    root: AnyNodeId,
) -> Result<LeafNodeId, TreeError> {
    transaction.read_node(root, |node| match node.as_any() {
        AnyNodeKind::Interior(interior_node_reader) => {
            first_leaf(transaction, interior_node_reader.first_value().unwrap())
        }
        AnyNodeKind::Leaf(_) => Ok(LeafNodeId::from_any(root)),
    })?
}

pub(super) fn last_leaf<TStorage: Storage, TKey: Pod + Ord>(
    transaction: &TreeTransaction<TStorage, TKey>,
    root: AnyNodeId,
) -> Result<LeafNodeId, TreeError> {
    transaction.read_node(root, |node| match node.as_any() {
        AnyNodeKind::Interior(interior_node_reader) => {
            last_leaf(transaction, interior_node_reader.last_value().unwrap())
        }
        AnyNodeKind::Leaf(_) => Ok(LeafNodeId::from_any(root)),
    })?
}

fn create_new_root<'storage, TStorage: Storage, TKey: Pod + Ord>(
    transaction: &TreeTransaction<'storage, TStorage, TKey>,
    reservation: <TStorage as Storage>::PageReservation<'storage>,
    left: AnyNodeId,
    key: TKey,
    right: AnyNodeId,
) -> Result<(), TreeError> {
    let new_root_id = InteriorNodeId::new(reservation.index());
    let mut new_root = InteriorNode::<TKey>::new();

    new_root.set_first_pointer(left);
    match new_root.insert_node(&key, right) {
        InteriorInsertResult::Ok => {}
        // TODO this can only happen if the key + 2*leaf_id cannot fit into the node
        InteriorInsertResult::Split => todo!(),
    }

    transaction.insert_reserved(reservation, Page::from_data(new_root))?;

    transaction.write_header(|header| header.root = new_root_id.page())?;

    Ok(())
}

fn split_leaf_root<TStorage: Storage, TKey: Pod + Ord>(
    transaction: &TreeTransaction<TStorage, TKey>,
) -> Result<(), TreeError> {
    let root_id = transaction.read_header(|header| header.root)?;
    let root_id = LeafNodeId::new(root_id);

    let new_root_reservation = transaction.reserve_node()?;
    let new_root_id = InteriorNodeId::new(new_root_reservation.index());

    let new_leaf_reservation = transaction.reserve_node()?;
    let new_leaf_id = LeafNodeId::new(new_leaf_reservation.index());

    let new_leaf = transaction.write_node(root_id, |root| {
        let mut new_leaf = root.split();

        root.set_links(Some(new_root_id), None, Some(new_leaf_id));
        new_leaf.set_links(Some(new_root_id), Some(root_id), None);

        new_leaf
    })?;

    transaction.insert_reserved(new_leaf_reservation, Page::from_data(new_leaf))?;
    create_new_root(
        transaction,
        new_root_reservation,
        root_id.into(),
        new_leaf.first_key().unwrap(),
        new_leaf_id.into(),
    )?;

    Ok(())
}

fn split_interior_node<TStorage: Storage, TKey: Pod + Ord>(
    transaction: &TreeTransaction<TStorage, TKey>,
    target: InteriorNodeId,
) -> Result<(TKey, InteriorNodeId), TreeError> {
    let parent = transaction.read_node(target, |target_node| target_node.parent())?;

    let new_node_reservation = transaction.reserve_node()?;
    let new_node_id = InteriorNodeId::new(new_node_reservation.index());

    let (split_key, new_node) = transaction.write_node(target, |node| node.split())?;

    for child in new_node.values() {
        transaction.write_node(child, |node| node.set_parent(Some(new_node_id)))?;
    }

    transaction.insert_reserved(new_node_reservation, Page::from_data(new_node))?;

    match parent {
        Some(parent) => {
            eprintln!("split interior node {target:?} into new node {new_node_id:?}");
            insert_child(transaction, parent, split_key, new_node_id.into())?;
        }
        None => {
            let new_root_reservation = transaction.reserve_node()?;
            let new_root_id = InteriorNodeId::new(new_root_reservation.index());

            transaction.write_node(target, |node| node.set_parent(Some(new_root_id)))?;
            transaction.write_node(new_node_id, |node| node.set_parent(Some(new_root_id)))?;

            create_new_root(
                transaction,
                new_root_reservation,
                target.into(),
                split_key,
                new_node_id.into(),
            )?;
        }
    };

    Ok((split_key, new_node_id))
}

fn insert_child<TStorage: Storage, TKey: Pod + Ord>(
    transaction: &TreeTransaction<TStorage, TKey>,
    target: InteriorNodeId,
    key: TKey,
    child_id: AnyNodeId,
) -> Result<(), TreeError> {
    let insert_node_result =
        transaction.write_node(target, |node| node.insert_node(&key, child_id))?;
    transaction.write_node(child_id, |x| x.set_parent(Some(target)))?;

    match insert_node_result {
        InteriorInsertResult::Split => {
            let (split_key, new_node_id) = split_interior_node(transaction, target)?;

            let target = if key < split_key { target } else { new_node_id };

            insert_child(transaction, target, key, child_id)
        }
        InteriorInsertResult::Ok => Ok(()),
    }
}

fn split_leaf<TStorage: Storage, TKey: Pod + Ord>(
    transaction: &TreeTransaction<TStorage, TKey>,
    target_node_id: LeafNodeId,
) -> Result<(), TreeError> {
    let parent = transaction.read_node(target_node_id, |node| node.parent())?;

    match parent {
        // the leaf is the root
        None => {
            split_leaf_root(transaction)?;
        }
        Some(parent) => {
            let new_leaf_reservation = transaction.reserve_node()?;
            let new_leaf_id = LeafNodeId::new(new_leaf_reservation.index());

            let new_leaf = transaction.write_node(target_node_id, |target_node| {
                let mut new_leaf = target_node.split();

                new_leaf.set_links(Some(parent), Some(target_node_id), target_node.next());
                target_node.set_links(Some(parent), target_node.previous(), Some(new_leaf_id));

                new_leaf
            })?;
            if let Some(next_leaf) = new_leaf.next() {
                transaction.write_node(next_leaf, |node| {
                    node.set_links(node.parent(), Some(new_leaf_id), node.next());
                })?;
            }

            let split_key = new_leaf.first_key().unwrap();

            transaction.insert_reserved(new_leaf_reservation, Page::from_data(new_leaf))?;

            insert_child(transaction, parent, split_key, new_leaf_id.into())?;
        }
    }

    Ok(())
}

pub fn insert<TStorage: Storage, TKey: Pod + Ord>(
    transaction: &TreeTransaction<TStorage, TKey>,
    key: TKey,
    value: &[u8],
) -> Result<(), TreeError> {
    let root_index = AnyNodeId::new(transaction.read_header(|h| h.root)?);
    let target_node_id = leaf_search(transaction, root_index, &key)?;

    let insert_result = transaction.write_node(target_node_id, |node| node.insert(key, value))?;
    let insert_result = insert_result?;

    match insert_result {
        LeafInsertResult::Done => Ok(()),
        LeafInsertResult::Split => {
            split_leaf(transaction, target_node_id)?;

            insert(transaction, key, value)
        }
    }
}
