use crate::bplustree::node::AnyNodeKind;
use crate::bplustree::node::interior::InteriorInsertResult;
use crate::bplustree::{InteriorNode, InteriorNodeId, LeafInsertResult, LeafNode, NodeId as _};
use crate::storage::PageReservation as _;
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

pub fn insert<TStorage: Storage, TKey: Pod + Ord>(
    transaction: &TreeTransaction<TStorage, TKey>,
    key: TKey,
    value: &[u8],
) -> Result<(), TreeError> {
    let root_index = AnyNodeId::new(transaction.read_header(|h| h.root)?);
    let target_node_id = leaf_search(transaction, root_index, &key)?;

    let (parent_index, insert_result) = transaction.write_node(target_node_id, |node| {
        // TODO parent_index should be a part of insert_result
        let parent_index = node.parent();
        let insert_result = node.insert(key, value);
        (parent_index, insert_result)
    })?;
    let insert_result = insert_result?;

    match insert_result {
        LeafInsertResult::Done => Ok(()),
        // TODO extract insert_split_leaf()
        LeafInsertResult::Split { mut new_node } => {
            let new_node_reservation = transaction.reserve_node()?;
            let new_node_id = LeafNodeId::new(new_node_reservation.index());

            let next = transaction.read_node(target_node_id, |node| node.next())?;

            if let Some(parent_id) = parent_index {
                let new_leaf_reservation = transaction.reserve_node().unwrap();
                let new_leaf_id = LeafNodeId::new(new_leaf_reservation.index());

                let grandparent_id = transaction.read_node(parent_id, |reader| reader.parent())?;
                let new_grandparent_reservation = transaction.reserve_node()?;

                let split_node = transaction.write_node(parent_id, |node| {
                    match node.insert_node(&new_node.first_key().unwrap(), new_node_id.into()) {
                        InteriorInsertResult::Ok => None,
                        // TODO extract insert_split_interior_node
                        InteriorInsertResult::Split(mut new_node) => {
                            // TODO get rid of the direct writes to node.data here, and make
                            // the node expose some API
                            let new_leaf_id = LeafNodeId::new(new_leaf_reservation.index());
                            let mut new_leaf = LeafNode::<TKey>::new();

                            new_leaf.set_links(Some(parent_id), Some(new_leaf_id), next);

                            new_node.set_first_pointer(new_leaf_id.into());
                            new_node.set_parent(grandparent_id);

                            Some((new_leaf, new_node))
                        }
                    }
                })?;

                match split_node {
                    Some((new_leaf, mut new_interior)) => {
                        let new_interior_reservation = transaction.reserve_node()?;
                        let new_interior_id = InteriorNodeId::new(new_interior_reservation.index());

                        let grandparent_id = match grandparent_id {
                            None => {
                                let mut new_grandparent = InteriorNode::<TKey>::new();
                                let new_grandparent_id =
                                    InteriorNodeId::new(new_grandparent_reservation.index());
                                transaction.write_header(|h| h.root = new_grandparent_id.page())?;

                                new_grandparent.set_first_pointer(parent_id.into());

                                transaction.insert_reserved(
                                    new_grandparent_reservation,
                                    Page::from_data(new_grandparent),
                                )?;

                                new_grandparent_id
                            }
                            Some(x) => x,
                        };

                        transaction.write_node(grandparent_id, |node| {
                            let grandparent_insert_result = node.insert_node(
                                &new_interior.first_key().unwrap(),
                                new_interior_id.into(),
                            );
                            match grandparent_insert_result {
                                InteriorInsertResult::Ok => {}
                                InteriorInsertResult::Split(_) => todo!(),
                            }
                        })?;

                        new_interior.set_first_pointer(new_leaf_id.into());

                        transaction
                            .insert_reserved(new_leaf_reservation, Page::from_data(new_leaf))?;
                        transaction.insert_reserved(
                            new_interior_reservation,
                            Page::from_data(*new_interior),
                        )?;
                    }
                    None => {
                        transaction.write_node(target_node_id, |target_node| {
                            target_node.set_links(
                                target_node.parent(),
                                target_node.previous(),
                                Some(new_node_id),
                            );
                        })?;

                        new_node.set_links(Some(parent_id), Some(target_node_id), next);
                    }
                }
            } else {
                let new_root_page = Page::from_data(InteriorNode::create_root(
                    &[&new_node.first_key().unwrap()],
                    &[root_index, new_node_id.into()],
                ));
                let new_root_page_index = transaction.insert(new_root_page)?;
                let new_root_page_id = InteriorNodeId::new(new_root_page_index);

                new_node.set_links(Some(new_root_page_id), Some(target_node_id), next);

                transaction.write_node(target_node_id, |target_node| {
                    target_node.set_links(
                        Some(new_root_page_id),
                        target_node.previous(),
                        Some(new_node_id),
                    );
                })?;

                transaction.write_header(|header| header.root = new_root_page_index)?;
            }

            transaction.insert_reserved(new_node_reservation, Page::from_data(*new_node))?;

            Ok(())
        }
    }
}
