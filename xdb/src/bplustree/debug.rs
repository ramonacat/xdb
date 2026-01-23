use log::debug;
use pretty_assertions::assert_eq;
use std::collections::BTreeMap;

use crate::{
    bplustree::{
        AnyNodeId, InteriorNodeId, Node as _, Tree, TreeKey, TreeTransaction,
        algorithms::{first_leaf, last_leaf},
    },
    storage::Storage,
};

use super::node::AnyNodeKind;

pub fn assert_tree_equal<TStorage: Storage, TKey: TreeKey, TRightKey: TreeKey>(
    left: &Tree<TStorage, TKey>,
    right: &BTreeMap<TRightKey, Vec<u8>>,
    key_convert: impl Fn(TKey) -> TRightKey,
) {
    assert_eq!(
        left.iter()
            .unwrap()
            .map(|x| x.unwrap())
            .map(|(k, v)| (key_convert(k), v))
            .collect::<Vec<_>>(),
        right
            .iter()
            .map(|(x, y)| (*x, y.clone()))
            .collect::<Vec<_>>(),
    );
    assert_eq!(
        left.iter()
            .unwrap()
            .rev()
            .map(|x| x.unwrap())
            .map(|(k, v)| (key_convert(k), v))
            .collect::<Vec<_>>(),
        right
            .iter()
            .rev()
            .map(|(x, y)| (*x, y.clone()))
            .collect::<Vec<_>>(),
    );
}

pub fn assert_properties<TStorage: Storage, TKey: TreeKey>(
    transaction: &TreeTransaction<TStorage, TKey>,
) {
    if !cfg!(debug_assertions) {
        return;
    }

    assert_keys_lower_than_parent(transaction, None, None, None);
    assert_tree_balanced(transaction, None);
    assert_correct_topology(transaction, None, None, None, None);
}

fn assert_keys_lower_than_parent<TStorage: Storage, TKey: TreeKey>(
    transaction: &TreeTransaction<TStorage, TKey>,
    start_id: Option<AnyNodeId>,
    start_min_key: Option<TKey>,
    start_max_key: Option<TKey>,
) {
    let start_id = start_id.unwrap_or_else(|| transaction.get_root().unwrap());

    let limits: Vec<(Option<TKey>, Option<TKey>, AnyNodeId)> = transaction
        .read_nodes(start_id, |node| {
            let mut result = vec![];

            match node.as_any() {
                AnyNodeKind::Interior(interior_node) => {
                    for (index, key) in interior_node.keys() {
                        result.push((
                            index.key_before().map_or(start_min_key, |key_before| {
                                interior_node.key_at(key_before)
                            }),
                            Some(key),
                            interior_node.value_at(index.value_before()).unwrap(),
                        ));
                    }

                    if let Some(last_value) = interior_node.last_value() {
                        let keys = interior_node.keys().collect::<Vec<_>>();

                        result.push((
                            if keys.is_empty() {
                                start_min_key
                            } else {
                                keys.last().copied().map(|x| x.1)
                            },
                            start_max_key,
                            last_value,
                        ));
                    }
                }
                AnyNodeKind::Leaf(leaf_node) => {
                    for entry in leaf_node.entries() {
                        if let Some(max_key) = start_max_key {
                            assert!(entry.key() < max_key);
                        }

                        if let Some(min_key) = start_min_key {
                            assert!(entry.key() >= min_key);
                        }
                    }
                }
            }

            result
        })
        .unwrap();

    for (min_key, max_key, node_id) in limits {
        assert_keys_lower_than_parent(
            transaction,
            Some(node_id),
            min_key.or(start_min_key),
            max_key,
        );
    }
}

fn assert_tree_balanced<TStorage: Storage, TKey: TreeKey>(
    transaction: &TreeTransaction<TStorage, TKey>,
    root_id: Option<AnyNodeId>,
) {
    let root_id = root_id.unwrap_or_else(|| transaction.get_root().unwrap());
    let root_children = transaction
        .read_nodes(root_id, |root| match root.as_any() {
            AnyNodeKind::Interior(interior_node) => interior_node.values().collect::<Vec<_>>(),
            AnyNodeKind::Leaf(_) => {
                vec![]
            }
        })
        .unwrap();

    let mut heights = vec![];
    for (_, child) in root_children {
        assert_tree_balanced(transaction, Some(child));
        heights.push(calculate_height(transaction, child));
    }

    assert!(
        heights.iter().max().copied().unwrap_or(0) - heights.iter().min().copied().unwrap_or(0)
            <= 1
    );
}

fn calculate_height<TStorage: Storage, TKey: TreeKey>(
    transaction: &TreeTransaction<TStorage, TKey>,
    node_id: AnyNodeId,
) -> usize {
    let children = transaction
        .read_nodes(node_id, |node| match node.as_any() {
            AnyNodeKind::Interior(interior_node) => interior_node.values().collect::<Vec<_>>(),
            AnyNodeKind::Leaf(_) => {
                vec![]
            }
        })
        .unwrap();

    let max_height = children
        .iter()
        .map(|x| calculate_height(transaction, x.1))
        .max();

    1 + max_height.unwrap_or(0)
}

fn assert_correct_topology<TStorage: Storage, TKey: TreeKey>(
    transaction: &TreeTransaction<TStorage, TKey>,
    parent_id: Option<InteriorNodeId>,
    node_id: Option<AnyNodeId>,
    previous: Option<AnyNodeId>,
    next: Option<AnyNodeId>,
) {
    let node_id = node_id.unwrap_or_else(|| transaction.get_root().unwrap());

    debug!(
        "assert_correct_topology for {node_id:?} (parent: {parent_id:?}, previous: {previous:?}, next: {next:?})"
    );

    let children = transaction
        .read_nodes(node_id, |node| {
            assert_eq!(node.parent(), parent_id);

            match node.as_any() {
                AnyNodeKind::Interior(interior_node) => interior_node.values().collect::<Vec<_>>(),
                AnyNodeKind::Leaf(leaf_node) => {
                    assert_eq!(previous, leaf_node.previous().map(Into::into));
                    assert_eq!(next, leaf_node.next().map(Into::into));

                    vec![]
                }
            }
        })
        .unwrap();

    for child in children.windows(3) {
        assert_correct_topology(
            transaction,
            Some(InteriorNodeId::from_any(node_id)),
            Some(child[1].1),
            Some(child[0].1),
            Some(child[2].1),
        );
    }

    if let Some(first) = children.first() {
        assert_correct_topology(
            transaction,
            Some(InteriorNodeId::from_any(node_id)),
            Some(first.1),
            previous.map(|x| last_leaf(transaction, x).unwrap().into()),
            children
                .get(1)
                .map(|x| x.1)
                .or_else(|| next.map(|x| first_leaf(transaction, x).unwrap().into())),
        );
    }

    if children.len() > 1
        && let Some(last) = children.last()
    {
        assert_correct_topology(
            transaction,
            Some(InteriorNodeId::from_any(node_id)),
            Some(last.1),
            children
                .len()
                .checked_sub(2)
                .map(|x| children.get(x).unwrap())
                .map(|x| x.1),
            next.map(|x| first_leaf(transaction, x).unwrap().into()),
        );
    }
}
