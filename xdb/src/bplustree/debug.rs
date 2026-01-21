use pretty_assertions::assert_eq;
use std::collections::BTreeMap;

use crate::{
    bplustree::{AnyNodeId, Tree, TreeKey, TreeTransaction},
    storage::Storage,
};

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
    // TODO verify the topology
    // TODO verify the tree is balanced
    // TODO verify all the nodes are at least half-full
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
                super::node::AnyNodeKind::Interior(interior_node) => {
                    for (index, key) in interior_node.keys().enumerate() {
                        result.push((
                            if index > 0 {
                                interior_node.keys().nth(index - 1).copied()
                            } else {
                                start_min_key
                            },
                            Some(*key),
                            interior_node.value_at(index).unwrap(),
                        ));
                    }

                    if let Some(last_value) = interior_node.last_value() {
                        let keys = interior_node.keys().collect::<Vec<_>>();

                        result.push((
                            if keys.is_empty() {
                                start_min_key
                            } else {
                                keys.last().map(|x| **x)
                            },
                            start_max_key,
                            last_value,
                        ));
                    }
                }
                super::node::AnyNodeKind::Leaf(leaf_node) => {
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
