use bytemuck::Pod;

use crate::{
    bplustree::{
        TreeTransaction,
        node::{AnyNodeId, LeafNodeId},
    },
    storage::Storage,
};

// TODO return a Result, remove unwraps
pub(super) fn leaf_search<TStorage: Storage, TKey: Pod + PartialOrd>(
    transaction: &TreeTransaction<TStorage, TKey>,
    node_index: AnyNodeId,
    key: &TKey,
) -> LeafNodeId {
    // TODO the closure should just get the node reader as the argument here
    let result = transaction.read_node(node_index, |node| {
        let reader = match node {
            crate::bplustree::AnyNodeReader::Interior(reader) => reader,
            // TODO this should maybe be held by the reader which would do the conversion so we
            // don't share the `From`???
            crate::bplustree::AnyNodeReader::Leaf(_) => {
                return LeafNodeId::from_any(node_index);
            }
        };

        for (key_index, node_key) in reader.keys().enumerate() {
            if node_key > key {
                let child_page = reader.value_at(key_index).unwrap();

                return leaf_search(transaction, child_page, key);
            }
        }

        leaf_search(transaction, reader.last_value().unwrap(), key)
    });

    result.unwrap()
}

// TODO return a result, don't unwrap
pub(super) fn first_leaf<TStorage: Storage, TKey: Pod + PartialOrd>(
    transaction: &TreeTransaction<TStorage, TKey>,
    root: AnyNodeId,
) -> LeafNodeId {
    transaction
        .read_node(root, |reader| match reader {
            super::AnyNodeReader::Interior(interior_node_reader) => {
                first_leaf(transaction, interior_node_reader.first_value().unwrap())
            }
            super::AnyNodeReader::Leaf(_) => LeafNodeId::from_any(root),
        })
        .unwrap()
}
