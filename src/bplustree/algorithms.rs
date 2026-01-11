use crate::{
    bplustree::{
        TreeTransaction,
        node::{AnyNodeId, LeafNodeId},
    },
    storage::Storage,
};

// TODO use a separate type for NodeIndex
// // TODO return a Result, remove unwraps
pub(super) fn leaf_search<TStorage: Storage>(
    transaction: &TreeTransaction<TStorage>,
    node_index: AnyNodeId,
    key: &[u8],
) -> LeafNodeId {
    // TODO the closure should just get the node reader as the argument here
    let result = transaction.read_node(node_index, |node| {
        let reader = match node {
            crate::bplustree::AnyNodeReader::Interior(reader) => reader,
            // TODO this should maybe be held by the reader which would do the conversion so we
            // don't share the `From`???
            crate::bplustree::AnyNodeReader::Leaf(_) => {
                return LeafNodeId::from_unknown(node_index);
            }
        };

        for (key_index, node_key) in reader.keys().enumerate() {
            if node_key > key {
                let child_page = reader.value_at(key_index).unwrap();

                return leaf_search(transaction, child_page, key);
            }
        }

        leaf_search(transaction, reader.last_value(), key)
    });

    result.unwrap()
}
