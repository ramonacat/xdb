use bytemuck::Zeroable as _;

use crate::{
    bplustree::TreeTransaction,
    storage::{PageIndex, Storage},
};

// TODO use a separate type for NodeIndex
// // TODO return a Result, remove unwraps
pub(super) fn leaf_search<TStorage: Storage>(
    transaction: &TreeTransaction<TStorage>,
    node_index: PageIndex,
    key: &[u8],
) -> PageIndex {
    assert!(node_index != PageIndex::zeroed());

    // TODO the closure should just get the node reader as the argument here
    let result = transaction.read_node(node_index, |node| {
        let reader = match node {
            crate::bplustree::NodeReader::Interior(reader) => reader,
            crate::bplustree::NodeReader::Leaf(_) => return node_index,
        };

        for (key_index, node_key) in reader.keys().enumerate() {
            if node_key > key {
                let child_page: PageIndex = reader.value_at(key_index).unwrap();

                return leaf_search(transaction, child_page, key);
            }
        }

        reader.last_value()
    });

    result.unwrap()
}
