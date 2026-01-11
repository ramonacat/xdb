use crate::bplustree::InteriorNodeReader;
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
        if node.is_leaf() {
            return node_index;
        }

        let interior_node_reader = InteriorNodeReader::new(node, transaction.key_size);

        let mut found_page_index = None;

        for (key_index, node_key) in interior_node_reader.keys().enumerate() {
            if node_key > key {
                let child_page: PageIndex = interior_node_reader.value_at(key_index).unwrap();

                found_page_index = Some(child_page);
            }
        }

        match found_page_index {
            Some(child_page_index) => leaf_search(transaction, child_page_index, key),
            None => interior_node_reader.last_value(),
        }
    });

    result.unwrap()
}
