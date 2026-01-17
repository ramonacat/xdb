use bytemuck::Pod;

use crate::{
    bplustree::{TreeError, TreeTransaction, algorithms::leaf_search},
    storage::Storage,
};

pub fn delete<TStorage: Storage, TKey: Pod + Ord>(
    transaction: &TreeTransaction<TStorage, TKey>,
    key: TKey,
) -> Result<Option<Vec<u8>>, TreeError> {
    let starting_leaf = leaf_search(transaction, transaction.get_root()?, &key)?;

    let deleted = transaction.write_node(starting_leaf, |node| node.delete(key))?;

    // TODO check if the node needs to be merged, and do the merge here

    Ok(deleted)
}
