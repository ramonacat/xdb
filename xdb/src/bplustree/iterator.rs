use crate::{
    bplustree::{
        LeafNodeId, TreeError, TreeKey, TreeTransaction,
        algorithms::{first_leaf, last_leaf},
    },
    storage::Storage,
};

pub(super) type TreeIteratorItem<TKey> = Result<(TKey, Vec<u8>), TreeError>;

pub(super) struct TreeIterator<'tree, T: Storage, TKey> {
    transaction: TreeTransaction<'tree, T, TKey>,
    current_forward_leaf: LeafNodeId,
    forward_index: usize,
    current_backward_leaf: LeafNodeId,
    backward_index: usize,
}

impl<'tree, T: Storage, TKey: TreeKey> TreeIterator<'tree, T, TKey> {
    pub fn new(mut transaction: TreeTransaction<'tree, T, TKey>) -> Result<Self, TreeError> {
        let root = transaction.get_root()?;
        let starting_leaf_forwards = first_leaf(&mut transaction, root)?;
        let starting_leaf_backwards = last_leaf(&mut transaction, root)?;

        let backward_index =
            transaction.read_nodes(starting_leaf_backwards, super::node::leaf::LeafNode::len)?;

        Ok(Self {
            transaction,
            current_forward_leaf: starting_leaf_forwards,
            current_backward_leaf: starting_leaf_backwards,
            forward_index: 0,
            backward_index,
        })
    }
}

enum IteratorResult<TKey> {
    Value(TreeIteratorItem<TKey>),
    Next(LeafNodeId),
    None,
}

impl<T: Storage, TKey: TreeKey> Iterator for TreeIterator<'_, T, TKey> {
    type Item = Result<(TKey, Vec<u8>), TreeError>;

    // TODO get rid of all the unwraps!
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_forward_leaf == self.current_backward_leaf
            && self.forward_index == self.backward_index
        {
            return None;
        }

        let read_result = self
            .transaction
            .read_nodes(self.current_forward_leaf, |node| {
                let entry = node.entry(self.forward_index);
                match entry {
                    Some(entry) => {
                        self.forward_index += 1;

                        IteratorResult::Value(Ok((entry.key(), entry.value().to_vec())))
                    }
                    None => node
                        .next()
                        .map_or(IteratorResult::None, IteratorResult::Next),
                }
            })
            .unwrap();

        match read_result {
            IteratorResult::Value(x) => Some(x),
            IteratorResult::Next(next_leaf) => {
                self.current_forward_leaf = next_leaf;
                self.forward_index = 0;

                self.next()
            }
            IteratorResult::None => None,
        }
    }
}

impl<T: Storage, TKey: TreeKey> DoubleEndedIterator for TreeIterator<'_, T, TKey> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.current_forward_leaf == self.current_backward_leaf
            && self.forward_index == self.backward_index
        {
            return None;
        }

        let read_result = self
            .transaction
            .read_nodes(self.current_backward_leaf, |node| {
                let entry = if self.backward_index == 0 {
                    None
                } else {
                    node.entry(self.backward_index - 1)
                };

                match entry {
                    Some(entry) => {
                        self.backward_index -= 1;

                        IteratorResult::Value(Ok((entry.key(), entry.value().to_vec())))
                    }
                    None => node
                        .previous()
                        .map_or(IteratorResult::None, IteratorResult::Next),
                }
            })
            .unwrap();

        match read_result {
            IteratorResult::Value(x) => Some(x),
            IteratorResult::Next(next_leaf) => {
                self.current_backward_leaf = next_leaf;
                self.backward_index = self
                    .transaction
                    .read_nodes(next_leaf, super::node::leaf::LeafNode::len)
                    .unwrap();

                self.next_back()
            }
            IteratorResult::None => None,
        }
    }
}
