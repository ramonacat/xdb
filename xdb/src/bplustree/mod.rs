pub mod algorithms;
pub mod debug;
pub mod dot;
mod node;

use std::marker::PhantomData;

use crate::bplustree::algorithms::first_leaf;
use crate::bplustree::algorithms::last_leaf;
use crate::bplustree::node::AnyNodeId;
use crate::bplustree::node::InteriorNodeId;
use crate::bplustree::node::LeafNodeId;
use crate::bplustree::node::Node;
use crate::bplustree::node::NodeId;
use crate::bplustree::node::interior::InteriorNode;
use crate::bplustree::node::leaf::LeafNode;
use crate::page::Page;
use crate::storage::PageIndex;
use crate::storage::PageReservation;
use crate::storage::Storage;
use crate::storage::StorageError;
use crate::storage::Transaction;
use bytemuck::{Pod, Zeroable};
use thiserror::Error;

use crate::page::PAGE_DATA_SIZE;

const ROOT_NODE_TAIL_SIZE: usize = PAGE_DATA_SIZE - size_of::<u64>() - size_of::<PageIndex>();

struct TreeIterator<'tree, T: Storage, TKey, const REVERSE: bool> {
    transaction: TreeTransaction<'tree, T, TKey>,
    current_leaf: LeafNodeId,
    index: usize,
}

impl<'tree, T: Storage, TKey: Pod + Ord, const REVERSE: bool>
    TreeIterator<'tree, T, TKey, REVERSE>
{
    fn new(transaction: TreeTransaction<'tree, T, TKey>) -> Result<Self, TreeError> {
        // TODO introduce some better/more abstract API for reading the header?
        let root = transaction.read_header(|h| AnyNodeId::new(h.root))?;
        let starting_leaf = if !REVERSE {
            first_leaf(&transaction, root)?
        } else {
            last_leaf(&transaction, root)?
        };

        Ok(Self {
            transaction,
            current_leaf: starting_leaf,
            index: 0,
        })
    }
}

enum IteratorResult<TKey> {
    Value(TreeIteratorItem<TKey>),
    Next,
    None,
}

impl<'tree, T: Storage, TKey: Pod + Ord, const REVERSE: bool> Iterator
    for TreeIterator<'tree, T, TKey, REVERSE>
{
    type Item = Result<(TKey, Vec<u8>), TreeError>;

    // TODO get rid of all the unwraps!
    fn next(&mut self) -> Option<Self::Item> {
        let read_result = self
            .transaction
            .read_node(self.current_leaf, |node| {
                // TODO expose an API on node that will allow us to avoid collecting all the
                // entries here
                let entries = node.entries().collect::<Vec<_>>();

                let entry = if !REVERSE {
                    entries.get(self.index)
                } else if entries.is_empty() || self.index >= entries.len() {
                    None
                } else {
                    Some(&entries[entries.len() - self.index - 1])
                };

                match entry {
                    Some(entry) => {
                        self.index += 1;

                        IteratorResult::Value(Ok((entry.key(), entry.value().to_vec())))
                    }
                    None => {
                        if let Some(next_leaf) = if !REVERSE {
                            node.next()
                        } else {
                            node.previous()
                        } {
                            self.current_leaf = next_leaf;
                            self.index = 0;

                            IteratorResult::Next
                        } else {
                            IteratorResult::None
                        }
                    }
                }
            })
            .unwrap();

        match read_result {
            IteratorResult::Value(x) => Some(x),
            IteratorResult::Next => self.next(),
            IteratorResult::None => None,
        }
    }
}

#[derive(Debug)]
pub struct Tree<T: Storage, TKey> {
    storage: T,
    _key: PhantomData<TKey>,
}

pub struct TreeTransaction<'storage, TStorage: Storage + 'storage, TKey>
where
    Self: 'storage,
{
    transaction: TStorage::Transaction<'storage>,
    _key: PhantomData<&'storage TKey>,
}

impl<'storage, TStorage: Storage + 'storage, TKey: Pod + Ord>
    TreeTransaction<'storage, TStorage, TKey>
{
    fn read_header<TReturn>(
        &self,
        read: impl FnOnce(&TreeHeader) -> TReturn,
    ) -> Result<TReturn, TreeError> {
        Ok(self
            .transaction
            .read(PageIndex::zero(), |page| read(page.data()))?)
    }

    fn write_header<TReturn>(
        &self,
        write: impl FnOnce(&mut TreeHeader) -> TReturn,
    ) -> Result<TReturn, TreeError> {
        Ok(self
            .transaction
            .write(PageIndex::zero(), |page| write(page.data_mut()))?)
    }

    fn read_node<TReturn, TNodeId: NodeId>(
        &self,
        index: TNodeId,
        read: impl for<'node> FnOnce(&TNodeId::Node<TKey>) -> TReturn,
    ) -> Result<TReturn, TreeError> {
        Ok(self
            .transaction
            .read(index.page(), |page| read(page.data()))?)
    }

    fn write_node<TReturn, TNodeId: NodeId>(
        &self,
        index: TNodeId,
        write: impl for<'node> FnOnce(&mut TNodeId::Node<TKey>) -> TReturn,
    ) -> Result<TReturn, TreeError> {
        Ok(self
            .transaction
            .write(index.page(), |page| write(page.data_mut()))?)
    }

    fn reserve_node(&self) -> Result<TStorage::PageReservation<'storage>, TreeError> {
        Ok(self.transaction.reserve()?)
    }

    fn insert_reserved(
        &self,
        reservation: TStorage::PageReservation<'storage>,
        page: Page,
    ) -> Result<(), TreeError> {
        self.transaction.insert_reserved(reservation, page)?;

        Ok(())
    }
}

type TreeIteratorItem<TKey> = Result<(TKey, Vec<u8>), TreeError>;

impl<T: Storage, TKey: Pod + Ord> Tree<T, TKey> {
    // TODO also create a "new_read" method, or something like that (that reads a tree that already
    // exists from storage)
    pub fn new(mut storage: T) -> Result<Self, TreeError> {
        // TODO assert that the storage is empty, and that the header get's the 0th page, as we
        // depend on that invariant (i.e. PageIndex=0 must always refer to the TreeData and not to
        // a node)!

        TreeHeader::new_in(&mut storage, size_of::<TKey>())?;

        Ok(Self {
            storage,
            _key: PhantomData,
        })
    }

    pub fn iter(&self) -> Result<impl Iterator<Item = TreeIteratorItem<TKey>>, TreeError> {
        TreeIterator::<_, _, false>::new(self.transaction()?)
    }

    // TODO probably should just use iter with DoubleEndedIterator
    pub fn iter_reverse(&self) -> Result<impl Iterator<Item = TreeIteratorItem<TKey>>, TreeError> {
        TreeIterator::<_, _, true>::new(self.transaction()?)
    }

    pub fn transaction(&self) -> Result<TreeTransaction<'_, T, TKey>, TreeError> {
        Ok(TreeTransaction::<T, TKey> {
            transaction: self.storage.transaction()?,
            _key: PhantomData,
        })
    }
}

#[derive(Debug, Pod, Zeroable, Clone, Copy)]
#[repr(C)]
struct TreeHeader {
    key_size: u64,
    root: PageIndex,
    _unused: [u8; ROOT_NODE_TAIL_SIZE],
}

const _: () = assert!(
    size_of::<TreeHeader>() == PAGE_DATA_SIZE,
    "The Tree descriptor must have size of exactly one page"
);

#[derive(Debug, Error)]
pub enum TreeError {
    #[error("Storage error: {0}")]
    StorageError(#[from] StorageError),
}

impl TreeHeader {
    pub fn new_in<T: Storage>(storage: &mut T, key_size: usize) -> Result<(), TreeError> {
        let transaction = storage.transaction()?;

        let header_page = transaction.reserve()?;
        assert!(header_page.index() == PageIndex::zero());

        let root_index = transaction.insert(Page::from_data(LeafNode::<usize>::new()))?;

        let page = Page::from_data(Self {
            key_size: key_size as u64,
            root: root_index,
            _unused: [0; _],
        });

        transaction.insert_reserved(header_page, page)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::{
        collections::BTreeMap,
        fmt::{Debug, Display},
        io::Write,
        panic::{RefUnwindSafe, UnwindSafe, catch_unwind},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use crate::{
        bplustree::algorithms::insert,
        debug::BigKey,
        storage::in_memory::{InMemoryStorage, test::TestStorage},
    };
    use pretty_assertions::assert_eq;
    use tempfile::NamedTempFile;

    use super::*;

    // TODO assert on properties of the tree (balanced, etc.) where it makes sense
    // TODO print out a .dot file for failed tests

    #[test]
    fn node_accessor_entries() {
        let mut node = LeafNode::zeroed();

        assert!(matches!(node.entries().next(), None));

        node.insert(1usize, &[2; 16]).unwrap();

        let mut iter = node.entries();
        let first = iter.next().unwrap();
        assert!(first.key() == 1);
        assert!(first.value() == &[2; 16]);

        assert!(matches!(iter.next(), None));

        drop(iter);

        node.insert(2usize, &[1; 16]).unwrap();

        let mut iter = node.entries();

        let first = iter.next().unwrap();
        assert!(first.key() == 1);
        assert!(first.value() == &[2; 16]);

        let second = iter.next().unwrap();
        assert!(second.key() == 2);
        assert!(second.value() == &[1; 16]);

        assert!(matches!(iter.next(), None));
    }

    // TODO the below two tests are mostly copy-paste, refactor some sorta abstraction over them
    #[test]
    // TODO optimize this test
    fn insert_multiple_nodes() {
        let page_count = Arc::new(AtomicUsize::new(0));

        let storage = TestStorage::new(InMemoryStorage::new(), page_count.clone());
        let tree = Tree::<_, usize>::new(storage).unwrap();

        // TODO: find a more explicit way of counting nodes
        let mut i = 0usize;

        let tree_transaction = tree.transaction().unwrap();

        while page_count.load(Ordering::Relaxed) < 1024 {
            // make the value bigger with repeat so fewer inserts are needed and the test runs faster
            insert(
                &tree_transaction,
                i,
                &(u16::max_value() - i as u16).to_be_bytes().repeat(128),
            )
            .unwrap();

            i += 1;
        }

        let entry_count = i;

        let mut final_i = 0;

        for (i, item) in tree.iter().unwrap().enumerate() {
            assert!(i < entry_count);
            let (key, value) = item.unwrap();

            assert!(value == value[..size_of::<u16>()].repeat(128));

            let value: u16 = u16::from_be_bytes(value[0..size_of::<u16>()].try_into().unwrap());

            assert!(key == i);
            assert!(value == u16::max_value() - i as u16);

            final_i = i;
        }

        assert!(final_i == entry_count - 1);

        for (i, item) in tree.iter_reverse().unwrap().enumerate() {
            let i = entry_count - i - 1;

            assert!(i < entry_count);
            let (key, value) = item.unwrap();

            assert!(value == value[..size_of::<u16>()].repeat(128));

            let value = u16::from_be_bytes(value[0..size_of::<u16>()].try_into().unwrap());

            assert!(key == i);
            assert!(value == u16::max_value() - i as u16);
        }

        // TODO iterate the tree and ensure that all the keys are there, in correct order with
        // correct values
    }

    #[test]
    // TODO optimize this test
    fn variable_sized_keys() {
        let page_count = Arc::new(AtomicUsize::new(0));

        let storage = TestStorage::new(InMemoryStorage::new(), page_count.clone());
        let tree = Tree::<_, usize>::new(storage).unwrap();

        // TODO: find a more explicit way of counting nodes
        let mut i = 0usize;

        let transaction = tree.transaction().unwrap();

        // 10 pages should give us a resonable number of node splits to assume that the basic logic
        //    works
        while page_count.load(Ordering::Relaxed) < 10 {
            let value: &[u8] = match i % 8 {
                0 | 7 | 6 | 5 => &(i as u64).to_be_bytes(),
                4 | 3 => &(i as u32).to_be_bytes(),
                2 => &(i as u16).to_be_bytes(),
                1 => &(i as u8).to_be_bytes(),
                _ => unreachable!(),
            };

            // make the value bigger with repeat so fewer inserts are needed and the test runs faster
            insert(&transaction, i, &value.repeat(128)).unwrap();

            i += 1;
        }

        let entry_count = i;

        for (i, item) in tree.iter().unwrap().enumerate() {
            assert!(i < entry_count);
            let (key, value) = item.unwrap();

            let value_matches_expected = match i % 8 {
                0 | 7 | 6 | 5 => {
                    i as u64 == u64::from_be_bytes(value[..size_of::<u64>()].try_into().unwrap())
                }
                4 | 3 => {
                    i as u32 == u32::from_be_bytes(value[..size_of::<u32>()].try_into().unwrap())
                }
                2 => i as u16 == u16::from_be_bytes(value[..size_of::<u16>()].try_into().unwrap()),
                1 => i as u8 == u8::from_be_bytes(value[..size_of::<u8>()].try_into().unwrap()),
                _ => unreachable!(),
            };

            assert!(value[..value.len() / 128].repeat(128) == value);
            assert!(key == i);
            assert!(value_matches_expected);
        }
    }

    #[test]
    fn insert_reverse() {
        let storage = InMemoryStorage::new();
        let tree = Tree::new(storage).unwrap();
        let transaction = tree.transaction().unwrap();

        insert(&transaction, 1, &[0]).unwrap();
        insert(&transaction, 0, &[0]).unwrap();

        let result = tree.iter().unwrap().map(|x| x.unwrap()).collect::<Vec<_>>();

        assert!(result == &[(0, vec![0]), (1, vec![0])]);
    }

    #[test]
    fn same_key_overrides() {
        let storage = InMemoryStorage::new();
        let tree = Tree::new(storage).unwrap();
        let transaction = tree.transaction().unwrap();

        insert(&transaction, 1, &0u8.to_ne_bytes()).unwrap();
        insert(&transaction, 1, &1u8.to_ne_bytes()).unwrap();

        let result = tree.iter().unwrap().map(|x| x.unwrap()).collect::<Vec<_>>();

        assert_eq!(result, vec![(1, 1u8.to_ne_bytes().to_vec())]);
    }

    fn test_from_data<TKey: Pod + Ord + Debug + RefUnwindSafe + Display + UnwindSafe>(
        data: Vec<(TKey, Vec<u8>)>,
    ) {
        let storage = InMemoryStorage::new();
        let tree = Tree::new(storage).unwrap();
        let transaction = tree.transaction().unwrap();

        let mut rust_tree = BTreeMap::new();

        let result = catch_unwind(|| {
            for (key, value) in data {
                insert(&transaction, key, &value).unwrap();
                rust_tree.insert(key, value);
            }

            assert_eq!(
                rust_tree.clone().into_iter().collect::<Vec<_>>(),
                tree.iter().unwrap().map(|x| x.unwrap()).collect::<Vec<_>>()
            );
            assert_eq!(
                rust_tree.into_iter().rev().collect::<Vec<_>>(),
                tree.iter_reverse()
                    .unwrap()
                    .map(|x| x.unwrap())
                    .collect::<Vec<_>>()
            );
        });

        if let Err(_) = result {
            let dot_data = tree
                .into_dot(|value| {
                    let mut last_value_state: Option<(u8, usize)> = None;

                    let mut formatted_value = value.iter().fold(String::new(), |acc, x| {
                        let mut result = "".to_string();
                        if let Some((last_value, repeats)) = last_value_state {
                            if last_value == *x {
                                last_value_state = Some((last_value, repeats + 1));

                                return acc;
                            } else {
                                last_value_state = Some((*x, 1));

                                result += &format!("({repeats})");
                            }
                        } else {
                            last_value_state = Some((*x, 1));
                        }

                        if !acc.is_empty() {
                            result += ",";
                        }

                        format!("{result}{x:#x}")
                    });

                    if let Some((_, repeats)) = last_value_state
                        && repeats > 1
                    {
                        formatted_value += &format!("({repeats})");
                    }

                    formatted_value
                })
                .unwrap();

            let mut output = NamedTempFile::new().unwrap();
            output.write_all(dot_data.as_bytes()).unwrap();
            let output_path = output.keep().unwrap().1;

            eprintln!("dot data written to: {}", output_path.to_string_lossy());
        }

        result.unwrap();
    }

    #[test]
    fn reverse_with_splits() {
        // this case came from fuzzing, hence the slightly unhinged input
        let to_insert = vec![
            (BigKey::<u64>::new(1095228325891), vec![0u8; 2]),
            (BigKey::new(23552), vec![0u8; 2]),
            (BigKey::new(749004913038733311), vec![0u8; 1]),
            (BigKey::new(11730937), vec![0u8; 1]),
            (BigKey::new(18446735329151090432), vec![0u8; 1]),
            (BigKey::new(128434), vec![0u8; 2]),
            (BigKey::new(4160773120), vec![0u8; 2]),
            (BigKey::new(7277816997842399231), vec![0u8; 1]),
            (BigKey::new(18446744069414780850), vec![0u8; 2]),
            (BigKey::new(280375565746354), vec![0u8; 1]),
            (BigKey::new(45568), vec![0u8; 1]),
            (BigKey::new(8808972877568), vec![0u8; 1]),
            (BigKey::new(196530), vec![0u8; 2]),
            (BigKey::new(272678883712000), vec![0u8; 2]),
            (BigKey::new(28428972659453951), vec![0u8; 1]),
            (BigKey::new(18446735294791352064), vec![0u8; 1]),
            (BigKey::new(193970), vec![0u8; 2]),
            (BigKey::new(1096776417280), vec![0u8; 2]),
            (BigKey::new(28428972659453944), vec![0u8; 1]),
            (BigKey::new(18386508424398700466), vec![0u8; 2]),
            (BigKey::new(280375565746354), vec![0u8; 1]),
            (BigKey::new(270479860478464), vec![0u8; 1]),
            (BigKey::new(227629727488), vec![0u8; 2]),
            (BigKey::new(2986409983), vec![0u8; 1]),
            (BigKey::new(866673871104), vec![0u8; 2]),
            (BigKey::new(749004913038733311), vec![0u8; 1]),
            (BigKey::new(11730937), vec![0u8; 1]),
            (BigKey::new(18446735329151090432), vec![0u8; 1]),
            (BigKey::new(128434), vec![0u8; 2]),
            (BigKey::new(4160773120), vec![0u8; 2]),
            (BigKey::new(759169024), vec![0u8; 1]),
            (BigKey::new(41944653103338), vec![0u8; 1]),
            (BigKey::new(400308568064), vec![0u8; 1]),
            (BigKey::new(41956837944524949), vec![0u8; 1]),
            (BigKey::new(17593749602304), vec![0u8; 1]),
            (BigKey::new(1563623424), vec![0u8; 1]),
            (BigKey::new(1560281088), vec![0u8; 1]),
            (BigKey::new(12813251448442880), vec![0u8; 1]),
            (BigKey::new(10740950511298543765), vec![0u8; 1]),
            (BigKey::new(855638016), vec![0u8; 1]),
            (BigKey::new(17955007290084764969), vec![0u8; 17]),
            (BigKey::new(327869), vec![0u8; 1]),
            (BigKey::new(281471419940864), vec![0u8; 1]),
            (BigKey::new(53198770610748672), vec![0u8; 1]),
            (BigKey::new(661184721051266345), vec![0u8; 1]),
            (BigKey::new(8796093034496), vec![0u8; 1]),
            (BigKey::new(257449567191040), vec![0u8; 1]),
            (BigKey::new(4194816), vec![0u8; 1]),
            (BigKey::new(257449567200806), vec![0u8; 1]),
            (BigKey::new(519695237120), vec![0u8; 1]),
            (BigKey::new(3255307760466471209), vec![0u8; 1]),
            (BigKey::new(2522068567888101421), vec![0u8; 1]),
            (BigKey::new(17955007289400229888), vec![0u8; 1]),
            (BigKey::new(32768), vec![0u8; 1]),
            (BigKey::new(70650219154374656), vec![0u8; 1]),
            (BigKey::new(9884556757906042153), vec![0u8; 1]),
            (BigKey::new(12288), vec![0u8; 1]),
            (BigKey::new(1383349474033664), vec![0u8; 1]),
            (BigKey::new(70136747227152896), vec![0u8; 1]),
            (BigKey::new(0), vec![0u8; 1]),
            (BigKey::new(275977418571776), vec![0u8; 1]),
            (BigKey::new(255), vec![0u8; 1]),
            (BigKey::new(905955839), vec![0u8; 458]),
        ];

        test_from_data(to_insert);
    }

    #[test]
    fn fuzzer_a() {
        // this case came from fuzzing, hence the slightly unhinged input
        let to_insert = vec![
            (BigKey::<u64>::new(1095228325891), vec![0u8; 2]),
            (BigKey::new(3096224743840768), vec![0u8; 2]),
            (BigKey::new(749004913038733311), vec![0u8; 1]),
            (BigKey::new(18230289816630788089), vec![0u8; 1]),
            (BigKey::new(18446735329151090432), vec![0u8; 1]),
            (BigKey::new(128434), vec![0u8; 2]),
            (BigKey::new(4294967258), vec![0u8; 2]),
            (BigKey::new(7277816997842399231), vec![0u8; 1]),
            (BigKey::new(18446744069414780850), vec![0u8; 2]),
            (BigKey::new(280375565746354), vec![0u8; 1]),
            (BigKey::new(45568), vec![0u8; 1]),
            (BigKey::new(8808972877568), vec![0u8; 1]),
            (BigKey::new(196530), vec![0u8; 2]),
            (BigKey::new(272678900451785), vec![0u8; 2]),
            (BigKey::new(28428972659453951), vec![0u8; 1]),
            (BigKey::new(18446735294791352064), vec![0u8; 1]),
            (BigKey::new(193970), vec![0u8; 2]),
            (BigKey::new(1096776417280), vec![0u8; 2]),
            (BigKey::new(28428972659453944), vec![0u8; 1]),
            (BigKey::new(18386508424398700466), vec![0u8; 2]),
            (BigKey::new(280375565877426), vec![0u8; 1]),
            (BigKey::new(270479860478464), vec![0u8; 1]),
            (BigKey::new(219039792896), vec![0u8; 2]),
            (BigKey::new(2986409983), vec![0u8; 1]),
            (BigKey::new(866673871104), vec![0u8; 2]),
            (BigKey::new(749004913038733311), vec![0u8; 1]),
            (BigKey::new(11730937), vec![0u8; 1]),
            (BigKey::new(18446735329151090432), vec![0u8; 1]),
            (BigKey::new(128434), vec![0u8; 2]),
            (BigKey::new(4160773120), vec![0u8; 2]),
            (BigKey::new(759169024), vec![0u8; 1]),
            (BigKey::new(41944653103338), vec![0u8; 1]),
            (BigKey::new(3773172062810537984), vec![0u8; 1]),
            (BigKey::new(41956837944524949), vec![0u8; 1]),
            (BigKey::new(17593749733376), vec![0u8; 1]),
            (BigKey::new(1563623424), vec![0u8; 1]),
            (BigKey::new(1560281088), vec![0u8; 1]),
            (BigKey::new(12813251448442880), vec![0u8; 1]),
            (BigKey::new(10740950511298543765), vec![0u8; 1]),
            (BigKey::new(838860800), vec![0u8; 1]),
            (BigKey::new(491736783624786638), vec![0u8; 17]),
            (BigKey::new(327869), vec![0u8; 1]),
            (BigKey::new(281471419940864), vec![0u8; 1]),
            (BigKey::new(53198770610748672), vec![0u8; 1]),
            (BigKey::new(661184721051266345), vec![0u8; 1]),
            (BigKey::new(8796093034496), vec![0u8; 1]),
            (BigKey::new(268444683468800), vec![0u8; 1]),
            (BigKey::new(2199027450368), vec![0u8; 1]),
            (BigKey::new(257449567200806), vec![0u8; 1]),
            (BigKey::new(519695237120), vec![0u8; 1]),
            (BigKey::new(3255307760466471209), vec![0u8; 1]),
            (BigKey::new(2522068567888101421), vec![0u8; 1]),
            (BigKey::new(1125056745783855), vec![0u8; 5]),
            (BigKey::new(4863), vec![0u8; 11]),
            (BigKey::new(848840156512003), vec![0u8; 1]),
            (BigKey::new(142284501207154471), vec![0u8; 1]),
            (BigKey::new(15204011600974444839), vec![0u8; 1]),
            (BigKey::new(217298682054180864), vec![0u8; 1]),
            (BigKey::new(277076930199551), vec![0u8; 4]),
            (BigKey::new(17432379), vec![0u8; 1]),
            (BigKey::new(4863), vec![0u8; 11]),
            (BigKey::new(47855161267191555), vec![0u8; 1]),
            (BigKey::new(142284501207154471), vec![0u8; 1]),
            (BigKey::new(15204011600974444839), vec![0u8; 1]),
            (BigKey::new(217298682054184960), vec![0u8; 1]),
            (BigKey::new(4398046511103), vec![0u8; 4]),
            (BigKey::new(9223372036854775801), vec![0u8; 1]),
            (BigKey::new(3298534883194), vec![0u8; 4]),
            (BigKey::new(9223372036854774055), vec![0u8; 1]),
            (BigKey::new(576460752286590842), vec![0u8; 4]),
            (BigKey::new(251638629179457535), vec![0u8; 4]),
            (BigKey::new(30), vec![0u8; 1]),
            (BigKey::new(3206556144328376103), vec![0u8; 1]),
            (BigKey::new(4398046511104), vec![0u8; 1]),
            (BigKey::new(3819055799724934143), vec![0u8; 1]),
            (BigKey::new(576460752303367975), vec![0u8; 4]),
            (BigKey::new(289079216299769639), vec![0u8; 1]),
            (BigKey::new(142284501106491175), vec![0u8; 1]),
            (BigKey::new(15204011463535491367), vec![0u8; 1]),
            (BigKey::new(217298686248484864), vec![0u8; 1]),
            (BigKey::new(1244967), vec![0u8; 1]),
            (BigKey::new(288231475663273984), vec![0u8; 1]),
            (BigKey::new(577309575280328703), vec![0u8; 1]),
            (BigKey::new(18446743523953737721), vec![0u8; 1]),
            (BigKey::new(3298534883327), vec![0u8; 4]),
            (BigKey::new(9223372036854774271), vec![0u8; 1]),
            (BigKey::new(576460752303368058), vec![0u8; 4]),
            (BigKey::new(251638629179457319), vec![0u8; 4]),
            (BigKey::new(142284501106360103), vec![0u8; 1]),
            (BigKey::new(15204010544412490023), vec![0u8; 1]),
            (BigKey::new(288230376151711744), vec![0u8; 1]),
            (BigKey::new(3458767829535294463), vec![0u8; 1]),
            (BigKey::new(576367293815007015), vec![0u8; 4]),
            (BigKey::new(848840148057895), vec![0u8; 1]),
            (BigKey::new(142284501106469415), vec![0u8; 1]),
            (BigKey::new(15204011600974444839), vec![0u8; 1]),
            (BigKey::new(1095233372169), vec![0u8; 1]),
            (BigKey::new(9), vec![0u8; 1]),
            (BigKey::new(5), vec![0u8; 1]),
            (BigKey::new(15663113), vec![0u8; 1]),
            (BigKey::new(23817), vec![0u8; 1]),
            (BigKey::new(262383), vec![0u8; 1]),
            (BigKey::new(399599728127), vec![0u8; 1]),
            (BigKey::new(1095216660489), vec![0u8; 1]),
            (BigKey::new(18374686879271089407), vec![0u8; 61]),
            (BigKey::new(23817), vec![0u8; 1]),
            (BigKey::new(262153), vec![0u8; 1]),
            (BigKey::new(399599728127), vec![0u8; 1]),
            (BigKey::new(1095216660489), vec![0u8; 1]),
            (BigKey::new(399599465727), vec![0u8; 1]),
            (BigKey::new(3989292031), vec![0u8; 1]),
            (BigKey::new(237), vec![0u8; 10]),
            (BigKey::new(647714935328997376), vec![0u8; 1]),
            (BigKey::new(18374961357578502399), vec![0u8; 61]),
            (BigKey::new(262381), vec![0u8; 1]),
            (BigKey::new(537038681599), vec![0u8; 1]),
            (BigKey::new(71776119067901961), vec![0u8; 1]),
            (BigKey::new(576461151902889215), vec![0u8; 1]),
            (BigKey::new(3979885823), vec![0u8; 1]),
            (BigKey::new(237), vec![0u8; 10]),
            (BigKey::new(71254183025573888), vec![0u8; 1]),
            (BigKey::new(71106559), vec![0u8; 1]),
            (BigKey::new(332009393485), vec![0u8; 1]),
            (BigKey::new(524293), vec![0u8; 1]),
            (BigKey::new(399447621641), vec![0u8; 1]),
            (BigKey::new(41956837944524949), vec![0u8; 1]),
            (BigKey::new(17593749602304), vec![0u8; 1]),
            (BigKey::new(1563623424), vec![0u8; 1]),
            (BigKey::new(1560281088), vec![0u8; 1]),
            (BigKey::new(12813251448442880), vec![0u8; 1]),
            (BigKey::new(10740950511298543765), vec![0u8; 1]),
            (BigKey::new(855638016), vec![0u8; 1]),
            (BigKey::new(12667444087565609), vec![0u8; 1]),
            (BigKey::new(0), vec![0u8; 1]),
            (BigKey::new(189), vec![0u8; 1]),
            (BigKey::new(71776115504447488), vec![0u8; 1]),
            (BigKey::new(17955007290153970985), vec![0u8; 1]),
            (BigKey::new(70650219137597440), vec![0u8; 1]),
            (BigKey::new(53198770610748672), vec![0u8; 1]),
            (BigKey::new(661184721051266345), vec![0u8; 1]),
            (BigKey::new(8796093296640), vec![0u8; 1]),
            (BigKey::new(257449567191040), vec![0u8; 1]),
            (BigKey::new(4194816), vec![0u8; 1]),
            (BigKey::new(257449567200806), vec![0u8; 1]),
            (BigKey::new(1792), vec![0u8; 1]),
            (BigKey::new(3255307760466471209), vec![0u8; 1]),
            (BigKey::new(2522068567888101421), vec![0u8; 1]),
            (BigKey::new(17955007289400229888), vec![0u8; 256]),
            (BigKey::new(8863083360943013888), vec![0u8; 1024]),
        ];
        test_from_data(to_insert);
    }

    #[test]
    fn fuzzer_b() {
        let data = vec![
            (BigKey::<u64>::new(1095228325891), vec![0u8; 2]),
            (BigKey::new(3096224743840768), vec![0u8; 2]),
            (BigKey::new(749004913038733311), vec![0u8; 1]),
            (BigKey::new(18230289816630788089), vec![0u8; 1]),
            (BigKey::new(18446735329151090432), vec![0u8; 1]),
            (BigKey::new(128434), vec![0u8; 2]),
            (BigKey::new(4294967258), vec![0u8; 2]),
            (BigKey::new(7277816997842399231), vec![0u8; 1]),
            (BigKey::new(18385945474445279154), vec![0u8; 2]),
            (BigKey::new(280375565746354), vec![0u8; 1]),
            (BigKey::new(45568), vec![0u8; 1]),
            (BigKey::new(8808972877568), vec![0u8; 1]),
            (BigKey::new(8590131122), vec![0u8; 2]),
            (BigKey::new(272678883712000), vec![0u8; 2]),
            (BigKey::new(28428972659453951), vec![0u8; 1]),
            (BigKey::new(18446735294791352064), vec![0u8; 1]),
            (BigKey::new(193970), vec![0u8; 2]),
            (BigKey::new(1096776417280), vec![0u8; 2]),
            (BigKey::new(28428972659453944), vec![0u8; 1]),
            (BigKey::new(18386508424398700466), vec![0u8; 2]),
            (BigKey::new(280375565877426), vec![0u8; 1]),
            (BigKey::new(270479860478464), vec![0u8; 1]),
            (BigKey::new(227629727488), vec![0u8; 2]),
            (BigKey::new(2986409983), vec![0u8; 1]),
            (BigKey::new(866673871104), vec![0u8; 2]),
            (BigKey::new(749075281782910975), vec![0u8; 1]),
            (BigKey::new(11730937), vec![0u8; 1]),
            (BigKey::new(18446735329151090432), vec![0u8; 1]),
            (BigKey::new(128434), vec![0u8; 2]),
            (BigKey::new(4160773120), vec![0u8; 2]),
            (BigKey::new(759169024), vec![0u8; 1]),
            (BigKey::new(41944653103338), vec![0u8; 1]),
            (BigKey::new(3773172062810537984), vec![0u8; 1]),
            (BigKey::new(41956837944524949), vec![0u8; 1]),
            (BigKey::new(17593749733376), vec![0u8; 1]),
            (BigKey::new(1563623424), vec![0u8; 1]),
            (BigKey::new(1560281344), vec![0u8; 1]),
            (BigKey::new(12813251448442880), vec![0u8; 1]),
            (BigKey::new(10740950511298543765), vec![0u8; 1]),
            (BigKey::new(838860800), vec![0u8; 1]),
            (BigKey::new(491736783624786638), vec![0u8; 17]),
            (BigKey::new(327869), vec![0u8; 1]),
            (BigKey::new(18446462598732840960), vec![0u8; 1]),
            (BigKey::new(53198770610748672), vec![0u8; 1]),
            (BigKey::new(661184721051266345), vec![0u8; 1]),
            (BigKey::new(8796093034496), vec![0u8; 1]),
            (BigKey::new(268444683468800), vec![0u8; 1]),
            (BigKey::new(2199027450368), vec![0u8; 1]),
            (BigKey::new(257449567200806), vec![0u8; 1]),
            (BigKey::new(519695237120), vec![0u8; 1]),
            (BigKey::new(3255307760466471209), vec![0u8; 1]),
            (BigKey::new(2522068567888101421), vec![0u8; 1]),
            (BigKey::new(1125056745783855), vec![0u8; 5]),
            (BigKey::new(4863), vec![0u8; 11]),
            (BigKey::new(848840156512003), vec![0u8; 1]),
            (BigKey::new(142284501207154471), vec![0u8; 1]),
            (BigKey::new(18410708676077879079), vec![0u8; 1]),
            (BigKey::new(217298682054180864), vec![0u8; 1]),
            (BigKey::new(277076930199551), vec![0u8; 4]),
            (BigKey::new(17432379), vec![0u8; 1]),
            (BigKey::new(4863), vec![0u8; 11]),
            (BigKey::new(47855161267191555), vec![0u8; 1]),
            (BigKey::new(142284501207154471), vec![0u8; 1]),
            (BigKey::new(15204011600974444839), vec![0u8; 1]),
            (BigKey::new(217298682054381568), vec![0u8; 1]),
            (BigKey::new(4398046511103), vec![0u8; 4]),
            (BigKey::new(9223372036854775801), vec![0u8; 1]),
            (BigKey::new(12388197510152058), vec![0u8; 4]),
            (BigKey::new(9223372036854774055), vec![0u8; 1]),
            (BigKey::new(576460752286590842), vec![0u8; 4]),
            (BigKey::new(251638629179457535), vec![0u8; 4]),
            (BigKey::new(30), vec![0u8; 1]),
            (BigKey::new(3206556144328376103), vec![0u8; 1]),
            (BigKey::new(4398046511104), vec![0u8; 1]),
            (BigKey::new(3819055799724934143), vec![0u8; 1]),
            (BigKey::new(576460752303367975), vec![0u8; 4]),
            (BigKey::new(289079216299769639), vec![0u8; 1]),
            (BigKey::new(142284501106491175), vec![0u8; 1]),
            (BigKey::new(15204011463535491367), vec![0u8; 1]),
            (BigKey::new(217298686248484864), vec![0u8; 1]),
            (BigKey::new(1244967), vec![0u8; 1]),
            (BigKey::new(288231475663273984), vec![0u8; 1]),
            (BigKey::new(577309575280328703), vec![0u8; 1]),
            (BigKey::new(18446743523953737721), vec![0u8; 1]),
            (BigKey::new(3298534883194), vec![0u8; 4]),
            (BigKey::new(9223372036854774271), vec![0u8; 1]),
            (BigKey::new(576460752303368058), vec![0u8; 4]),
            (BigKey::new(251638629179457319), vec![0u8; 4]),
            (BigKey::new(142284501106360103), vec![0u8; 1]),
            (BigKey::new(15204010544412490023), vec![0u8; 1]),
            (BigKey::new(288230376151711744), vec![0u8; 1]),
            (BigKey::new(3458767829535294463), vec![0u8; 1]),
            (BigKey::new(576367293815007015), vec![0u8; 4]),
            (BigKey::new(848840148057895), vec![0u8; 1]),
            (BigKey::new(142284501106469415), vec![0u8; 1]),
            (BigKey::new(15132094747964866560), vec![0u8; 1]),
            (BigKey::new(1095233372169), vec![0u8; 1]),
            (BigKey::new(9), vec![0u8; 1]),
            (BigKey::new(5), vec![0u8; 1]),
            (BigKey::new(15663113), vec![0u8; 1]),
            (BigKey::new(23817), vec![0u8; 1]),
            (BigKey::new(262383), vec![0u8; 1]),
            (BigKey::new(399599728127), vec![0u8; 1]),
            (BigKey::new(1095216660489), vec![0u8; 1]),
            (BigKey::new(18374686879271089407), vec![0u8; 61]),
            (BigKey::new(23817), vec![0u8; 1]),
            (BigKey::new(262153), vec![0u8; 1]),
            (BigKey::new(399599728127), vec![0u8; 1]),
            (BigKey::new(1095216660489), vec![0u8; 1]),
            (BigKey::new(399599465727), vec![0u8; 1]),
            (BigKey::new(3989292031), vec![0u8; 1]),
            (BigKey::new(237), vec![0u8; 10]),
            (BigKey::new(647714935328997376), vec![0u8; 1]),
            (BigKey::new(18374961357578502399), vec![0u8; 61]),
            (BigKey::new(262381), vec![0u8; 1]),
            (BigKey::new(537038681599), vec![0u8; 1]),
            (BigKey::new(71776119067901961), vec![0u8; 1]),
            (BigKey::new(4611686418026853631), vec![0u8; 1]),
            (BigKey::new(3979885823), vec![0u8; 1]),
            (BigKey::new(237), vec![0u8; 10]),
            (BigKey::new(71254183025573888), vec![0u8; 1]),
            (BigKey::new(71106559), vec![0u8; 1]),
            (BigKey::new(332009393485), vec![0u8; 1]),
            (BigKey::new(524293), vec![0u8; 1]),
            (BigKey::new(399447621641), vec![0u8; 1]),
            (BigKey::new(41956837944524949), vec![0u8; 1]),
            (BigKey::new(17593749602304), vec![0u8; 1]),
            (BigKey::new(1563623424), vec![0u8; 1]),
            (BigKey::new(1560281088), vec![0u8; 1]),
            (BigKey::new(12813251448442880), vec![0u8; 1]),
            (BigKey::new(10740950511298543765), vec![0u8; 1]),
            (BigKey::new(855638016), vec![0u8; 1]),
            (BigKey::new(12667444087565609), vec![0u8; 1]),
            (BigKey::new(0), vec![0u8; 1]),
            (BigKey::new(189), vec![0u8; 1]),
            (BigKey::new(71776115504447488), vec![0u8; 1]),
            (BigKey::new(17955007290153970985), vec![0u8; 1]),
            (BigKey::new(70650219137597440), vec![0u8; 1]),
            (BigKey::new(53198770610748672), vec![0u8; 1]),
            (BigKey::new(661184721051266345), vec![0u8; 1]),
            (BigKey::new(8796093062912), vec![0u8; 1]),
            (BigKey::new(257449567191040), vec![0u8; 1]),
            (BigKey::new(4194816), vec![0u8; 1]),
            (BigKey::new(257449569681446), vec![0u8; 1]),
            (BigKey::new(1792), vec![0u8; 1]),
            (BigKey::new(3255307760466471209), vec![0u8; 1]),
            (BigKey::new(2522068567888101421), vec![0u8; 1]),
            (BigKey::new(17955007289400229888), vec![0u8; 1]),
            (BigKey::new(36028797018996736), vec![0u8; 1]),
            (BigKey::new(12840605863068565248), vec![0u8; 1]),
            (BigKey::new(12839761439816155136), vec![0u8; 1]),
            (BigKey::new(13509701064982528), vec![0u8; 1]),
            (BigKey::new(3206556144328376103), vec![0u8; 1]),
            (BigKey::new(4398046511104), vec![0u8; 1]),
            (BigKey::new(18446736545355923455), vec![0u8; 1024]),
        ];
        test_from_data(data);
    }
}
