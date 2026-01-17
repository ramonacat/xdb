pub mod algorithms;
pub mod debug;
pub mod dot;
mod iterator;
mod node;

use crate::bplustree::iterator::TreeIterator;
use std::marker::PhantomData;

use crate::bplustree::iterator::TreeIteratorItem;
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
    fn get_root(&self) -> Result<AnyNodeId, TreeError> {
        Ok(AnyNodeId::new(self.read_header(|x| x.root)?))
    }

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

    pub fn iter(
        &self,
    ) -> Result<impl DoubleEndedIterator<Item = TreeIteratorItem<TKey>>, TreeError> {
        TreeIterator::<_, _>::new(self.transaction()?)
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
    };

    use crate::{
        bplustree::algorithms::{delete::delete, insert::insert},
        debug::BigKey,
        storage::in_memory::InMemoryStorage,
    };
    use pretty_assertions::assert_eq;
    use tempfile::NamedTempFile;

    use super::*;

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

    #[test]
    fn insert_multiple_nodes() {
        let mut data = vec![];

        for i in 0..5000 {
            // make the value bigger with repeat so fewer inserts are needed and the test runs faster
            data.push((
                BigKey::<usize>::new(i),
                (u16::max_value() - i as u16).to_be_bytes().repeat(8),
            ));
        }

        test_from_data(data);
    }

    #[test]
    fn variable_sized_keys() {
        let mut data = vec![];

        for i in 0..5000 {
            let value: &[u8] = match i % 8 {
                0 | 7 | 6 | 5 => &(i as u64).to_be_bytes(),
                4 | 3 => &(i as u32).to_be_bytes(),
                2 => &(i as u16).to_be_bytes(),
                1 => &(i as u8).to_be_bytes(),
                _ => unreachable!(),
            };

            data.push((BigKey::new(i), value.repeat(8)));
        }

        test_from_data(data);
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
                tree.iter()
                    .unwrap()
                    .rev()
                    .map(|x| x.unwrap())
                    .collect::<Vec<_>>()
            );
        });

        if let Err(_) = result {
            let dot_data = tree
                .into_dot(|value| {
                    let mut last_value_state: Option<(u8, usize)> = None;

                    // TODO extract this formatter into bplustree::debug probably
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

    #[test]
    fn simple_delete() {
        let storage = InMemoryStorage::new();
        let tree = Tree::new(storage).unwrap();
        let transaction = tree.transaction().unwrap();

        insert(&transaction, BigKey::new(1), &[1, 2, 3]).unwrap();
        insert(&transaction, BigKey::new(2), &[4, 5, 6]).unwrap();

        let deleted_value = delete(&transaction, BigKey::new(2)).unwrap();
        assert_eq!(deleted_value, Some(vec![4, 5, 6]));

        assert_eq!(
            tree.iter().unwrap().map(|x| x.unwrap()).collect::<Vec<_>>(),
            &[(BigKey::new(1), vec![1, 2, 3])]
        );
    }
}
