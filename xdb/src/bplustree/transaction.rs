use crate::storage::PageId as _;
use std::marker::PhantomData;

use crate::storage::page::Page;
use crate::{
    bplustree::{AnyNodeId, Node, NodeId as _, NodeIds, TreeError, TreeHeader, TreeKey},
    storage::{
        FIRST_PAGE_ID, PageReservation as _, SENTINEL_PAGE_ID, Storage, Transaction as _,
        TransactionId,
    },
};

#[derive(Debug)]
pub struct TreeTransaction<'storage, TStorage: Storage + 'storage, TKey>
where
    Self: 'storage,
{
    transaction: TStorage::Transaction<'storage>,
    _key: PhantomData<&'storage TKey>,
}

impl<'storage, TStorage: Storage + 'storage, TKey: TreeKey>
    TreeTransaction<'storage, TStorage, TKey>
{
    pub(super) const fn new(storage_transaction: TStorage::Transaction<'storage>) -> Self {
        Self {
            transaction: storage_transaction,
            _key: PhantomData,
        }
    }

    pub fn id(&self) -> TransactionId {
        self.transaction.id()
    }

    pub(super) fn get_root(&mut self) -> Result<AnyNodeId, TreeError<TStorage::PageId>> {
        Ok(AnyNodeId::new(self.read_header(|x| x.root)?))
    }

    fn read_header<TReturn>(
        &mut self,
        read: impl FnOnce(&TreeHeader) -> TReturn,
    ) -> Result<TReturn, TreeError<TStorage::PageId>> {
        let txid = self.transaction.id();

        Ok(self
            .transaction
            .read(TStorage::PageId::deserialize(FIRST_PAGE_ID), |[page]| {
                let data: &TreeHeader = page.data();

                assert!(
                    data.root != SENTINEL_PAGE_ID,
                    "root is zero! txid: {txid:?} Header: {page:?}"
                );
                read(data)
            })?)
    }

    pub(super) fn write_header<TReturn>(
        &mut self,
        write: impl FnOnce(&mut TreeHeader) -> TReturn,
    ) -> Result<TReturn, TreeError<TStorage::PageId>> {
        Ok(self
            .transaction
            .write(TStorage::PageId::deserialize(FIRST_PAGE_ID), |[page]| {
                write(page.data_mut())
            })?)
    }

    // TODO we should probably get rid of the callable, and just return a reference that has the
    // same lifetime as the transaction
    pub(super) fn read_nodes<TReturn, TIndices: NodeIds<N>, const N: usize>(
        &mut self,
        indices: TIndices,
        read: impl for<'node> FnOnce(TIndices::Nodes<'node, TKey>) -> TReturn,
    ) -> Result<TReturn, TreeError<TStorage::PageId>> {
        let page_indices = indices.to_page_indices().map(TStorage::PageId::deserialize);
        debug_assert!(
            !page_indices
                .iter()
                .any(|x| x.serialize() == SENTINEL_PAGE_ID)
        );

        Ok(self.transaction.read(page_indices, |pages| {
            read(TIndices::pages_to_nodes(pages.map(|x| x)))
        })?)
    }

    pub(super) fn write_nodes<TReturn, TIndices: NodeIds<N>, const N: usize>(
        &mut self,
        indices: TIndices,
        write: impl for<'node> FnOnce(TIndices::NodesMut<'node, TKey>) -> TReturn,
    ) -> Result<TReturn, TreeError<TStorage::PageId>> {
        let page_indices = indices.to_page_indices().map(TStorage::PageId::deserialize);
        debug_assert!(
            !page_indices
                .iter()
                .any(|x| x.serialize() == SENTINEL_PAGE_ID)
        );

        Ok(self.transaction.write(page_indices, |pages| {
            write(TIndices::pages_to_nodes_mut(pages.map(|x| x)))
        })?)
    }

    // TODO separete reserve_interior_node and reserve_leaf_node, so that callers don't have to
    // touch the ID directly?
    pub fn reserve_node(
        &mut self,
    ) -> Result<TStorage::PageReservation<'storage>, TreeError<TStorage::PageId>> {
        Ok(self.transaction.reserve()?)
    }

    #[allow(clippy::large_types_passed_by_value)] // TODO perhaps we should do something to avoid
    // passing whole nodes here?
    pub(super) fn insert_reserved(
        &mut self,
        reservation: TStorage::PageReservation<'storage>,
        page: impl Node<TKey>,
    ) -> Result<(), TreeError<TStorage::PageId>> {
        debug_assert!(reservation.index() != TStorage::PageId::sentinel());

        self.transaction
            .insert_reserved(reservation, Page::from_data(page))?;

        Ok(())
    }

    pub(super) fn delete_node(
        &mut self,
        node_id: AnyNodeId,
    ) -> Result<(), TreeError<TStorage::PageId>> {
        self.transaction
            .delete(TStorage::PageId::deserialize(node_id.page()))?;

        Ok(())
    }

    pub fn commit(self) -> Result<(), TreeError<TStorage::PageId>> {
        let Self { transaction, _key } = self;

        transaction.commit()?;

        Ok(())
    }

    pub fn rollback(self) -> Result<(), TreeError<TStorage::PageId>> {
        let Self { transaction, _key } = self;

        transaction.rollback()?;

        Ok(())
    }
}
