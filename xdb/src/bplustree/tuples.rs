use crate::{bplustree::TreeKey, page::Page};

use crate::{bplustree::NodeId, storage::PageIndex};

pub(super) trait NodeIds<const N: usize> {
    type Nodes<'a, TKey: TreeKey>;
    type NodesMut<'a, TKey: TreeKey>;

    fn to_page_indices(self) -> [PageIndex; N];
    fn pages_to_nodes<TKey: TreeKey>(pages: [&Page; N]) -> Self::Nodes<'_, TKey>;
    fn pages_to_nodes_mut<TKey: TreeKey>(pages: [&mut Page; N]) -> Self::NodesMut<'_, TKey>;
}

impl<T> NodeIds<1> for T
where
    T: NodeId,
{
    type Nodes<'a, TKey: TreeKey> = &'a T::Node<TKey>;
    type NodesMut<'a, TKey: TreeKey> = &'a mut T::Node<TKey>;

    fn to_page_indices(self) -> [PageIndex; 1] {
        [self.page()]
    }

    fn pages_to_nodes<TKey: TreeKey>(pages: [&Page; 1]) -> Self::Nodes<'_, TKey> {
        pages[0].data()
    }

    fn pages_to_nodes_mut<TKey: TreeKey>(pages: [&mut Page; 1]) -> Self::NodesMut<'_, TKey> {
        pages[0].data_mut()
    }
}

impl<T1, T2> NodeIds<2> for (T1, T2)
where
    T1: NodeId,
    T2: NodeId,
{
    type Nodes<'a, TKey: TreeKey> = (&'a T1::Node<TKey>, &'a T2::Node<TKey>);
    type NodesMut<'a, TKey: TreeKey> = (&'a mut T1::Node<TKey>, &'a mut T2::Node<TKey>);

    fn to_page_indices(self) -> [PageIndex; 2] {
        let (i0, i1) = self;

        [i0.page(), i1.page()]
    }

    fn pages_to_nodes<TKey: TreeKey>(pages: [&Page; 2]) -> Self::Nodes<'_, TKey> {
        let [p0, p1] = pages;

        (p0.data(), p1.data())
    }

    fn pages_to_nodes_mut<TKey: TreeKey>(pages: [&mut Page; 2]) -> Self::NodesMut<'_, TKey> {
        let [p0, p1] = pages;
        (p0.data_mut(), p1.data_mut())
    }
}

impl<T1, T2, T3> NodeIds<3> for (T1, T2, T3)
where
    T1: NodeId,
    T2: NodeId,
    T3: NodeId,
{
    type Nodes<'a, TKey: TreeKey> = (&'a T1::Node<TKey>, &'a T2::Node<TKey>, &'a T3::Node<TKey>);
    type NodesMut<'a, TKey: TreeKey> = (
        &'a mut T1::Node<TKey>,
        &'a mut T2::Node<TKey>,
        &'a mut T3::Node<TKey>,
    );

    fn to_page_indices(self) -> [PageIndex; 3] {
        let (i0, i1, i2) = self;

        [i0.page(), i1.page(), i2.page()]
    }

    fn pages_to_nodes_mut<TKey: TreeKey>(pages: [&mut Page; 3]) -> Self::NodesMut<'_, TKey> {
        let [p0, p1, p2] = pages;
        (p0.data_mut(), p1.data_mut(), p2.data_mut())
    }

    fn pages_to_nodes<TKey: TreeKey>(pages: [&Page; 3]) -> Self::Nodes<'_, TKey> {
        let [p0, p1, p2] = pages;

        (p0.data(), p1.data(), p2.data())
    }
}
