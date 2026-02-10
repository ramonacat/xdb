use crate::bplustree::NodeId;
use crate::storage::PageId;
use crate::{bplustree::TreeKey, page::Page};

pub(super) trait NodeIds<const N: usize, TPageId: PageId> {
    type Nodes<'a, TKey: TreeKey>;
    type NodesMut<'a, TKey: TreeKey>;

    fn to_page_indices(self) -> [TPageId; N];
    fn pages_to_nodes<TKey: TreeKey>(pages: [&'_ Page; N]) -> Self::Nodes<'_, TKey>;
    fn pages_to_nodes_mut<TKey: TreeKey>(pages: [&'_ mut Page; N]) -> Self::NodesMut<'_, TKey>;
}

impl<T, TPageId: PageId> NodeIds<1, TPageId> for T
where
    T: NodeId<TPageId>,
{
    type Nodes<'a, TKey: TreeKey> = &'a T::Node<TKey>;
    type NodesMut<'a, TKey: TreeKey> = &'a mut T::Node<TKey>;

    fn to_page_indices(self) -> [TPageId; 1] {
        [self.page()]
    }

    fn pages_to_nodes<TKey: TreeKey>(pages: [&'_ Page; 1]) -> Self::Nodes<'_, TKey> {
        pages[0].data()
    }

    fn pages_to_nodes_mut<TKey: TreeKey>(pages: [&'_ mut Page; 1]) -> Self::NodesMut<'_, TKey> {
        pages[0].data_mut()
    }
}

impl<T1, T2, TPageId: PageId> NodeIds<2, TPageId> for (T1, T2)
where
    T1: NodeId<TPageId>,
    T2: NodeId<TPageId>,
{
    type Nodes<'a, TKey: TreeKey> = (&'a T1::Node<TKey>, &'a T2::Node<TKey>);
    type NodesMut<'a, TKey: TreeKey> = (&'a mut T1::Node<TKey>, &'a mut T2::Node<TKey>);

    fn to_page_indices(self) -> [TPageId; 2] {
        let (i0, i1) = self;

        [i0.page(), i1.page()]
    }

    fn pages_to_nodes<TKey: TreeKey>(pages: [&'_ Page; 2]) -> Self::Nodes<'_, TKey> {
        let [p0, p1] = pages;

        (p0.data(), p1.data())
    }

    fn pages_to_nodes_mut<TKey: TreeKey>(pages: [&'_ mut Page; 2]) -> Self::NodesMut<'_, TKey> {
        let [p0, p1] = pages;
        (p0.data_mut(), p1.data_mut())
    }
}

impl<T1, T2, T3, TPageId: PageId> NodeIds<3, TPageId> for (T1, T2, T3)
where
    T1: NodeId<TPageId>,
    T2: NodeId<TPageId>,
    T3: NodeId<TPageId>,
{
    type Nodes<'a, TKey: TreeKey> = (&'a T1::Node<TKey>, &'a T2::Node<TKey>, &'a T3::Node<TKey>);
    type NodesMut<'a, TKey: TreeKey> = (
        &'a mut T1::Node<TKey>,
        &'a mut T2::Node<TKey>,
        &'a mut T3::Node<TKey>,
    );

    fn to_page_indices(self) -> [TPageId; 3] {
        let (i0, i1, i2) = self;

        [i0.page(), i1.page(), i2.page()]
    }

    fn pages_to_nodes<TKey: TreeKey>(pages: [&'_ Page; 3]) -> Self::Nodes<'_, TKey> {
        let [p0, p1, p2] = pages;

        (p0.data(), p1.data(), p2.data())
    }

    fn pages_to_nodes_mut<TKey: TreeKey>(pages: [&'_ mut Page; 3]) -> Self::NodesMut<'_, TKey> {
        let [p0, p1, p2] = pages;
        (p0.data_mut(), p1.data_mut(), p2.data_mut())
    }
}
