use crate::page::Page;
use bytemuck::Pod;

use crate::{bplustree::NodeId, storage::PageIndex};

pub(super) trait NodeIds<const N: usize> {
    // TODO create a trait for TKey instead of having this constraint repeated all over
    type Nodes<'a, TKey: Pod>;
    type NodesMut<'a, TKey: Pod>;

    fn to_page_indices(self) -> [PageIndex; N];
    fn pages_to_nodes_mut<'a, TKey: Pod>(pages: [&'a mut Page; N]) -> Self::NodesMut<'a, TKey>;
}

impl<T> NodeIds<1> for T
where
    T: NodeId,
{
    type Nodes<'a, TKey: Pod> = &'a T::Node<TKey>;
    type NodesMut<'a, TKey: Pod> = &'a mut T::Node<TKey>;

    fn to_page_indices(self) -> [PageIndex; 1] {
        [self.page()]
    }

    fn pages_to_nodes_mut<'a, TKey: Pod>(pages: [&'a mut Page; 1]) -> Self::NodesMut<'a, TKey> {
        pages[0].data_mut()
    }
}

impl<T1, T2> NodeIds<2> for (T1, T2)
where
    T1: NodeId,
    T2: NodeId,
{
    type Nodes<'a, TKey: Pod> = (&'a T1::Node<TKey>, &'a T2::Node<TKey>);
    type NodesMut<'a, TKey: Pod> = (&'a mut T1::Node<TKey>, &'a mut T2::Node<TKey>);

    fn to_page_indices(self) -> [PageIndex; 2] {
        let (i0, i1) = self;

        [i0.page(), i1.page()]
    }

    fn pages_to_nodes_mut<'a, TKey: Pod>(pages: [&'a mut Page; 2]) -> Self::NodesMut<'a, TKey> {
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
    type Nodes<'a, TKey: Pod> = (&'a T1::Node<TKey>, &'a T2::Node<TKey>, &'a T3::Node<TKey>);
    type NodesMut<'a, TKey: Pod> = (
        &'a mut T1::Node<TKey>,
        &'a mut T2::Node<TKey>,
        &'a mut T3::Node<TKey>,
    );

    fn to_page_indices(self) -> [PageIndex; 3] {
        let (i0, i1, i2) = self;

        [i0.page(), i1.page(), i2.page()]
    }

    fn pages_to_nodes_mut<'a, TKey: Pod>(pages: [&'a mut Page; 3]) -> Self::NodesMut<'a, TKey> {
        let [p0, p1, p2] = pages;
        (p0.data_mut(), p1.data_mut(), p2.data_mut())
    }
}
