use std::{
    fmt::{Debug, Display},
    marker::PhantomData,
};

use arbitrary::Arbitrary;
use bytemuck::{Pod, Zeroable, bytes_of, pod_read_unaligned};

use crate::bplustree::TreeKey;

#[derive(Clone, Copy, Pod, Zeroable)]
#[repr(transparent)]
pub struct BigKey<T, const SIZE: usize>([u8; SIZE], PhantomData<T>);

impl<T: TreeKey, const SIZE: usize> PartialOrd for BigKey<T, SIZE> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: TreeKey, const SIZE: usize> Ord for BigKey<T, SIZE> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.value().cmp(&other.value())
    }
}

impl<T: TreeKey, const SIZE: usize> Eq for BigKey<T, SIZE> {}

impl<T: TreeKey, const SIZE: usize> PartialEq for BigKey<T, SIZE> {
    fn eq(&self, other: &Self) -> bool {
        self.value() == other.value()
    }
}

impl<T: TreeKey, const SIZE: usize> TreeKey for BigKey<T, SIZE> {}

impl<T: TreeKey, const SIZE: usize> Debug for BigKey<T, SIZE> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BigKey({:?})", self.value())
    }
}

impl<'a, T: Arbitrary<'a> + TreeKey, const SIZE: usize> Arbitrary<'a> for BigKey<T, SIZE> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let value = u.arbitrary()?;

        Ok(Self::new(value))
    }
}

impl<T: Display + TreeKey, const SIZE: usize> Display for BigKey<T, SIZE> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.validate();

        write!(f, "{}", self.value())
    }
}

impl<T: TreeKey, const SIZE: usize> BigKey<T, SIZE> {
    const VALUE_COUNT: usize = size_of::<Self>() / size_of::<T>();

    pub fn new(value: T) -> Self {
        let bytes = bytes_of(&value).repeat(Self::VALUE_COUNT);

        Self(bytes.try_into().unwrap(), PhantomData)
    }

    #[must_use]
    pub fn value(&self) -> T {
        self.validate();

        pod_read_unaligned(&self.0[0..size_of::<T>()])
    }

    fn validate(&self) {
        assert!(self.0 == *self.0[0..size_of::<T>()].repeat(Self::VALUE_COUNT));
    }
}
