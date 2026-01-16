use std::{
    fmt::{Debug, Display},
    marker::PhantomData,
};

use arbitrary::Arbitrary;
use bytemuck::{Pod, Zeroable, bytes_of, pod_read_unaligned};

#[derive(Clone, Copy, Pod, Zeroable, Ord, PartialOrd, PartialEq, Eq)]
#[repr(transparent)]
pub struct BigKey<T>([u8; 256], PhantomData<T>);

impl<T: Pod + Display> Debug for BigKey<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BigKey({})", self.value())
    }
}

impl<'a, T: Arbitrary<'a> + Pod> Arbitrary<'a> for BigKey<T> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let value = u.arbitrary()?;

        Ok(Self::new(value))
    }
}

impl<T: Display + Pod> Display for BigKey<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.validate();

        write!(f, "{}", self.value())
    }
}

impl<T: Pod> BigKey<T> {
    const VALUE_COUNT: usize = size_of::<BigKey<T>>() / size_of::<T>();

    pub fn new(value: T) -> Self {
        let bytes = bytes_of(&value).repeat(Self::VALUE_COUNT);

        Self(bytes.try_into().unwrap(), PhantomData)
    }

    pub fn value(&self) -> T {
        self.validate();

        pod_read_unaligned(&self.0[0..size_of::<T>()])
    }

    fn validate(&self) {
        assert!(self.0 == *self.0[0..size_of::<T>()].repeat(Self::VALUE_COUNT));
    }
}
