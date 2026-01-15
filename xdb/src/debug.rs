use std::fmt::{Debug, Display};

use arbitrary::Arbitrary;
use bytemuck::{Pod, Zeroable};

const BIG_KEY_SIZE: usize = 32;

#[derive(Clone, Copy, Pod, Zeroable, Ord, PartialOrd, PartialEq, Eq)]
#[repr(transparent)]
pub struct BigKey([u64; BIG_KEY_SIZE]);

impl Debug for BigKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BigKey({})", self.0[0])
    }
}

impl<'a> Arbitrary<'a> for BigKey {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let value: u64 = u.arbitrary()?;

        Ok(Self(vec![value; BIG_KEY_SIZE].try_into().unwrap()))
    }
}

impl Display for BigKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.validate();

        write!(f, "{}", self.0[0])
    }
}

impl BigKey {
    pub fn new(value: u64) -> Self {
        Self(vec![value; BIG_KEY_SIZE].try_into().unwrap())
    }

    pub fn value(&self) -> u64 {
        self.validate();

        self.0[0]
    }

    fn validate(&self) {
        assert!(vec![self.0[0]; self.0.len()] == self.0);
    }
}
