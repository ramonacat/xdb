// TODO replace xdb-client/xdb-server with a single app that runs random parallel transactions and
// ensures the tree is consistent when doing that
// TODO see what we can prove with creusot
// TODO remove xdb-cli
// TODO replace InMemoryStorage with a file-backed one
// TODO setup clippy/cargo-fmt to automatically format and group imports
// TODO multithreaded fuzzing? not ideal, because it's not deterministic, but maybe could be a bit
// better at finding bugs?

// TODO require `SAFETY: ...` comments
#![deny(clippy::all, clippy::pedantic, clippy::nursery, warnings)]
#![allow(clippy::missing_errors_doc)]
// TODO clean up panics and then enable and document them
#![allow(clippy::missing_panics_doc)]
// this one appears to suggest invalid changes
#![allow(clippy::significant_drop_tightening)]

use std::ops::{Add, Div, Mul};
pub mod bplustree;
mod checksum;
pub mod debug;
mod page;
mod platform;
pub mod storage;

#[cfg(feature = "shuttle")]
pub use shuttle::{hint, sync, thread};

#[cfg(not(feature = "shuttle"))]
pub use std::{hint, sync, thread};

#[derive(Debug, Clone, Copy)]
enum Size {
    GiB(usize),
    #[allow(unused)]
    MiB(usize),
    #[allow(unused)]
    KiB(usize),
    B(usize),
}

impl Size {
    const fn of<T>() -> Self {
        Self::B(size_of::<T>())
    }

    const fn of_val<T: ?Sized>(val: &T) -> Self {
        Self::B(size_of_val(val))
    }

    const fn as_bytes(self) -> usize {
        match self {
            Self::GiB(x) => x * 1024 * 1024 * 1024,
            Self::MiB(x) => x * 1024 * 1024,
            Self::KiB(x) => x * 1024,
            Self::B(x) => x,
        }
    }

    const fn add(self, value: Self) -> Self {
        Self::B(self.as_bytes() + value.as_bytes())
    }

    const fn multiply(self, value: usize) -> Self {
        Self::B(self.as_bytes() * value)
    }

    // TODO also add a divide by usize that returns Self
    const fn divide(self, value: Self) -> usize {
        self.as_bytes() / value.as_bytes()
    }

    const fn subtract(self, value: Self) -> Self {
        Self::B(self.as_bytes() - value.as_bytes())
    }

    const fn is_equal(self, value: Self) -> bool {
        self.as_bytes() == value.as_bytes()
    }
}

impl PartialOrd for Size {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Size {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_bytes().cmp(&other.as_bytes())
    }
}

impl Mul<usize> for Size {
    type Output = Self;

    fn mul(self, rhs: usize) -> Self::Output {
        Self::B(self.as_bytes() * rhs)
    }
}

impl Div<usize> for Size {
    type Output = Self;

    fn div(self, rhs: usize) -> Self::Output {
        Self::B(self.as_bytes() / rhs)
    }
}

impl Div<Self> for Size {
    type Output = usize;

    fn div(self, rhs: Self) -> Self::Output {
        self.as_bytes() / rhs.as_bytes()
    }
}

impl Add<Self> for Size {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self::B(self.as_bytes() + rhs.as_bytes())
    }
}

impl PartialEq for Size {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for Size {}
