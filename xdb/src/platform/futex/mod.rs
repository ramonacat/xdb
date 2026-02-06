mod fake;
#[cfg(not(feature = "shuttle"))]
mod linux;

#[cfg(not(feature = "shuttle"))]
pub use linux::Futex;

#[cfg(feature = "shuttle")]
pub use fake::Futex;
