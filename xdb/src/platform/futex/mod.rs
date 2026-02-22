mod fake;
#[cfg(not(feature = "shuttle"))]
mod linux;

#[cfg(feature = "shuttle")]
pub use fake::Futex;
#[cfg(not(feature = "shuttle"))]
pub use linux::Futex;
