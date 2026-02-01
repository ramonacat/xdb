use thiserror::Error;

#[derive(Debug, Error)]
pub enum FutexError {
    #[allow(unused)] // TODO try to simulate this case in the fake futex
    #[error("the value has changed while the wait was attempted")]
    Race,
}

mod fake;
#[cfg(not(feature = "shuttle"))]
mod linux;

#[cfg(not(feature = "shuttle"))]
pub use linux::Futex;

#[cfg(feature = "shuttle")]
pub use fake::Futex;
