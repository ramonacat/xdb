use thiserror::Error;

#[derive(Debug, Error)]
#[allow(unused)]
pub enum FutexError {
    #[error("the value has changed while the wait was attempted")]
    Race,
    #[error("timed out")]
    Timeout,
    #[error("the kernel state is inconsistent with the method called")]
    InconsistentState,
}

mod fake;
#[cfg(not(feature = "shuttle"))]
mod linux;

#[cfg(not(feature = "shuttle"))]
pub use linux::Futex;

#[cfg(feature = "shuttle")]
pub use fake::Futex;
