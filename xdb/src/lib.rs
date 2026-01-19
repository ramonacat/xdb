#![deny(clippy::all, clippy::pedantic, clippy::nursery, warnings)]
#![allow(clippy::missing_errors_doc)]
// TODO clean up panics and then enable and document them
#![allow(clippy::missing_panics_doc)]
// this one appears to suggest invalid changes
#![allow(clippy::significant_drop_tightening)]
pub mod bplustree;
mod checksum;
pub mod debug;
mod page;
pub mod storage;
