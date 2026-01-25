// TODO a script that will minimize the corpuses for all the fuzz targets
// TODO store the fuzz corpuses somewhere (git or not git? idk)
// TODO write a script which will run all the tests (+miri) and all the fuzz targets
// TODO create a server + a test app which will run many threads with many simulatenous
// transactions to strees test

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
