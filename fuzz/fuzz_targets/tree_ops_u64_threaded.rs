#![no_main]

mod tree_ops;

use libfuzzer_sys::fuzz_target;

use crate::tree_ops::{InThreadAction, run_ops_threaded};

fuzz_target!(|actions: Vec<InThreadAction<u64, 1024>>| {
    run_ops_threaded(&actions);
});
