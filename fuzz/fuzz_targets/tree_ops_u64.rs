#![no_main]

mod tree_ops;

use libfuzzer_sys::fuzz_target;
use tree_ops::{TreeAction, run_ops};

fuzz_target!(|actions: Vec<TreeAction<u64, 1024>>| {
    run_ops(&actions);
});
