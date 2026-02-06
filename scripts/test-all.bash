#!/usr/bin/env bash

set -euo pipefail

cargo test
cargo clippy
cargo fmt --check
cargo +nightly miri nextest run

pushd xdb-shuttle
# it is important that the tests are ran with nextest, as it runs each test in a separate process, and without that we will run into trouble with static atomics when running shuttle
# TODO figure out why shuttle keeps crashing...
#cargo nextest run --release
cargo clippy
cargo fmt --check
popd

pushd xdb-tests
cargo run --release multi-threaded-random
cargo run --release single-threaded-random
cargo run --release multi-threaded-predictable
cargo run --release single-threaded-predictable
popd

# TODO run xdb-tests here, once its stable enough

while read -r line
do
        # leak detection is disabled, as there are some minor leaks being detected in rust's std::sync::mpmc, over which we have no control
        ASAN_OPTIONS="detect_leaks=0" cargo +nightly fuzz run "$line" -j"$(nproc)" -- -max_total_time=300
done < <(cargo +nightly fuzz list)
