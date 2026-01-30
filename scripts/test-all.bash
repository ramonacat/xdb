#!/usr/bin/env bash

set -euo pipefail

cargo test
cargo clippy
cargo fmt --check
cargo +nightly miri nextest run
pushd xdb-shuttle
cargo test --release
cargo clippy
cargo fmt --check
popd

while read -r line
do
        cargo +nightly fuzz run "$line" -j"$(nproc)" -- -max_total_time=300
done < <(cargo +nightly fuzz list)
