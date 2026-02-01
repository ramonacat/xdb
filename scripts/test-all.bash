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

# TODO run xdb-tests here, once its stable enough

while read -r line
do
        cargo +nightly fuzz run "$line" -j"$(nproc)" -- -max_total_time=300
done < <(cargo +nightly fuzz list)
