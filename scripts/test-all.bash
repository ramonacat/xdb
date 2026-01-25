#!/usr/bin/env bash

set -euo pipefail

cargo test
cargo clippy
cargo fmt --check
cargo +nightly miri nextest run

while read -r line
do
        cargo +nightly fuzz run "$line" -j"$(nproc)" -- -max_total_time=300
done < <(cargo +nightly fuzz list)
