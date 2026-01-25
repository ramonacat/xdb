#!/usr/bin/env bash

set -euo pipefail

cargo +nightly fuzz list | xargs -n1 cargo +nightly fuzz cmin
