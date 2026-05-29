#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$RUST_DIR/../.." && pwd)"

LIB_DIR="${OTTERBRIX_LIB_DIR:-$REPO_ROOT/build/integration/c}"

if [[ ! -f "$LIB_DIR/libotterbrix.so" ]]; then
    echo "error: libotterbrix.so not found in $LIB_DIR" >&2
    echo "       run scripts/build-cpp.sh first or set OTTERBRIX_LIB_DIR." >&2
    exit 1
fi

cd "$RUST_DIR"

# Embed rpath for every artifact (lib unit tests, integration tests, doctests)
# so libotterbrix.so is found at runtime without LD_LIBRARY_PATH.
RPATH="-C link-arg=-Wl,-rpath,$LIB_DIR"
export RUSTFLAGS="${RUSTFLAGS:-} $RPATH"
export RUSTDOCFLAGS="${RUSTDOCFLAGS:-} $RPATH"

cargo test --workspace "$@"
cargo test --workspace --doc "$@"
