#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$RUST_DIR/../.." && pwd)"

cd "$REPO_ROOT"

for tool in conan cmake ninja; do
    command -v "$tool" >/dev/null 2>&1 || {
        echo "error: '$tool' not found in PATH" >&2
        exit 1
    }
done

conan install . -pr:h profiles/release -pr:b default --build=missing -of build/release
cmake --preset release
# Only the C facade (libotterbrix.so) is needed by the Rust crates; building
# every C++ test target is wasteful and pulls in upstream tests that may not
# compile cleanly on every host toolchain.
cmake --build --preset release --target c_otterbrix

if [[ "$(uname -s)" == Darwin ]]; then
    LIB="$REPO_ROOT/build/release/integration/c/libotterbrix.dylib"
else
    LIB="$REPO_ROOT/build/release/integration/c/libotterbrix.so"
fi
[[ -f "$LIB" ]] || {
    echo "error: build finished but $LIB is missing" >&2
    exit 2
}
