#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$RUST_DIR/../.." && pwd)"

BUILD_TYPE="${BUILD_TYPE:-Release}"
BUILD_DIR="${BUILD_DIR:-build}"
JOBS="${JOBS:-$(nproc)}"

cd "$REPO_ROOT"

for tool in conan cmake ninja; do
    command -v "$tool" >/dev/null 2>&1 || {
        echo "error: '$tool' not found in PATH" >&2
        exit 1
    }
done

mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

[[ -f conanfile.py ]] || cp ../conanfile.py .

if [[ ! -d "build/$BUILD_TYPE/generators" ]]; then
    conan install . --build missing \
        -s "build_type=$BUILD_TYPE" \
        -s compiler.cppstd=20
fi

cmake .. -G Ninja \
    -DCMAKE_TOOLCHAIN_FILE="./build/$BUILD_TYPE/generators/conan_toolchain.cmake" \
    -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
    -DDEV_MODE=ON
# Only the C facade (libotterbrix.so) is needed by the Rust crates; building
# every C++ test target is wasteful and pulls in upstream tests that may not
# compile cleanly on every host toolchain.
cmake --build . --target c_otterbrix -- -j "$JOBS"

LIB="$REPO_ROOT/$BUILD_DIR/integration/c/libotterbrix.so"
[[ -f "$LIB" ]] || {
    echo "error: build finished but $LIB is missing" >&2
    exit 2
}
