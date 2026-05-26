#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${ROOT_DIR}/build/integration/cpp/test/test_otterbrix"

if [[ ! -x "${BIN}" ]]; then
  echo "Executable not found: ${BIN}"
  echo "Build it first:"
  echo "  cmake --build build --target test_otterbrix -j4"
  exit 1
fi

echo "Running manual index_join benchmark via executable:"
echo "  ${BIN}"
echo

# -s prints INFO/REQUIRE context for successful tests too.
"${BIN}" -s "integration::cpp::manual_index_join_benchmark"

