#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-build-py}"
BUILD_TYPE="${BUILD_TYPE:-Debug}"
JOBS="${JOBS:-$(nproc)}"
DB_PATH="${DB_PATH:-${ROOT_DIR}/tmp/manual_sql_db}"

echo "[1/4] Checking tools"
command -v cmake >/dev/null 2>&1 || { echo "cmake is required"; exit 1; }
command -v conan >/dev/null 2>&1 || { echo "conan is required"; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "python3 is required"; exit 1; }

if command -v ninja >/dev/null 2>&1; then
  CMAKE_GENERATOR="Ninja"
  BUILD_CMD=(cmake --build . --target otterbrix -- -j "${JOBS}")
else
  CMAKE_GENERATOR="Unix Makefiles"
  BUILD_CMD=(cmake --build . --target otterbrix -- -j "${JOBS}")
fi

if [ ! -f "${HOME}/.conan2/profiles/default" ]; then
  echo "[2/4] Creating Conan profile"
  conan profile detect --force
else
  echo "[2/4] Conan profile exists"
fi

mkdir -p "${ROOT_DIR}/${BUILD_DIR}"
cd "${ROOT_DIR}/${BUILD_DIR}"

echo "[3/4] Installing Conan dependencies"
conan install ../conanfile.py \
  --output-folder=. \
  --build=missing \
  -o build_python=True \
  -s build_type="${BUILD_TYPE}" \
  -s compiler.cppstd=gnu20

TOOLCHAIN_FILE="$(find "${ROOT_DIR}/${BUILD_DIR}" -type f -name conan_toolchain.cmake | head -n1 || true)"
if [ -z "${TOOLCHAIN_FILE}" ]; then
  echo "conan_toolchain.cmake not found under ${ROOT_DIR}/${BUILD_DIR}"
  exit 1
fi

echo "[4/4] Configuring and building Python module"
cmake .. -G "${CMAKE_GENERATOR}" \
  -DCMAKE_TOOLCHAIN_FILE="${TOOLCHAIN_FILE}" \
  -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" \
  -DDEV_MODE=ON \
  -DENABLE_WERROR=OFF \
  -DBUILD_PYTHON=ON

"${BUILD_CMD[@]}"

cd "${ROOT_DIR}"
mkdir -p "${DB_PATH}"

echo "Starting SQL session (db path: ${DB_PATH})"
PYTHONPATH="${ROOT_DIR}/${BUILD_DIR}/integration/python:${PYTHONPATH:-}" \
  python3 "${ROOT_DIR}/scripts/sql_repl.py" --db-path "${DB_PATH}"
