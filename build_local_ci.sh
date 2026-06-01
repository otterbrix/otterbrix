#!/bin/bash
# Mirrors .github/workflows/manylinux2014.yml build-wheel job.
# Build is incremental — reuses _skbuild/linux-x86_64-3.9/cmake-build if present.
set -euo pipefail

export PYTHON_PATH=/opt/python/cp39-cp39
export PYTHON_EXECUTABLE=$PYTHON_PATH/bin/python
export PATH=$PYTHON_PATH:$PYTHON_PATH/bin:$HOME/.local/bin:$PATH

echo "==> installing yum deps"
yum install -y ninja-build wget unzip zip flex bison git > /dev/null

echo "==> upgrading pip and installing build deps"
"$PYTHON_EXECUTABLE" -m pip install --user --retries=30 --upgrade pip > /dev/null
"$PYTHON_EXECUTABLE" -m pip install --user --retries=30 conan==2.24.0 wheel setuptools scikit-build pytest==6.2.5 cmake==3.28.3 > /dev/null

echo "==> conan setup"
conan profile detect --force > /dev/null
if [ ! -d /tmp/conan-duckstax ]; then
  git clone --depth 1 https://github.com/duckstax/conan-duckstax.git /tmp/conan-duckstax > /dev/null
fi
conan remote add duckstax /tmp/conan-duckstax --type=local-recipes-index 2>/dev/null || true

echo "==> staging wheel scaffolding at repo root"
cp integration/python/otterbrix/get_path.py .
cp integration/python/otterbrix/setup.py .
cp integration/python/otterbrix/pyproject.toml .
touch MANIFEST.in

BUILD_OUT="./$("$PYTHON_EXECUTABLE" get_path.py)"
echo "==> BUILD_OUT=$BUILD_OUT"
mkdir -p "$BUILD_OUT"

# Always re-run conan install — it's idempotent if /root/.conan2 cache is mounted.
# Toolchain files reference absolute paths inside the conan cache, so a stale
# toolchain from a prior container points at nonexistent libs.
echo "==> wiping stale conan generator files"
rm -rf "$BUILD_OUT/build"

echo "==> conan install"
conan install conanfile.py --build missing -s build_type=Release -o build_python=True -of="$BUILD_OUT"

echo "==> building wheel"
# Cap ninja parallelism: Docker-on-Mac defaults to ~4 GB, and the heaviest
# files (e.g. dispatcher.cpp, 1052 LoC heavy templates) need ~3 GB each at -O3.
# -j1 is the only safe ceiling at the default Docker memory.
export CMAKE_BUILD_PARALLEL_LEVEL=1
"$PYTHON_EXECUTABLE" setup.py bdist_wheel --verbose 2>&1 | tail -200

echo "==> built wheels:"
ls -la dist/*.whl 2>/dev/null || { echo "NO WHEELS BUILT"; exit 1; }

echo "==> dumping undefined symbols from the freshly-built .so"
SO=$(find _skbuild -name "otterbrix.cpython*.so" | head -1)
echo "SO=$SO"
nm -D --undefined-only "$SO" | c++filt | grep -E "components::|otterbrix" | sort -u || echo "(no otterbrix-related undefined symbols)"
