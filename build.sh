#!/bin/bash

set -e  # Exit on error
set -u  # Exit on undefined variable

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
ACTOR_ZETA_VERSION="1.1.1"
BUILD_TYPE="${BUILD_TYPE:-Debug}"
CXX_STANDARD="${CXX_STANDARD:-20}"
JOBS="${JOBS:-$(nproc)}"
DEV_MODE="${DEV_MODE:-ON}"
ENABLE_TESTS="${ENABLE_TESTS:-OFF}"

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Otterbrix Build Script${NC}"
echo -e "${GREEN}========================================${NC}"
echo "Build Type: $BUILD_TYPE"
echo "C++ Standard: $CXX_STANDARD"
echo "Jobs: $JOBS"
echo "Dev Mode: $DEV_MODE"
echo ""

# Function to print section headers
print_section() {
    echo -e "\n${YELLOW}>>> $1${NC}\n"
}

# Function to print error and exit
error_exit() {
    echo -e "${RED}ERROR: $1${NC}" >&2
    exit 1
}

# Check if running in the correct directory
if [ ! -f "CMakeLists.txt" ] || [ ! -d "actor-zeta" ]; then
    error_exit "Please run this script from the otterbrix root directory"
fi

# Step 1: Initialize git submodules
print_section "Step 1: Initializing git submodules"
if [ ! -f "actor-zeta/CMakeLists.txt" ]; then
    echo "Initializing actor-zeta submodule..."
    git submodule init
    git submodule update --recursive
else
    echo "actor-zeta submodule already initialized"
    git submodule update --recursive
fi

# Step 2: Check dependencies
print_section "Step 2: Checking dependencies"

# Check for required tools
command -v cmake >/dev/null 2>&1 || error_exit "cmake is not installed. Please install cmake."
command -v conan >/dev/null 2>&1 || error_exit "conan is not installed. Please install conan 2.20.0: pip3 install conan==2.20.0"
command -v ninja >/dev/null 2>&1 || error_exit "ninja is not installed. Please install ninja-build."

# Check Conan version
CONAN_VERSION=$(conan --version | grep -oP '\d+\.\d+\.\d+' | head -1)
echo "Found Conan version: $CONAN_VERSION"
if [[ "$CONAN_VERSION" < "2.0.0" ]]; then
    error_exit "Conan 2.x is required. Found version $CONAN_VERSION. Please upgrade: pip3 install conan==2.20.0"
fi

# Step 3: Setup Conan profile
print_section "Step 3: Setting up Conan profile"
if [ ! -f "$HOME/.conan2/profiles/default" ]; then
    echo "Detecting Conan profile..."
    conan profile detect --force
else
    echo "Conan profile already exists"
fi

# Step 4: Create Conan package for actor-zeta
print_section "Step 4: Creating local Conan package for actor-zeta"

# Create a temporary conanfile.py for actor-zeta
ACTOR_ZETA_CONANFILE="actor-zeta/conanfile_temp.py"
cat > "$ACTOR_ZETA_CONANFILE" << 'EOF'
from conan import ConanFile
from conan.tools.cmake import CMake, CMakeToolchain, CMakeDeps, cmake_layout
from conan.tools.files import copy

class ActorZetaConan(ConanFile):
    name = "actor-zeta"
    version = "1.1.1"
    license = "BSD-3-Clause"
    url = "https://github.com/otterbrix/actor-zeta"
    description = "Actor-Zeta is a C++ actor framework"
    settings = "os", "compiler", "build_type", "arch"
    options = {
        "cxx_standard": [11, 14, 17, 20],
        "fPIC": [True, False],
        "exceptions_disable": [True, False],
        "rtti_disable": [True, False],
        "allow_tests": [True, False],
        "allow_examples": [True, False],
        "allow_benchmark": [True, False],
    }
    default_options = {
        "cxx_standard": 17,
        "fPIC": True,
        "exceptions_disable": False,
        "rtti_disable": False,
        "allow_tests": False,
        "allow_examples": False,
        "allow_benchmark": False,
    }
    exports_sources = "CMakeLists.txt", "header/*", "source/*", "cmake/*"

    def requirements(self):
        if self.options.allow_tests:
            self.requires("catch2/2.13.6")
        if self.options.allow_benchmark:
            self.requires("benchmark/1.6.2")

    def layout(self):
        cmake_layout(self)

    def generate(self):
        tc = CMakeToolchain(self)
        tc.variables["CMAKE_CXX_STANDARD"] = str(self.options.cxx_standard)
        tc.variables["ALLOW_TESTS"] = "ON" if self.options.allow_tests else "OFF"
        tc.variables["ALLOW_EXAMPLES"] = "ON" if self.options.allow_examples else "OFF"
        tc.variables["ALLOW_BENCHMARK"] = "ON" if self.options.allow_benchmark else "OFF"
        tc.variables["EXCEPTIONS_DISABLE"] = "ON" if self.options.exceptions_disable else "OFF"
        tc.variables["RTTI_DISABLE"] = "ON" if self.options.rtti_disable else "OFF"
        tc.generate()

        deps = CMakeDeps(self)
        deps.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        # Copy headers
        copy(self, "*.hpp", src=self.source_folder + "/header", dst=self.package_folder + "/include", keep_path=True)
        copy(self, "*.ipp", src=self.source_folder + "/header", dst=self.package_folder + "/include", keep_path=True)
        # Copy library file
        copy(self, "*.a", src=self.build_folder, dst=self.package_folder + "/lib", keep_path=False)
        copy(self, "*.so*", src=self.build_folder, dst=self.package_folder + "/lib", keep_path=False)

    def package_info(self):
        self.cpp_info.libs = ["actor-zeta"]
        self.cpp_info.includedirs = ["include"]
        self.cpp_info.set_property("cmake_file_name", "actor-zeta")
        self.cpp_info.set_property("cmake_target_name", "actor-zeta::actor-zeta")
EOF

echo "Created temporary conanfile.py for actor-zeta"

# Build and create the Conan package locally
cd actor-zeta
echo "Creating Conan package for actor-zeta..."
conan create conanfile_temp.py \
    --version="$ACTOR_ZETA_VERSION" \
    --build=missing \
    -s build_type="$BUILD_TYPE" \
    -s compiler.cppstd=gnu"$CXX_STANDARD" \
    -o "actor-zeta/*:cxx_standard=$CXX_STANDARD" \
    -o "actor-zeta/*:fPIC=True" \
    -o "actor-zeta/*:exceptions_disable=False" \
    -o "actor-zeta/*:rtti_disable=False"

cd ..
echo -e "${GREEN}✓ actor-zeta package created successfully${NC}"

# Step 5: Install Otterbrix dependencies
print_section "Step 5: Installing Otterbrix dependencies with Conan"

mkdir -p build
cd build

# Install all dependencies including the local actor-zeta
echo "Installing dependencies..."
conan install ../conanfile.py \
    --build=missing \
    -s build_type="$BUILD_TYPE" \
    -s compiler.cppstd=gnu"$CXX_STANDARD"

echo -e "${GREEN}✓ Dependencies installed successfully${NC}"

# Step 6: Configure CMake
print_section "Step 6: Configuring CMake"

# Remove stale CMake cache to avoid using wrong build type toolchain
rm -f CMakeCache.txt

cmake .. -G Ninja \
    -DCMAKE_TOOLCHAIN_FILE=./$BUILD_TYPE/generators/conan_toolchain.cmake \
    -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
    -DDEV_MODE="$DEV_MODE" \
    -DEXAMPLE=ON

echo -e "${GREEN}✓ CMake configured successfully${NC}"

# Step 7: Build Otterbrix
print_section "Step 7: Building Otterbrix"

cmake --build . --target all -- -j "$JOBS"

echo -e "${GREEN}✓ Otterbrix built successfully${NC}"

# Tests disabled

# Summary
print_section "Build Summary"
echo -e "${GREEN}✓ Build completed successfully!${NC}"
echo ""
echo "Build artifacts location: $(pwd)"
echo "Main library: $(pwd)/integration/c/libotterbrix.so"
echo ""
echo "To run the build again with different options, use:"
echo "  BUILD_TYPE=Release ./build.sh"
echo "  JOBS=8 ./build.sh"
echo ""
echo -e "${GREEN}========================================${NC}"
