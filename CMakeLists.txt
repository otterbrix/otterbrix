cmake_minimum_required(VERSION 3.22...3.31 FATAL_ERROR)

set_property(GLOBAL PROPERTY GLOBAL_DEPENDS_NO_CYCLES TRUE)

set(CMAKE_PROJECT_VERSION_MAJOR 1)
set(CMAKE_PROJECT_VERSION_MINOR 0)
set(CMAKE_PROJECT_VERSION_PATCH 0)
set(CMAKE_PROJECT_VERSION "${CMAKE_PROJECT_VERSION_MAJOR}.${CMAKE_PROJECT_VERSION_MINOR}.${CMAKE_PROJECT_VERSION_PATCH}")
PROJECT(otterbrix VERSION "${CMAKE_PROJECT_VERSION}" LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED YES)
set(CMAKE_CXX_EXTENSIONS OFF)

# Coroutines support for GCC (Clang enables coroutines by default in C++20)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    add_compile_options(-fcoroutines)
endif()

message (STATUS "CMAKE_BUILD_TYPE = ${CMAKE_BUILD_TYPE}")
message (STATUS "CXX_STANDARD = ${CMAKE_CXX_STANDARD}")
message(STATUS "ARCH : ${CMAKE_SYSTEM_NAME}")

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DBOOST_ASIO_HAS_MOVE -DEXCEPTIONS_DISABLE=OFF " CACHE STRING "compile flags" FORCE)

option(DEV_MODE "developer mode" OFF)
option(ENABLE_ASAN "address sanitizer" OFF)
option(EXAMPLE "Example" OFF)
option(ALLOW_BENCHMARK "Allow benchmark" OFF)
option(CONAN_EXPERIMENTAL_INCLUDE "CONAN EXPERIMENT INCLUDE" ON)
option(APPLICATION_SERVER "application server" OFF)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
add_definitions(-D_GNU_SOURCE)

if (DEV_MODE)
    message("DEV_MOD ON")
    add_definitions(-DDEV_MODE)
    enable_testing()
endif()

if (ALLOW_BENCHMARK)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
endif()

if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")

elseif(${CMAKE_SYSTEM_NAME} MATCHES "Emscripten")

elseif(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    # stack track
    # unused by clang and generates an error with [-Werror,-Wunused-command-line-argument]
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -rdynamic")
endif()

# We do not use -Werror flag with sanitizers for the following reasons:
# 1) Sanitizers force us to build absl locally, which does contains warnings
# 2) Incomplete support of macOS makes development too cumbersome with -Werror enabled
set(USE_Werror "$<NOT:$<OR:$<BOOL:${APPLE}>,$<BOOL:${ENABLE_ASAN}>>>")

if ((CMAKE_CXX_COMPILER_ID MATCHES "GNU") OR (CMAKE_CXX_COMPILER_ID MATCHES "Clang"))
    add_compile_options(

            $<${USE_Werror}:-Werror>

            -Wall
            -Wextra
            -Wpedantic

            -Wcast-align
            -Wcast-qual
            -Wconversion
            -Wctor-dtor-privacy
            -Wenum-compare
            -Wfloat-equal
            -Wnon-virtual-dtor
            -Wold-style-cast
            -Woverloaded-virtual
            -Wredundant-decls
            -Wsign-conversion
            -Wsign-promo
    )
elseif (CMAKE_CXX_COMPILER_ID MATCHES "MSVC")
    add_compile_options(/W4 /WX)
endif ()

# configuration
#############################################################################################
#clang tools

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")
find_package(Sanitizers REQUIRED)

if (ENABLE_ASAN)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g -fno-omit-frame-pointer -fsanitize=address")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
endif()

# TODO remove
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g -fno-omit-frame-pointer -fsanitize=thread")
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g -fno-omit-frame-pointer -fsanitize=address")
# set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")


#clang tools
#############################################################################################
# conan
message(STATUS "DCMAKE_TOOLCHAIN_FILE = ${CMAKE_TOOLCHAIN_FILE}")

if(POLICY CMP0167)
    cmake_policy(SET CMP0167 OLD)
endif()
find_package(Boost 1.86.0 REQUIRED)
find_package(fmt 11.1.3 REQUIRED)
find_package(spdlog 1.15.1 REQUIRED)
find_package(pybind11 2.10.0 REQUIRED)
find_package(msgpack 4.1.1 REQUIRED)
find_package(Catch2 2.13.7 REQUIRED)
find_package(benchmark 1.6.1 REQUIRED)
find_package(ZLIB 1.2.12 REQUIRED)
find_package(BZip2 1.0.8 REQUIRED)
find_package(magic_enum 0.8.1 REQUIRED)
find_package(actor-zeta REQUIRED)

if (ENABLE_ASAN)
    # build abseil-cpp locally to avoid ABI issues
    include(FetchContent)
    FetchContent_Declare(
            absl
            URL https://github.com/abseil/abseil-cpp/archive/refs/tags/20230802.1.zip
    )
    FetchContent_MakeAvailable(absl)
else()
    find_package(absl 20230802.1 REQUIRED)
endif()


# conan
#############################################################################################
# ccache

find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
    message(STATUS "Using ccache: ${CCACHE_FOUND}")
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ${CCACHE_FOUND})
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ${CCACHE_FOUND})
    set(ENV{CCACHE_COMMENTS} "1")
endif(CCACHE_FOUND)

# ccache
#############################################################################################
# wasm
if(WASM_BUILD)

endif()
# wasm
#############################################################################################

#set(CMAKE_BUILD_WITH_INSTALL_RPATH ON)
#set( CMAKE_SKIP_RPATH ON)
set(CMAKE_SKIP_BUILD_RPATH FALSE)

# when building, don't use the install RPATH already
# (but later on when installing)
set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)

#set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")

# add the automatically determined parts of the RPATH
# which point to directories outside the build tree to the install RPATH
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)


include_directories(components)
add_subdirectory(core)
add_subdirectory(components)

if (APPLICATION_SERVER)
    set(WASM_OBJECT OFF)
    set(WASM_BUILD OFF)
endif ()

include_directories(services)
add_subdirectory(services)
add_subdirectory(integration)
if(EXAMPLE)
    add_subdirectory(example)
endif()



#set(CPACK_GENERATOR "DEB")
#set(CPACK_SET_DESTDIR true)
#set(CPACK_PROJECT_CONFIG_FILE "${CMAKE_MODULE_PATH}/CPackOptions.cmake" )
#set(CPACK_DEB_COMPONENT_INSTALL ON)
#set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)#
#set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE "amd64")
#set(CPACK_DEBIAN_PACKAGE_DESCRIPTION "")
#set(CPACK_DEBIAN_PACKAGE_MAINTAINER "")


#include (CPack)
