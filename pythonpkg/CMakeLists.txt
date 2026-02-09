cmake_minimum_required(VERSION 3.16)

project(pythonpkg)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Python REQUIRED COMPONENTS Interpreter Development)
find_package(pybind11 2.10 CONFIG REQUIRED)

find_package(otterbrix CONFIG REQUIRED)

find_package(Catch2 2.13.7 REQUIRED)

find_package(magic_enum 0.8.1 REQUIRED)

find_package(utf8proc 2.9.0 CONFIG REQUIRED)
add_library(otterbrix_vector SHARED IMPORTED)
set_target_properties(otterbrix_vector PROPERTIES
    IMPORTED_LOCATION "/home/imalofeev98/.conan2/p/b/otter545620f2add5e/p/lib/libotterbrix_vector.so"
)

add_library(otterbrix_table SHARED IMPORTED)
set_target_properties(otterbrix_table PROPERTIES
    IMPORTED_LOCATION "/home/imalofeev98/.conan2/p/b/otter545620f2add5e/p/lib/libotterbrix_table.so"
)


add_subdirectory(core)
add_subdirectory(integration)
#add_subdirectory(tmp)
