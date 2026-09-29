# A downstream project as a conan consumer sees otterbrix: find_package(otterbrix) alone must define
# otterbrix_add_parser_extension(). The grammar is components/sql/demo_extension's, copied in by the driver.
cmake_minimum_required(VERSION 3.25)
project(otterbrix_package_consumer CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(otterbrix REQUIRED)

otterbrix_add_parser_extension(demo_extension PREFIX demo SOURCES demo_extension.cpp)

add_executable(consumer main.cpp)
target_link_libraries(consumer PRIVATE otterbrix::demo_extension)
