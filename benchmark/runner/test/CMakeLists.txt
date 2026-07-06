set(project benchmark_runner_test)

cmake_policy(SET CMP0048 NEW)
PROJECT(${project} VERSION "${CMAKE_PROJECT_VERSION}" LANGUAGES CXX)

add_definitions(-DDEV_MODE)

# Compile the runner sources under test alongside the test TU (the runner is an
# executable, not a library, so there is nothing to link against). main.cpp is
# deliberately excluded — Catch provides main via CATCH_CONFIG_MAIN.
set(${PROJECT_NAME}_SOURCES
        test_benchmark_reset.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../benchmark_runner.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../interpreted_benchmark.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../sql_benchmark.cpp
)

message(STATUS "PROJECT_NAME = ${PROJECT_NAME}")

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_include_directories(${PROJECT_NAME} PRIVATE
        ${CMAKE_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/..
)

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        cpp_otterbrix
        Catch2::Catch2
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME} PROPERTIES TIMEOUT 600)
