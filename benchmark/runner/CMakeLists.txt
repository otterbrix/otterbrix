set(project benchmark_runner)

cmake_policy(SET CMP0048 NEW)
PROJECT(${project} VERSION "${CMAKE_PROJECT_VERSION}" LANGUAGES CXX)

set(${PROJECT_NAME}_SOURCES
        main.cpp
        benchmark_runner.cpp
        interpreted_benchmark.cpp
        sql_benchmark.cpp
)

message(STATUS "PROJECT_NAME = ${PROJECT_NAME}")

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_include_directories(${PROJECT_NAME} PRIVATE
        ${CMAKE_SOURCE_DIR}
)

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        cpp_otterbrix
)