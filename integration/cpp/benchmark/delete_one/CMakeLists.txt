set(project benchmark_delete_one)

cmake_policy(SET CMP0048 NEW)
PROJECT(${project} VERSION "${CMAKE_PROJECT_VERSION}" LANGUAGES CXX)

set(${PROJECT_NAME}_SOURCES
        main.cpp
        )

message(STATUS "PROJECT_NAME = ${PROJECT_NAME}")

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES} ${${PROJECT_NAME}_HEADERS})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        benchmark::benchmark
        cpp_otterbrix
        otterbrix::test_generaty
)

file(COPY start-perf DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
