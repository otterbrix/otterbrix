include_directories()

add_subdirectory(runner)

file(COPY start-benchmark DESTINATION ${CMAKE_CURRENT_BINARY_DIR})