include_directories()

add_subdirectory(runner)
add_subdirectory(write)

file(COPY start-benchmark DESTINATION ${CMAKE_CURRENT_BINARY_DIR})