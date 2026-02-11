include_directories()

add_subdirectory(init)
add_subdirectory(insert_one)
add_subdirectory(insert_many)
add_subdirectory(find_one)
add_subdirectory(find_many)
add_subdirectory(delete_one)
add_subdirectory(delete_many)
add_subdirectory(index)

file(COPY start-benchmark DESTINATION ${CMAKE_CURRENT_BINARY_DIR})