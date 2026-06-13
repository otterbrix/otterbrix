include_directories(${CMAKE_SOURCE_DIR})



add_subdirectory(assert)
add_subdirectory(b_plus_tree)
add_subdirectory(date)
add_subdirectory(spinlock)
add_subdirectory(string_heap)
add_subdirectory(non_thread_scheduler)
add_subdirectory(file)

if (DEV_MODE)
    add_subdirectory(tests)
endif ()
