include_directories(${CMAKE_SOURCE_DIR})

add_library(otterbrix_pmr pmr.cpp)
add_library(otterbrix::pmr ALIAS otterbrix_pmr)
set_property(TARGET otterbrix_pmr PROPERTY EXPORT_NAME pmr)



add_subdirectory(assert)
add_subdirectory(b_plus_tree)
add_subdirectory(date)
add_subdirectory(regex)
add_subdirectory(spinlock)
add_subdirectory(string_buffer)
add_subdirectory(non_thread_scheduler)
add_subdirectory(file)

if (DEV_MODE)
    add_subdirectory(tests)
endif ()
