include_directories(${CMAKE_SOURCE_DIR})

if ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")

endif()

add_subdirectory(c)
add_subdirectory(cpp)
add_subdirectory(python)
