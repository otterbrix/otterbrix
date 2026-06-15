project(test_core)

add_definitions(-DDEV_MODE)

set(${PROJECT_NAME}_SOURCES
        test_buffer.cpp
        test_scalar.cpp
        test_uvector.cpp
        )

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        Catch2::Catch2
        Boost::boost
        otterbrix::assert
        otterbrix::log
        ${CMAKE_THREAD_LIBS_INIT}
)

include(CTest)
include(Catch)
otterbrix_discover_tests(${PROJECT_NAME})
