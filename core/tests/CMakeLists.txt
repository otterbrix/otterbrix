project(test_core)

add_definitions(-DDEV_MODE)

set(${PROJECT_NAME}_SOURCES
        test_buffer.cpp
        test_scalar.cpp
        test_uvector.cpp
        test_resource_tracer.cpp
        test_regex.cpp
        test_error_residency.cpp
        )

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        Catch2::Catch2WithMain
        Boost::boost
        otterbrix::assert
        otterbrix::log
        otterbrix::regex
        ${CMAKE_THREAD_LIBS_INIT}
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
