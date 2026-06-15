project(test_assert)

add_definitions(-DDEV_MODE)

set(${PROJECT_NAME}_SOURCES
        test_assert.cpp
        )

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::log
        Boost::boost
        otterbrix::assert
        Catch2::Catch2
)

include(CTest)
include(Catch)
otterbrix_discover_tests(${PROJECT_NAME})
