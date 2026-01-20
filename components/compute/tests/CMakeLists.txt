project(test_compute)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES
        test_registry.cpp
        test_execution.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::compute
        Boost::boost
        Catch2::Catch2
        actor-zeta::actor-zeta
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
