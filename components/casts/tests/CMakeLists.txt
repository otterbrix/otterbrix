project(test_casts)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES
        test_registry.cpp
        test_default_casts.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::casts
        Boost::boost
        Catch2::Catch2WithMain
        actor-zeta::actor-zeta
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})