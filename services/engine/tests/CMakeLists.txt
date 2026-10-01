project(test_engine)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES
        test_engine.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::log
        otterbrix::logical_plan
        otterbrix::session
        otterbrix::services
        otterbrix::sql
        otterbrix::cursor

        absl::crc32c
        absl::int128
        actor-zeta::actor-zeta
        Catch2::Catch2WithMain
        Boost::boost
)

target_include_directories(${PROJECT_NAME} PUBLIC
    ..
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
