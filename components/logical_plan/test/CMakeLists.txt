project(test_logical_plan)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES
        test_node_create_type.cpp
        test_node_identity.cpp
        test_param_storage.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::logical_plan
        otterbrix::expressions
        otterbrix::catalog
        otterbrix::table
        otterbrix::types
        otterbrix::vector
        otterbrix::file
        otterbrix::session
        otterbrix::log

        absl::int128
        absl::crc32c
        Boost::boost
        Catch2::Catch2WithMain
        spdlog::spdlog
)

target_include_directories(${PROJECT_NAME} PUBLIC
    ..
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
