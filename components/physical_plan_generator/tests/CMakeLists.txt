project(test_physical_plan_generator)

add_definitions(-DDEV_MODE)

set(${PROJECT_NAME}_SOURCES
        planner/test_create_plan_match.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        non_thread_scheduler
        otterbrix::log
        otterbrix::logical_plan
        otterbrix::collection
        otterbrix::memory_storage
        otterbrix::dispatcher
        otterbrix::wal
        otterbrix::physical_plan
        otterbrix::cursor
        otterbrix::session
        otterbrix::test_generaty
        spdlog::spdlog
        absl::int128
        absl::flat_hash_map
        absl::node_hash_map
        Boost::boost
        Catch2::Catch2
)

target_include_directories(${PROJECT_NAME} PUBLIC
        ..
        )

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
