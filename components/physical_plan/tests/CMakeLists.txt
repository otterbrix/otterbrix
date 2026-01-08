project(test_physical_plan_collection)

add_definitions(-DDEV_MODE)

set(${PROJECT_NAME}_SOURCES
        operators/test_aggregate_operators.cpp
        operators/test_group_operator.cpp
        operators/test_operators.cpp
        operators/test_get_operators.cpp
        operators/test_merge_operators.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        non_thread_scheduler
        otterbrix::log
        otterbrix::collection
        otterbrix::memory_storage
        otterbrix::cursor
        otterbrix::session
        otterbrix::physical_plan
        otterbrix::test_generaty
        spdlog::spdlog
        absl::int128
        Boost::boost
        Catch2::Catch2
        # Re-link disk at end to resolve circular dependency symbols (index uses disk symbols)
        otterbrix::disk
)

target_include_directories(${PROJECT_NAME} PUBLIC
        ..
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
