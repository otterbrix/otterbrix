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
        otterbrix::cursor
        otterbrix::session
        otterbrix::physical_plan
        otterbrix::test_generaty
        spdlog::spdlog
        absl::int128
        Boost::boost
        Catch2::Catch2
)

# Handle circular dependency: index <-> disk <-> collection
# Use linker group on Linux to resolve mutual symbol references
if(UNIX AND NOT APPLE)
    target_link_libraries(${PROJECT_NAME} PRIVATE
        -Wl,--start-group
        otterbrix::collection
        otterbrix::disk
        otterbrix::index
        otterbrix::dispatcher
        otterbrix::wal
        -Wl,--end-group
    )
else()
    # macOS/other: link twice to resolve circular deps
    target_link_libraries(${PROJECT_NAME} PRIVATE
        otterbrix::collection
        otterbrix::disk
        otterbrix::dispatcher
        otterbrix::wal
        otterbrix::dispatcher
        otterbrix::disk
    )
endif()

target_include_directories(${PROJECT_NAME} PUBLIC
        ..
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
