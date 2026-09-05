project(test_index_service)

add_definitions(-DDEV_MODE)

set(${PROJECT_NAME}_SOURCES
    test_index_disk.cpp
    test_bitcask_index_disk.cpp
    test_disk_hash_table.cpp
    test_index_agent_lifetime.cpp
    test_index_agent_dropped_writes.cpp
    test_index_agent_reaping.cpp
    test_index_agent_buffer.cpp
    test_index_agent_commit_retry.cpp
    test_index_registry.cpp
    test_index_delete_horizon.cpp
    test_index_catchup_delete_bucket.cpp
    test_index_manager_gates.cpp
    test_index_agent_rebuild_clear.cpp

    # stress tests with sanitizer are incredibly slow
    $<$<NOT:$<BOOL:${ENABLE_ASAN}>>:stress_test_index.cpp>
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    otterbrix::services
    otterbrix::index
    otterbrix::test_generaty

    absl::int128
    absl::crc32c
    actor-zeta::actor-zeta
    Boost::boost
    Catch2::Catch2WithMain
    actor-zeta::actor-zeta
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
