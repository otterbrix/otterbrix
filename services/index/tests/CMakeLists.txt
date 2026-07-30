project(test_index_service)

add_definitions(-DDEV_MODE)

set(${PROJECT_NAME}_SOURCES
    test_index_disk.cpp
    test_bitcask_index_disk.cpp
    test_disk_hash_table.cpp
    test_manager_index_enqueue.cpp

    # stress tests with sanitizer are incredibly slow
    $<$<NOT:$<BOOL:${ENABLE_ASAN}>>:stress_test_index.cpp>
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    non_thread_scheduler
    otterbrix::services
    otterbrix::index
    otterbrix::test_generaty

    absl::int128
    actor-zeta::actor-zeta
    Boost::boost
    Catch2::Catch2WithMain
    actor-zeta::actor-zeta
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
