project(test_wal)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES
        test_wal.cpp
        test_wal_binary.cpp
        test_wal_page.cpp
        test_wal_worker.cpp
        test_wal_manager.cpp
        test_wal_torn_write.cpp
        test_wal_write_refusal.cpp
        test_wal_crc_reissue.cpp
        test_wal_load_hole.cpp
        test_wal_chain_refusal.cpp
        test_wal_retention_guard.cpp
        test_wal_truncate_header_race.cpp
        test_wal_replay_classification.cpp
        test_wal_txn_id_reuse.cpp
        test_wal_record_residency.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        non_thread_scheduler
        otterbrix::log
        otterbrix::logical_plan
        otterbrix::session
        otterbrix::services
        otterbrix::test_generaty

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
