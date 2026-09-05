project(test_table)

set(${PROJECT_NAME}_SOURCES
        test_column.cpp
        test_table.cpp
        test_block_manager.cpp
        test_block_reachability.cpp
        test_fault_injection.cpp
        test_shadow_header.cpp
        test_shadow_free_list.cpp
        test_big_strings.cpp
        test_storage_adapter_fetch.cpp
        test_nested_fetch_channel.cpp
        test_nested_error_channel.cpp
        test_fetch_visibility.cpp
        test_eviction_guard.cpp
        test_buffer_spill.cpp
        test_eviction_queue_leak_repro.cpp
        test_metadata.cpp
        test_checkpoint_load.cpp
        test_transaction_manager.cpp
        test_mvcc_operations.cpp
        test_statistics.cpp
        test_block_e_procarray.cpp
        test_disk_backed_scan.cpp
        test_root_reclaim.cpp
        test_nested_compact_reclaim.cpp
        test_failed_round_rollback.cpp
        test_checkpoint_crash_matrix.cpp
        test_alter_column_sharing.cpp
        test_alter_version_sharing.cpp
        test_collection_ownership.cpp
        test_wide_decimal_storage.cpp
        test_update_merge.cpp
        test_block_accounting_bounds.cpp
        test_wave_table.cpp
        test_segment_tree_reinitialize.cpp
        test_storage_update_rollback.cpp
        test_column_identity_restamp.cpp
        test_predicate_fetch_state.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    otterbrix::expressions
    otterbrix::logical_plan
    otterbrix::table
    otterbrix::file
    otterbrix::types
    otterbrix::vector
    otterbrix::session

    absl::crc32c
    Catch2::Catch2WithMain
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
