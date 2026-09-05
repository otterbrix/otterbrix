project(test_otterbrix)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES
        test_instances.cpp
        test_collection.cpp
        test_collection_logical_plan.cpp
        test_collection_sql.cpp
        test_explain.cpp
        test_index.cpp
        test_insert.cpp
        test_delete_column_pruning.cpp
        test_index_create_failure.cpp
        test_index_key_type_gate.cpp
        test_catalog_scan_cost.cpp
        test_log_laziness.cpp
        test_index_mirror_gate.cpp
        test_insert_type_promotion.cpp
        test_index_key_binding.cpp
        test_s3_commit_scaling.cpp
        test_s3_cleanup_scaling.cpp
        test_index_flush_scope.cpp
        test_ssb_load_scaling.cpp
        test_insert_scaling.cpp
        test_scan_pin_scope.cpp
        test_string_projection.cpp
        test_check_on_update.cpp
        test_index_bootstrap_failure.cpp
        test_index_type_persistence.cpp
        test_index_threshold_config.cpp
        test_index_txn_log_routing.cpp
        test_index_read_through_agent.cpp
        test_index_concurrent_merge.cpp
        test_index_fetch_visibility.cpp
        test_index_delete_horizon.cpp
        test_index_agent_owns_hash_storage.cpp
        test_index_ordered_read_through_agent.cpp
        test_index_rebuild_crash.cpp
        test_index_stale_after_compact.cpp
        test_index_rebuild_live_snapshot.cpp
        test_dml_dictionary_rowids.cpp
        test_persistence_gaps.cpp
        test_null_persistence.cpp
        test_struct_null_field_read.cpp
        test_pump_hops.cpp
        test_append_pin_lifetime.cpp
        test_gather_string_lifetime.cpp
        test_fk_check_on_update.cpp
        test_fk_cascade_row_identity.cpp
        test_fk_arity_mismatch.cpp
        test_fk_omitted_ref_columns.cpp
        test_fk_parent_column_drop.cpp
        test_inline_constraints.cpp
        test_text_column_storage.cpp
        test_update.cpp
        test_wal_pool.cpp
        test_persistence.cpp
        test_alter_drop_column_reclaim.cpp
        test_alter_rename_column.cpp
        test_wal_nested_payload_replay.cpp
        test_type_spec_write_gate.cpp
        test_alter_add_column_unmaterialized.cpp
        test_sql_features.cpp
        test_unique_constraint_operator.cpp
        test_create_index_backfill_batched.cpp
        test_correctness_bugs.cpp
        test_order_by_expressions.cpp
        test_order_by_nulls.cpp
        test_arithmetic_type_guards.cpp
        test_like_type_guards.cpp
        test_unary_plus.cpp
        test_multi_database_isolation.cpp
        test_list_tables.cpp
        test_index_null_correctness.cpp
        test_aggregate_filter.cpp
        test_dispatcher_watchdog_log.cpp
        test_null_three_valued_logic.cpp
        test_null_arithmetic.cpp
        test_null_aggregates.cpp
        test_null_deep_path.cpp
        test_null_array_element.cpp
        test_null_comparison.cpp
        test_null_predicate_3vl.cpp
        test_null_case_3vl.cpp
        test_null_semantics_matrix.cpp
        test_production_scenarios.cpp
        test_join.cpp
        test_extension_source.cpp
        test_host_tables.cpp
        test_join_raw.cpp
        test_name_resolution.cpp
        test_connections.cpp
        test_udfs.cpp
        test_register_cast.cpp
        test_batch_execution.cpp
        test_table_function.cpp
        test_lateral_subquery.cpp
        test_group_by_node_data.cpp
        test_group_cardinality.cpp
        test_group_operator_contracts.cpp
        test_arithmetic.cpp
        test_clean_break_startup.cpp
        test_column_projection.cpp
        # TODO: casts for dynamic schema
        # test_computed_schema.cpp
        test_pushdown_filter.cpp
        test_cte_pushdown.cpp
        test_subqueries.cpp
        test_hash_join.cpp
        test_star_join_e2e.cpp
        test_returning.cpp
        test_parser_extension.cpp
        test_engine_lifecycle.cpp
        test_parameterized_queries.cpp
        test_list_array.cpp
        test_lost_wakeup_streaming.cpp
        test_lost_wakeup_realpath.cpp
        test_bounded_execution.cpp
        test_streaming_dml.cpp
        test_streaming_match.cpp
        test_streaming_index_scan.cpp
        test_index_oid_selection.cpp
        test_streaming_join.cpp
        test_streaming_ddl_leaf.cpp
        test_streaming_recursive_cte.cpp
        test_large_aggregate_dml.cpp
        test_stacked_constraints.cpp
        test_unique_constraint_e2e.cpp
        test_bounded_dml_flush.cpp
        test_aggregate_pushdown_e2e.cpp
        test_pushdown_traffic.cpp
        test_pushed_reduce_scan_lateral.cpp
        # TODO: casts for dynamic schema
        # test_jsonb_support.cpp
        test_select_intermediate.cpp

        # TODO: casts for dynamic schema
        # stress tests with sanitizer are incredibly slow
        # $<$<NOT:$<BOOL:${ENABLE_ASAN}>>:stress_test_sql_features.cpp>
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE

        cpp_otterbrix
        non_thread_scheduler

        otterbrix::demo_extension
        otterbrix::test_generaty
        otterbrix::log
        otterbrix::cursor
        otterbrix::session
        otterbrix::services
        otterbrix::planner
        otterbrix::locks

        spdlog::spdlog
        #CONAN_PKG::date
        Catch2::Catch2WithMain
        absl::crc32c
        absl::int128
        benchmark::benchmark
        actor-zeta::actor-zeta
        Boost::boost
        fmt::fmt
        #CONAN_PKG::libcurl
        ZLIB::ZLIB
        BZip2::BZip2
        #libbacktrace::libbacktrace

)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME} PROPERTIES TIMEOUT 1800)

# Profiling binary for arithmetic
add_executable(profile_arithmetic profile_arithmetic.cpp)
target_link_libraries(profile_arithmetic PRIVATE
        cpp_otterbrix
        otterbrix::test_generaty
        otterbrix::log
        otterbrix::cursor
        otterbrix::session
        otterbrix::services
        otterbrix::locks
        spdlog::spdlog
        Catch2::Catch2
        absl::crc32c
        absl::int128
        benchmark::benchmark
        actor-zeta::actor-zeta
        Boost::boost
        fmt::fmt
        ZLIB::ZLIB
        BZip2::BZip2
)

# Storage-cost measurement harness (plan tasks T1 / B7). Deliberately NOT a
# Catch2 test and NOT registered with CTest: it prints numbers and asserts
# nothing, so a slow machine can never turn it red.
add_executable(measure_storage_costs measure_storage_costs.cpp)
target_link_libraries(measure_storage_costs PRIVATE
        cpp_otterbrix
        otterbrix::test_generaty
        otterbrix::log
        otterbrix::cursor
        otterbrix::session
        otterbrix::services
        otterbrix::planner
        otterbrix::locks
        spdlog::spdlog
        Catch2::Catch2
        absl::crc32c
        absl::int128
        benchmark::benchmark
        actor-zeta::actor-zeta
        Boost::boost
        fmt::fmt
        ZLIB::ZLIB
        BZip2::BZip2
)
