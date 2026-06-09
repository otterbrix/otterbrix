project(physical_plan)


set(${PROJECT_NAME}_SOURCES
        operators/operator.cpp
        operators/operator_batch.cpp
        operators/operator_data.cpp
        operators/operator_write_data.cpp
        operators/operator_empty.cpp
        operators/operator_raw_data.cpp

        operators/aggregate/operator_aggregate.cpp
        operators/aggregate/operator_func.cpp
        operators/aggregate/grouped_aggregate.cpp

        operators/predicates/predicate.cpp
        operators/predicates/simple_predicate.cpp
        operators/predicates/function_predicate.cpp
        operators/predicates/utils.cpp

        operators/scan/full_scan.cpp
        operators/scan/index_scan.cpp
        operators/scan/primary_key_scan.cpp
        operators/scan/transfer_scan.cpp

        operators/sort/sort.cpp

        operators/operator_insert.cpp
        operators/operator_delete.cpp
        operators/operator_update.cpp
        operators/operator_match.cpp
        operators/operator_distinct.cpp
        operators/operator_group.cpp
        operators/operator_select.cpp
        operators/operator_sort.cpp
        operators/operator_join.cpp
        operators/operator_hash_join.cpp
        operators/operator_union.cpp
        operators/operator_cte_scan.cpp
        operators/operator_recursive_cte.cpp

        operators/operator_check_constraint.cpp
        operators/operator_fk_check.cpp
        operators/operator_fk_cascade.cpp
        operators/operator_sequence.cpp
        operators/operator_primitive_write.cpp
        operators/operator_primitive_delete.cpp
        operators/operator_create_collection.cpp
        operators/operator_create_matview.cpp
        operators/operator_create_index_metadata.cpp
        operators/operator_create_index_backfill.cpp
        operators/operator_drop_index.cpp
        operators/operator_dynamic_cascade_delete.cpp
        operators/alter_validators.cpp
        operators/operator_alter_column_add.cpp
        operators/operator_alter_column_drop.cpp
        operators/operator_alter_column_rename.cpp
        operators/operator_checkpoint.cpp
        operators/operator_set_timezone.cpp
        operators/operator_vacuum.cpp
        operators/operator_register_udf.cpp
        operators/operator_unregister_udf.cpp
        operators/operator_commit_transaction.cpp
        operators/operator_abort_transaction.cpp
        operators/operator_begin_transaction.cpp
        operators/operator_computed_field_register.cpp
        operators/operator_computed_field_unregister.cpp
        operators/operator_resolve_namespace.cpp
        operators/operator_resolve_database.cpp
        operators/operator_resolve_table.cpp
        operators/operator_resolve_type.cpp
        operators/operator_resolve_function.cpp
        operators/operator_resolve_constraint.cpp
        operators/operator_allocate_oids.cpp
        operators/resolved_table_metadata.cpp

        operators/arithmetic_eval.cpp
)

add_library(otterbrix_${PROJECT_NAME} OBJECT
        ${${PROJECT_NAME}_HEADERS}
        ${${PROJECT_NAME}_SOURCES}
)


add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
        otterbrix::date
        otterbrix::types
        otterbrix::vector
        otterbrix::cursor
        otterbrix::session
        otterbrix::context
        otterbrix::index
        otterbrix::logical_plan
        otterbrix::catalog
        spdlog::spdlog
        absl::flat_hash_map
        absl::node_hash_map
        absl::int128
        Boost::boost
)

target_link_libraries(otterbrix_${PROJECT_NAME} PRIVATE FastFloat::fast_float)

target_include_directories(otterbrix_${PROJECT_NAME} PUBLIC
)

