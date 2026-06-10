project(logical_plan CXX)

set(SOURCE_${PROJECT_NAME}
        forward.cpp
        execution_plan.cpp
        node.cpp
        node_aggregate.cpp
        node_alter_column_add.cpp
        node_alter_column_drop.cpp
        node_alter_column_rename.cpp
        node_alter_table.cpp
        node_check_constraint.cpp
        node_fk_check.cpp
        node_fk_cascade.cpp
        node_sequence.cpp
        node_primitive_write.cpp
        node_primitive_delete.cpp
        node_dynamic_cascade_delete.cpp
        node_register_udf.cpp
        node_unregister_udf.cpp
        node_commit_transaction.cpp
        node_abort_transaction.cpp
        node_begin_transaction.cpp
        node_computed_field_register.cpp
        node_computed_field_unregister.cpp
        node_catalog_resolve_table.cpp
        node_catalog_resolve_namespace.cpp
        node_catalog_resolve_database.cpp
        node_catalog_resolve_type.cpp
        node_catalog_resolve_function.cpp
        node_catalog_resolve_constraint.cpp
        node_allocate_oids.cpp
        node_checkpoint.cpp
        node_create_collection.cpp
        node_create_constraint.cpp
        node_create_database.cpp
        node_create_index.cpp
        node_create_macro.cpp
        node_create_matview.cpp
        node_create_sequence.cpp
        node_create_type.cpp
        node_create_view.cpp
        node_refresh_matview.cpp
        node_data.cpp
        node_delete.cpp
        node_drop_collection.cpp
        node_drop_database.cpp
        node_drop_index.cpp
        node_drop_macro.cpp
        node_drop_sequence.cpp
        node_drop_type.cpp
        node_drop_view.cpp
        node_function.cpp
        node_group.cpp
        node_having.cpp
        node_insert.cpp
        node_join.cpp
        node_limit.cpp
        node_match.cpp
        node_select.cpp
        node_sort.cpp
        node_union.cpp
        node_recursive_cte.cpp
        node_cte_scan.cpp
        node_update.cpp
        node_set_timezone.cpp
        node_vacuum.cpp
        param_storage.cpp
)

add_library(otterbrix_${PROJECT_NAME}
        ${SOURCE_${PROJECT_NAME}}
)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
        otterbrix::types
        otterbrix::expressions
        otterbrix::table
        otterbrix::catalog

        absl::int128
        Boost::boost
        msgpackc-cxx
)

target_include_directories(
        otterbrix_${PROJECT_NAME}
        PUBLIC
)
