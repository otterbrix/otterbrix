project(physical_plan_generator)

set(${PROJECT_NAME}_SOURCES
        create_plan.cpp
        impl/create_plan_aggregate.cpp
        impl/create_plan_alter_column_add.cpp
        impl/create_plan_alter_column_drop.cpp
        impl/create_plan_alter_column_rename.cpp
        impl/create_plan_data.cpp
        impl/create_plan_delete.cpp
        impl/create_plan_group.cpp
        impl/create_plan_insert.cpp
        impl/create_plan_match.cpp
        impl/create_plan_select.cpp
        impl/create_plan_sort.cpp
        impl/create_plan_update.cpp
        impl/create_plan_join.cpp
        impl/create_plan_hash_join.cpp
        impl/create_plan_union.cpp
        impl/create_plan_cte_scan.cpp
        impl/create_plan_recursive_cte.cpp
        impl/create_plan_check_constraint.cpp
        impl/create_plan_fk_check.cpp
        impl/create_plan_fk_cascade.cpp
        impl/create_plan_sequence.cpp
        impl/create_plan_primitive_write.cpp
        impl/create_plan_primitive_delete.cpp
        impl/create_plan_dynamic_cascade_delete.cpp
        impl/create_plan_checkpoint.cpp
        impl/create_plan_set_timezone.cpp
        impl/create_plan_vacuum.cpp
        impl/create_plan_create_matview.cpp
        impl/create_plan_register_udf.cpp
        impl/create_plan_unregister_udf.cpp
        impl/create_plan_commit_transaction.cpp
        impl/create_plan_abort_transaction.cpp
        impl/create_plan_begin_transaction.cpp
        impl/create_plan_computed_field_register.cpp
        impl/create_plan_computed_field_unregister.cpp
        impl/create_plan_resolve_namespace.cpp
        impl/create_plan_resolve_database.cpp
        impl/create_plan_resolve_function.cpp
        impl/create_plan_resolve_table.cpp
        impl/create_plan_resolve_type.cpp
        impl/create_plan_resolve_constraint.cpp
        impl/create_plan_allocate_oids.cpp
)

add_library(otterbrix_${PROJECT_NAME} OBJECT
        ${${PROJECT_NAME}_HEADERS}
        ${${PROJECT_NAME}_SOURCES}
)


add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
        otterbrix::compute
        otterbrix::cursor
        otterbrix::session
        otterbrix::index
        otterbrix::logical_plan
        otterbrix::planner
        otterbrix::physical_plan
        spdlog::spdlog
        absl::int128
        absl::flat_hash_map
        absl::node_hash_map
        Boost::boost
        actor-zeta::actor-zeta
)

target_include_directories(otterbrix_${PROJECT_NAME} PUBLIC
)

