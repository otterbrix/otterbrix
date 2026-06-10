#pragma once

#include <cstdint>
#include <string>

namespace components::logical_plan {
    enum class node_type : uint8_t
    {
        aggregate_t,
        alias_t,
        create_collection_t,
        create_database_t,
        create_index_t,
        create_type_t,
        data_t,
        delete_t,
        drop_collection_t,
        drop_database_t,
        drop_index_t,
        drop_type_t,
        function_t,
        insert_t,
        join_t,
        // Equi-join variant produced by the optimizer (rewrite_hash_joins) from a
        // join_t whose ON condition is a single eq(left.key, right.key). Carries the
        // matched column indices so the planner lowers it straight to
        // operator_hash_join_t — the equi-detection no longer lives in the planner.
        hash_join_t,
        intersect_t,
        limit_t,
        match_t,
        group_t,
        select_t,
        sort_t,
        update_t,
        union_t,
        recursive_cte_t,
        cte_scan_t,
        create_sequence_t,
        drop_sequence_t,
        create_view_t,
        drop_view_t,
        create_macro_t,
        drop_macro_t,
        // CREATE MATERIALIZED VIEW (relkind='m'). Carries body_sql + body_plan as
        // child[0]. Planner derives output schema from body_plan + stamped
        // source metadata, then lowers to sequence_t(create_collection +
        // pg_class/pg_attribute/pg_rewrite/pg_depend writes + insert_t).
        create_matview_t,
        refresh_matview_t,
        checkpoint_t,
        vacuum_t,
        having_t,
        alter_table_t,
        create_constraint_t,
        // Planner-emitted rewrite nodes
        check_constraint_t,
        fk_check_t,
        fk_cascade_t,
        // DDL sequencing node
        sequence_t,
        // DDL primitive write/delete (planner-built pg_catalog rows)
        primitive_write_t,
        primitive_delete_t,
        // ALTER TABLE per-clause primitives (planner-rewritten from alter_table_t)
        alter_column_add_t,
        alter_column_rename_t,
        alter_column_drop_t,
        // Universal cascade-delete driver (planner-emitted): walks pg_depend
        // at runtime starting from a (classid, oid) seed and deletes the
        // transitive closure. Subsumes the dispatcher BFS in execute_ddl.
        dynamic_cascade_delete_t,
        // REGISTER_UDF / UNREGISTER_UDF: operator-pipeline
        // replacement for inline manager_dispatcher_t::{register,unregister}_udf.
        // Carries the UDF function payload (or name + arg-type signature for
        // unregister); the operator fans out to per-executor registries, the
        // global default function_registry_t, and pg_proc.
        register_udf_t,
        unregister_udf_t,
        // COMMIT / ROLLBACK: operator-pipeline replacement for inline
        // manager_dispatcher_t::{commit,abort}_transaction. The leaf
        // node carries no fields (session is on pipeline::context_t); the
        // operator drives txn_manager.commit/abort + pg_catalog MVCC swap on
        // disk via storage_publish_commits / storage_revert_appends.
        commit_transaction_t,
        abort_transaction_t,
        // BEGIN / START TRANSACTION: leaf lowered to operator_begin_transaction_t,
        // which marks the session's transaction_t explicit (see node_begin_transaction.hpp).
        begin_transaction_t,
        // COMPUTED_FIELD_REGISTER / COMPUTED_FIELD_UNREGISTER: pipeline
        // operators that maintain pg_computed_column rows for relkind='g'
        // (Mongo-style dynamic-schema) tables. The register variant runs
        // after an INSERT and adds rows for any newly-seen columns (or bumps
        // attversion on type evolution); the unregister variant appends a
        // refcount=0 tombstone for one column.
        computed_field_register_t,
        computed_field_unregister_t,
        // Catalog-resolve leaf nodes. Each carries a name reference and is
        // replaced by the corresponding operator_resolve_*_t during physical
        // plan generation. Resolves through standard pipeline (logical_plan →
        // planner → optimizer → physical_plan_generator → executor → disk).
        catalog_resolve_table_t,
        catalog_resolve_namespace_t,
        // Resolves a database name to its pg_database OID (distinct from
        // pg_namespace); populates execution_context_t.database_oid for WAL routing.
        catalog_resolve_database_t,
        catalog_resolve_type_t,
        catalog_resolve_function_t,
        catalog_resolve_constraint_t,
        // Leaf that allocates a batch of OIDs from the disk-side oid_generator;
        // the DDL planner reads the batch via node_allocate_oids_t::oids().
        allocate_oids_t,
        set_timezone_t,
        unused
    };

    std::string to_string(node_type type);

#define node_type_from_string(STR)                                                                                     \
    do {                                                                                                               \
        return node_type::STR;                                                                                         \
    } while (false);

    enum class visitation : uint8_t
    {
        visit_inputs,
        do_not_visit_inputs
    };

    enum class input_side : uint8_t
    {
        left,
        right
    };

    enum class expression_iteration : uint8_t
    {
        continue_t,
        break_t
    };

    namespace aggregate {
        enum class operator_type : int16_t
        {
            invalid = 1,
            count, ///group + project
            group,
            limit,
            match,
            merge,
            out,
            project,
            skip,
            sort,
            unset,
            unwind,
            finish
        };

        operator_type get_aggregate_type(const std::string& key);

    } // namespace aggregate

} // namespace components::logical_plan
