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
        drop_t,
        function_t,
        insert_t,
        join_t,
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
        create_view_t,
        create_macro_t,
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
        // ALTER TABLE per-clause primitive (planner-rewritten from alter_table_t).
        alter_column_t,
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
        // BEGIN / COMMIT / ROLLBACK transaction-control leaf, selected by op().
        // The operators drive txn_manager.commit/abort + pg_catalog MVCC swap on
        // disk via storage_publish_commits / storage_revert_appends. See
        // node_transaction.hpp.
        transaction_t,
        // Catalog-resolve leaf carrying a resolve_kind (table/namespace_/database/
        // type/constraint); each kind is lowered to the corresponding
        // operator_resolve_*_t at plan-gen (create_plan switches on kind()).
        // See node_catalog_resolve.hpp.
        catalog_resolve_t,
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
