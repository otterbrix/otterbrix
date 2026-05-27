#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/table/column_definition.hpp>
#include <components/vector/data_chunk.hpp>
#include <vector>

namespace components::operators {

    // Composite physical operator for CREATE MATERIALIZED VIEW (relkind='m').
    //
    // Performs all matview creation steps atomically in a single async coroutine:
    //   1. Create physical heap storage for the matview.
    //   2. Register the matview with the index manager.
    //   3. Write pg_class + pg_attribute + pg_rewrite + pg_depend rows.
    //   4. Drive body_op_ (compiled body SELECT plan) to completion, gathering
    //      its output chunk.
    //   5. Append the body's rows into the matview's heap (mirror of
    //      operator_insert: storage_append + WAL + index forwarding).
    //
    // Pipeline-canonical: dispatched as a single logical_plan node
    // (create_matview_t) → planner stamps catalog_writes + mv_oid →
    // physical_plan_generator builds this operator with body_op_ compiled via
    // the standard create_plan recursion. No re-parsing in dispatcher, no
    // follow-up plan dispatches.
    class operator_create_matview_t final : public read_write_operator_t {
    public:
        using catalog_write_t = std::pair<components::catalog::oid_t, vector::data_chunk_t>;

        operator_create_matview_t(std::pmr::memory_resource* resource,
                                  log_t log,
                                  components::catalog::oid_t mv_oid,
                                  components::catalog::oid_t namespace_oid,
                                  std::vector<table::column_definition_t> columns,
                                  bool is_disk_storage,
                                  std::vector<catalog_write_t> catalog_writes,
                                  operator_ptr body_op);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        components::catalog::oid_t mv_oid_;
        components::catalog::oid_t namespace_oid_;
        std::vector<table::column_definition_t> columns_;
        bool is_disk_storage_;
        std::vector<catalog_write_t> catalog_writes_;
        operator_ptr body_op_;
    };

} // namespace components::operators
