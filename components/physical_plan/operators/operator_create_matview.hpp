#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/table/column_definition.hpp>
#include <components/vector/data_chunk.hpp>
#include <vector>

namespace components::operators {

    // Composite physical operator for CREATE MATERIALIZED VIEW ... WITH NO DATA
    // (relkind='m').
    //
    // Performs all matview creation steps atomically in a single async coroutine:
    //   1. Create physical heap storage for the matview.
    //   2. Register the matview with the index manager.
    //   3. Write pg_class + pg_attribute + pg_rewrite + pg_depend rows.
    //
    // It does NOT populate the matview, and it no longer pretends it might. It used
    // to take the compiled body plan as `body_op` and then never drive it, silenced
    // with `(void) body_op_;` — so the steps 4 and 5 this comment described (drive
    // the body, append its rows) never existed. Driving a scan from inside this
    // operator's own await hits an actor_zeta nested-await failure, and the
    // alternative — lowering CREATE to sequence_t(create, insert) — needs an insert
    // whose column bindings are stamped by validate/enrich, both of which run BEFORE
    // the planner mints the matview's oid and against a relation that does not exist
    // yet. Rather than keep a dead parameter that reads like a feature, the form
    // that needs population (the implicit WITH DATA) is refused in the transformer
    // (components/sql/transformer/impl/transform_matview.cpp).
    //
    // Pipeline-canonical: dispatched as a single logical_plan node
    // (create_matview_t) → planner stamps catalog_writes + mv_oid →
    // physical_plan_generator builds this operator. No re-parsing in dispatcher, no
    // follow-up plan dispatches.
    class operator_create_matview_t final : public read_write_operator_t {
    public:
        using catalog_write_t = std::pair<components::catalog::oid_t, vector::data_chunk_t>;

        operator_create_matview_t(std::pmr::memory_resource* resource,
                                  log_t log,
                                  components::catalog::oid_t mv_oid,
                                  components::catalog::oid_t namespace_oid,
                                  std::vector<table::column_definition_t> columns,
                                  std::vector<catalog_write_t> catalog_writes);

        // Sourceless SINK leaf (no left-chain data source): the executor admits it
        // as a streaming sink-root and drives await_async_and_resume via the
        // bottom-up needs_async_finalize pass. push()/finalize() inherit no-op
        // defaults.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        components::catalog::oid_t mv_oid_;
        components::catalog::oid_t namespace_oid_;
        std::vector<table::column_definition_t> columns_;
        std::vector<catalog_write_t> catalog_writes_;
    };

} // namespace components::operators
