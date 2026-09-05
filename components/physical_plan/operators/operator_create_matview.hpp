#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/table/column_definition.hpp>
#include <components/vector/data_chunk.hpp>
#include <vector>

namespace components::operators {

    // Composite CREATE MATERIALIZED VIEW ... WITH NO DATA (relkind='m') operator: create storage, register
    // with the index manager, and write pg_class/pg_attribute/pg_rewrite/pg_depend rows, atomically in one
    // coroutine. Does NOT populate the matview — a nested scan from inside this operator's own await hits
    // an actor_zeta nested-await failure, and sequence_t(create, insert) doesn't work either since the
    // insert's column bindings are stamped before the planner mints the matview's oid — so the implicit
    // WITH DATA form is refused in the transformer (transform_matview.cpp).
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
