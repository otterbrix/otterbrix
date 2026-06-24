#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <cstdint>
#include <string>
#include <vector>

namespace components::operators {

    // DROP INDEX runtime: removes the in-memory index engine entry and scrubs
    // the pg_class/pg_index/pg_depend rows for the index oid.
    //
    // The catalog-row deletes are produced upstream by rewrite_drop_index from
    // catalog-delete node_delete_t leaves. Co-locating them with the index-actor teardown
    // keeps the failure boundary simple — a partial scrub surfaces as a single
    // operator error before the engine entry is removed (so a retry can find
    // and finish the cleanup).
    class operator_drop_index_t final : public read_write_operator_t {
    public:
        struct catalog_delete_t {
            components::catalog::oid_t catalog_table_oid;
            std::int64_t oid_col_idx;
            components::catalog::oid_t target_oid;
        };

        operator_drop_index_t(std::pmr::memory_resource* resource,
                              log_t log,
                              components::catalog::oid_t table_oid,
                              std::string index_name,
                              std::vector<catalog_delete_t> catalog_deletes);

        // Sourceless SINK leaf (no data pipeline, no children): the executor
        // admits it as a streaming sink-root and drives await_async_and_resume via
        // the bottom-up needs_async_finalize pass. push()/finalize() inherit the
        // no-op defaults.
        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::sink; }
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        components::catalog::oid_t table_oid_;
        std::string index_name_;
        std::vector<catalog_delete_t> catalog_deletes_;
    };

} // namespace components::operators
