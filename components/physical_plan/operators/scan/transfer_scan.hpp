#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <vector>

namespace components::operators {

    class transfer_scan final : public read_only_operator_t {
    public:
        // Unified ctor: OID-routed (our PR direction) + index-based projection
        // (main's column_pruning output). Empty projected_cols means
        // "pass-through, read all columns".
        //
        // Column indices reference the canonical table schema. This is stable
        // within a single query for both relkind='r' (regular) and relkind='g'
        // (computed/dynamic-schema) tables — chunks are materialized from the
        // table's shared `types_`, and tombstones are filtered at resolve time.
        transfer_scan(std::pmr::memory_resource* resource,
                      components::catalog::oid_t table_oid,
                      logical_plan::limit_t limit,
                      std::vector<size_t> projected_cols = {});

        components::catalog::oid_t table_oid() const noexcept { return table_oid_; }
        const logical_plan::limit_t& limit() const { return limit_; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        components::catalog::oid_t table_oid_;
        const logical_plan::limit_t limit_;
        std::vector<size_t> projected_cols_;
    };

} // namespace components::operators