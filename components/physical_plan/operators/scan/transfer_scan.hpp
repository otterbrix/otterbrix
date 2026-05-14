#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <string>
#include <unordered_set>
#include <vector>

namespace components::operators {

    class transfer_scan final : public read_only_operator_t {
    public:
        // Phase 13 M7: replaces the side-channel that inlined pg_class +
        // pg_computed_column scans. The plan generator passes the live-columns
        // projection mask (alias names) computed from the resolve_table node's
        // resolved_metadata. Empty mask == no projection (pass through).
        transfer_scan(std::pmr::memory_resource* resource,
                      components::catalog::oid_t table_oid,
                      logical_plan::limit_t limit,
                      std::vector<std::string> live_column_aliases = {});

        components::catalog::oid_t table_oid() const noexcept { return table_oid_; }
        const logical_plan::limit_t& limit() const { return limit_; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        components::catalog::oid_t table_oid_;
        const logical_plan::limit_t limit_;
        std::unordered_set<std::string> live_column_aliases_;
        bool has_projection_{false};
    };

} // namespace components::operators
