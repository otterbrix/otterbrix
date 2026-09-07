#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/key.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <cstdint>
#include <string>
#include <vector>

namespace components::operators {

    // Counts non-empty storage_fetch_next_batch replies the backfill consumes; a table larger
    // than one batch must bump this past 1 (proving it streams, not materializes).
#ifdef DEV_MODE
    uint64_t create_index_backfill_batches() noexcept;
#endif

    // pg_class/pg_index(indisvalid=false)/pg_depend rows are already written by
    // operator_create_index_metadata_t, so recovery sees a half-built index as invalid until this commits.
    class operator_create_index_backfill_t final : public read_write_operator_t {
    public:
        operator_create_index_backfill_t(std::pmr::memory_resource* resource,
                                         log_t log,
                                         components::logical_plan::index_type index_type,
                                         std::pmr::vector<components::expressions::key_t> keys,
                                         components::catalog::oid_t table_oid,
                                         components::catalog::oid_t index_oid,
                                         std::string indkey);

        // CREATE INDEX lowers to a 2-node all-sink chain [backfill(root) -> metadata(left leaf)]
        // (create_plan_sequence); bottom-up needs_async_finalize drives metadata first, so
        // pg_catalog rows are durable before this step scans and flips indisvalid=true.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

    private:
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        components::logical_plan::index_type index_type_;
        std::pmr::vector<components::expressions::key_t> keys_;
        components::catalog::oid_t table_oid_;
        components::catalog::oid_t index_oid_;
        std::string indkey_;
    };

} // namespace components::operators
