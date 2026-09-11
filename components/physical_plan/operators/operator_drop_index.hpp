#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <cstdint>
#include <string>
#include <vector>

namespace components::operators {

    // Scrubs pg_class/pg_index/pg_depend for the index oid, co-located with the engine-entry teardown so a partial
    // scrub surfaces as one error before the entry is removed, letting a retry finish the cleanup.
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
                              components::catalog::oid_t index_oid,
                              std::vector<catalog_delete_t> catalog_deletes);

        // Sourceless SINK leaf, driven bottom-up by needs_async_finalize; push()/finalize() inherit the no-op defaults.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        components::catalog::oid_t table_oid_;
        components::catalog::oid_t index_oid_;
        std::vector<catalog_delete_t> catalog_deletes_;
    };

} // namespace components::operators
