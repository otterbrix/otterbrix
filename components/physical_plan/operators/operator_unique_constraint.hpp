#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/types/logical_value.hpp>

#include <string>
#include <utility>
#include <vector>

namespace components::operators {

#ifdef DEV_MODE
    // Counts scan_by_keys sends from the existing-row duplicate check — each is a full table
    // scan, so it must stay unchanged when no statement can change a unique key.
    uint64_t unique_constraint_scan_sends() noexcept;
#endif

    // Duplicate detection (per constraint group) runs after the child DML commits: within-batch
    // collisions via a typed hash + verify, existing-row collisions via scan_by_keys — a key
    // whose scan returns more than the just-written row collides with a pre-existing one.
    // A key with any NULL column is skipped, since SQL UNIQUE treats NULLs as distinct.
    class operator_unique_constraint_t final : public read_write_operator_t {
    public:
        operator_unique_constraint_t(std::pmr::memory_resource* resource,
                                     log_t log,
                                     catalog::oid_t table_oid,
                                     std::vector<std::vector<std::string>> unique_groups);

        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        // No-ops (no streaming input reaches them); kept explicit to skip the base "not a pipeline operator" error.
        [[nodiscard]] core::error_t push(pipeline::context_t*, vector::data_chunk_t&&, chunks_vector_t&) override {
            return core::error_t::no_error();
        }
        [[nodiscard]] core::error_t finalize(pipeline::context_t*, chunks_vector_t&) override {
            return core::error_t::no_error();
        }

    private:
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        catalog::oid_t table_oid_;
        std::vector<std::vector<std::string>> unique_groups_;
    };

} // namespace components::operators
