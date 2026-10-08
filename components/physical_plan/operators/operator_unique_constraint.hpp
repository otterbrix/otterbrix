#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/types/logical_value.hpp>

#include <memory_resource>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

namespace components::operators {

#ifdef DEV_MODE
    // Counts scan_by_keys sends from the existing-row duplicate check — each is a full table
    // scan, so it must stay unchanged when no statement can change a unique key.
    uint64_t unique_constraint_scan_sends() noexcept;
#endif

    struct conflict_holder_t {
        int64_t row_id;
        bool written_by_statement; // a row the same statement wrote earlier, not one it found
    };

    class operator_unique_constraint_t final : public read_write_operator_t {
    public:
        operator_unique_constraint_t(std::pmr::memory_resource* resource,
                                     log_t log,
                                     catalog::oid_t table_oid,
                                     std::vector<std::vector<std::string>> unique_groups,
                                     std::vector<std::vector<std::string>> conflict_groups = {});

        actor_zeta::unique_future<core::error_t> check_rows(pipeline::context_t* ctx, const chunks_vector_t& in_chunks);

        const operator_data_ptr& conflict_rows() const noexcept { return conflict_rows_; }
        const std::pmr::vector<conflict_holder_t>& conflict_holders() const noexcept { return conflict_holders_; }

        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        // No-ops (no streaming input reaches them); kept explicit to skip the base "not a pipeline operator" error.
        [[nodiscard]] core::error_t push(pipeline::context_t*, vector::data_chunk_t&&, chunks_vector_t&) override {
            return core::error_t::no_error();
        }
        [[nodiscard]] core::error_t finalize(pipeline::context_t*, chunks_vector_t&) override {
            return core::error_t::no_error();
        }

    private:
        using row_flags_t = std::pmr::vector<std::pmr::vector<bool>>;
        using row_ids_t = std::pmr::vector<std::pmr::vector<int64_t>>;

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        actor_zeta::unique_future<core::error_t> find_held_keys_(pipeline::context_t* ctx,
                                                                 const std::vector<std::string>& group,
                                                                 std::pmr::vector<vector::data_chunk_t>& key_chunks,
                                                                 const std::pmr::unordered_set<int64_t>& written,
                                                                 const row_flags_t& skip,
                                                                 bool first_only,
                                                                 row_flags_t* held,
                                                                 row_ids_t* holders);

        catalog::oid_t table_oid_;
        std::vector<std::vector<std::string>> unique_groups_;
        std::vector<std::vector<std::string>> conflict_groups_;
        operator_data_ptr conflict_rows_{nullptr};
        std::pmr::vector<conflict_holder_t> conflict_holders_{resource_};
    };

} // namespace components::operators
