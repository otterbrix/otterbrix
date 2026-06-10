#include "operator_fk_check.hpp"

#include <components/context/context.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector_operations.hpp>
#include <services/disk/manager_disk.hpp>

#include <limits>

namespace components::operators {

    operator_fk_check_t::operator_fk_check_t(std::pmr::memory_resource* resource, log_t log, catalog::fk_info_t fk)
        : read_write_operator_t(resource, log, operator_type::fk_check)
        , fk_(std::move(fk)) {}

    void operator_fk_check_t::on_execute_impl(pipeline::context_t* /*ctx*/) {
        if (!left_)
            return;
        // After intercept_dml_io_, the DML operator's output is replaced with a
        // zero-column result chunk. Fall back to the DML op's data source.
        output_ = left_->output();
        if (!output_ || output_->data_chunk().column_count() == 0) {
            if (left_->left() && left_->left()->output()) {
                output_ = left_->left()->output();
            }
        }
        if (output_ && output_->size() > 0) {
            async_wait();
        }
    }

    actor_zeta::unique_future<void> operator_fk_check_t::await_async_and_resume(pipeline::context_t* ctx) {
        if (!output_ || output_->size() == 0) {
            mark_executed();
            co_return;
        }
        const auto& chunk = output_->data_chunk();
        execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        const auto& indices = fk_.child_col_indices;
        const std::size_t absent = std::numeric_limits<std::size_t>::max();

        // Parent key column names are the same for every row; hoist them once.
        std::pmr::vector<std::string> parent_col_names(resource_);
        parent_col_names.reserve(fk_.parent_col_names.size());
        for (const auto& n : fk_.parent_col_names) {
            parent_col_names.emplace_back(n);
        }

        // Collect the qualifying child rows as a SELECTION into the input chunk,
        // preserving the MATCH null policy + error path.
        // selection[k] = source row of the k-th qualifying key; matches[k] empty -> violation.
        components::vector::indexing_vector_t selection(resource_, chunk.size() == 0 ? 1 : chunk.size());
        uint64_t qcount = 0;

        for (uint64_t row = 0; row < chunk.size(); ++row) {
            bool any_null = false;
            bool all_null = true;
            for (std::size_t i = 0; i < indices.size(); ++i) {
                const auto idx = indices[i];
                const bool is_null = (idx == absent || !chunk.data[idx].validity().row_is_valid(row));
                if (is_null)
                    any_null = true;
                else
                    all_null = false;
            }

            if (fk_.matchtype == 'f') {
                // MATCH FULL: all-NULL → skip; partial-NULL → error; no-NULL → check.
                if (all_null)
                    continue;
                if (any_null) {
                    set_error(core::error_t{
                        core::error_code_t::other_error,
                        std::pmr::string{"FK MATCH FULL: partial null in foreign key columns", resource_}});
                    co_return;
                }
            } else {
                // MATCH SIMPLE (default): any-NULL → skip.
                if (any_null)
                    continue;
            }

            // Any unresolved key column position voids this row's check.
            bool has_absent = false;
            for (auto idx : indices) {
                if (idx == absent) {
                    has_absent = true;
                    break;
                }
            }
            if (has_absent || indices.empty())
                continue;

            selection.set_index(qcount, row);
            ++qcount;
        }

        if (qcount == 0) {
            mark_executed();
            co_return;
        }

        // Build the keys-chunk: column j == child key column indices[j], rows == the
        // qcount qualifying source rows selected above. Must be an OWNED copy (not a
        // reference into the input): it crosses the mailbox and actors must not share
        // buffers (actor isolation).
        std::pmr::vector<types::complex_logical_type> key_types(resource_);
        key_types.reserve(indices.size());
        for (auto idx : indices) {
            key_types.push_back(chunk.data[idx].type());
        }
        components::vector::data_chunk_t keys(resource_, key_types, qcount);
        for (std::size_t j = 0; j < indices.size(); ++j) {
            components::vector::vector_ops::copy(chunk.data[indices[j]], keys.data[j], selection, qcount, 0, 0);
        }
        keys.set_cardinality(qcount);

        // One batched scan verifies every qualifying key against the parent table.
        auto [_, fut] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::scan_by_keys,
                                         exec_ctx,
                                         fk_.parent_table_oid,
                                         std::move(parent_col_names),
                                         std::move(keys));
        auto matches = co_await std::move(fut);

        // Any missing parent (empty match list) is a violation.
        for (std::size_t i = 0; i < matches.size(); ++i) {
            if (matches[i].empty()) {
                set_error(core::error_t{
                    core::error_code_t::other_error,
                    std::pmr::string{"FK constraint violated: referenced row not found in parent table", resource_}});
                co_return;
            }
        }
        mark_executed();
    }

} // namespace components::operators
