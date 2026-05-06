#include "operator_fk_check.hpp"

#include <components/context/context.hpp>
#include <components/types/logical_value.hpp>
#include <services/disk/manager_disk.hpp>

namespace components::operators {

    operator_fk_check_t::operator_fk_check_t(std::pmr::memory_resource* resource,
                                               log_t                      log,
                                               catalog::fk_info_t         fk)
        : read_write_operator_t(resource, log, operator_type::fk_check)
        , fk_(std::move(fk)) {}

    void operator_fk_check_t::on_execute_impl(pipeline::context_t* /*ctx*/) {
        if (left_ && left_->output()) {
            output_ = left_->output();
            if (output_ && output_->size() > 0) {
                async_wait();
            }
        }
    }

    actor_zeta::unique_future<void>
    operator_fk_check_t::await_async_and_resume(pipeline::context_t* ctx) {
        if (!output_ || output_->size() == 0) {
            mark_executed();
            co_return;
        }
        const auto& chunk = output_->data_chunk();
        execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        // Find column indices in the chunk for each FK child col.
        std::vector<uint64_t> child_col_indices;
        for (const auto& col_name : fk_.child_col_names) {
            auto idx = chunk.column_index(col_name);
            if (idx == static_cast<std::size_t>(-1)) {
                // Column not in chunk — skip FK check for this constraint.
                mark_executed();
                co_return;
            }
            child_col_indices.push_back(static_cast<uint64_t>(idx));
        }

        for (uint64_t row = 0; row < chunk.size(); ++row) {
            // SIMPLE match: skip row if any FK col is NULL.
            bool any_null = false;
            if (fk_.matchtype == 's') {
                for (auto cidx : child_col_indices) {
                    if (!chunk.data[cidx].validity().row_is_valid(row)) {
                        any_null = true;
                        break;
                    }
                }
            }
            if (any_null) continue;

            // Build key_values from the FK child columns.
            std::vector<types::logical_value_t> key_values;
            key_values.reserve(child_col_indices.size());
            for (auto cidx : child_col_indices) {
                key_values.push_back(chunk.value(cidx, row));
            }

            auto [_, fut] = actor_zeta::send(ctx->disk_address,
                                              &services::disk::manager_disk_t::scan_by_table_oid,
                                              exec_ctx,
                                              fk_.parent_table_oid,
                                              std::vector<std::string>(fk_.parent_col_names),
                                              std::move(key_values));
            auto parent_ids = co_await std::move(fut);
            if (parent_ids.empty()) {
                set_error("FK constraint violated: referenced row not found in parent table");
                co_return;
            }
        }
        mark_executed();
    }

} // namespace components::operators