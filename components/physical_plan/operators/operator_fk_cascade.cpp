#include "operator_fk_cascade.hpp"

#include <components/base/collection_full_name.hpp>
#include <components/context/context.hpp>
#include <components/types/logical_value.hpp>
#include <services/disk/manager_disk.hpp>

namespace components::operators {

    operator_fk_cascade_t::operator_fk_cascade_t(std::pmr::memory_resource* resource,
                                                   log_t                      log,
                                                   catalog::fk_info_t         fk)
        : read_write_operator_t(resource, log, operator_type::fk_cascade)
        , fk_(std::move(fk)) {}

    void operator_fk_cascade_t::on_execute_impl(pipeline::context_t* /*ctx*/) {
        if (left_ && left_->output()) {
            output_ = left_->output();
            async_wait();
        }
    }

    actor_zeta::unique_future<void>
    operator_fk_cascade_t::await_async_and_resume(pipeline::context_t* ctx) {
        if (!output_ || output_->size() == 0) {
            mark_executed();
            co_return;
        }
        const auto& chunk = output_->data_chunk();
        execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        // Find parent col indices in the deleted chunk (parent_col_names are referenced cols).
        std::vector<uint64_t> parent_col_indices;
        for (const auto& col_name : fk_.parent_col_names) {
            auto idx = chunk.column_index(col_name);
            if (idx == static_cast<std::size_t>(-1)) {
                mark_executed();
                co_return;
            }
            parent_col_indices.push_back(static_cast<uint64_t>(idx));
        }

        for (uint64_t row = 0; row < chunk.size(); ++row) {
            std::vector<types::logical_value_t> key_values;
            key_values.reserve(parent_col_indices.size());
            for (auto pidx : parent_col_indices) {
                key_values.push_back(chunk.value(pidx, row));
            }

            auto [_, fut] = actor_zeta::send(ctx->disk_address,
                                              &disk::manager_disk_t::scan_by_table_oid,
                                              exec_ctx,
                                              fk_.child_table_oid,
                                              std::vector<std::string>(fk_.child_col_names),
                                              std::move(key_values));
            auto child_ids = co_await std::move(fut);
            if (child_ids.empty()) continue;

            switch (fk_.del_action) {
            case 'a': // NO ACTION
            case 'r': // RESTRICT
                set_error("FK constraint violated: child rows reference deleted parent row");
                co_return;

            case 'c': { // CASCADE — delete child rows via storage_delete_rows
                const components::base::collection_full_name_t child_coll{
                    fk_.child_database, fk_.child_schema, fk_.child_collection_name};
                execution_context_t del_ctx{ctx->session, ctx->txn, child_coll};

                components::vector::vector_t row_ids_vec(resource_,
                                                         types::logical_type::BIGINT,
                                                         child_ids.size());
                for (std::size_t i = 0; i < child_ids.size(); ++i) {
                    row_ids_vec.data<int64_t>()[i] = child_ids[i];
                }
                auto [_d, dfut] = actor_zeta::send(ctx->disk_address,
                                                    &disk::manager_disk_t::storage_delete_rows,
                                                    del_ctx,
                                                    std::move(row_ids_vec),
                                                    static_cast<uint64_t>(child_ids.size()));
                co_await std::move(dfut);
                break;
            }
            // 'n' SET NULL, 'd' SET DEFAULT — complex; silently pass for now.
            default:
                break;
            }
        }
        mark_executed();
    }

} // namespace components::operators