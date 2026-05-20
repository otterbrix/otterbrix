#include "operator_fk_cascade.hpp"

#include <components/base/collection_full_name.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <components/types/logical_value.hpp>
#include <services/disk/manager_disk.hpp>

#include <limits>

namespace components::operators {

    operator_fk_cascade_t::operator_fk_cascade_t(std::pmr::memory_resource* resource, log_t log, catalog::fk_info_t fk)
        : read_write_operator_t(resource, log, operator_type::fk_cascade)
        , fk_(std::move(fk)) {}

    void operator_fk_cascade_t::on_execute_impl(pipeline::context_t* /*ctx*/) {
        if (!left_)
            return;
        // The delete operator's output is schema-typed but has no actual values
        // (intercept_dml_io_ clears the data while keeping the column types).
        // Prefer the scan operator's output (left_->left()) which holds the
        // pre-delete row values needed to look up referencing child rows.
        output_ = nullptr;
        if (left_->left() && left_->left()->output() && left_->left()->output()->size() > 0) {
            output_ = left_->left()->output();
        }
        if (!output_) {
            output_ = left_->output();
        }
        if (output_ && output_->size() > 0) {
            async_wait();
        }
    }

    actor_zeta::unique_future<void> operator_fk_cascade_t::await_async_and_resume(pipeline::context_t* ctx) {
        if (!output_ || output_->size() == 0) {
            mark_executed();
            co_return;
        }
        const auto& chunk = output_->data_chunk();
        execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        const auto& par_indices = fk_.parent_col_indices;
        const std::size_t absent = std::numeric_limits<std::size_t>::max();
        // If indices weren't resolved at plan time, skip cascade.
        for (auto idx : par_indices) {
            if (idx == absent) {
                mark_executed();
                co_return;
            }
        }
        if (par_indices.empty()) {
            mark_executed();
            co_return;
        }

        for (uint64_t row = 0; row < chunk.size(); ++row) {
            std::pmr::vector<types::logical_value_t> key_values(resource_);
            key_values.reserve(par_indices.size());
            for (auto pidx : par_indices) {
                key_values.push_back(chunk.value(pidx, row));
            }

            std::pmr::vector<std::string> key_cols(resource_);
            key_cols.reserve(fk_.child_col_names.size());
            for (const auto& n : fk_.child_col_names) {
                key_cols.emplace_back(n);
            }
            auto [_, fut] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::scan_by_key,
                                             exec_ctx,
                                             fk_.child_table_oid,
                                             std::move(key_cols),
                                             std::move(key_values));
            auto child_ids = co_await std::move(fut);
            if (child_ids.empty())
                continue;

            switch (fk_.del_action) {
                case 'a': // NO ACTION
                case 'r': // RESTRICT
                    set_error(core::error_t{
                        core::error_code_t::other_error,
                        std::pmr::string{"FK constraint violated: child rows reference deleted parent row",
                                         resource_}});
                    co_return;

                case 'c': { // CASCADE — delete child rows via storage_delete_rows
                    // Use txn_id=0 so the delete is committed immediately. The parent
                    // DELETE tracks its own commit; cascade child ops are not tracked by
                    // execute_plan_'s storage_commit_delete, which only covers the parent.
                    execution_context_t del_ctx{ctx->session, {}, {}};

                    components::vector::vector_t row_ids_vec(resource_, types::logical_type::BIGINT, child_ids.size());
                    for (std::size_t i = 0; i < child_ids.size(); ++i) {
                        row_ids_vec.data<int64_t>()[i] = child_ids[i];
                    }
                    auto [_d, dfut] = actor_zeta::send(ctx->disk_address,
                                                       &services::disk::manager_disk_t::storage_delete_rows,
                                                       del_ctx,
                                                       fk_.child_table_oid,
                                                       std::move(row_ids_vec),
                                                       static_cast<uint64_t>(child_ids.size()));
                    co_await std::move(dfut);
                    break;
                }
                case 'n':   // SET NULL
                case 'd': { // SET DEFAULT
                    components::vector::vector_t fetch_ids(resource_, types::logical_type::BIGINT, child_ids.size());
                    for (std::size_t i = 0; i < child_ids.size(); ++i) {
                        fetch_ids.data<int64_t>()[i] = child_ids[i];
                    }
                    auto [_f, ffut] = actor_zeta::send(ctx->disk_address,
                                                       &services::disk::manager_disk_t::storage_fetch,
                                                       ctx->session,
                                                       fk_.child_table_oid,
                                                       fetch_ids,
                                                       static_cast<uint64_t>(child_ids.size()));
                    auto fetched = co_await std::move(ffut);
                    if (!fetched || fetched->size() == 0)
                        break;

                    const bool is_set_null = (fk_.del_action == 'n');
                    for (std::size_t ci = 0; ci < fk_.child_col_schema_indices.size(); ++ci) {
                        const auto schema_idx = fk_.child_col_schema_indices[ci];
                        if (schema_idx == absent || schema_idx >= fetched->column_count())
                            continue;
                        if (is_set_null) {
                            for (uint64_t r = 0; r < fetched->size(); ++r) {
                                fetched->data[schema_idx].validity().set_invalid(r);
                            }
                        } else {
                            // SET DEFAULT: decode attdefspec; NULL default → same as SET NULL.
                            const auto& spec = ci < fk_.child_col_default_specs.size() ? fk_.child_col_default_specs[ci]
                                                                                       : std::string{};
                            auto default_val =
                                spec.empty() ? std::nullopt : components::catalog::decode_default_spec(resource_, spec);
                            for (uint64_t r = 0; r < fetched->size(); ++r) {
                                if (default_val.has_value()) {
                                    fetched->set_value(schema_idx, r, *default_val);
                                } else {
                                    fetched->data[schema_idx].validity().set_invalid(r);
                                }
                            }
                        }
                    }

                    components::vector::vector_t upd_ids(resource_, types::logical_type::BIGINT, child_ids.size());
                    for (std::size_t i = 0; i < child_ids.size(); ++i) {
                        upd_ids.data<int64_t>()[i] = child_ids[i];
                    }
                    execution_context_t upd_ctx{ctx->session, {}, {}};
                    auto [_u, ufut] = actor_zeta::send(ctx->disk_address,
                                                       &services::disk::manager_disk_t::storage_update,
                                                       upd_ctx,
                                                       fk_.child_table_oid,
                                                       std::move(upd_ids),
                                                       std::move(fetched));
                    co_await std::move(ufut);
                    break;
                }
                default:
                    break;
            }
        }
        mark_executed();
    }

} // namespace components::operators