#include "operator_fk_cascade.hpp"

#include <components/base/collection_full_name.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector_operations.hpp>
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

        // Child key column names are the same for every row; hoist them once.
        std::pmr::vector<std::string> key_cols(resource_);
        key_cols.reserve(fk_.child_col_names.size());
        for (const auto& n : fk_.child_col_names) {
            key_cols.emplace_back(n);
        }

        // Stage A: build one key per deleted parent row, then a single batched
        // scan of the child table. per_row_child_ids[row] = referencing child
        // row_ids for that parent row (empty -> nothing references it). The parent
        // key columns are copied into an OWNED keys-chunk (it crosses the mailbox and
        // actors must not share buffers). All rows are copied in input order, so
        // keys-chunk row i pairs with parent row i (result[i] mapping).
        std::pmr::vector<types::complex_logical_type> key_types(resource_);
        key_types.reserve(par_indices.size());
        for (auto pidx : par_indices) {
            key_types.push_back(chunk.data[pidx].type());
        }
        components::vector::data_chunk_t keys(resource_, key_types, chunk.size() == 0 ? 1 : chunk.size());
        for (std::size_t j = 0; j < par_indices.size(); ++j) {
            components::vector::vector_ops::copy(chunk.data[par_indices[j]], keys.data[j], chunk.size(), 0, 0);
        }
        keys.set_cardinality(chunk.size());

        auto [_s, sfut] = actor_zeta::send(ctx->disk_address,
                                           &services::disk::manager_disk_t::scan_by_keys,
                                           exec_ctx,
                                           fk_.child_table_oid,
                                           std::move(key_cols),
                                           std::move(keys));
        auto per_row_child_ids = co_await std::move(sfut);

        switch (fk_.del_action) {
            case 'a': // NO ACTION
            case 'r': // RESTRICT
                // Any referencing child row blocks the parent delete.
                for (const auto& child_ids : per_row_child_ids) {
                    if (!child_ids.empty()) {
                        set_error(core::error_t{
                            core::error_code_t::other_error,
                            std::pmr::string{"FK constraint violated: child rows reference deleted parent row",
                                             resource_}});
                        co_return;
                    }
                }
                break;

            case 'c': { // CASCADE — delete child rows via storage_delete_rows
                // Aggregate every referencing child row_id across all parent rows
                // into one delete. txn_id=0 commits the child delete immediately:
                // execute_plan_'s storage_publish_delete only covers the parent, so
                // cascade child ops aren't tracked there.
                std::pmr::vector<int64_t> all_child_ids(resource_);
                for (const auto& child_ids : per_row_child_ids) {
                    for (auto id : child_ids) {
                        all_child_ids.push_back(id);
                    }
                }
                if (all_child_ids.empty())
                    break;

                execution_context_t del_ctx{ctx->session, {}, {}};
                components::vector::vector_t row_ids_vec(resource_, types::logical_type::BIGINT, all_child_ids.size());
                for (std::size_t i = 0; i < all_child_ids.size(); ++i) {
                    row_ids_vec.data<int64_t>()[i] = all_child_ids[i];
                }
                auto [_d, dfut] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::storage_delete_rows,
                                                   del_ctx,
                                                   fk_.child_table_oid,
                                                   std::move(row_ids_vec),
                                                   static_cast<uint64_t>(all_child_ids.size()));
                co_await std::move(dfut);
                break;
            }
            case 'n':   // SET NULL
            case 'd': { // SET DEFAULT
                // Mirror the CASCADE branch's flattening: aggregate EVERY referencing
                // child row_id across all parent rows into ONE set, then do a single
                // fetch + single update against the SAME child_table_oid (one owning
                // agent). The SET NULL / SET DEFAULT transform is uniform across rows
                // — it keys off per-COLUMN child_col_schema_indices / per-COLUMN
                // child_col_default_specs, never off the parent row — so a single
                // combined update chunk is value-correct. Each child row_id stays
                // paired with its fetched chunk position because storage_fetch returns
                // rows positionally aligned with the requested row_ids, and
                // storage_update applies data[i] to row_ids[i] positionally.
                std::pmr::vector<int64_t> all_child_ids(resource_);
                for (const auto& child_ids : per_row_child_ids) {
                    for (auto id : child_ids) {
                        all_child_ids.push_back(id);
                    }
                }
                if (all_child_ids.empty())
                    break;

                // Single fetch for the whole set.
                components::vector::vector_t fetch_ids(resource_, types::logical_type::BIGINT, all_child_ids.size());
                for (std::size_t i = 0; i < all_child_ids.size(); ++i) {
                    fetch_ids.data<int64_t>()[i] = all_child_ids[i];
                }
                auto [_f, ffut] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::storage_fetch,
                                                   ctx->session,
                                                   fk_.child_table_oid,
                                                   std::move(fetch_ids),
                                                   static_cast<uint64_t>(all_child_ids.size()));
                auto fetched = co_await std::move(ffut);
                if (!fetched || fetched->size() == 0)
                    break;

                const bool is_set_null = (fk_.del_action == 'n');
                // Apply the uniform per-column transform to every fetched row.
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

                // Single update for the whole set.
                components::vector::vector_t upd_ids(resource_, types::logical_type::BIGINT, all_child_ids.size());
                for (std::size_t i = 0; i < all_child_ids.size(); ++i) {
                    upd_ids.data<int64_t>()[i] = all_child_ids[i];
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
        mark_executed();
    }

} // namespace components::operators
