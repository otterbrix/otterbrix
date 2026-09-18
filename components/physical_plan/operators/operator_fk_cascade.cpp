#include "operator_fk_cascade.hpp"

#include "constraint_util.hpp"

#include <cstring>

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

    actor_zeta::unique_future<void> operator_fk_cascade_t::await_async_and_resume(pipeline::context_t* ctx) {
        // fk_cascade is the plan root; output_ is the DELETE's matched-parent-rows cursor (R6).
        const auto& source = constraint_detail::resolve_constraint_source(left_);
        output_ = source;
        if (!source || source->size() == 0) {
            mark_executed();
            co_return;
        }
        const auto& in_chunks = output_->chunks();
        execution_context_t exec_ctx{ctx->session, ctx->txn, ctx->execution_context.timezone_offset};

        const auto& par_indices = fk_.parent_col_indices;
        const std::size_t absent = std::numeric_limits<std::size_t>::max();

        if (par_indices.size() != fk_.child_col_names.size()) {
            std::pmr::string what{"FK constraint: foreign key column count mismatch — ", resource_};
            what.append(std::to_string(fk_.child_col_names.size()).c_str());
            what.append(" referencing column(s) vs ");
            what.append(std::to_string(par_indices.size()).c_str());
            what.append(" referenced column(s)");
            set_error(core::error_t{core::error_code_t::invalid_constraint, std::move(what)});
            mark_failed();
            co_return;
        }

        // `absent` means the key never resolved, not that it matched nothing.
        for (std::size_t i = 0; i < par_indices.size(); ++i) {
            if (par_indices[i] != absent) {
                continue;
            }
            std::pmr::string what{"FK constraint: referenced column ", resource_};
            if (i < fk_.parent_col_names.size()) {
                what.append("\"");
                what.append(fk_.parent_col_names[i].c_str());
                what.append("\" ");
            }
            what.append("has no resolved position in the parent row — the ON DELETE action cannot be evaluated");
            set_error(core::error_t{core::error_code_t::invalid_constraint, std::move(what)});
            mark_failed();
            co_return;
        }
        if (par_indices.empty()) {
            set_error(
                core::error_t{core::error_code_t::invalid_constraint,
                              std::pmr::string{"FK constraint: no referenced columns resolved — the ON DELETE action "
                                               "cannot be evaluated",
                                               resource_}});
            mark_failed();
            co_return;
        }

        if (fk_.del_action == 'd' && fk_.child_col_default_specs.size() < fk_.child_col_schema_indices.size()) {
            std::pmr::string what{"FK constraint: ON DELETE SET DEFAULT has ", resource_};
            what.append(std::to_string(fk_.child_col_default_specs.size()).c_str());
            what.append(" default spec(s) for ");
            what.append(std::to_string(fk_.child_col_schema_indices.size()).c_str());
            what.append(" referencing column(s) — a column with no spec would silently be set to "
                        "NULL instead of its default");
            set_error(core::error_t{core::error_code_t::invalid_constraint, std::move(what)});
            mark_failed();
            co_return;
        }

        std::pmr::vector<std::string> key_cols(resource_);
        key_cols.reserve(fk_.child_col_names.size());
        for (const auto& n : fk_.child_col_names) {
            key_cols.emplace_back(n);
        }

        // chunk.data[] doesn't bound-check, so this guards par_indices explicitly. The scan below runs under
        // exec_ctx's transaction, so a child row this txn already deleted is filtered out.
        auto refuse_narrow_parent = [&](std::size_t width, std::size_t pidx, std::size_t slot) {
            std::pmr::string what{"FK constraint: the matched parent rows have ", resource_};
            what.append(std::to_string(width).c_str());
            what.append(" column(s), too few to hold referenced column ");
            if (slot < fk_.parent_col_names.size()) {
                what.append("\"");
                what.append(fk_.parent_col_names[slot].c_str());
                what.append("\" ");
            }
            what.append("at position ");
            what.append(std::to_string(pidx).c_str());
            what.append(" — the ON DELETE action cannot be evaluated");
            set_error(core::error_t{core::error_code_t::invalid_constraint, std::move(what)});
            mark_failed();
        };

        std::pmr::vector<types::complex_logical_type> key_types(resource_);
        key_types.reserve(par_indices.size());
        for (std::size_t j = 0; j < par_indices.size(); ++j) {
            if (par_indices[j] >= in_chunks.front().column_count()) {
                refuse_narrow_parent(in_chunks.front().column_count(), par_indices[j], j);
                co_return;
            }
            key_types.push_back(in_chunks.front().data[par_indices[j]].type());
        }
        std::pmr::vector<std::pmr::vector<std::int64_t>> per_row_child_ids(resource_);
        for (const auto& chunk : in_chunks) {
            if (chunk.size() == 0) {
                continue;
            }
            components::vector::data_chunk_t keys(resource_, key_types, chunk.size());
            for (std::size_t j = 0; j < par_indices.size(); ++j) {
                if (par_indices[j] >= chunk.column_count()) {
                    refuse_narrow_parent(chunk.column_count(), par_indices[j], j);
                    co_return;
                }
                components::vector::vector_ops::copy(chunk.data[par_indices[j]], keys.data[j], chunk.size(), 0, 0);
            }
            keys.set_cardinality(chunk.size());

            std::pmr::vector<std::string> col_names(resource_);
            col_names.reserve(key_cols.size());
            for (const auto& n : key_cols) {
                col_names.emplace_back(n);
            }
            auto [_s, sfut] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                          &services::disk::manager_disk_t::scan_by_keys,
                                                          exec_ctx,
                                                          fk_.child_table_oid,
                                                          std::move(col_names),
                                                          std::move(keys));
            auto chunk_child_ids_r = co_await std::move(sfut);
            if (chunk_child_ids_r.has_error()) {
                set_error(chunk_child_ids_r.error());
                co_return;
            }
            auto& chunk_child_ids = chunk_child_ids_r.value();
            for (auto& ids : chunk_child_ids) {
                per_row_child_ids.push_back(std::move(ids));
            }
        }

        switch (fk_.del_action) {
            case 'a': // NO ACTION
            case 'r': // RESTRICT
                for (const auto& child_ids : per_row_child_ids) {
                    if (!child_ids.empty()) {
                        set_error(core::error_t{
                            core::error_code_t::invalid_constraint,
                            std::pmr::string{"FK constraint violated: child rows reference deleted parent row",
                                             resource_}});
                        co_return;
                    }
                }
                break;

            case 'c': { // CASCADE
                // Stamped with the parent txn; CASCADE never reads a row back.
                std::pmr::vector<int64_t> all_child_ids(resource_);
                for (const auto& child_ids : per_row_child_ids) {
                    for (auto id : child_ids) {
                        all_child_ids.push_back(id);
                    }
                }
                if (all_child_ids.empty())
                    break;

                components::vector::vector_t row_ids_vec(resource_, types::logical_type::BIGINT, all_child_ids.size());
                for (std::size_t i = 0; i < all_child_ids.size(); ++i) {
                    row_ids_vec.data<int64_t>()[i] = all_child_ids[i];
                }
                auto [_d, dfut] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                              &services::disk::manager_disk_t::storage_delete_rows,
                                                              exec_ctx,
                                                              fk_.child_table_oid,
                                                              std::move(row_ids_vec),
                                                              static_cast<uint64_t>(all_child_ids.size()));
                auto deleted_r = co_await std::move(dfut);
                if (deleted_r.has_error()) {
                    set_error(deleted_r.error());
                    mark_failed();
                    co_return;
                }
                if (ctx->txn.transaction_id != 0) {
                    ctx->dml_deletes.push_back(
                        components::table::dml_delete_range_t{fk_.child_table_oid, ctx->txn.transaction_id});
                }
                break;
            }
            case 'n':   // SET NULL
            case 'd': { // SET DEFAULT
                // Mirrors CASCADE, but paired back via chunk.row_ids since the reply isn't positional.
                std::pmr::vector<int64_t> all_child_ids(resource_);
                for (const auto& child_ids : per_row_child_ids) {
                    for (auto id : child_ids) {
                        all_child_ids.push_back(id);
                    }
                }
                if (all_child_ids.empty())
                    break;

                components::vector::vector_t fetch_ids(resource_, types::logical_type::BIGINT, all_child_ids.size());
                for (std::size_t i = 0; i < all_child_ids.size(); ++i) {
                    fetch_ids.data<int64_t>()[i] = all_child_ids[i];
                }
                auto [_f, ffut] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                              &services::disk::manager_disk_t::storage_fetch,
                                                              ctx->session,
                                                              fk_.child_table_oid,
                                                              std::move(fetch_ids),
                                                              static_cast<uint64_t>(all_child_ids.size()),
                                                              std::vector<size_t>{},
                                                              ctx->txn,
                                                              components::table::fetch_visibility_t::SNAPSHOT,
                                                              /*limit=*/int64_t{-1},
                                                              services::disk::k_fetch_epoch_unchecked);
                auto fetched_r = co_await std::move(ffut);
                if (fetched_r.has_error()) {
                    // Must abort — transforming empty cells on a failed read would corrupt rows.
                    set_error(fetched_r.error());
                    co_return;
                }
                auto fetched = std::move(fetched_r.value());
                if (fetched.empty())
                    break;

                const bool is_set_null = (fk_.del_action == 'n');
                for (std::size_t ci = 0; ci < fk_.child_col_schema_indices.size(); ++ci) {
                    const auto schema_idx = fk_.child_col_schema_indices[ci];
                    if (schema_idx == absent) {
                        std::pmr::string what{"FK constraint: referencing column ", resource_};
                        if (ci < fk_.child_col_names.size()) {
                            what.append("\"");
                            what.append(fk_.child_col_names[ci].c_str());
                            what.append("\" ");
                        }
                        what.append(is_set_null ? "has no resolved position in the child table — it cannot be "
                                                  "set to NULL"
                                                : "has no resolved position in the child table — it cannot be "
                                                  "set to its default");
                        set_error(core::error_t{core::error_code_t::invalid_constraint, std::move(what)});
                        mark_failed();
                        co_return;
                    }
                    std::optional<types::logical_value_t> default_val;
                    if (!is_set_null && ci < fk_.child_col_default_specs.size() &&
                        !fk_.child_col_default_specs[ci].empty() && !fetched.empty() &&
                        schema_idx < fetched.front().column_count()) {
                        if (auto ec = components::catalog::decode_default_spec(resource_,
                                                                               fetched.front().data[schema_idx].type(),
                                                                               fk_.child_col_default_specs[ci],
                                                                               default_val);
                            ec.contains_error()) {
                            // A default that fails to decode is catalog corruption, not a fallback to SET NULL.
                            set_error(std::move(ec));
                            mark_failed();
                            co_return;
                        }
                        if (default_val.has_value() && default_val->is_null()) {
                            default_val.reset();
                        }
                    }
                    for (auto& chunk : fetched) {
                        if (schema_idx >= chunk.column_count()) {
                            std::pmr::string what{"FK constraint: the child row batch has ", resource_};
                            what.append(std::to_string(chunk.column_count()).c_str());
                            what.append(" column(s), too few to hold referencing column at position ");
                            what.append(std::to_string(schema_idx).c_str());
                            what.append(" — the ON DELETE action cannot be applied");
                            set_error(core::error_t{core::error_code_t::invalid_constraint, std::move(what)});
                            mark_failed();
                            co_return;
                        }
                        for (uint64_t r = 0; r < chunk.size(); ++r) {
                            if (!is_set_null && default_val.has_value()) {
                                chunk.set_value(schema_idx, r, *default_val);
                            } else {
                                chunk.data[schema_idx].validity().set_invalid(r);
                            }
                        }
                    }
                }

                std::pmr::vector<components::vector::vector_t> upd_ids_batch(resource_);
                std::pmr::vector<components::vector::data_chunk_t> upd_data_batch(resource_);
                for (auto& chunk : fetched) {
                    const uint64_t n = chunk.size();
                    if (n == 0) {
                        continue;
                    }
                    components::vector::vector_t ids(resource_, types::logical_type::BIGINT, n);
                    std::memcpy(ids.data(), chunk.row_ids.data(), n * sizeof(int64_t));
                    upd_ids_batch.emplace_back(std::move(ids));
                    upd_data_batch.emplace_back(std::move(chunk));
                }
                auto [_u, ufut] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                              &services::disk::manager_disk_t::storage_update,
                                                              exec_ctx,
                                                              fk_.child_table_oid,
                                                              std::move(upd_ids_batch),
                                                              std::move(upd_data_batch));
                auto update_result = co_await std::move(ufut);
                if (update_result.has_error()) {
                    set_error(update_result.error());
                    mark_failed();
                    co_return;
                }
                if (ctx->txn.transaction_id != 0) {
                    auto [upd_row_start, upd_row_count] = update_result.value();
                    if (upd_row_count > 0) {
                        ctx->dml_appends.push_back(
                            components::table::dml_append_range_t{fk_.child_table_oid, upd_row_start, upd_row_count});
                    }
                    ctx->dml_deletes.push_back(
                        components::table::dml_delete_range_t{fk_.child_table_oid, ctx->txn.transaction_id});
                }
                break;
            }
            default: {
                // Falling through would report SUCCESS with no cascade; confdeltype should be one of {a,r,c,n,d}.
                std::pmr::string what{"FK constraint: ON DELETE action '", resource_};
                what.append(std::pmr::string(1, fk_.del_action, resource_));
                what.append("' in pg_constraint.confdeltype is not one of the actions this build can apply "
                            "— the cascade cannot be evaluated");
                set_error(core::error_t{core::error_code_t::invalid_constraint, std::move(what)});
                mark_failed();
                co_return;
            }
        }
        mark_executed();
    }

} // namespace components::operators
