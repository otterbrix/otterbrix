#include "index_scan.hpp"
#include "guard_chunk.hpp"

#include <components/expressions/forward.hpp>
#include <components/vector/operations/apply_operator.hpp>
#include <components/vector/vector_builders.hpp>
#include <cstring>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>

namespace components::operators {

#ifdef DEV_MODE
    namespace {
        index_fetch_gate_t* g_index_fetch_gate = nullptr;
    } // namespace
    void dev_set_index_fetch_gate(index_fetch_gate_t* gate) { g_index_fetch_gate = gate; }
    index_fetch_gate_t* dev_index_fetch_gate() { return g_index_fetch_gate; }
#endif

    index_scan::index_scan(std::pmr::memory_resource* resource,
                           log_t log,
                           components::catalog::oid_t table_oid,
                           const expressions::key_t& key,
                           core::parameter_id_t key_parameter,
                           expressions::compare_type compare_type,
                           components::logical_plan::index_type preferred_index_type,
                           logical_plan::row_cap_t cap,
                           std::vector<size_t> projected_cols,
                           int64_t key_chunk_col)
        : read_only_operator_t(resource, log, operator_type::index_scan)
        , table_oid_(table_oid)
        // Copied onto the operator's arena — key_ is read after the logical node's is released.
        , key_(key, resource)
        , key_parameter_(key_parameter)
        , compare_type_(compare_type)
        , preferred_index_type_(preferred_index_type)
        , cap_(cap)
        , projected_cols_(std::move(projected_cols))
        , key_chunk_col_(key_chunk_col) {}

    core::error_t index_scan::recheck_answered_rows_(const pipeline::context_t* ctx,
                                                     const vector::vector_t& key) const {
        if (key_chunk_col_ < 0) {
            return core::error_t::no_error();
        }
        const auto key_col = static_cast<uint64_t>(key_chunk_col_);
        const bool null_test = compare_type_ == expressions::compare_type::is_null ||
                               compare_type_ == expressions::compare_type::is_not_null;
        const auto code = expressions::to_operator_code(compare_type_);
        for (const auto& chunk : batch_) {
            if (key_col >= chunk.column_count()) {
                // The fetched width disagrees with the width plan-gen resolved against: nothing here can
                // be trusted to be the row the index meant, so refuse rather than skip the check.
                return core::error_t{core::error_code_t::internal_error,
                                     std::pmr::string{"index_scan: the fetched row is narrower than the "
                                                      "indexed column position the plan resolved; the index "
                                                      "answer cannot be verified",
                                                      resource_}};
            }
            if (chunk.size() == 0) {
                continue;
            }
            vector::vector_t selected(resource_,
                                      types::complex_logical_type{types::logical_type::BOOLEAN},
                                      chunk.size());
            if (null_test) {
                RETURN_IF_ERROR(vector::operations::apply_unary(code,
                                                                chunk.data[key_col],
                                                                &selected,
                                                                ctx->execution_context,
                                                                chunk.size()));
            } else {
                RETURN_IF_ERROR(vector::operations::apply_binary(code,
                                                                 chunk.data[key_col],
                                                                 key,
                                                                 &selected,
                                                                 ctx->execution_context,
                                                                 chunk.size()));
            }
            for (uint64_t row = 0; row < chunk.size(); ++row) {
                // The WHERE rule: an UNKNOWN (a NULL cell) fails the recheck too -- the index had no
                // business naming a row the predicate does not select.
                if (!selected.is_null(row) && selected.get_value<bool>(row)) {
                    continue;
                }
                return core::error_t{core::error_code_t::index_corrupted,
                                     std::pmr::string{"index_scan: the index named a row that does not "
                                                      "satisfy the predicate it was searched with — its "
                                                      "row ids no longer name the rows it indexed (a "
                                                      "restart that left the index ahead of its table); "
                                                      "rebuild the index and retry the statement",
                                                      resource_}};
            }
        }
        return core::error_t::no_error();
    }

    bool index_scan::key_selects_nothing_(const pipeline::context_t* ctx) const {
        return compare_type_ != expressions::compare_type::is_null &&
               compare_type_ != expressions::compare_type::is_not_null && ctx->parameters.is_null(key_parameter_);
    }

    actor_zeta::unique_future<core::error_t> index_scan::open_index_window(pipeline::context_t* ctx) {
        const auto& key = ctx->parameters.at(key_parameter_);
        auto [_s, sf] = preferred_index_type_ == logical_plan::index_type::no_valid
                            ? actor_zeta::otterbrix::send(ctx->index_address,
                                                          &services::index::manager_index_t::search,
                                                          ctx->session,
                                                          table_oid_,
                                                          index::keys_base_storage_t{{key_}},
                                                          vector::make_constant_copy(resource_, key),
                                                          compare_type_,
                                                          ctx->txn.start_time,
                                                          ctx->txn.transaction_id,
                                                          ctx->execution_context.timezone_offset)
                            : actor_zeta::otterbrix::send(ctx->index_address,
                                                          &services::index::manager_index_t::search_with_preferred_type,
                                                          ctx->session,
                                                          table_oid_,
                                                          index::keys_base_storage_t{{key_}},
                                                          vector::make_constant_copy(resource_, key),
                                                          compare_type_,
                                                          preferred_index_type_,
                                                          ctx->txn.start_time,
                                                          ctx->txn.transaction_id,
                                                          ctx->execution_context.timezone_offset);
        auto matched = co_await std::move(sf);
        if (matched.has_error()) {
            // Refuse instead of publishing an empty window, indistinguishable from "matched nothing".
            pos_ = 0;
            end_ = 0;
            co_return matched.error();
        }
        row_ids_vec_ = std::move(matched.value().row_ids);
        built_compact_epoch_ = matched.value().built_compact_epoch;

        // Read-cap not applied here: it applies below the visibility filter, in storage_fetch's limit.
        pos_ = 0;
        end_ = row_ids_vec_.size();
        co_return core::error_t::no_error();
    }

    actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<vector::data_chunk_t>>>
    index_scan::fetch_matched_window(pipeline::context_t* ctx) {
        const size_t count = (end_ > pos_) ? (end_ - pos_) : 0;
        if (count == 0) {
            co_return std::pmr::vector<vector::data_chunk_t>{resource_};
        }
        auto row_ids = table::make_row_id_batch(resource_, std::span{row_ids_vec_.data() + pos_, count});

        auto [_f, ff] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                    &services::disk::manager_disk_t::storage_fetch,
                                                    ctx->session,
                                                    table_oid_,
                                                    std::move(row_ids),
                                                    projected_cols_,
                                                    ctx->txn,
                                                    table::fetch_visibility_t::SNAPSHOT,
                                                    // -1 uncapped, else stops after this many rows.
                                                    cap_.to_disk(),
                                                    // Epoch minted against; refuses (write_conflict) if renumbered.
                                                    built_compact_epoch_);
        co_return co_await std::move(ff);
    }

    actor_zeta::unique_future<void> index_scan::release_cursor(pipeline::context_t* ctx) {
        if (hold_id_ == 0) {
            co_return;
        }
        const uint64_t id = hold_id_;
        // Clear first so a re-entry cannot double-send (same discipline as full_scan).
        hold_id_ = 0;
        auto [_c, cf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                    &services::disk::manager_disk_t::storage_close_cursor,
                                                    ctx->session,
                                                    table_oid_,
                                                    id);
        co_await std::move(cf);
        co_return;
    }

    // Awaits live in this nested coroutine, not a behavior() handler — no lost-wakeup between them.
    actor_zeta::unique_future<core::result_wrapper_t<std::optional<vector::data_chunk_t>>>
    index_scan::source_next(pipeline::context_t* ctx) {
        if (drained_) {
            co_return std::nullopt;
        }

        if (!opened_) {
            opened_ = true;
            if (ctx->index_address == actor_zeta::address_t::empty_address()) {
                pos_ = 0;
                end_ = 0;
                core::error_t unwired{core::error_code_t::internal_error,
                                      std::pmr::string{"index_scan: no index service is wired into this topology; "
                                                       "a planned index_scan cannot be answered",
                                                       resource_}};
                set_error(unwired);
                mark_failed();
                co_return std::move(unwired);
            }
            if (key_selects_nothing_(ctx)) {
                // An empty window: no hold to take and no search to ask. The schema below is still needed for the
                // 0-row guard chunk.
                row_ids_vec_.clear();
                pos_ = 0;
                end_ = 0;
            } else {
                // Compact-hold first, search second: defers compaction so minted row ids stay valid, not
                // already stale.
                if (hold_id_ == 0) {
                    auto [_h, hf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                                &services::disk::manager_disk_t::storage_open_scan_hold,
                                                                ctx->session,
                                                                table_oid_);
                    auto hold_r = co_await std::move(hf);
                    if (hold_r.has_error()) {
                        set_error(hold_r.error());
                        mark_failed();
                        co_return hold_r.convert_error<std::optional<vector::data_chunk_t>>();
                    }
                    hold_id_ = hold_r.value();
                }
                if (auto search_error = co_await open_index_window(ctx); search_error.contains_error()) {
                    set_error(search_error);
                    mark_failed();
                    co_return std::move(search_error);
                }
            }
            auto [_t, tf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                        &services::disk::manager_disk_t::storage_types,
                                                        ctx->session,
                                                        table_oid_);
            auto types_result = co_await std::move(tf);
            if (types_result.has_error()) {
                set_error(types_result.error());
                mark_failed();
                co_return types_result.convert_error<std::optional<vector::data_chunk_t>>();
            }
            guard_types_ = std::move(types_result.value());
        }

        if (!fetched_) {
            fetched_ = true;
#ifdef DEV_MODE
            // Test seam (test_index_scan_compact_race): lets another session's checkpoint land here.
            while (auto* gate = dev_index_fetch_gate()) {
                if (!gate->hold(table_oid_)) {
                    break;
                }
                auto [_g, gf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                            &services::disk::manager_disk_t::storage_total_rows,
                                                            ctx->session,
                                                            table_oid_);
                auto ping = co_await std::move(gf);
                if (ping.has_error()) {
                    break;
                }
            }
#endif
            auto batch_r = co_await fetch_matched_window(ctx);
            // Released now — holding through the pump would defer checkpoints for the whole emit phase.
            co_await release_cursor(ctx);
            if (batch_r.has_error()) {
                set_error(batch_r.error());
                mark_failed();
                co_return batch_r.convert_error<std::optional<vector::data_chunk_t>>();
            }
            batch_ = std::move(batch_r.value());
            batch_pos_ = 0;
            // Before ANY row leaves: a refusal that let the first chunk through would already have
            // handed the caller a wrong row.
            if (auto stale = recheck_answered_rows_(ctx, ctx->parameters.at(key_parameter_)); stale.contains_error()) {
                batch_.clear();
                set_error(stale);
                mark_failed();
                co_return std::move(stale);
            }
        }

        // No cap here: already applied below the visibility filter.
        if (batch_pos_ < batch_.size()) {
            auto chunk = std::move(batch_[batch_pos_++]);
            emitted_any_ = true;
            co_return std::move(chunk);
        }

        drained_ = true;
        if (!emitted_any_) {
            // 0-row guard: lets a scalar aggregate emit COUNT=0 / OUTER join NULL-pad.
            emitted_any_ = true;
            co_return make_guard_chunk(resource_, guard_types_, projected_cols_);
        }
        co_return std::nullopt;
    }

} // namespace components::operators
