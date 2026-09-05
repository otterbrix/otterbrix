#include "index_scan.hpp"

#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>

namespace components::operators {

    index_scan::index_scan(std::pmr::memory_resource* resource,
                           log_t log,
                           components::catalog::oid_t table_oid,
                           const expressions::key_t& key,
                           const types::logical_value_t& value,
                           expressions::compare_type compare_type,
                           components::logical_plan::index_type preferred_index_type,
                           logical_plan::limit_t limit,
                           std::vector<size_t> projected_cols)
        : read_only_operator_t(resource, log, operator_type::index_scan)
        , table_oid_(table_oid)
        // Copied onto the OPERATOR's arena (`resource`), not left on the LOGICAL node's: that arena is
        // released when the plan is torn down, while key_ is read during execution, after that.
        , key_(key, resource)
        , value_(value)
        , compare_type_(compare_type)
        , preferred_index_type_(preferred_index_type)
        , limit_(limit)
        , projected_cols_(std::move(projected_cols)) {}

    // --- Windowing core -------------------------------------------------------------------------
    // Run the ONE-SHOT index search and compute the read-cap window [pos_=0, end_) over the matched
    // ids. source_next calls this exactly once (the first call), so the search + windowing logic
    // lives in ONE place.
    actor_zeta::unique_future<core::error_t> index_scan::open_index_window(pipeline::context_t* ctx) {
        if (ctx->index_address == actor_zeta::address_t::empty_address()) {
            // An index_scan is built ONLY when the planner proved an index exists (can_use_index); an unwired
            // index service is that invariant broken. Refuse rather than hand back an empty window, which
            // would be indistinguishable from "no row matches the predicate".
            pos_ = 0;
            end_ = 0;
            co_return core::error_t{core::error_code_t::index_not_exists,
                                    std::pmr::string{"index_scan: no index service is wired into this topology; "
                                                     "a planned index_scan cannot be answered",
                                                     resource_}};
        }

        // Search index for matching row IDs (txn-aware visibility). One-shot: the whole matched
        // set comes back in this single future.
        auto [_s, sf] = preferred_index_type_ == logical_plan::index_type::no_valid
                            ? actor_zeta::otterbrix::send(ctx->index_address,
                                                          &services::index::manager_index_t::search,
                                                          ctx->session,
                                                          table_oid_,
                                                          index::keys_base_storage_t{{key_}},
                                                          types::logical_value_t{resource_, value_},
                                                          compare_type_,
                                                          ctx->txn.start_time,
                                                          ctx->txn.transaction_id,
                                                          ctx->execution_context.timezone_offset)
                            : actor_zeta::otterbrix::send(ctx->index_address,
                                                          &services::index::manager_index_t::search_with_preferred_type,
                                                          ctx->session,
                                                          table_oid_,
                                                          index::keys_base_storage_t{{key_}},
                                                          types::logical_value_t{resource_, value_},
                                                          compare_type_,
                                                          preferred_index_type_,
                                                          ctx->txn.start_time,
                                                          ctx->txn.transaction_id,
                                                          ctx->execution_context.timezone_offset);
        auto matched = co_await std::move(sf);
        if (matched.has_error()) {
            // The index manager could not answer (no engine, no index on the key, or a failed read) — refuse
            // rather than publish an empty window indistinguishable from "matched nothing".
            pos_ = 0;
            end_ = 0;
            co_return matched.error();
        }
        row_ids_vec_ = std::move(matched.value());

        // The whole matched set is the fetch window -- the read-cap is deliberately NOT applied here: the
        // index answer is a SUPERSET (the fetch below still drops invisible rows), so cutting to `limit`
        // first could drop ids that would have survived the visibility filter. The cap rides BELOW that
        // filter instead, as storage_fetch's own limit (same shape full_scan uses); OFFSET is applied by
        // operator_limit above, so the seek starts at 0.
        pos_ = 0;
        end_ = row_ids_vec_.size();
        co_return core::error_t::no_error();
    }

    // Fetch the matched window [pos_, end_) in ONE storage_fetch, capped at limit_.head_cap() visible rows —
    // the agent stops as soon as it hits the cap, so a LIMIT 1 over a million matched ids reads one window
    // instead of a thousand. An oid with no storage is a refusal on the wrapper, not an empty vector.
    actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<vector::data_chunk_t>>>
    index_scan::fetch_matched_window(pipeline::context_t* ctx) {
        const size_t count = (end_ > pos_) ? (end_ - pos_) : 0;
        if (count == 0) {
            co_return std::pmr::vector<vector::data_chunk_t>{resource_};
        }
        // Build the absolute-row-id vector for the whole window.
        vector::vector_t row_ids(resource_, types::logical_type::BIGINT, count);
        std::memcpy(row_ids.data(), row_ids_vec_.data() + pos_, count * sizeof(int64_t));

        auto [_f, ff] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                    &services::disk::manager_disk_t::storage_fetch,
                                                    ctx->session,
                                                    table_oid_,
                                                    std::move(row_ids),
                                                    count,
                                                    projected_cols_,
                                                    // The reader's own snapshot: the index answered with a
                                                    // superset of ids and the table decides which of them
                                                    // this transaction may see.
                                                    ctx->txn,
                                                    table::fetch_visibility_t::SNAPSHOT,
                                                    // POST-VISIBILITY row cap. -1 == uncapped; otherwise the
                                                    // agent hands back exactly this many visible rows (fewer
                                                    // if the window runs out first) and reads no further.
                                                    limit_.head_cap());
        co_return co_await std::move(ff);
    }

    // --- Push-based streaming pipeline source (buffered batch point-fetch) ----------------------
    // FIRST call: open_index_window (await #1: the one-shot index search) + cache schema (await #2:
    //   storage_types) + ONE storage_fetch over the whole [pos_, end_) window (await #3). The disk
    //   agent batches the reply into ≤ DEFAULT_VECTOR_CAPACITY chunks, buffered in batch_.
    // EACH call: emit the next buffered chunk (no await); advance batch_pos_.
    // DRAIN: batch_ exhausted ⇒ if nothing was emitted, ONE schema'd 0-row guard (scalar aggregate
    //   COUNT=0 / OUTER-join NULL-pad), else the 0-column drain sentinel.
    // The FIRST call's sequential cross-actor awaits live in this nested operator coroutine (driven by
    // co_await from execute_pipeline), not a behavior() handler, so the single-slot awaited
    // continuation is republished+cleared between awaits — no lost-wakeup.
    actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
    index_scan::source_next(pipeline::context_t* ctx) {
        if (drained_) {
            co_return core::result_wrapper_t<vector::data_chunk_t>(
                vector::data_chunk_t{resource_, std::pmr::vector<types::complex_logical_type>{resource_}, 0});
        }

        if (!opened_) {
            opened_ = true;
            if (auto search_error = co_await open_index_window(ctx); search_error.contains_error()) {
                // Same channel the window fetch below uses: the source reports the
                // failure instead of draining to zero rows, which would look exactly
                // like a predicate nothing matched.
                set_error(search_error);
                mark_failed();
                co_return core::result_wrapper_t<vector::data_chunk_t>(std::move(search_error));
            }
            // Cache the table schema for the no-row empty-guard below.
            auto [_t, tf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                        &services::disk::manager_disk_t::storage_types,
                                                        ctx->session,
                                                        table_oid_);
            auto types_result = co_await std::move(tf);
            if (types_result.has_error()) {
                set_error(types_result.error());
                mark_failed();
                co_return types_result.convert_error<vector::data_chunk_t>();
            }
            guard_types_ = std::move(types_result.value());
        }

        // FIRST fetch: pull the whole matched window in ONE storage_fetch; the disk batches it into
        // ≤ DEFAULT_VECTOR_CAPACITY chunks buffered in batch_. Subsequent calls just drain the buffer.
        if (!fetched_) {
            fetched_ = true;
            auto batch_r = co_await fetch_matched_window(ctx);
            if (batch_r.has_error()) {
                // Surface a failed point-fetch through the error channel (same convention as full_scan)
                // instead of emitting silently empty rows.
                set_error(batch_r.error());
                mark_failed();
                co_return batch_r.convert_error<vector::data_chunk_t>();
            }
            batch_ = std::move(batch_r.value());
            batch_pos_ = 0;
        }

        // No cap here: the agent already spent it below the visibility filter, so batch_ holds at most
        // limit_.head_cap() rows total. Re-applying it would just be a duplicated rule to keep in sync.
        if (batch_pos_ < batch_.size()) {
            auto chunk = std::move(batch_[batch_pos_++]);
            emitted_any_ = true;
            co_return core::result_wrapper_t<vector::data_chunk_t>(std::move(chunk));
        }

        // Buffer exhausted ⇒ drain.
        drained_ = true;
        if (!emitted_any_) {
            // ONE schema'd 0-row guard so a scalar aggregate emits COUNT=0 and an OUTER join
            // NULL-pads, then the 0-column sentinel next call.
            emitted_any_ = true;
            co_return core::result_wrapper_t<vector::data_chunk_t>(vector::data_chunk_t{resource_, guard_types_, 0});
        }
        co_return core::result_wrapper_t<vector::data_chunk_t>(
            vector::data_chunk_t{resource_, std::pmr::vector<types::complex_logical_type>{resource_}, 0});
    }

} // namespace components::operators
