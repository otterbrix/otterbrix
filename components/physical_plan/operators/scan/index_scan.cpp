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
        , key_(key)
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
            // No index service — empty window (no matched ids).
            pos_ = 0;
            end_ = 0;
            co_return core::error_t::no_error();
        }

        // Search index for matching row IDs (txn-aware visibility). One-shot: the whole matched
        // set comes back in this single future.
        auto [_s, sf] = preferred_index_type_ == logical_plan::index_type::no_valid
                            ? actor_zeta::send(ctx->index_address,
                                               &services::index::manager_index_t::search,
                                               ctx->session,
                                               table_oid_,
                                               index::keys_base_storage_t{{key_}},
                                               types::logical_value_t{resource_, value_},
                                               compare_type_,
                                               ctx->txn.start_time,
                                               ctx->txn.transaction_id,
                                               ctx->execution_context.timezone_offset)
                            : actor_zeta::send(ctx->index_address,
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
            // The index manager could not ANSWER — no engine for the oid, no index on
            // the predicate key, or a failed read in the index's disk agent. An empty
            // window here would be indistinguishable from "matched nothing" and would
            // publish a silently short result set.
            pos_ = 0;
            end_ = 0;
            co_return matched.error();
        }
        row_ids_vec_ = std::move(matched.value());

        // The whole matched set is the fetch window. The read-cap (offset+limit head cap) is
        // deliberately NOT applied here any more (C4b): the index answer is a SUPERSET —
        // manager_index says so — and since C4b the fetch DROPS the rows this transaction may
        // not see, so cutting the id list to `limit` before the fetch can cut away the very
        // ids whose rows survive it and return fewer rows than the LIMIT asked for. The cap
        // now rides BELOW that filter, in source_next, over the rows actually produced —
        // the same order full_scan uses, where the agent counts POST-filter matched rows.
        // SELECT OFFSET is applied by operator_limit above, so the seek starts at 0.
        pos_ = 0;
        end_ = row_ids_vec_.size();
        co_return core::error_t::no_error();
    }

    // Fetch the whole matched window [pos_, end_) in ONE storage_fetch. The disk agent windows the
    // request into ≤ DEFAULT_VECTOR_CAPACITY chunks and stamps each chunk's absolute row_ids (so a
    // downstream DELETE/UPDATE/index sees the right rows), returning them as a vector that source_next
    // buffers. An empty window (or an OID this agent does not own) yields an empty vector.
    actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<vector::data_chunk_t>>>
    index_scan::fetch_matched_window(pipeline::context_t* ctx) {
        const size_t count = (end_ > pos_) ? (end_ - pos_) : 0;
        if (count == 0) {
            co_return std::pmr::vector<vector::data_chunk_t>{resource_};
        }
        // Build the absolute-row-id vector for the whole window.
        vector::vector_t row_ids(resource_, types::logical_type::BIGINT, count);
        std::memcpy(row_ids.data(), row_ids_vec_.data() + pos_, count * sizeof(int64_t));

        auto [_f, ff] = actor_zeta::send(ctx->disk_address,
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
                                         table::fetch_visibility_t::SNAPSHOT);
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
            auto [_t, tf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::storage_types,
                                             ctx->session,
                                             table_oid_);
            guard_types_ = co_await std::move(tf);
        }

        // FIRST fetch: pull the whole matched window in ONE storage_fetch; the disk batches it into
        // ≤ DEFAULT_VECTOR_CAPACITY chunks buffered in batch_. Subsequent calls just drain the buffer.
        if (!fetched_) {
            fetched_ = true;
            auto batch_r = co_await fetch_matched_window(ctx);
            if (batch_r.has_error()) {
                // The disk agent's point-fetch failed (buffer-pool OOM / corrupt
                // overflow block): surface it through the source's own error channel
                // instead of emitting silently empty rows — same convention as
                // full_scan's fetch error path.
                set_error(batch_r.error());
                mark_failed();
                co_return batch_r.convert_error<vector::data_chunk_t>();
            }
            batch_ = std::move(batch_r.value());
            batch_pos_ = 0;
        }

        // Emit the next buffered chunk, capped BELOW the visibility filter. The cap counts
        // rows the fetch actually produced, so a row the snapshot hid never consumes budget —
        // which is exactly what applying it to the raw index answer used to do.
        const int64_t cap = limit_.head_cap();
        if (cap >= 0 && emitted_rows_ >= static_cast<uint64_t>(cap)) {
            batch_pos_ = batch_.size();
        }
        if (batch_pos_ < batch_.size()) {
            auto chunk = std::move(batch_[batch_pos_++]);
            if (cap >= 0) {
                const uint64_t budget = static_cast<uint64_t>(cap) - emitted_rows_;
                if (chunk.size() > budget) {
                    chunk.set_cardinality(budget);
                }
            }
            emitted_rows_ += chunk.size();
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
