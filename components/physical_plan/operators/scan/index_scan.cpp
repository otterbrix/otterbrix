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
                           logical_plan::limit_t limit)
        : read_only_operator_t(resource, log, operator_type::index_scan)
        , table_oid_(table_oid)
        , key_(key)
        , value_(value)
        , compare_type_(compare_type)
        , preferred_index_type_(preferred_index_type)
        , limit_(limit) {}

    // --- Windowing core -------------------------------------------------------------------------
    // Run the ONE-SHOT index search and compute the OFFSET/LIMIT window [pos_=start, end_) over the
    // matched ids. source_next calls this exactly once (the first call), so the search + windowing
    // logic lives in ONE place.
    actor_zeta::unique_future<void> index_scan::open_index_window(pipeline::context_t* ctx) {
        if (ctx->index_address == actor_zeta::address_t::empty_address()) {
            // No index service — empty window (no matched ids).
            pos_ = 0;
            end_ = 0;
            co_return;
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
                                               ctx->session_tz)
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
                                               ctx->session_tz);
        row_ids_vec_ = co_await std::move(sf);

        // Apply offset and limit to compute the [pos_, end_) window over the matched ids.
        const size_t total = row_ids_vec_.size();
        const size_t offset_val = static_cast<size_t>(std::max(int64_t{0}, limit_.offset()));
        const size_t start = std::min(offset_val, total);
        const size_t available = total - start;
        const int64_t limit_val = limit_.limit();
        const size_t count = (limit_val >= 0) ? std::min(available, static_cast<size_t>(limit_val)) : available;
        pos_ = start;
        end_ = start + count;
        co_return;
    }

    // Fetch the whole matched window [pos_, end_) in ONE storage_fetch. The disk agent windows the
    // request into ≤ DEFAULT_VECTOR_CAPACITY chunks and stamps each chunk's absolute row_ids (so a
    // downstream DELETE/UPDATE/index sees the right rows), returning them as a vector that source_next
    // buffers. An empty window (or an OID this agent does not own) yields an empty vector.
    actor_zeta::unique_future<std::pmr::vector<vector::data_chunk_t>>
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
                                         count);
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
            co_await open_index_window(ctx);
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
            batch_ = co_await fetch_matched_window(ctx);
            batch_pos_ = 0;
        }

        // Emit the next buffered chunk.
        if (batch_pos_ < batch_.size()) {
            emitted_any_ = true;
            co_return core::result_wrapper_t<vector::data_chunk_t>(std::move(batch_[batch_pos_++]));
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
