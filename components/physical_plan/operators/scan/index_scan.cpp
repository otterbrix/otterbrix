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

    // --- Shared windowing core (Rule 6) ---------------------------------------------------------
    // Run the ONE-SHOT index search and compute the OFFSET/LIMIT window [pos_=start, end_) over the
    // matched ids. Both await_async_and_resume (materialized) and source_next (per-window) call this
    // exactly once, so the search + windowing logic lives in ONE place.
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

    // Fetch the absolute-row-id window [start, start+count) as ONE chunk via the existing per-row-id
    // storage_fetch handler. Caller guarantees count > 0. Null reply ⇒ schema'd 0-row chunk.
    actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
    index_scan::fetch_window(pipeline::context_t* ctx, size_t start, size_t count) {
        // Build the row_ids vector for this window (absolute ids — the agent stamps them onto the
        // fetched chunk's row_ids so a downstream DELETE/UPDATE/index sees the right rows).
        vector::vector_t row_ids(resource_, types::logical_type::BIGINT, count);
        std::memcpy(row_ids.data(), row_ids_vec_.data() + start, count * sizeof(int64_t));

        auto [_f, ff] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::storage_fetch,
                                         ctx->session,
                                         table_oid_,
                                         std::move(row_ids),
                                         count);
        auto data = co_await std::move(ff);
        if (data) {
            co_return core::result_wrapper_t<vector::data_chunk_t>(std::move(*data));
        }
        co_return core::result_wrapper_t<vector::data_chunk_t>(co_await make_empty_chunk(ctx));
    }

    actor_zeta::unique_future<vector::data_chunk_t> index_scan::make_empty_chunk(pipeline::context_t* ctx) {
        auto [_t, tf] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::storage_types,
                                         ctx->session,
                                         table_oid_);
        auto types = co_await std::move(tf);
        co_return vector::data_chunk_t{resource_, types, 0};
    }

    actor_zeta::unique_future<void> index_scan::await_async_and_resume(pipeline::context_t* ctx) {
        if (log_.is_valid()) {
            trace(log(), "index_scan::await_async_and_resume on oid={}", static_cast<unsigned>(table_oid_));
        }

        // ONE-SHOT search + OFFSET/LIMIT window (shared core).
        co_await open_index_window(ctx);

        // Materialized entry: loop ALL windows of the [pos_, end_) range into output_, so a legacy
        // parent / build-side consumer gets the full result. Windowing ≤ DEFAULT_VECTOR_CAPACITY
        // keeps each storage_fetch bounded; split_chunk_into_batches is a no-op (already ≤ cap).
        if (pos_ >= end_) {
            output_ = make_operator_data(resource_, co_await make_empty_chunk(ctx));
            mark_executed();
            co_return;
        }

        chunks_vector_t batches{resource_};
        while (pos_ < end_) {
            const size_t count = std::min<size_t>(vector::DEFAULT_VECTOR_CAPACITY, end_ - pos_);
            auto fetched = co_await fetch_window(ctx, pos_, count);
            if (fetched.has_error()) {
                set_error(fetched.error());
                mark_failed();
                co_return;
            }
            pos_ += count;
            batches.push_back(std::move(fetched.value()));
        }
        output_ = make_operator_data(resource_, std::move(batches));
        mark_executed();
        co_return;
    }

    // --- Push-based streaming pipeline source (WINDOWED point-fetch) ----------------------------
    // FIRST call: open_index_window (await #1: the one-shot index search) + compute [pos_, end_).
    // EACH call: fetch the next ≤ DEFAULT_VECTOR_CAPACITY window [pos_, ..) (await #2: storage_fetch)
    //   and return it as ONE chunk; advance pos_.
    // DRAIN: pos_ >= end_ ⇒ if nothing was emitted, ONE schema'd 0-row guard (storage_types await),
    //   else the 0-column drain sentinel.
    // Each call does at most ONE cross-actor fetch await; the N awaits are sequential across calls in
    // this nested operator coroutine (driven by execute_pipeline), so the single-slot awaited
    // continuation is republished+cleared between awaits — no lost-wakeup (same shape as
    // await_async_and_resume's sequential awaits).
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

        // DRAIN: the window is exhausted (or never had rows).
        if (pos_ >= end_) {
            drained_ = true;
            if (!emitted_any_) {
                // ONE schema'd 0-row guard so a scalar aggregate emits COUNT=0 and an OUTER join
                // NULL-pads, then the 0-column sentinel next call.
                emitted_any_ = true;
                co_return core::result_wrapper_t<vector::data_chunk_t>(
                    vector::data_chunk_t{resource_, guard_types_, 0});
            }
            co_return core::result_wrapper_t<vector::data_chunk_t>(
                vector::data_chunk_t{resource_, std::pmr::vector<types::complex_logical_type>{resource_}, 0});
        }

        // Fetch the next window (≤ DEFAULT_VECTOR_CAPACITY ids) as ONE chunk.
        const size_t count = std::min<size_t>(vector::DEFAULT_VECTOR_CAPACITY, end_ - pos_);
        auto fetched = co_await fetch_window(ctx, pos_, count);
        if (fetched.has_error()) {
            set_error(fetched.error());
            mark_failed();
            co_return core::result_wrapper_t<vector::data_chunk_t>(fetched.error());
        }
        pos_ += count;
        emitted_any_ = true;
        co_return std::move(fetched);
    }

} // namespace components::operators
