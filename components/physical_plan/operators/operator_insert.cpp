#include "operator_insert.hpp"

#include "dml_util.hpp"

#include <algorithm>
#include <components/context/context.hpp>
#include <components/context/execution_context.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>

namespace components::operators {

    operator_insert::operator_insert(std::pmr::memory_resource* resource,
                                     log_t log,
                                     catalog::oid_t table_oid,
                                     std::pmr::vector<select_column_t> returning)
        : read_write_operator_t(resource, log, operator_type::insert)
        , table_oid_(table_oid)
        , returning_(std::move(returning)) {}

    core::error_t
    operator_insert::push(pipeline::context_t* /*ctx*/, vector::data_chunk_t&& input, chunks_vector_t& /*out*/) {
        // STREAMING DML SINK: fold each scan batch into a bounded accumulator and
        // emit nothing (out stays empty). await_async_and_resume iterates
        // output_->chunks() (the accumulated batches) and runs the single
        // WAL->storage->index commit — filled one batch at a time instead of
        // adopting left_->output() wholesale. modified_ is initialized here too.
        if (!output_) {
            output_ = make_operator_data(resource_, chunks_vector_t{resource_});
            modified_ = make_operator_write_data(resource());
        }
        if (input.size() > 0) {
            output_->append_chunk(std::move(input));
        }
        return core::error_t::no_error();
    }

    actor_zeta::unique_future<void> operator_insert::await_async_and_resume(pipeline::context_t* ctx) {
        using components::vector::data_chunk_t;

        // 3b-B INCREMENTAL drive: the executor calls this once per "buffer full"
        // during the pump (dml_flush_is_final==false) and once at finalize
        // (==true). Each call flushes the currently-buffered slice (if any) and
        // ONLY the final call materializes the accumulated result into output_.
        // With threshold==0 the executor makes a single is_final==true call, so
        // this collapses to one flush + finalize (behavior-preserving).
        const bool is_final = ctx->dml_flush_is_final;
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, ctx->session_tz, table_oid_};

        // Catalog-table insert (DDL pg_catalog row): delegate to the WAL-first
        // append_pg_catalog_row instead of the user append-first path. The row is
        // a ready-made pg_catalog tuple built by ddl_metadata_builder (atttypid /
        // attoid already allocated), so the user preprocess — _id dedup, NOT-NULL
        // checks, DEFAULT fill, type promotion, RETURNING readback — is skipped;
        // append_pg_catalog_row runs the lighter catalog preprocess on the agent.
        // The returned range MUST land in ctx->pg_catalog_appends (NOT dml_*):
        // operator_commit_transaction publishes catalog rows via
        // storage_publish_commits keyed off that vector — pushing to dml_* would
        // silently leave the row unpublished. build_*_writes emits 1-row chunks
        // (one node per row), so each chunk is sent as a single catalog row.
        // buffered_rows() returns 0 for catalog tables, so the mid-pump flush gate
        // never fires here: this single-shot branch only runs on the final drive.
        if (components::catalog::is_catalog_table(table_oid_)) {
            if (output_ && output_->size() > 0) {
                for (auto& out_chunk : output_->chunks()) {
                    if (out_chunk.size() == 0) {
                        continue;
                    }
                    data_chunk_t row(resource_, out_chunk.types(), out_chunk.size());
                    out_chunk.copy(row, 0);
                    auto [_c, cf] = actor_zeta::send(ctx->disk_address,
                                                     &services::disk::manager_disk_t::append_pg_catalog_row,
                                                     exec_ctx,
                                                     table_oid_,
                                                     std::move(row));
                    auto rng = co_await std::move(cf);
                    if (rng.count > 0) {
                        ctx->pg_catalog_appends.push_back(std::move(rng));
                    }
                }
            }
            // DDL is not row-returning: leave no output so the cursor reports 0
            // affected rows. pg_catalog_appends was pushed above.
            set_output(nullptr);
            mark_executed();
            co_return;
        }

        const bool mirror_index = ctx->index_address != actor_zeta::address_t::empty_address();

        // ONE flush of the currently-buffered slice. Wrapped in a NAMED coroutine
        // lambda so the DIVERGENT storage op (append + optional index mirror +
        // optional RETURNING readback) is co_awaited in one place and hands a
        // normalized flush_outcome_t to record_flush() for the COMMON bookkeeping.
        // `op` is a named local awaited immediately, so its closure (captures by
        // reference) outlives the awaited coroutine.
        if (output_ && output_->size() > 0) {
            auto op = [&]([[maybe_unused]] std::pmr::memory_resource* res)
                -> actor_zeta::unique_future<dml_detail::flush_outcome_t> {
                auto copy_of = [this](const data_chunk_t& src) {
                    data_chunk_t dst(resource_, src.types(), src.size());
                    src.copy(dst, 0);
                    return dst;
                };

                // Build the whole slice up front: storage_append consumes its copy
                // (schema adoption / _id dedup mutate it), while index needs the
                // submitted rows intact. WAL is written WAL-FIRST by the disk agent
                // inside storage_append (preprocess, allocate start_row, write
                // PHYSICAL_INSERT, materialize — mailbox-atomic), so the operator
                // issues no WAL record. Chunks append sequentially, so the per-chunk
                // segments coalesce into one [start, start + count) range.
                chunks_vector_t append_data(resource_);
                chunks_vector_t idx_chunks(resource_);
                for (auto& out_chunk : output_->chunks()) {
                    if (out_chunk.size() == 0) {
                        continue;
                    }
                    append_data.emplace_back(copy_of(out_chunk));
                    if (mirror_index) {
                        idx_chunks.emplace_back(copy_of(out_chunk));
                    }
                }
                if (append_data.empty()) {
                    co_return dml_detail::flush_outcome_t{};
                }

                // storage_append — WAL-FIRST canonical append (batched, handles
                // schema adoption + _id dedup). The reply carries any
                // write_conflict / out_of_memory as a value.
                auto [_a, af] = actor_zeta::send(ctx->disk_address,
                                                 &services::disk::manager_disk_t::storage_append,
                                                 exec_ctx,
                                                 table_oid_,
                                                 std::move(append_data));
                auto append_result = co_await std::move(af);
                if (append_result.has_error()) {
                    co_return dml_detail::flush_outcome_t{append_result.error()};
                }
                auto [start_row, count] = append_result.value();

                // Mirror to index (txn-aware) — one batched send. Skipped when the
                // append dropped every row (count==0, e.g. all duplicate _id).
                if (mirror_index && count > 0) {
                    auto [_ix, ixf] = actor_zeta::send(ctx->index_address,
                                                       &services::index::manager_index_t::insert_rows,
                                                       exec_ctx,
                                                       table_oid_,
                                                       std::move(idx_chunks),
                                                       start_row,
                                                       count);
                    co_await std::move(ixf);
                }

                if (returning_.empty()) {
                    // No RETURNING: tally the affected-row count; the count chunks
                    // are built once on the final drive.
                    affected_rows_ += count;
                } else if (count > 0) {
                    // RETURNING: read the just-appended range back from storage so
                    // DB-applied DEFAULTs and generated columns (filled on
                    // storage_append's own copy, not the submitted chunks) are
                    // present in the projected rows. The read returns
                    // ≤DEFAULT_VECTOR_CAPACITY chunks; project each into the
                    // cross-flush accumulator.
                    auto [_s, sf] = actor_zeta::send(ctx->disk_address,
                                                     &services::disk::manager_disk_t::storage_scan_segment,
                                                     ctx->session,
                                                     table_oid_,
                                                     static_cast<int64_t>(start_row),
                                                     count);
                    auto segments = co_await std::move(sf);
                    for (auto& seg : segments) {
                        if (seg.size() == 0) {
                            continue;
                        }
                        auto proj =
                            evaluate_projection(resource_, returning_, &seg, ctx->parameters, ctx->session_tz);
                        if (proj.has_error()) {
                            // The rows ARE already appended (WAL-first): carry the range
                            // with the error so record_flush registers it and the failed-
                            // statement abort tail can revert the physical append.
                            co_return dml_detail::flush_outcome_t{proj.error(),
                                                                  true,
                                                                  static_cast<int64_t>(start_row),
                                                                  count};
                        }
                        returning_accum_.emplace_back(std::move(proj.value()));
                    }
                }

                co_return dml_detail::flush_outcome_t{core::error_t::no_error(),
                                                      true,
                                                      static_cast<int64_t>(start_row),
                                                      count};
            };

            auto outcome = co_await op(resource_);
            // record_flush accumulates the constraint copy from the JUST-FLUSHED
            // rows, so pass output_->chunks() BEFORE clearing them below.
            auto err = dml_detail::record_flush(ctx,
                                                resource_,
                                                table_oid_,
                                                outcome,
                                                ctx->dml_has_parent_constraint,
                                                constraint_input_,
                                                output_->chunks());
            if (err.contains_error()) {
                set_error(err);
                mark_failed();
                co_return;
            }
            // Drop the flushed slice so the buffer stays bounded across flushes.
            output_->chunks().clear();
        }

        // Mid-flush call: leave output_/state untouched — the executor will drive
        // us again (eventually with is_final==true to materialize the result).
        if (!is_final) {
            co_return;
        }

        // FINAL drive: materialize the accumulated result from the cross-flush
        // accumulators (this also finalizes an empty buffer, e.g. an
        // INSERT...SELECT whose scan produced nothing).
        if (returning_.empty()) {
            if (affected_rows_ != 0) {
                // No RETURNING: emit column-less chunks whose cardinalities sum to the
                // affected-row count (the cursor totals chunk sizes).
                set_output(make_operator_data(
                    resource_, dml_detail::make_affected_count_chunks(resource_, affected_rows_, {})));
            } else {
                set_output(nullptr);
            }
        } else {
            set_output(make_operator_data(resource_, std::move(returning_accum_)));
        }
        mark_executed();
    }

} // namespace components::operators
