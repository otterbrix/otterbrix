#include "operator_insert.hpp"

#include <atomic>

#include "dml_util.hpp"

#include <algorithm>
#include <components/context/context.hpp>
#include <components/context/execution_context.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>

namespace components::operators {

#ifdef DEV_MODE
    namespace {
        std::atomic<uint64_t> g_insert_index_mirror_sends{0};
    } // namespace
    uint64_t insert_index_mirror_sends() noexcept {
        return g_insert_index_mirror_sends.load(std::memory_order_relaxed);
    }
    void reset_insert_index_mirror_sends() noexcept { g_insert_index_mirror_sends.store(0, std::memory_order_relaxed); }
#endif

    operator_insert::operator_insert(std::pmr::memory_resource* resource,
                                     log_t log,
                                     catalog::oid_t table_oid,
                                     std::pmr::vector<projected_column_t> returning)
        : read_write_operator_t(resource, log, operator_type::insert)
        , table_oid_(table_oid)
        , returning_(std::move(returning)) {}

    core::error_t
    operator_insert::push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& /*out*/) {
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
            // Rename each column to the target it lands in (the append routes by name) and
            // convert it to the stored type. The cast runs here, upstream of storage_append,
            // so the WAL — written from the chunk handed to it — holds stored types.
            const uint64_t bound = std::min<uint64_t>(input.column_count(), column_bindings_.size());
            for (uint64_t i = 0; i < bound; ++i) {
                const auto& binding = column_bindings_[i];
                if (!binding.cast) {
                    input.data[i].set_type_alias(std::string(binding.target_name));
                    continue;
                }
                auto target_type = binding.target_type;
                target_type.set_alias(std::string(binding.target_name));
                vector::vector_t casted(resource_, target_type, input.size());
                auto error =
                    binding.cast(casts::cast_kind::cast, input.data[i], &casted, ctx->execution_context, input.size());
                if (error.contains_error()) {
                    return error;
                }
                input.data[i] = std::move(casted);
            }
            // DEFAULT expansion, ABOVE the journal. Every table column the statement omitted is appended here
            // carrying the value the catalog says it defaults to (or a typed NULL when it has none). Downstream —
            // storage_append, the PHYSICAL_INSERT record, and the constraint operators reading the written-row
            // snapshot — all see one full-width row, so nothing has to re-derive what an absent column "would
            // have" become and nothing can disagree about it.
            //
            // EXPLICITLY RELEASED, as PostgreSQL releases CREATE TABLE AS, matviews and catalog inserts from its
            // rewriter: a catalog-table insert is a ready-made pg_catalog tuple from ddl_metadata_builder and skips
            // the whole user preprocess (see await_async_and_resume). operator_create_matview and operator_vacuum
            // never reach this operator at all; their fill list is empty anyway, and the guard states the decision
            // rather than leaving it to that.
            if (!fill_list_.empty() && !components::catalog::is_catalog_table(table_oid_)) {
                const uint64_t rows = input.size();
                const uint64_t capacity = input.capacity();
                input.data.reserve(input.data.size() + fill_list_.size());
                for (const auto& column : fill_list_) {
                    auto column_type = column.type;
                    column_type.set_alias(std::string{column.name.c_str()});
                    // RULE 1. ONE logical_value_t per column per chunk: build the vector
                    // as a CONSTANT over the plan-node value and let flatten() broadcast
                    // it typed, rather than set_value(row, default) once PER ROW — a
                    // logical_value_t round trip on the row path.
                    if (column.value.is_null()) {
                        vector::vector_t filled(resource_, column_type, capacity);
                        filled.validity().set_all_invalid(rows);
                        input.data.emplace_back(std::move(filled));
                        continue;
                    }
                    vector::vector_t filled(resource_, column.value, capacity);
                    filled.flatten(rows);
                    filled.set_type_alias(std::string{column.name.c_str()});
                    input.data.emplace_back(std::move(filled));
                }
            }
            output_->append_chunk(std::move(input));
        }
        return core::error_t::no_error();
    }

    actor_zeta::unique_future<void> operator_insert::await_async_and_resume(pipeline::context_t* ctx) {
        using components::vector::data_chunk_t;

        // INCREMENTAL drive: the executor calls this once per "buffer full" during the pump
        // (dml_flush_is_final==false) and once at finalize (==true). Each call flushes the currently-buffered
        // slice (if any) and ONLY the final call materializes the accumulated result into output_. With
        // threshold==0 the executor makes a single is_final==true call, so this collapses to one flush + finalize.
        const bool is_final = ctx->dml_flush_is_final;
        components::execution_context_t exec_ctx{ctx->session,
                                                 ctx->txn,
                                                 ctx->execution_context.timezone_offset,
                                                 table_oid_};

        // Catalog-table insert (DDL pg_catalog row): delegate to the WAL-first append_pg_catalog_row instead of
        // the user append-first path. The row is a ready-made pg_catalog tuple built by ddl_metadata_builder
        // (atttypid / attoid already allocated), so the user preprocess — NOT-NULL checks, the DEFAULT fill in
        // push(), type promotion, RETURNING readback — is skipped; append_pg_catalog_row runs the lighter catalog
        // preprocess on the agent. The returned range MUST land in ctx->pg_catalog_appends (NOT dml_*):
        // operator_commit_transaction publishes catalog rows via storage_publish_commits keyed off that vector,
        // and pushing to dml_* would silently leave the row unpublished. build_*_writes emits 1-row chunks (one
        // node per row), so each chunk is sent as a single catalog row. buffered_rows() returns 0 for catalog
        // tables, so the mid-pump flush gate never fires here: this branch only runs on the final drive.
        if (components::catalog::is_catalog_table(table_oid_)) {
            if (output_ && output_->size() > 0) {
                for (auto& out_chunk : output_->chunks()) {
                    if (out_chunk.size() == 0) {
                        continue;
                    }
                    data_chunk_t row(resource_, out_chunk.types(), out_chunk.size());
                    out_chunk.copy(row, 0);
                    auto [_c, cf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                                &services::disk::manager_disk_t::append_pg_catalog_row,
                                                                exec_ctx,
                                                                table_oid_,
                                                                std::move(row));
                    auto rng_r = co_await std::move(cf);
                    if (rng_r.has_error()) {
                        // A catalog INSERT that could not write its row is a failed statement,
                        // not an insert of zero rows.
                        set_error(rng_r.error());
                        mark_failed();
                        co_return;
                    }
                    if (rng_r.value().count > 0) {
                        ctx->pg_catalog_appends.push_back(std::move(rng_r.value()));
                    }
                }
            }
            // DDL is not row-returning: leave no output so the cursor reports 0
            // affected rows. pg_catalog_appends was pushed above.
            set_output(nullptr);
            mark_executed();
            co_return;
        }

        // "An index manager exists" is true for every table — register_collection creates an
        // engine per table regardless of whether an index was ever declared — so that test
        // alone made every INSERT copy its chunk a second time and ship it to a manager that
        // then walked the rows against an empty index list. The real question is whether the
        // TABLE has an index, which enrich stamps on the plan node.
        const bool mirror_index = table_has_indexes_ && ctx->index_address != actor_zeta::address_t::empty_address();

        // ONE flush of the currently-buffered slice. Wrapped in a NAMED coroutine lambda so the DIVERGENT
        // storage op (append + optional index mirror + optional RETURNING readback) is co_awaited in one place
        // and hands a normalized flush_outcome_t to record_flush() for the COMMON bookkeeping. `op` is a named
        // local awaited immediately, so its closure (captures by reference) outlives the awaited coroutine.
        if (output_ && output_->size() > 0) {
            auto op = [&]([[maybe_unused]] std::pmr::memory_resource* res)
                -> actor_zeta::unique_future<dml_detail::flush_outcome_t> {
                auto copy_of = [this](const data_chunk_t& src) {
                    data_chunk_t dst(resource_, src.types(), src.size());
                    src.copy(dst, 0);
                    return dst;
                };

                // Build the whole slice up front: storage_append consumes its copy (schema adoption / column
                // expansion mutate it), while index needs the submitted rows intact. WAL is written WAL-FIRST by
                // the disk agent inside storage_append (preprocess, allocate start_row, write PHYSICAL_INSERT,
                // materialize — mailbox-atomic), so the operator issues no WAL record. Chunks append sequentially,
                // so the per-chunk segments coalesce into one [start, start + count) range.
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
                // schema adoption + column expansion). The reply carries any
                // write_conflict / out_of_memory as a value.
                auto [_a, af] = actor_zeta::otterbrix::send(ctx->disk_address,
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
                // append materialized nothing (count==0, e.g. a not-owned-oid no-op).
                if (mirror_index && count > 0) {
#ifdef DEV_MODE
                    g_insert_index_mirror_sends.fetch_add(1, std::memory_order_relaxed);
#endif
                    auto [_ix, ixf] = actor_zeta::otterbrix::send(ctx->index_address,
                                                                  &services::index::manager_index_t::insert_rows,
                                                                  exec_ctx,
                                                                  table_oid_,
                                                                  std::move(idx_chunks),
                                                                  start_row,
                                                                  count);
                    auto index_error = co_await std::move(ixf);
                    if (index_error.contains_error()) {
                        // The rows are in the table but not in the index. Reporting success here
                        // would leave the two disagreeing with nobody the wiser.
                        co_return dml_detail::flush_outcome_t{std::move(index_error), false, 0, 0};
                    }
                }

                if (returning_.empty()) {
                    // No RETURNING: tally the affected-row count; the count chunks
                    // are built once on the final drive.
                    affected_rows_ += count;
                } else if (count > 0) {
                    // RETURNING: read the just-appended range back from storage so anything the storage layer
                    // itself derives is present in the projected rows. (DEFAULTs no longer need this — push()
                    // expands them into the submitted chunk — but generated columns will.) The read returns
                    // <=DEFAULT_VECTOR_CAPACITY chunks; project each into the cross-flush accumulator.
                    //
                    // This is a POINT read by row id, not a positional window. Appends within one txn are
                    // contiguous, so storage_append's reply range [start_row, start_row + count) IS the set of ids
                    // just written — name them explicitly instead of asking for whatever currently sits at those
                    // positions.
                    vector::vector_t row_ids(resource_, types::logical_type::BIGINT, count);
                    auto* ids = row_ids.data<int64_t>();
                    for (uint64_t i = 0; i < count; i++) {
                        ids[i] = static_cast<int64_t>(start_row + i);
                    }
                    auto [_s, sf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                                &services::disk::manager_disk_t::storage_fetch,
                                                                ctx->session,
                                                                table_oid_,
                                                                std::move(row_ids),
                                                                count,
                                                                std::vector<size_t>{},
                                                                // This txn's OWN just-appended, still
                                                                // uncommitted rows. It sees them by the MVCC
                                                                // self-write rule, not by the positions
                                                                // happening to line up.
                                                                ctx->txn,
                                                                components::table::fetch_visibility_t::SNAPSHOT,
                                                                // Reads back exactly the rows just
                                                                // appended — nothing to cap.
                                                                /*limit=*/int64_t{-1});
                    auto segments_r = co_await std::move(sf);
                    if (segments_r.has_error()) {
                        // A failed re-read (buffer-pool OOM / corrupt overflow block) must fail the statement
                        // — RETURNING built from silently empty cells is the data loss this channel exists to
                        // stop. The rows ARE already appended (WAL-first): carry the range with the error so
                        // record_flush registers it and the failed-statement abort tail can revert the append.
                        co_return dml_detail::flush_outcome_t{segments_r.error(),
                                                              true,
                                                              static_cast<int64_t>(start_row),
                                                              count};
                    }
                    auto segments = std::move(segments_r.value());
                    for (auto& seg : segments) {
                        if (seg.size() == 0) {
                            continue;
                        }
                        auto proj = evaluate_projection(resource_,
                                                        returning_,
                                                        &seg,
                                                        ctx->parameters,
                                                        ctx->execution_context,
                                                        &returning_graph_);
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
                set_output(make_operator_data(resource_,
                                              dml_detail::make_affected_count_chunks(resource_, affected_rows_, {})));
            } else {
                set_output(nullptr);
            }
        } else {
            set_output(make_operator_data(resource_, std::move(returning_accum_)));
        }
        mark_executed();
    }

} // namespace components::operators
