#include "operator_insert.hpp"

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

    void operator_insert::accept_resolved_metadata(resolved_table_metadata_t metadata) {
        // Capture metadata so await_async_and_resume can build a
        // chunk_position -> table_position translation before storage_append.
        // Adopt table_oid_ from the resolver when this operator was constructed
        // without one (oid-only DML routing typically passes the oid through
        // node_insert, but the resolve-sibling form is the alternative).
        if (table_oid_ == catalog::INVALID_OID && metadata.table_oid != catalog::INVALID_OID) {
            table_oid_ = metadata.table_oid;
        }
        resolved_metadata_ = std::move(metadata);
    }

    void operator_insert::on_execute_impl(pipeline::context_t* /*pipeline_context*/) {
        if (left_ && left_->output()) {
            output_ = left_->output();
            modified_ = operators::make_operator_write_data(resource());
        }
        if (output_ && output_->size() > 0 && table_oid_ != catalog::INVALID_OID) {
            async_wait();
        }
    }

    actor_zeta::unique_future<void> operator_insert::await_async_and_resume(pipeline::context_t* ctx) {
        using components::vector::data_chunk_t;

        if (!output_) {
            mark_executed();
            co_return;
        }
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, ctx->session_tz, table_oid_};
        // database_oid selects the WAL worker. Hardcoded to main_database
        // until catalog_resolve_database_t populates ctx->database_oid.
        constexpr auto db_oid = components::catalog::well_known_oid::main_database;

        // Hold the input chunks alive while we ship them; set_output below rebinds output_.
        auto input = output_;

        auto copy_of = [this](const data_chunk_t& src) {
            data_chunk_t dst(resource_, src.types(), src.size());
            src.copy(dst, 0);
            return dst;
        };

        // Build the whole batch up front: storage_append consumes its copy (schema
        // adoption / _id dedup mutate it), while WAL and index need the submitted rows
        // intact. The chunks append sequentially, so the per-chunk segments stay
        // contiguous and coalesce into one [start_row, start_row + total_count) range.
        chunks_vector_t append_data(resource_);
        chunks_vector_t wal_chunks(resource_);
        chunks_vector_t idx_chunks(resource_);
        const bool mirror_index = ctx->index_address != actor_zeta::address_t::empty_address();
        for (auto& out_chunk : input->chunks()) {
            if (out_chunk.size() == 0) {
                continue;
            }
            // When a resolver sibling supplied catalog metadata, compute a
            // chunk_position -> table_position translation via alias matching.
            // The disk-actor's storage_append already aligns by alias (with
            // positional fallback), so the translation is built only to surface
            // resolver/data drift at trace level — the wiring hook for a future
            // storage_append(...,key_translation) signature change.
            if (resolved_metadata_.has_value() && out_chunk.column_count() > 0) {
                auto translation = build_column_key_translation(*resolved_metadata_, out_chunk);
                for (std::size_t i = 0; i < translation.size(); ++i) {
                    if (translation[i] < 0 && out_chunk.data[i].type().has_alias()) {
                        trace(log_,
                              "operator_insert: resolved metadata has no column matching chunk alias '{}'",
                              std::string(out_chunk.data[i].type().alias()));
                    }
                }
            }
            append_data.emplace_back(copy_of(out_chunk));
            wal_chunks.emplace_back(copy_of(out_chunk));
            if (mirror_index) {
                idx_chunks.emplace_back(copy_of(out_chunk));
            }
        }

        if (append_data.empty()) {
            set_output(nullptr);
            mark_executed();
            co_return;
        }

        // 1. storage_append (handles schema adoption, _id dedup) — one batched send.
        auto [_a, af] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::storage_append,
                                         exec_ctx,
                                         table_oid_,
                                         std::move(append_data));
        auto [start_row, total_count] = co_await std::move(af);

        if (total_count == 0) {
            // Nothing inserted across all chunks (e.g. every row a duplicate _id).
            set_output(nullptr);
            mark_executed();
            co_return;
        }

        // 2. WAL physical_insert: one record for the whole range (flush is fire-and-forget).
        if (ctx->wal_address != actor_zeta::address_t::empty_address()) {
            auto [_w, wf] = actor_zeta::send(ctx->wal_address,
                                             &services::wal::manager_wal_replicate_t::write_physical_insert,
                                             ctx->session,
                                             table_oid_,
                                             std::move(wal_chunks),
                                             start_row,
                                             total_count,
                                             ctx->txn.transaction_id,
                                             db_oid);
            auto wal_id = co_await std::move(wf);
            auto [_d, df] =
                actor_zeta::send(ctx->disk_address, &services::disk::manager_disk_t::flush, ctx->session, wal_id);
            ctx->add_pending_disk_future(std::move(df));
        }

        // 3. Mirror to index (txn-aware) — one batched send.
        if (mirror_index) {
            auto [_ix, ixf] = actor_zeta::send(ctx->index_address,
                                               &services::index::manager_index_t::insert_rows,
                                               exec_ctx,
                                               table_oid_,
                                               std::move(idx_chunks),
                                               start_row,
                                               total_count);
            co_await std::move(ixf);
        }

        // Record swap-info on context for executor's commit-side block.
        ctx->dml_append_row_start = static_cast<int64_t>(start_row);
        ctx->dml_append_row_count = total_count;
        ctx->dml_table_oid = table_oid_;

        if (returning_.empty()) {
            // No RETURNING: emit column-less chunks whose cardinalities sum to the
            // affected-row count (the cursor totals chunk sizes). Split into
            // ≤DEFAULT_VECTOR_CAPACITY chunks so no oversized data_chunk_t is built.
            const uint64_t cap = vector::DEFAULT_VECTOR_CAPACITY;
            chunks_vector_t count_chunks(resource_);
            uint64_t remaining = total_count;
            while (remaining > 0) {
                const uint64_t batch = std::min<uint64_t>(cap, remaining);
                data_chunk_t res_chunk(resource_, {}, batch);
                res_chunk.set_cardinality(batch);
                count_chunks.emplace_back(std::move(res_chunk));
                remaining -= batch;
            }
            set_output(make_operator_data(resource_, std::move(count_chunks)));
            mark_executed();
            co_return;
        }

        // RETURNING: read the just-appended range back from storage so that DB-applied
        // DEFAULTs and generated columns (which storage_append fills on its own copy,
        // not on the submitted chunks) are present in the projected rows. The
        // projection's field paths were resolved against the full table schema, so the
        // canonical stored row is what lines up. The read returns the range as a vector
        // of ≤DEFAULT_VECTOR_CAPACITY chunks; project each.
        auto [_s, sf] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::storage_scan_segment,
                                         ctx->session,
                                         table_oid_,
                                         static_cast<int64_t>(start_row),
                                         total_count);
        auto segments = co_await std::move(sf);
        chunks_vector_t projected(resource_);
        for (auto& seg : segments) {
            if (seg.size() == 0) {
                continue;
            }
            auto proj = evaluate_projection(resource_, returning_, &seg, ctx->parameters, ctx->session_tz);
            if (proj.has_error()) {
                set_error(proj.error());
                mark_executed();
                co_return;
            }
            projected.emplace_back(std::move(proj.value()));
        }
        set_output(make_operator_data(resource_, std::move(projected)));
        mark_executed();
    }

} // namespace components::operators
