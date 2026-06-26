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

        // The streaming executor drives await unconditionally for an async-finalize
        // sink, so guard the empty case here (no rows to append — e.g. an
        // INSERT...SELECT whose scan returned nothing).
        if (!output_ || output_->size() == 0) {
            mark_executed();
            co_return;
        }
        // Snapshot the to-be-inserted rows for a parent fk_check / check_constraint
        // BEFORE output_ is replaced with the RETURNING / affected-count chunk. The
        // streaming async-finalize drive runs this DML's await first, then the
        // constraint's — by which point output_ no longer holds the input rows. The
        // intrusive_ptr alias is cheap; the constraint only reads it.
        constraint_input_ = output_;
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, ctx->session_tz, table_oid_};

        auto input = output_;

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
        if (components::catalog::is_catalog_table(table_oid_)) {
            for (auto& out_chunk : input->chunks()) {
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
            // DDL is not row-returning: leave no output so the cursor reports 0
            // affected rows. pg_catalog_appends was pushed above.
            set_output(nullptr);
            mark_executed();
            co_return;
        }

        const bool mirror_index = ctx->index_address != actor_zeta::address_t::empty_address();

        auto copy_of = [this](const data_chunk_t& src) {
            data_chunk_t dst(resource_, src.types(), src.size());
            src.copy(dst, 0);
            return dst;
        };

        // Build the whole batch up front: storage_append consumes its copy (schema
        // adoption / _id dedup mutate it), while index needs the submitted rows intact.
        // WAL is now written WAL-FIRST by the disk agent inside storage_append (the agent
        // preprocesses, allocates start_row, writes PHYSICAL_INSERT, then materializes —
        // mailbox-atomic), so the operator no longer issues its own WAL record. The chunks
        // append sequentially, so the per-chunk segments stay contiguous and coalesce into
        // one [start_row, start_row + total_count) range.
        chunks_vector_t append_data(resource_);
        chunks_vector_t idx_chunks(resource_);
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
            if (mirror_index) {
                idx_chunks.emplace_back(copy_of(out_chunk));
            }
        }

        if (append_data.empty()) {
            set_output(nullptr);
            mark_executed();
            co_return;
        }

        // 1. storage_append — WAL-FIRST canonical append (batched, handles schema
        //    adoption + _id dedup). The disk agent owns the WAL write; the reply carries
        //    any write_conflict / out_of_memory as a value, surfaced as an error cursor so
        //    the txn aborts gracefully.
        auto [_a, af] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::storage_append,
                                         exec_ctx,
                                         table_oid_,
                                         std::move(append_data));
        // The append reply carries any write_conflict / out_of_memory the table-layer append
        // chain surfaced; surface it as a clean error cursor (the executor turns has_error()
        // into an error cursor) so the txn aborts gracefully.
        auto append_result = co_await std::move(af);
        if (append_result.has_error()) {
            set_error(append_result.error());
            mark_failed();
            co_return;
        }
        auto [start_row, total_count] = append_result.value();

        if (total_count == 0) {
            // Nothing inserted across all chunks (e.g. every row a duplicate _id).
            set_output(nullptr);
            mark_executed();
            co_return;
        }

        // 2. Mirror to index (txn-aware) — one batched send.
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

        // 3. Record swap-info on context for executor's commit-side block.
        ctx->dml_append_row_start = static_cast<int64_t>(start_row);
        ctx->dml_append_row_count = total_count;
        ctx->dml_table_oid = table_oid_;

        // 4. Build the result chunk.
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
