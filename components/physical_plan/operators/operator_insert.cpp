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
        auto& out_chunk = output_->data_chunk();
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, ctx->session_tz, table_oid_};

        // When a resolver sibling supplied catalog metadata, compute a
        // chunk_position -> table_position translation via alias matching.
        // The disk-actor's storage_append already aligns by alias (with
        // positional fallback), so the translation is built and stashed for
        // diagnostics today — the field is the wiring hook for a future
        // storage_append(...,key_translation) signature change. We log
        // mismatches at trace level so resolver/data drift is visible.
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

        // 1. Capture WAL data BEFORE storage_append moves the chunk.
        auto wal_data = std::make_unique<data_chunk_t>(resource_, out_chunk.types(), out_chunk.size());
        out_chunk.copy(*wal_data, 0);

        // 2. storage_append (handles schema adoption, _id dedup).
        auto data_copy = std::make_unique<data_chunk_t>(resource_, out_chunk.types(), out_chunk.size());
        out_chunk.copy(*data_copy, 0);
        auto [_a, af] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::storage_append,
                                         exec_ctx,
                                         table_oid_,
                                         std::move(data_copy));
        auto [start_row, actual_count] = co_await std::move(af);

        if (actual_count == 0) {
            // Nothing inserted (e.g. duplicate _id). Clear output and drop WAL.
            set_output(nullptr);
            mark_executed();
            co_return;
        }

        // 3. WAL physical_insert (synchronous; flush is fire-and-forget).
        if (ctx->wal_address != actor_zeta::address_t::empty_address()) {
            // database_oid selects the WAL worker. Hardcoded to main_database
            // until catalog_resolve_database_t populates ctx->database_oid.
            constexpr auto db_oid = components::catalog::well_known_oid::main_database;
            auto [_w, wf] = actor_zeta::send(ctx->wal_address,
                                             &services::wal::manager_wal_replicate_t::write_physical_insert,
                                             ctx->session,
                                             table_oid_,
                                             std::move(wal_data),
                                             static_cast<uint64_t>(start_row),
                                             actual_count,
                                             ctx->txn.transaction_id,
                                             db_oid);
            auto wal_id = co_await std::move(wf);
            auto [_d, df] =
                actor_zeta::send(ctx->disk_address, &services::disk::manager_disk_t::flush, ctx->session, wal_id);
            ctx->add_pending_disk_future(std::move(df));
        }

        // 4. Mirror to index (txn-aware).
        if (ctx->index_address != actor_zeta::address_t::empty_address()) {
            auto idx_data = std::make_unique<data_chunk_t>(resource_, out_chunk.types(), out_chunk.size());
            out_chunk.copy(*idx_data, 0);
            auto [_ix, ixf] = actor_zeta::send(ctx->index_address,
                                               &services::index::manager_index_t::insert_rows,
                                               exec_ctx,
                                               table_oid_,
                                               std::move(idx_data),
                                               static_cast<uint64_t>(start_row),
                                               actual_count);
            co_await std::move(ixf);
        }

        // 5. Record swap-info on context for executor's commit-side block.
        ctx->dml_append_row_start = static_cast<int64_t>(start_row);
        ctx->dml_append_row_count = actual_count;
        ctx->dml_table_oid = table_oid_;

        // 6. Build the result chunk.
        if (returning_.empty()) {
            // No RETURNING: emit a column-less chunk whose cardinality carries
            // the affected-row count for the cursor.
            data_chunk_t res_chunk(resource_, {}, actual_count);
            res_chunk.set_cardinality(actual_count);
            set_output(make_operator_data(resource_, std::move(res_chunk)));
            mark_executed();
            co_return;
        }

        // RETURNING: read the just-appended segment back from storage so that
        // DB-applied DEFAULTs and generated columns (which storage_append fills
        // on its own copy, not on out_chunk) are present in the projected rows.
        // The projection's field paths were resolved against the full table
        // schema, so a partial/reordered insert chunk would not line up — the
        // canonical stored row does. Read in DEFAULT_VECTOR_CAPACITY windows so
        // every projected chunk stays within the vector capacity bound.
        chunks_vector_t projected(resource_);
        for (uint64_t offset = 0; offset < actual_count; offset += vector::DEFAULT_VECTOR_CAPACITY) {
            const uint64_t window = std::min<uint64_t>(vector::DEFAULT_VECTOR_CAPACITY, actual_count - offset);
            auto [_s, sf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::storage_scan_segment,
                                             ctx->session,
                                             table_oid_,
                                             static_cast<int64_t>(start_row) + static_cast<int64_t>(offset),
                                             window);
            auto seg = co_await std::move(sf);
            if (!seg || seg->size() == 0) {
                continue;
            }
            auto proj = evaluate_projection(resource_, returning_, &*seg, ctx->parameters, ctx->session_tz);
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
