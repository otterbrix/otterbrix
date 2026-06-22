#include "operator_delete.hpp"
#include "predicates/predicate.hpp"
#include <components/vector/vector_operations.hpp>

#include <algorithm>
#include <components/context/context.hpp>
#include <components/context/execution_context.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>

namespace components::operators {

    operator_delete::operator_delete(std::pmr::memory_resource* resource,
                                     log_t log,
                                     components::catalog::oid_t table_oid,
                                     std::pmr::vector<select_column_t> returning,
                                     expressions::expression_ptr expr)
        : read_write_operator_t(resource, log, operator_type::remove)
        , table_oid_(table_oid)
        , expression_(std::move(expr))
        , returning_(std::move(returning)) {}

    operator_delete::operator_delete(std::pmr::memory_resource* resource,
                                     log_t log,
                                     components::catalog::oid_t catalog_table_oid,
                                     std::int64_t oid_col_idx,
                                     components::catalog::oid_t target_oid)
        : read_write_operator_t(resource, log, operator_type::remove)
        , table_oid_(catalog_table_oid)
        , returning_(resource)
        , oid_col_idx_(oid_col_idx)
        , target_oid_(target_oid) {}

    void operator_delete::accept_resolved_metadata(resolved_table_metadata_t metadata) {
        // See operator_insert for the contract.
        if (table_oid_ == components::catalog::INVALID_OID && metadata.table_oid != components::catalog::INVALID_OID) {
            table_oid_ = metadata.table_oid;
        }
        resolved_metadata_ = std::move(metadata);
    }

    void operator_delete::on_execute_impl(pipeline::context_t* pipeline_context) {
        // Catalog-delete mode: no predicate scan / no children — the (oid_col_idx,
        // target_oid) spec is shipped straight to delete_pg_catalog_rows in
        // await_async_and_resume.
        if (oid_col_idx_ >= 0) {
            async_wait();
            return;
        }
        // Predicate matching only — table.delete_rows() is now handled by
        // await_async_and_resume via send(disk_address_, &manager_disk_t::storage_delete_rows).
        // RETURNING rows are gathered chunk-by-chunk into ≤DEFAULT_VECTOR_CAPACITY
        // projected batches as matches are found, while the scan data is in memory.
        const bool collect_returning = !returning_.empty();

        if (left_ && left_->output() && right_ && right_->output()) {
            auto* resource = left_->output()->resource();
            modified_ = operators::make_operator_write_data(resource);
            const auto& left_chunks = left_->output()->chunks();
            const auto& right_chunks = right_->output()->chunks();
            std::pmr::vector<types::complex_logical_type> types_left(resource);
            std::pmr::vector<types::complex_logical_type> types_right(resource);
            if (!left_chunks.empty()) {
                types_left = left_chunks.front().types();
            }
            if (!right_chunks.empty()) {
                types_right = right_chunks.front().types();
            }

            auto predicate = expression_ ? predicates::create_predicate(resource,
                                                                        pipeline_context->function_registry,
                                                                        expression_,
                                                                        types_left,
                                                                        types_right,
                                                                        &pipeline_context->parameters,
                                                                        pipeline_context->session_tz)
                                         : predicates::create_all_true_predicate(resource);

            // RETURNING accumulators: matched target rows and their paired USING rows
            // are buffered in aligned chunks and projected once a batch fills.
            chunks_vector_t projected(resource);
            vector::data_chunk_t ret_left(resource, types_left, vector::DEFAULT_VECTOR_CAPACITY);
            vector::data_chunk_t ret_right(resource, types_right, vector::DEFAULT_VECTOR_CAPACITY);
            uint64_t ret_count = 0;
            auto flush_returning = [&]() -> bool {
                if (ret_count == 0) {
                    return true;
                }
                ret_left.set_cardinality(ret_count);
                ret_right.set_cardinality(ret_count);
                auto proj = evaluate_projection(resource,
                                                returning_,
                                                &ret_left,
                                                pipeline_context->parameters,
                                                pipeline_context->session_tz,
                                                &ret_right);
                if (proj.has_error()) {
                    set_error(proj.error());
                    return false;
                }
                projected.emplace_back(std::move(proj.value()));
                ret_left = vector::data_chunk_t(resource, types_left, vector::DEFAULT_VECTOR_CAPACITY);
                ret_right = vector::data_chunk_t(resource, types_right, vector::DEFAULT_VECTOR_CAPACITY);
                ret_count = 0;
                return true;
            };

            // left_base tracks the global target-row position across chunks; it is the
            // delete id shipped to storage, preserving the single-merged-chunk semantics.
            size_t total_matched = 0;
            uint64_t left_base = 0;
            for (const auto& chunk_left : left_chunks) {
                for (size_t i = 0; i < chunk_left.size(); i++) {
                    bool matched = false;
                    for (size_t rc = 0; rc < right_chunks.size() && !matched; rc++) {
                        const auto& chunk_right = right_chunks[rc];
                        for (size_t j = 0; j < chunk_right.size(); j++) {
                            auto check_result = predicate->check(chunk_left, chunk_right, i, j);
                            if (!check_result.has_error() && check_result.value()) {
                                modified_->append(static_cast<size_t>(left_base + i));
                                ++total_matched;
                                if (collect_returning) {
                                    for (size_t k = 0; k < chunk_left.column_count(); ++k) {
                                        ret_left.set_value(k, ret_count, chunk_left.data[k].value(i));
                                    }
                                    for (size_t k = 0; k < chunk_right.column_count(); ++k) {
                                        ret_right.set_value(k, ret_count, chunk_right.data[k].value(j));
                                    }
                                    if (++ret_count >= vector::DEFAULT_VECTOR_CAPACITY && !flush_returning()) {
                                        return;
                                    }
                                }
                                // UPDATE-style semi-join: a target row deletes once
                                // regardless of how many USING rows it matches.
                                matched = true;
                                break;
                            }
                        }
                    }
                }
                left_base += chunk_left.size();
            }
            if (collect_returning && !flush_returning()) {
                return;
            }
            for (const auto& type : types_left) {
                modified_->updated_types_map()[{std::pmr::string(type.alias(), resource), type}] += total_matched;
            }
            if (collect_returning) {
                set_output(make_operator_data(resource, std::move(projected)));
            }
        } else if (left_ && left_->output()) {
            output_ = left_->output(); // pass-through for downstream fk_cascade operators
            auto* resource = left_->output()->resource();
            modified_ = operators::make_operator_write_data(resource);
            const auto& in_chunks = left_->output()->chunks();
            std::pmr::vector<types::complex_logical_type> types(resource);
            if (!in_chunks.empty()) {
                types = in_chunks.front().types();
            }

            auto predicate = expression_ ? predicates::create_predicate(resource,
                                                                        pipeline_context->function_registry,
                                                                        expression_,
                                                                        types,
                                                                        types,
                                                                        &pipeline_context->parameters,
                                                                        pipeline_context->session_tz)
                                         : predicates::create_all_true_predicate(resource);

            chunks_vector_t projected(resource);
            size_t total_matched = 0;
            for (const auto& chunk : in_chunks) {
                if (chunk.size() == 0) {
                    continue;
                }
                // Per-chunk RETURNING selection into this chunk's matched rows.
                vector::indexing_vector_t ret_idx(resource);
                if (collect_returning) {
                    ret_idx.reset(chunk.size());
                }
                size_t ret_count = 0;
                for (size_t i = 0; i < chunk.size(); i++) {
                    auto check_result = predicate->check(chunk, i);
                    if (!check_result.has_error() && check_result.value()) {
                        // Dictionary scans carry the storage row id in the indexing;
                        // flat scans carry it in row_ids.
                        int64_t id = (chunk.data.front().get_vector_type() == vector::vector_type::DICTIONARY)
                                         ? static_cast<int64_t>(chunk.data.front().indexing().get_index(i))
                                         : chunk.row_ids.data<int64_t>()[i];
                        modified_->append(static_cast<size_t>(id));
                        ++total_matched;
                        if (collect_returning) {
                            ret_idx.set_index(ret_count++, i);
                        }
                    }
                }
                if (collect_returning && ret_count > 0) {
                    vector::data_chunk_t affected(resource, chunk.types(), ret_count);
                    chunk.copy(affected, ret_idx, ret_count);
                    auto proj = evaluate_projection(resource,
                                                    returning_,
                                                    &affected,
                                                    pipeline_context->parameters,
                                                    pipeline_context->session_tz,
                                                    nullptr);
                    if (proj.has_error()) {
                        set_error(proj.error());
                        return;
                    }
                    projected.emplace_back(std::move(proj.value()));
                }
            }
            for (const auto& type : types) {
                modified_->updated_types_map()[{std::pmr::string(type.alias(), resource), type}] += total_matched;
            }
            if (collect_returning) {
                set_output(make_operator_data(resource, std::move(projected)));
            }
        }

        if (modified_ && modified_->size() > 0 && table_oid_ != components::catalog::INVALID_OID) {
            async_wait();
        }
    }

    actor_zeta::unique_future<void> operator_delete::await_async_and_resume(pipeline::context_t* ctx) {
        using components::vector::data_chunk_t;
        using components::vector::vector_t;

        // Catalog-delete mode: delete pg_catalog rows by (oid_col_idx, target_oid)
        // via the WAL-first delete_pg_catalog_rows, then record the catalog table
        // on ctx->pg_catalog_delete_tables so operator_commit_transaction reverts/
        // publishes the MVCC tombstone for it. Bypasses the predicate-scan +
        // storage_delete_rows + WAL physical_delete + index path entirely.
        if (oid_col_idx_ >= 0) {
            components::execution_context_t exec_ctx{ctx->session, ctx->txn, ctx->session_tz, table_oid_};
            auto [_c, cf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::delete_pg_catalog_rows,
                                             exec_ctx,
                                             table_oid_,
                                             oid_col_idx_,
                                             target_oid_);
            co_await std::move(cf);
            if (ctx->txn.transaction_id != 0) {
                ctx->pg_catalog_delete_tables.insert(table_oid_);
            }
            mark_executed();
            co_return;
        }

        if (!modified_ || modified_->size() == 0) {
            mark_executed();
            co_return;
        }

        auto& ids = modified_->ids();
        const size_t modified_size = modified_->size();
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, ctx->session_tz, table_oid_};

        // When a resolver sibling supplied catalog metadata, build a
        // translation against the scan-output chunk to surface any
        // alias/schema drift. operator_delete only ships row ids to the
        // disk actor (no per-column data), so the translation itself isn't
        // fed downstream — this is purely a diagnostic + wiring hook for
        // future metadata-aware delete paths (e.g. index-only deletes).
        if (resolved_metadata_.has_value() && left_ && left_->output() && !left_->output()->chunks().empty()) {
            // Schema is identical across chunks, so the first chunk drives the
            // alias/translation diagnostic.
            auto& scan_chunk = left_->output()->chunks().front();
            if (scan_chunk.column_count() > 0) {
                auto translation = build_column_key_translation(*resolved_metadata_, scan_chunk);
                for (std::size_t i = 0; i < translation.size(); ++i) {
                    if (translation[i] < 0 && scan_chunk.data[i].type().has_alias()) {
                        trace(log_,
                              "operator_delete: resolved metadata has no column matching scan alias '{}'",
                              std::string(scan_chunk.data[i].type().alias()));
                    }
                }
            }
        }

        // 1. Capture WAL row IDs. The row_ids come from the upstream scan, so they
        //    are fully known before any storage mutation — unlike INSERT (whose final
        //    count depends on dedup), DELETE has no post-op dependency. This lets the
        //    user delete adopt the SAME WAL-first ordering as the catalog delete
        //    (delete_pg_catalog_rows_inner: WAL physical_delete, then the storage mark).
        std::pmr::vector<int64_t> wal_row_ids(resource_);
        wal_row_ids.reserve(modified_size);
        for (size_t i = 0; i < modified_size; i++) {
            wal_row_ids.push_back(static_cast<int64_t>(ids[i]));
        }

        // 2. WAL-FIRST: physical_delete BEFORE the storage mark, so a crash between
        //    the two replays the delete (uncommitted deletes are filtered by replay).
        if (ctx->wal_address != actor_zeta::address_t::empty_address()) {
            auto count = static_cast<uint64_t>(wal_row_ids.size());
            // See operator_insert comment on db_oid temporary hardcode.
            constexpr auto db_oid = components::catalog::well_known_oid::main_database;
            auto [_w, wf] = actor_zeta::send(ctx->wal_address,
                                             &services::wal::manager_wal_replicate_t::write_physical_delete,
                                             ctx->session,
                                             table_oid_,
                                             std::move(wal_row_ids),
                                             count,
                                             ctx->txn.transaction_id,
                                             db_oid);
            auto wal_id = co_await std::move(wf);
            auto [_df2, dff] =
                actor_zeta::send(ctx->disk_address, &services::disk::manager_disk_t::flush, ctx->session, wal_id);
            ctx->add_pending_disk_future(std::move(dff));
        }

        // 3. storage_delete_rows — mark the rows deleted under this txn (MVCC).
        vector_t row_ids(resource_, types::logical_type::BIGINT, modified_size);
        for (size_t i = 0; i < modified_size; i++) {
            row_ids.data<int64_t>()[i] = static_cast<int64_t>(ids[i]);
        }
        auto [_d, df] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::storage_delete_rows,
                                         exec_ctx,
                                         table_oid_,
                                         std::move(row_ids),
                                         static_cast<uint64_t>(modified_size));
        co_await std::move(df);

        // 4. Mirror to index (uses scan output for old data) — one batched send. Gather
        // the first modified_size scan rows in order: one old-data chunk per scan chunk
        // plus the flat row_ids aligned to the concatenation.
        if (ctx->index_address != actor_zeta::address_t::empty_address()) {
            if (auto scan_out = left_ ? left_->output() : nullptr) {
                chunks_vector_t idx_chunks(resource_);
                std::pmr::vector<int64_t> idx_ids(resource_);
                idx_ids.reserve(modified_size);
                size_t remaining = modified_size;
                for (const auto& sc : scan_out->chunks()) {
                    if (remaining == 0) {
                        break;
                    }
                    if (sc.size() == 0) {
                        continue;
                    }
                    const size_t take = std::min<size_t>(remaining, sc.size());
                    data_chunk_t idx_data(resource_, sc.types(), sc.size());
                    sc.copy(idx_data, 0);
                    idx_chunks.emplace_back(std::move(idx_data));
                    for (size_t i = 0; i < take; i++) {
                        idx_ids.emplace_back(sc.row_ids.data<int64_t>()[i]);
                    }
                    remaining -= take;
                }
                if (!idx_ids.empty()) {
                    auto [_ix, ixf] = actor_zeta::send(ctx->index_address,
                                                       &services::index::manager_index_t::delete_rows,
                                                       exec_ctx,
                                                       table_oid_,
                                                       std::move(idx_chunks),
                                                       std::move(idx_ids));
                    co_await std::move(ixf);
                }
            }
        }

        // 5. Record swap-info on context.
        ctx->dml_delete_txn_id = ctx->txn.transaction_id;
        ctx->dml_table_oid = table_oid_;

        // 6. Build result chunk. With RETURNING, output_ already holds the
        // projected deleted rows (set by on_execute_impl); otherwise emit a
        // typed chunk whose cardinality carries the affected-row count.
        if (returning_.empty()) {
            auto [_t, tf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::storage_types,
                                             ctx->session,
                                             table_oid_);
            auto types = co_await std::move(tf);
            // The result carries only the affected-row count as cardinality (no row data),
            // so emit it as a batch of ≤DEFAULT_VECTOR_CAPACITY chunks — a single chunk
            // cannot exceed one vector's worth of rows.
            const uint64_t cap = vector::DEFAULT_VECTOR_CAPACITY;
            chunks_vector_t batches(resource_);
            if (modified_size == 0) {
                data_chunk_t chunk(resource_, types, 1);
                chunk.set_cardinality(0);
                batches.emplace_back(std::move(chunk));
            } else {
                batches.reserve((modified_size + cap - 1) / cap);
                for (uint64_t base = 0; base < modified_size; base += cap) {
                    const uint64_t window = std::min<uint64_t>(cap, modified_size - base);
                    data_chunk_t chunk(resource_, types, window);
                    chunk.set_cardinality(window);
                    batches.emplace_back(std::move(chunk));
                }
            }
            set_output(make_operator_data(resource_, std::move(batches)));
        }
        mark_executed();
    }

} // namespace components::operators
