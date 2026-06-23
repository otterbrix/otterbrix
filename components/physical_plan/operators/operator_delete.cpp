#include "operator_delete.hpp"
#include "predicates/predicate.hpp"
#include <components/vector/vector_operations.hpp>

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

    void operator_delete::ensure_simple_init_() {
        if (simple_init_done_) {
            return;
        }
        modified_ = operators::make_operator_write_data(resource_);
        simple_init_done_ = true;
    }

    core::error_t operator_delete::consume_batch_(pipeline::context_t* pipeline_context,
                                                  const vector::data_chunk_t& chunk) {
        using components::vector::data_chunk_t;
        ensure_simple_init_();
        if (chunk.size() == 0) {
            return core::error_t::no_error();
        }
        const bool collect_returning = !returning_.empty();
        auto types = chunk.types();

        // Match each row. expression_ is null for the simple predicate-scan DELETE
        // (the scan already pushed the WHERE), so create_all_true_predicate matches
        // every scan row; a non-null expression_ (legacy callers) is honored too.
        auto predicate = expression_ ? predicates::create_predicate(resource_,
                                                                    pipeline_context->function_registry,
                                                                    expression_,
                                                                    types,
                                                                    types,
                                                                    &pipeline_context->parameters,
                                                                    pipeline_context->session_tz)
                                     : predicates::create_all_true_predicate(resource_);

        // Matched ABSOLUTE row-ids of THIS batch (kept separate so the index mirror
        // pairs each staged old-row with its own id, regardless of batch order).
        vector::vector_t batch_ids(resource_, types::logical_type::BIGINT, chunk.size());
        // Indexing of matched rows into `chunk`, for the gathered old-row / RETURNING copies.
        vector::indexing_vector_t matched_indexing(resource_);
        matched_indexing.reset(chunk.size());

        size_t index = 0;
        for (size_t i = 0; i < chunk.size(); i++) {
            auto check_result = predicate->check(chunk, i);
            if (check_result.has_error()) {
                return check_result.error();
            }
            if (!check_result.value()) {
                continue;
            }
            int64_t abs_id;
            if (chunk.data.front().get_vector_type() == vector::vector_type::DICTIONARY) {
                abs_id = static_cast<int64_t>(chunk.data.front().indexing().get_index(i));
            } else {
                abs_id = chunk.row_ids.data<int64_t>()[i];
            }
            batch_ids.data<int64_t>()[index] = abs_id;
            matched_indexing.set_index(index, i);
            index++;
        }
        if (index == 0) {
            return core::error_t::no_error();
        }

        for (size_t i = 0; i < index; i++) {
            modified_->append(static_cast<size_t>(batch_ids.data<int64_t>()[i]));
        }
        for (const auto& type : types) {
            modified_->updated_types_map()[{std::pmr::string(type.alias(), resource_), type}] += index;
        }

        // Stage the matched OLD scan rows + their absolute ids for the index mirror
        // (bounded: only matched rows). The merged staged chunk row k pairs with
        // index_old_row_ids_[k], so manager_index_t::delete_rows reads them aligned.
        {
            data_chunk_t old_matched(resource_, types, index);
            chunk.copy(old_matched, matched_indexing, index);
            old_matched.set_cardinality(index);
            index_old_chunks_.emplace_back(std::move(old_matched));
            for (size_t i = 0; i < index; i++) {
                index_old_row_ids_.push_back(batch_ids.data<int64_t>()[i]);
            }
        }

        // Stage matched RETURNING rows: gather the matched subset, then project the
        // requested columns straight into capacity-bounded chunks.
        if (collect_returning) {
            data_chunk_t affected(resource_, types, index);
            chunk.copy(affected, matched_indexing, index);
            affected.set_cardinality(index);
            auto batches = split_chunk_into_batches(resource_, std::move(affected));
            for (auto& batch : batches) {
                if (batch.size() == 0) {
                    continue;
                }
                auto proj = evaluate_projection(resource_,
                                                returning_,
                                                &batch,
                                                pipeline_context->parameters,
                                                pipeline_context->session_tz);
                if (proj.has_error()) {
                    return proj.error();
                }
                returning_staged_.emplace_back(std::move(proj.value()));
            }
        }
        return core::error_t::no_error();
    }

    core::error_t
    operator_delete::push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& /*out*/) {
        // STREAMING DML SINK: fold one scan batch into the matched-id / index-old /
        // RETURNING staging. Emits nothing (out stays empty); await_async_and_resume
        // drains the staged state into the single WAL->storage->index commit.
        return consume_batch_(ctx, input);
    }

    void operator_delete::on_execute_impl(pipeline::context_t* pipeline_context) {
        // Catalog-delete mode: no predicate scan / no children — the (oid_col_idx,
        // target_oid) spec is shipped straight to delete_pg_catalog_rows in
        // await_async_and_resume.
        if (oid_col_idx_ >= 0) {
            async_wait();
            return;
        }
        // RETURNING: record the scan positions of matched rows into a separate
        // indexing vector as we match (ids stays row-id/dict-index for the
        // storage delete). After matching, gather those rows from returning_source
        // in one vectorized copy and project them straight into output_.
        const bool collect_returning = !returning_.empty();
        vector::indexing_vector_t returning_indexing(resource_);
        size_t returning_count = 0;
        const vector::data_chunk_t* returning_source = nullptr;
        // Right (USING) side for joined RETURNING: the chosen right-row index per
        // matched target row, gathered in lockstep with returning_indexing so the
        // projection reads joined columns from the same matched pair.
        vector::indexing_vector_t returning_indexing_right(resource_);
        const vector::data_chunk_t* returning_source_right = nullptr;
        // Predicate matching only — table.delete_rows() is now handled by
        // await_async_and_resume via send(disk_address_, &manager_disk_t::storage_delete_rows).
        if (left_ && left_->output() && right_ && right_->output()) {
            modified_ = operators::make_operator_write_data(left_->output()->resource());
            auto& chunk_left = left_->output()->data_chunk();
            auto& chunk_right = right_->output()->data_chunk();
            if (collect_returning) {
                returning_source = &chunk_left;
                returning_indexing.reset(chunk_left.size());
                returning_source_right = &chunk_right;
                returning_indexing_right.reset(chunk_left.size());
            }
            auto types_left = chunk_left.types();
            auto types_right = chunk_right.types();
            auto ids_capacity = vector::DEFAULT_VECTOR_CAPACITY;
            vector::vector_t ids(left_->output()->resource(), types::logical_type::BIGINT, ids_capacity);
            auto predicate = expression_ ? predicates::create_predicate(left_->output()->resource(),
                                                                        pipeline_context->function_registry,
                                                                        expression_,
                                                                        types_left,
                                                                        types_right,
                                                                        &pipeline_context->parameters,
                                                                        pipeline_context->session_tz)
                                         : predicates::create_all_true_predicate(output_->resource());

            // Index into chunk_left of each matched target row (loop-relative), so
            // the matched OLD rows can be gathered for the index mirror in lockstep
            // with their absolute ids — the same staging the simple path uses.
            vector::indexing_vector_t matched_indexing(left_->output()->resource());
            matched_indexing.reset(chunk_left.size());
            size_t index = 0;
            for (size_t i = 0; i < chunk_left.size(); i++) {
                for (size_t j = 0; j < chunk_right.size(); j++) {
                    auto check_result = predicate->check(chunk_left, chunk_right, i, j);
                    if (!check_result.has_error() && check_result.value()) {
                        // Storage / index delete keys on the ABSOLUTE table row id of
                        // the matched left row, NOT the left-chunk loop index — the two
                        // diverge once the table has gaps, multiple row groups, or a
                        // non-zero row-group start. Mirror the simple branch's
                        // DICTIONARY fallback.
                        int64_t abs_id;
                        if (chunk_left.data.front().get_vector_type() == vector::vector_type::DICTIONARY) {
                            abs_id = static_cast<int64_t>(chunk_left.data.front().indexing().get_index(i));
                        } else {
                            abs_id = chunk_left.row_ids.data<int64_t>()[i];
                        }
                        ids.data<int64_t>()[index] = abs_id;
                        matched_indexing.set_index(index, i);
                        index++;
                        if (collect_returning) {
                            returning_indexing.set_index(returning_count, i);
                            returning_indexing_right.set_index(returning_count, j);
                            returning_count++;
                        }
                        if (index >= ids_capacity) {
                            ids.resize(ids_capacity, ids_capacity * 2);
                            ids_capacity *= 2;
                        }
                        // Match found, no need to scan further
                        break;
                    }
                }
            }
            for (size_t i = 0; i < index; i++) {
                size_t id = static_cast<size_t>(ids.data<int64_t>()[i]);
                modified_->append(id);
            }
            for (const auto& type : types_left) {
                modified_->updated_types_map()[{std::pmr::string(type.alias(), left_->output()->resource()), type}] +=
                    index;
            }

            // Stage the matched OLD left rows + their absolute ids for the index
            // mirror, exactly as the simple (consume_batch_) path does, so the
            // index delete_rows receives the MATCHED rows aligned with their own
            // ids — not the first-N scan rows. Routing the join path through the
            // shared index-mirror arm in await_async_and_resume.
            if (index > 0) {
                vector::data_chunk_t old_matched(left_->output()->resource(), types_left, index);
                chunk_left.copy(old_matched, matched_indexing, index);
                old_matched.set_cardinality(index);
                index_old_chunks_.emplace_back(std::move(old_matched));
                for (size_t i = 0; i < index; i++) {
                    index_old_row_ids_.push_back(ids.data<int64_t>()[i]);
                }
            }
        } else if (left_ && left_->output()) {
            // SIMPLE predicate-scan DELETE (no USING): the materialized entry point.
            // Stream each scan chunk through the SAME consume_batch_ core push()
            // uses (R6: one implementation, two entry points), so matching, modified_
            // accumulation, index-old staging and RETURNING staging are identical.
            output_ = left_->output(); // pass-through for downstream fk_cascade operators
            for (const auto& chunk : left_->output()->chunks()) {
                auto err = consume_batch_(pipeline_context, chunk);
                if (err.contains_error()) {
                    set_error(err);
                    return;
                }
            }
            // Drain staged RETURNING into output_ (the streaming entry point does
            // the same in await_async_and_resume's tail). Skip when this is a
            // fk_cascade pass-through (no RETURNING) so output_ keeps the scan rows.
            if (collect_returning) {
                set_output(make_operator_data(resource_, std::move(returning_staged_)));
            }
        }

        // RETURNING gather for the USING-join path only (the simple path staged its
        // RETURNING via consume_batch_ above). Gather the matched left + right rows
        // in lockstep and project them, splitting to honor the vector capacity bound.
        if (collect_returning && returning_source != nullptr) {
            chunks_vector_t projected(resource_);
            if (returning_count > 0) {
                vector::data_chunk_t affected(resource_, returning_source->types(), returning_count);
                returning_source->copy(affected, returning_indexing, returning_count);
                auto batches = split_chunk_into_batches(resource_, std::move(affected));

                // Joined RETURNING (DELETE ... USING): gather the matched right
                // (USING) rows in lockstep and split identically — same row count,
                // same windows — so batch b of each side covers the same pairs.
                chunks_vector_t batches_right(resource_);
                if (returning_source_right != nullptr) {
                    vector::data_chunk_t affected_right(resource_, returning_source_right->types(), returning_count);
                    returning_source_right->copy(affected_right, returning_indexing_right, returning_count);
                    batches_right = split_chunk_into_batches(resource_, std::move(affected_right));
                }

                for (size_t b = 0; b < batches.size(); b++) {
                    auto& batch = batches[b];
                    if (batch.size() == 0) {
                        continue;
                    }
                    vector::data_chunk_t* right_batch = b < batches_right.size() ? &batches_right[b] : nullptr;
                    auto proj = evaluate_projection(resource_,
                                                    returning_,
                                                    &batch,
                                                    pipeline_context->parameters,
                                                    pipeline_context->session_tz,
                                                    right_batch);
                    if (proj.has_error()) {
                        set_error(proj.error());
                        return;
                    }
                    projected.emplace_back(std::move(proj.value()));
                }
            }
            set_output(make_operator_data(resource_, std::move(projected)));
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
        if (resolved_metadata_.has_value() && left_ && left_->output()) {
            auto& scan_chunk = left_->output()->data_chunk();
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

        // 4. Mirror to index (old data). BOTH paths stage the MATCHED old rows +
        //    their absolute ids into index_old_chunks_/index_old_row_ids_: the
        //    SIMPLE path via consume_batch_ (push() OR on_execute_impl), the
        //    USING-join path in on_execute_impl's match loop. So the index
        //    delete_rows always receives the matched rows paired with their own
        //    ids — never the first-N scan rows — even when streaming leaves
        //    left_->output() empty.
        if (ctx->index_address != actor_zeta::address_t::empty_address() && !index_old_chunks_.empty()) {
            auto old_data = make_operator_data(resource_, std::move(index_old_chunks_));
            auto& merged = old_data->data_chunk();
            auto idx_data = std::make_unique<data_chunk_t>(resource_, merged.types(), merged.size());
            merged.copy(*idx_data, 0);
            auto [_ix, ixf] = actor_zeta::send(ctx->index_address,
                                               &services::index::manager_index_t::delete_rows,
                                               exec_ctx,
                                               table_oid_,
                                               std::move(idx_data),
                                               std::move(index_old_row_ids_));
            co_await std::move(ixf);
        }

        // 5. Record swap-info on context.
        ctx->dml_delete_txn_id = ctx->txn.transaction_id;
        ctx->dml_table_oid = table_oid_;

        // 6. Build result chunk. With RETURNING: the materialized simple path and
        // the USING path drained their projected rows into output_ in
        // on_execute_impl; the STREAMING path did not run on_execute_impl, so drain
        // the staged RETURNING here (returning_staged_ is non-empty only then).
        // Without RETURNING, emit a typed chunk whose cardinality carries the
        // affected-row count.
        if (!returning_.empty()) {
            if (!returning_staged_.empty()) {
                set_output(make_operator_data(resource_, std::move(returning_staged_)));
            }
        } else {
            auto [_t, tf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::storage_types,
                                             ctx->session,
                                             table_oid_);
            auto types = co_await std::move(tf);
            data_chunk_t chunk(resource_, types, modified_size);
            chunk.set_cardinality(modified_size);
            set_output(make_operator_data(resource_, std::move(chunk)));
        }
        mark_executed();
    }

} // namespace components::operators
