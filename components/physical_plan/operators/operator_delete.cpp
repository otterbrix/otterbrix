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

    void operator_delete::accept_resolved_metadata(resolved_table_metadata_t metadata) {
        // See operator_insert for the contract.
        if (table_oid_ == components::catalog::INVALID_OID && metadata.table_oid != components::catalog::INVALID_OID) {
            table_oid_ = metadata.table_oid;
        }
        resolved_metadata_ = std::move(metadata);
    }

    void operator_delete::on_execute_impl(pipeline::context_t* pipeline_context) {
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

            size_t index = 0;
            for (size_t i = 0; i < chunk_left.size(); i++) {
                for (size_t j = 0; j < chunk_right.size(); j++) {
                    auto check_result = predicate->check(chunk_left, chunk_right, i, j);
                    if (!check_result.has_error() && check_result.value()) {
                        ids.data<int64_t>()[index++] = static_cast<int64_t>(i);
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
        } else if (left_ && left_->output()) {
            output_ = left_->output(); // pass-through for downstream fk_cascade operators
            modified_ = operators::make_operator_write_data(left_->output()->resource());
            auto& chunk = left_->output()->data_chunk();
            if (collect_returning) {
                returning_source = &chunk;
                returning_indexing.reset(chunk.size());
            }
            auto types = chunk.types();

            vector::vector_t ids(left_->output()->resource(), types::logical_type::BIGINT, chunk.size());
            auto predicate = expression_ ? predicates::create_predicate(left_->output()->resource(),
                                                                        pipeline_context->function_registry,
                                                                        expression_,
                                                                        types,
                                                                        types,
                                                                        &pipeline_context->parameters,
                                                                        pipeline_context->session_tz)
                                         : predicates::create_all_true_predicate(left_->output()->resource());

            size_t index = 0;
            for (size_t i = 0; i < chunk.size(); i++) {
                auto check_result = predicate->check(chunk, i);
                if (!check_result.has_error() && check_result.value()) {
                    if (chunk.data.front().get_vector_type() == vector::vector_type::DICTIONARY) {
                        ids.data<int64_t>()[index++] = static_cast<int64_t>(chunk.data.front().indexing().get_index(i));
                    } else {
                        ids.data<int64_t>()[index++] = chunk.row_ids.data<int64_t>()[i];
                    }
                    if (collect_returning) {
                        returning_indexing.set_index(returning_count++, i);
                    }
                }
            }
            ids.resize(chunk.size(), index);
            for (size_t i = 0; i < index; i++) {
                size_t id = static_cast<size_t>(ids.data<int64_t>()[i]);
                modified_->append(id);
            }
            for (const auto& type : types) {
                modified_->updated_types_map()[{std::pmr::string(type.alias(), left_->output()->resource()), type}] +=
                    index;
            }
        }

        // RETURNING: gather the matched rows from the scan chunk in one
        // vectorized copy, then project the requested columns straight into
        // output_ (split to honor the vector capacity bound). Done here, before
        // the storage delete, while the scan data is still in memory.
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

        // 1. Capture WAL row IDs.
        std::pmr::vector<int64_t> wal_row_ids(resource_);
        wal_row_ids.reserve(modified_size);
        for (size_t i = 0; i < modified_size; i++) {
            wal_row_ids.push_back(static_cast<int64_t>(ids[i]));
        }

        // 2. storage_delete_rows.
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

        // 3. WAL physical_delete.
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

        // 4. Mirror to index (uses scan output for old data).
        if (ctx->index_address != actor_zeta::address_t::empty_address()) {
            if (auto scan_out = left_ ? left_->output() : nullptr) {
                auto& sc = scan_out->data_chunk();
                auto idx_data = std::make_unique<data_chunk_t>(resource_, sc.types(), sc.size());
                sc.copy(*idx_data, 0);
                auto idx_ids = std::pmr::vector<int64_t>(resource_);
                idx_ids.reserve(modified_size);
                for (size_t i = 0; i < modified_size; i++) {
                    idx_ids.emplace_back(sc.row_ids.data<int64_t>()[i]);
                }
                auto [_ix, ixf] = actor_zeta::send(ctx->index_address,
                                                   &services::index::manager_index_t::delete_rows,
                                                   exec_ctx,
                                                   table_oid_,
                                                   std::move(idx_data),
                                                   std::move(idx_ids));
                co_await std::move(ixf);
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
            data_chunk_t chunk(resource_, types, modified_size);
            chunk.set_cardinality(modified_size);
            set_output(make_operator_data(resource_, std::move(chunk)));
        }
        mark_executed();
    }

} // namespace components::operators
