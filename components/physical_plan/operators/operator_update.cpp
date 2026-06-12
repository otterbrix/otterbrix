#include "operator_update.hpp"
#include "predicates/predicate.hpp"
#include <components/vector/vector_operations.hpp>

#include <components/context/context.hpp>
#include <components/context/execution_context.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>

namespace components::operators {

    operator_update::operator_update(std::pmr::memory_resource* resource,
                                     log_t log,
                                     components::catalog::oid_t table_oid,
                                     std::pmr::vector<expressions::update_expr_ptr> updates,
                                     bool upsert,
                                     std::pmr::vector<select_column_t> returning,
                                     expressions::expression_ptr expr)
        : read_write_operator_t(resource, log, operator_type::update)
        , table_oid_(table_oid)
        , updates_(std::move(updates))
        , expr_(std::move(expr))
        , upsert_(upsert)
        , returning_(std::move(returning))
        , returning_from_chunks_(resource) {}

    void operator_update::accept_resolved_metadata(resolved_table_metadata_t metadata) {
        // See operator_insert for the contract.
        if (table_oid_ == components::catalog::INVALID_OID && metadata.table_oid != components::catalog::INVALID_OID) {
            table_oid_ = metadata.table_oid;
        }
        resolved_metadata_ = std::move(metadata);
    }

    namespace {
        // Applies all update expressions to out_chunk[0..match_count) and
        // populates modified_/no_modified_ lists.
        void apply_updates(std::pmr::memory_resource* resource,
                           const std::pmr::vector<expressions::update_expr_ptr>& updates,
                           vector::data_chunk_t& out_chunk,
                           const vector::data_chunk_t& from_chunk,
                           uint64_t match_count,
                           const logical_plan::storage_parameters& parameters,
                           core::date::timezone_offset_t session_tz,
                           operators::operator_write_data_ptr& modified,
                           operators::operator_write_data_ptr& no_modified) {
            std::pmr::vector<bool> any_modified(match_count, false, resource);
            for (const auto& expr : updates) {
                auto row_flags = expr->execute(resource, out_chunk, from_chunk, match_count, &parameters, session_tz);
                for (uint64_t i = 0; i < match_count; i++) {
                    if (i < row_flags.size() && row_flags[i]) {
                        any_modified[i] = true;
                    }
                }
            }
            for (uint64_t i = 0; i < match_count; i++) {
                if (any_modified[i]) {
                    modified->append(i);
                } else {
                    no_modified->append(i);
                }
            }
        }
    } // anonymous namespace

    void operator_update::on_execute_impl(pipeline::context_t* pipeline_context) {
        if (left_ && left_->output() && right_ && right_->output()) {
            auto* resource = left_->output()->resource();
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

            const uint64_t left_size = left_->output()->size();
            const uint64_t right_size = right_->output()->size();

            if (left_size == 0 && right_size == 0) {
                if (upsert_) {
                    output_ = operators::make_operator_data(resource, types_left);
                    // upsert path: synthesise a row by running update exprs against an empty context.
                    vector::data_chunk_t empty_left(resource, types_left);
                    vector::data_chunk_t empty_right(resource, types_right);
                    for (const auto& expr : updates_) {
                        expr->execute(resource,
                                      empty_left,
                                      empty_right,
                                      0,
                                      &pipeline_context->parameters,
                                      pipeline_context->session_tz);
                    }
                    modified_ = operators::make_operator_write_data(resource);
                }
            } else {
                modified_ = operators::make_operator_write_data(resource);
                no_modified_ = operators::make_operator_write_data(resource);

                auto predicate = expr_ ? predicates::create_predicate(resource,
                                                                      pipeline_context->function_registry,
                                                                      expr_,
                                                                      types_left,
                                                                      types_right,
                                                                      &pipeline_context->parameters,
                                                                      pipeline_context->session_tz)
                                       : predicates::create_all_true_predicate(resource);

                chunks_vector_t out_chunks(resource);
                out_chunks.reserve(left_chunks.size());

                for (auto& chunk_left : left_chunks) {
                    if (chunk_left.size() == 0) {
                        continue;
                    }
                    vector::data_chunk_t out_chunk(resource, types_left, chunk_left.size());
                    vector::data_chunk_t right_chunk(resource, types_right, chunk_left.size());
                    size_t index = 0;
                    for (size_t i = 0; i < chunk_left.size(); ++i) {
                        bool row_matched = false;
                        for (const auto& chunk_right : right_chunks) {
                            if (chunk_right.size() == 0) {
                                continue;
                            }
                            auto results =
                                predicates::batch_check_1vN(predicate, chunk_left, chunk_right, i, chunk_right.size());
                            if (results.has_error()) {
                                set_error(results.error());
                                return;
                            }
                            for (size_t j = 0; j < chunk_right.size(); ++j) {
                                if (!results.value()[j]) {
                                    continue;
                                }
                                out_chunk.row_ids.data<int64_t>()[index] = chunk_left.row_ids.data<int64_t>()[i];
                                for (size_t k = 0; k < chunk_left.column_count(); ++k) {
                                    vector::vector_ops::copy(chunk_left.data[k], out_chunk.data[k], i + 1, i, index);
                                }
                                for (size_t k = 0; k < chunk_right.column_count(); ++k) {
                                    vector::vector_ops::copy(chunk_right.data[k], right_chunk.data[k], j + 1, j, index);
                                }
                                ++index;
                                vector::validate_chunk_capacity(out_chunk, index);
                                vector::validate_chunk_capacity(right_chunk, index);
                                // UPDATE ... FROM is a semi-join: a target row is
                                // updated once regardless of how many FROM rows it
                                // matches. Stop after the first matching FROM row.
                                row_matched = true;
                                break;
                            }
                            if (row_matched) {
                                break;
                            }
                        }
                    }
                    out_chunk.set_cardinality(index);
                    right_chunk.set_cardinality(index);
                    if (index > 0) {
                        apply_updates(resource,
                                      updates_,
                                      out_chunk,
                                      right_chunk,
                                      index,
                                      pipeline_context->parameters,
                                      pipeline_context->session_tz,
                                      modified_,
                                      no_modified_);
                        out_chunks.emplace_back(std::move(out_chunk));
                        // Keep the matched FROM rows aligned with the updated rows
                        // so RETURNING can project joined (right-side) columns.
                        if (!returning_.empty()) {
                            returning_from_chunks_.emplace_back(std::move(right_chunk));
                        }
                    }
                }
                if (out_chunks.empty()) {
                    output_ = operators::make_operator_data(resource, types_left, 0);
                } else {
                    output_ = operators::make_operator_data(resource, std::move(out_chunks));
                }
            }
        } else if (left_ && left_->output()) {
            auto* resource = left_->output()->resource();
            const auto& in_chunks = left_->output()->chunks();
            std::pmr::vector<types::complex_logical_type> types(resource);
            if (!in_chunks.empty()) {
                types = in_chunks.front().types();
            }

            if (left_->output()->size() == 0) {
                if (upsert_) {
                    output_ = operators::make_operator_data(resource, types);
                }
            } else {
                modified_ = operators::make_operator_write_data(resource);
                no_modified_ = operators::make_operator_write_data(resource);

                auto predicate = expr_ ? predicates::create_predicate(resource,
                                                                      pipeline_context->function_registry,
                                                                      expr_,
                                                                      types,
                                                                      types,
                                                                      &pipeline_context->parameters,
                                                                      pipeline_context->session_tz)
                                       : predicates::create_all_true_predicate(resource);

                chunks_vector_t out_chunks(resource);
                out_chunks.reserve(in_chunks.size());

                for (auto& chunk : in_chunks) {
                    if (chunk.size() == 0) {
                        continue;
                    }
                    vector::data_chunk_t out_chunk(resource, types, chunk.size());
                    size_t index = 0;
                    for (size_t i = 0; i < chunk.size(); ++i) {
                        auto res = predicate->check(chunk, i);
                        if (res.has_error()) {
                            set_error(res.error());
                            return;
                        }
                        if (!res.value()) {
                            continue;
                        }
                        if (chunk.data.front().get_vector_type() == vector::vector_type::DICTIONARY) {
                            out_chunk.row_ids.data<int64_t>()[index] =
                                static_cast<int64_t>(chunk.data.front().indexing().get_index(i));
                        } else {
                            out_chunk.row_ids.data<int64_t>()[index] = chunk.row_ids.data<int64_t>()[i];
                        }
                        for (size_t k = 0; k < chunk.column_count(); ++k) {
                            vector::vector_ops::copy(chunk.data[k], out_chunk.data[k], i + 1, i, index);
                        }
                        vector::validate_chunk_capacity(out_chunk, ++index);
                    }
                    out_chunk.set_cardinality(index);
                    if (index > 0) {
                        apply_updates(resource,
                                      updates_,
                                      out_chunk,
                                      out_chunk,
                                      index,
                                      pipeline_context->parameters,
                                      pipeline_context->session_tz,
                                      modified_,
                                      no_modified_);
                        out_chunks.emplace_back(std::move(out_chunk));
                    }
                }
                if (out_chunks.empty()) {
                    output_ = operators::make_operator_data(resource, types, 0);
                } else {
                    output_ = operators::make_operator_data(resource, std::move(out_chunks));
                }
            }
        }

        if (output_ && modified_ && modified_->size() > 0 && table_oid_ != components::catalog::INVALID_OID) {
            async_wait();
        }
    }

    actor_zeta::unique_future<void> operator_update::await_async_and_resume(pipeline::context_t* ctx) {
        using components::vector::data_chunk_t;
        using components::vector::vector_t;

        if (!output_) {
            mark_executed();
            co_return;
        }
        auto& out_chunk = output_->data_chunk();
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, ctx->session_tz, table_oid_};

        // If a resolver sibling supplied catalog metadata, compute a
        // chunk_position -> table_position translation. See
        // operator_insert::await_async_and_resume for the rationale; the
        // disk path already aligns by alias, this is the wiring hook.
        if (resolved_metadata_.has_value() && out_chunk.column_count() > 0) {
            auto translation = build_column_key_translation(*resolved_metadata_, out_chunk);
            for (std::size_t i = 0; i < translation.size(); ++i) {
                if (translation[i] < 0 && out_chunk.data[i].type().has_alias()) {
                    trace(log_,
                          "operator_update: resolved metadata has no column matching chunk alias '{}'",
                          std::string(out_chunk.data[i].type().alias()));
                }
            }
        }

        // 1. Capture WAL data: row_ids + updated chunk.
        std::pmr::vector<int64_t> wal_row_ids(resource_);
        wal_row_ids.reserve(out_chunk.size());
        for (uint64_t i = 0; i < out_chunk.size(); i++) {
            wal_row_ids.push_back(out_chunk.row_ids.data<int64_t>()[i]);
        }
        auto wal_update_data = std::make_unique<data_chunk_t>(resource_, out_chunk.types(), out_chunk.size());
        out_chunk.copy(*wal_update_data, 0);

        // 2. storage_update (MVCC: delete old + insert new).
        vector_t row_ids(resource_, types::logical_type::BIGINT, out_chunk.size());
        for (uint64_t i = 0; i < out_chunk.size(); i++) {
            row_ids.data<int64_t>()[i] = out_chunk.row_ids.data<int64_t>()[i];
        }
        auto data_copy = std::make_unique<data_chunk_t>(resource_, out_chunk.types(), out_chunk.size());
        out_chunk.copy(*data_copy, 0);
        auto [_u, uf] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::storage_update,
                                         exec_ctx,
                                         table_oid_,
                                         std::move(row_ids),
                                         std::move(data_copy));
        auto [upd_row_start, upd_row_count] = co_await std::move(uf);

        // 3. WAL physical_update.
        if (ctx->wal_address != actor_zeta::address_t::empty_address()) {
            auto upd_count = static_cast<uint64_t>(wal_row_ids.size());
            // See operator_insert comment on db_oid temporary hardcode.
            constexpr auto db_oid = components::catalog::well_known_oid::main_database;
            auto [_w, wf] = actor_zeta::send(ctx->wal_address,
                                             &services::wal::manager_wal_replicate_t::write_physical_update,
                                             ctx->session,
                                             table_oid_,
                                             std::move(wal_row_ids),
                                             std::move(wal_update_data),
                                             upd_count,
                                             ctx->txn.transaction_id,
                                             db_oid);
            auto wal_id = co_await std::move(wf);
            auto [_df, dff] =
                actor_zeta::send(ctx->disk_address, &services::disk::manager_disk_t::flush, ctx->session, wal_id);
            ctx->add_pending_disk_future(std::move(dff));
        }

        // 4. Mirror to index (old + new data).
        if (ctx->index_address != actor_zeta::address_t::empty_address()) {
            if (auto scan_out = left_ ? left_->output() : nullptr) {
                auto& sc = scan_out->data_chunk();
                auto old_data = std::make_unique<data_chunk_t>(resource_, sc.types(), sc.size());
                sc.copy(*old_data, 0);
                auto new_data = std::make_unique<data_chunk_t>(resource_, out_chunk.types(), out_chunk.size());
                out_chunk.copy(*new_data, 0);
                auto idx_ids = std::pmr::vector<int64_t>(resource_);
                idx_ids.reserve(out_chunk.size());
                for (size_t i = 0; i < out_chunk.size(); i++) {
                    idx_ids.push_back(out_chunk.row_ids.data<int64_t>()[i]);
                }
                auto [_ix, ixf] = actor_zeta::send(ctx->index_address,
                                                   &services::index::manager_index_t::update_rows,
                                                   exec_ctx,
                                                   table_oid_,
                                                   std::move(old_data),
                                                   std::move(new_data),
                                                   std::move(idx_ids),
                                                   static_cast<int64_t>(upd_row_start));
                co_await std::move(ixf);
            }
        }

        // 5. Record swap-info on context. UPDATE = delete-old + append-new,
        // so both append_row_* and delete_txn_id must be populated.
        ctx->dml_append_row_start = upd_row_start;
        ctx->dml_append_row_count = upd_row_count;
        ctx->dml_delete_txn_id = ctx->txn.transaction_id;
        ctx->dml_table_oid = table_oid_;

        // RETURNING: project the requested columns from the updated rows.
        // out_chunk is the merged updated-rows chunk (all table columns, in
        // table order, matching the paths resolved by validate). Split it back
        // into capacity-bounded batches and project each. Without RETURNING,
        // output_ keeps the updated rows as set by on_execute_impl.
        // TODO: keep the updated rows batched end-to-end instead of merging in
        // data_chunk() and re-splitting here.
        if (!returning_.empty()) {
            chunks_vector_t projected(resource_);
            auto batches = split_chunk_into_batches(resource_, std::move(out_chunk));

            // UPDATE ... FROM: the matched FROM rows were gathered in lockstep with
            // the updated rows. Merge them the same way out_chunk was merged and
            // split identically, so batch b of each side covers the same matches.
            chunks_vector_t right_batches(resource_);
            if (!returning_from_chunks_.empty()) {
                auto right_data = make_operator_data(resource_, std::move(returning_from_chunks_));
                right_batches = split_chunk_into_batches(resource_, std::move(right_data->data_chunk()));
            }

            for (size_t b = 0; b < batches.size(); b++) {
                auto& batch = batches[b];
                if (batch.size() == 0) {
                    continue;
                }
                data_chunk_t* right_batch = b < right_batches.size() ? &right_batches[b] : nullptr;
                auto proj =
                    evaluate_projection(resource_, returning_, &batch, ctx->parameters, ctx->session_tz, right_batch);
                if (proj.has_error()) {
                    set_error(proj.error());
                    mark_executed();
                    co_return;
                }
                projected.emplace_back(std::move(proj.value()));
            }
            set_output(make_operator_data(resource_, std::move(projected)));
        }
        mark_executed();
    }

} // namespace components::operators
