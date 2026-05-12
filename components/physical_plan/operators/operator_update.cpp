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
                                     expressions::expression_ptr expr)
        : read_write_operator_t(resource, log, operator_type::update)
        , table_oid_(table_oid)
        , updates_(std::move(updates))
        , expr_(std::move(expr))
        , upsert_(upsert) {}

    void operator_update::accept_resolved_metadata(resolved_table_metadata_t metadata) {
        // Phase 13 T15 — see operator_insert for the contract.
        if (table_oid_ == components::catalog::INVALID_OID &&
            metadata.table_oid != components::catalog::INVALID_OID) {
            table_oid_ = metadata.table_oid;
        }
        resolved_metadata_ = std::move(metadata);
    }

    void operator_update::on_execute_impl(pipeline::context_t* pipeline_context) {
        // Predicate matching + data prep only — storage I/O is handled by await_async_and_resume.
        if (left_ && left_->output() && right_ && right_->output()) {
            auto& chunk_left = left_->output()->data_chunk();
            auto& chunk_right = right_->output()->data_chunk();
            auto types_left = chunk_left.types();
            auto types_right = chunk_right.types();
            if (left_->output()->data_chunk().size() == 0 && right_->output()->data_chunk().size() == 0) {
                if (upsert_) {
                    output_ = operators::make_operator_data(resource(), types_left);
                    for (const auto& expr : updates_) {
                        expr->execute(chunk_left, chunk_right, 0, 0, &pipeline_context->parameters);
                    }
                    modified_ = operators::make_operator_write_data(resource());
                }
            } else {
                modified_ = operators::make_operator_write_data(resource());
                no_modified_ = operators::make_operator_write_data(resource());
                output_ = operators::make_operator_data(left_->output()->resource(), types_left);
                auto& out_chunk = output_->data_chunk();
                auto predicate = expr_ ? predicates::create_predicate(left_->output()->resource(),
                                                                      pipeline_context->function_registry,
                                                                      expr_,
                                                                      types_left,
                                                                      types_right,
                                                                      &pipeline_context->parameters)
                                       : predicates::create_all_true_predicate(left_->output()->resource());
                size_t index = 0;
                for (size_t i = 0; i < chunk_left.size(); i++) {
                    for (size_t j = 0; j < chunk_right.size(); j++) {
                        if (predicate->check(chunk_left, chunk_right, i, j)) {
                            out_chunk.row_ids.data<int64_t>()[index] = chunk_left.row_ids.data<int64_t>()[i];
                            // Copy original values to output first (preserves scan data for executor)
                            for (size_t k = 0; k < chunk_left.column_count(); k++) {
                                vector::vector_ops::copy(chunk_left.data[k], out_chunk.data[k], i + 1, i, index);
                            }
                            bool modified = false;
                            for (const auto& expr : updates_) {
                                modified |=
                                    expr->execute(out_chunk, chunk_right, index, j, &pipeline_context->parameters);
                            }
                            if (modified) {
                                modified_->append(index);
                            } else {
                                no_modified_->append(index);
                            }
                            vector::validate_chunk_capacity(out_chunk, ++index);
                        }
                    }
                }
                out_chunk.set_cardinality(index);
            }
        } else if (left_ && left_->output()) {
            if (left_->output()->size() == 0) {
                if (upsert_) {
                    output_ = operators::make_operator_data(resource(), left_->output()->data_chunk().types());
                }
            } else {
                auto& chunk = left_->output()->data_chunk();
                auto types = chunk.types();
                output_ = operators::make_operator_data(left_->output()->resource(), types);
                auto& out_chunk = output_->data_chunk();
                modified_ = operators::make_operator_write_data(resource());
                no_modified_ = operators::make_operator_write_data(resource());
                auto predicate = expr_ ? predicates::create_predicate(left_->output()->resource(),
                                                                      pipeline_context->function_registry,
                                                                      expr_,
                                                                      types,
                                                                      types,
                                                                      &pipeline_context->parameters)
                                       : predicates::create_all_true_predicate(left_->output()->resource());
                size_t index = 0;
                for (size_t i = 0; i < chunk.size(); i++) {
                    if (predicate->check(chunk, i)) {
                        if (chunk.data.front().get_vector_type() == vector::vector_type::DICTIONARY) {
                            out_chunk.row_ids.data<int64_t>()[index] =
                                static_cast<int64_t>(chunk.data.front().indexing().get_index(i));
                        } else {
                            out_chunk.row_ids.data<int64_t>()[index] = chunk.row_ids.data<int64_t>()[i];
                        }

                        // Copy original values to output first (preserves scan data for executor)
                        for (size_t j = 0; j < chunk.column_count(); j++) {
                            vector::vector_ops::copy(chunk.data[j], out_chunk.data[j], i + 1, i, index);
                        }
                        bool modified = false;
                        for (const auto& expr : updates_) {
                            modified |=
                                expr->execute(out_chunk, out_chunk, index, index, &pipeline_context->parameters);
                        }
                        if (modified) {
                            modified_->append(index);
                        } else {
                            no_modified_->append(index);
                        }
                        vector::validate_chunk_capacity(out_chunk, ++index);
                    }
                }
                out_chunk.set_cardinality(index);
            }
        }

        if (output_ && modified_ && modified_->size() > 0 && table_oid_ != components::catalog::INVALID_OID) {
            async_wait();
        }
    }

    actor_zeta::unique_future<void>
    operator_update::await_async_and_resume(pipeline::context_t* ctx) {
        // Phase 5: side-effects previously implemented in
        // executor_t::intercept_dml_io_(::update) are now self-contained here.
        using components::vector::data_chunk_t;
        using components::vector::vector_t;

        if (!output_) {
            mark_executed();
            co_return;
        }
        auto& out_chunk = output_->data_chunk();
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, table_oid_};

        // Phase 13 T15 — if a resolver sibling supplied catalog metadata,
        // compute a chunk_position -> table_position translation. See
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
            auto [_w, wf] = actor_zeta::send(ctx->wal_address,
                                              &services::wal::manager_wal_replicate_t::write_physical_update,
                                              ctx->session,
                                              table_oid_,
                                              std::move(wal_row_ids),
                                              std::move(wal_update_data),
                                              upd_count,
                                              ctx->txn.transaction_id);
            auto wal_id = co_await std::move(wf);
            auto [_df, dff] = actor_zeta::send(ctx->disk_address,
                                                 &services::disk::manager_disk_t::flush,
                                                 ctx->session,
                                                 wal_id);
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
        ctx->dml_delete_txn_id    = ctx->txn.transaction_id;
        ctx->dml_table_oid        = table_oid_;

        // output_ already set by on_execute_impl (contains updated rows).
        mark_executed();
    }

} // namespace components::operators
