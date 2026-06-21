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
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, ctx->session_tz, table_oid_};
        // See operator_insert comment on db_oid temporary hardcode.
        constexpr auto db_oid = components::catalog::well_known_oid::main_database;

        // Hold the updated chunks alive while we ship them; set_output rebinds output_.
        auto input = output_;

        // Old-row cursor over the scan (left) output. update_rows pairs old_data[i]
        // with new_data[i]/row_ids[i] positionally, and on_execute gathered matches in
        // scan order, so the first N scan rows are the old versions of the N updated
        // rows. The cursor walks scan chunks to assemble each updated chunk's old data.
        const chunks_vector_t* scan_chunks = nullptr;
        if (auto scan_out = left_ ? left_->output() : nullptr) {
            scan_chunks = &scan_out->chunks();
        }
        size_t scan_chunk_idx = 0;
        uint64_t scan_row_in_chunk = 0;

        // UPDATE = delete-old + append-new. The new-row segments append sequentially
        // within the txn, so they are contiguous and coalesce into one range. Gather the
        // whole batch up front, then issue a single send per service.
        chunks_vector_t update_data(resource_);               // storage_update payload (mutated)
        std::pmr::vector<vector_t> update_row_ids(resource_); // storage_update row_ids, one per chunk
        chunks_vector_t wal_chunks(resource_);                // WAL payload (submitted new rows)
        std::pmr::vector<int64_t> wal_row_ids(resource_);     // WAL row_ids, flat
        chunks_vector_t idx_old(resource_);                   // index: old row versions, one per chunk
        chunks_vector_t idx_new(resource_);                   // index: new rows, one per chunk
        std::pmr::vector<int64_t> idx_row_ids(resource_);     // index row_ids, flat
        chunks_vector_t projected(resource_);
        // RETURNING: the matched FROM (right) rows were gathered in lockstep, one right
        // chunk per updated chunk (returning_from_chunks_), aligned by index.
        size_t right_idx = 0;
        const bool mirror_index =
            ctx->index_address != actor_zeta::address_t::empty_address() && scan_chunks != nullptr;

        auto copy_of = [this](const data_chunk_t& src) {
            data_chunk_t dst(resource_, src.types(), src.size());
            src.copy(dst, 0);
            return dst;
        };

        for (auto& out_chunk : input->chunks()) {
            if (out_chunk.size() == 0) {
                continue;
            }
            const uint64_t n = out_chunk.size();

            // If a resolver sibling supplied catalog metadata, surface alias drift.
            // See operator_insert::await_async_and_resume for the rationale.
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

            // storage_update needs a row_ids vector_t + payload copy per chunk.
            vector_t row_ids(resource_, types::logical_type::BIGINT, n);
            for (uint64_t i = 0; i < n; i++) {
                row_ids.data<int64_t>()[i] = out_chunk.row_ids.data<int64_t>()[i];
            }
            update_row_ids.emplace_back(std::move(row_ids));
            update_data.emplace_back(copy_of(out_chunk));

            // WAL needs the submitted new rows + their flat row_ids.
            wal_chunks.emplace_back(copy_of(out_chunk));
            for (uint64_t i = 0; i < n; i++) {
                wal_row_ids.push_back(out_chunk.row_ids.data<int64_t>()[i]);
            }

            // Index needs the n old row versions, pulled from the scan cursor in lockstep
            // with the matched scan order, plus the new rows and their row_ids.
            if (mirror_index) {
                data_chunk_t old_data(resource_, out_chunk.types(), n);
                uint64_t filled = 0;
                while (filled < n && scan_chunk_idx < scan_chunks->size()) {
                    const auto& sc = (*scan_chunks)[scan_chunk_idx];
                    if (scan_row_in_chunk >= sc.size()) {
                        ++scan_chunk_idx;
                        scan_row_in_chunk = 0;
                        continue;
                    }
                    const uint64_t take = std::min<uint64_t>(sc.size() - scan_row_in_chunk, n - filled);
                    for (uint64_t col = 0; col < sc.column_count() && col < old_data.column_count(); ++col) {
                        vector::vector_ops::copy(sc.data[col], old_data.data[col], take, scan_row_in_chunk, filled);
                    }
                    vector::vector_ops::copy(sc.row_ids, old_data.row_ids, take, scan_row_in_chunk, filled);
                    filled += take;
                    scan_row_in_chunk += take;
                }
                old_data.set_cardinality(filled);
                idx_old.emplace_back(std::move(old_data));
                idx_new.emplace_back(copy_of(out_chunk));
                for (uint64_t i = 0; i < n; i++) {
                    idx_row_ids.push_back(out_chunk.row_ids.data<int64_t>()[i]);
                }
            }

            // RETURNING projection is local (no storage read-back): project the updated
            // chunk now, paired with its lockstep FROM (right) chunk.
            if (!returning_.empty()) {
                data_chunk_t* right_batch =
                    right_idx < returning_from_chunks_.size() ? &returning_from_chunks_[right_idx] : nullptr;
                auto proj = evaluate_projection(resource_,
                                                returning_,
                                                &out_chunk,
                                                ctx->parameters,
                                                ctx->session_tz,
                                                right_batch);
                if (proj.has_error()) {
                    set_error(proj.error());
                    mark_executed();
                    co_return;
                }
                projected.emplace_back(std::move(proj.value()));
                ++right_idx;
            }
        }

        if (update_data.empty()) {
            if (!returning_.empty()) {
                set_output(make_operator_data(resource_, std::move(projected)));
            }
            mark_executed();
            co_return;
        }

        // 1. storage_update (MVCC: delete old + insert new) — one batched send.
        auto [_u, uf] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::storage_update,
                                         exec_ctx,
                                         table_oid_,
                                         std::move(update_row_ids),
                                         std::move(update_data));
        auto [range_start, total_count] = co_await std::move(uf);

        // 2. WAL physical_update: one record for the whole range.
        if (ctx->wal_address != actor_zeta::address_t::empty_address()) {
            const uint64_t wal_count = wal_row_ids.size();
            auto [_w, wf] = actor_zeta::send(ctx->wal_address,
                                             &services::wal::manager_wal_replicate_t::write_physical_update,
                                             ctx->session,
                                             table_oid_,
                                             std::move(wal_row_ids),
                                             std::move(wal_chunks),
                                             wal_count,
                                             ctx->txn.transaction_id,
                                             db_oid);
            auto wal_id = co_await std::move(wf);
            auto [_df, dff] =
                actor_zeta::send(ctx->disk_address, &services::disk::manager_disk_t::flush, ctx->session, wal_id);
            ctx->add_pending_disk_future(std::move(dff));
        }

        // 3. Mirror to index (old + new data) — one batched send.
        if (mirror_index) {
            auto [_ix, ixf] = actor_zeta::send(ctx->index_address,
                                               &services::index::manager_index_t::update_rows,
                                               exec_ctx,
                                               table_oid_,
                                               std::move(idx_old),
                                               std::move(idx_new),
                                               std::move(idx_row_ids),
                                               range_start);
            co_await std::move(ixf);
        }

        // 4. Record swap-info on context. UPDATE = delete-old + append-new, so both
        // append_row_* and delete_txn_id must be populated.
        ctx->dml_append_row_start = range_start;
        ctx->dml_append_row_count = total_count;
        ctx->dml_delete_txn_id = ctx->txn.transaction_id;
        ctx->dml_table_oid = table_oid_;

        if (!returning_.empty()) {
            set_output(make_operator_data(resource_, std::move(projected)));
        }
        mark_executed();
    }

} // namespace components::operators
