#include "operator_update.hpp"
#include "predicates/predicate.hpp"
#include <components/vector/vector_operations.hpp>

#include <components/context/context.hpp>
#include <components/context/execution_context.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>

#include <algorithm>
#include <unordered_map>

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
        bool is_unprojected_placeholder(const vector::vector_t& vector) noexcept {
            return vector.data() == nullptr && vector.auxiliary() == nullptr;
        }

        std::vector<size_t> materialized_columns(const vector::data_chunk_t& chunk) {
            std::vector<size_t> result;
            result.reserve(chunk.column_count());
            for (size_t column = 0; column < chunk.column_count(); column++) {
                if (!is_unprojected_placeholder(chunk.data[column])) {
                    result.push_back(column);
                }
            }
            return result;
        }

        void append_unique_column(std::vector<size_t>& columns, size_t column_index) {
            if (std::find(columns.begin(), columns.end(), column_index) == columns.end()) {
                columns.push_back(column_index);
            }
        }

        bool has_unprojected_placeholders(const vector::data_chunk_t& chunk) {
            for (const auto& column : chunk.data) {
                if (is_unprojected_placeholder(column)) {
                    return true;
                }
            }
            return false;
        }

        bool has_unprojected_placeholders(const chunks_vector_t& chunks) {
            for (const auto& chunk : chunks) {
                if (has_unprojected_placeholders(chunk)) {
                    return true;
                }
            }
            return false;
        }

        vector::data_chunk_t combine_update_chunks(std::pmr::memory_resource* resource,
                                                   const chunks_vector_t& chunks,
                                                   const std::vector<size_t>& projected_cols) {
            if (chunks.empty()) {
                std::pmr::vector<types::complex_logical_type> empty_types(resource);
                return vector::data_chunk_t(resource, empty_types, 0);
            }

            uint64_t total_count = 0;
            for (const auto& chunk : chunks) {
                total_count += chunk.size();
            }

            auto types = chunks.front().types();
            const bool sparse = projected_cols.size() < chunks.front().column_count();
            vector::data_chunk_t result =
                sparse ? vector::data_chunk_t(resource, types, projected_cols, total_count == 0 ? 1 : total_count)
                       : vector::data_chunk_t(resource, types, total_count == 0 ? 1 : total_count);

            uint64_t offset = 0;
            for (const auto& chunk : chunks) {
                if (chunk.size() == 0) {
                    continue;
                }
                for (const auto column : projected_cols) {
                    vector::vector_ops::copy(chunk.data[column], result.data[column], chunk.size(), 0, offset);
                }
                vector::vector_ops::copy(chunk.row_ids, result.row_ids, chunk.size(), 0, offset);
                offset += chunk.size();
            }
            result.set_cardinality(total_count);
            return result;
        }

        vector::data_chunk_t combine_update_chunks(std::pmr::memory_resource* resource, const chunks_vector_t& chunks) {
            if (chunks.empty()) {
                std::pmr::vector<types::complex_logical_type> empty_types(resource);
                return vector::data_chunk_t(resource, empty_types, 0);
            }
            return combine_update_chunks(resource, chunks, materialized_columns(chunks.front()));
        }

        void copy_materialized_columns_by_row_id(const vector::data_chunk_t& source,
                                                 vector::data_chunk_t& target,
                                                 const std::vector<size_t>& projected_cols) {
            std::unordered_map<int64_t, uint64_t> source_offsets;
            source_offsets.reserve(source.size());
            const auto* source_row_ids = source.row_ids.data<int64_t>();
            for (uint64_t row = 0; row < source.size(); row++) {
                source_offsets.emplace(source_row_ids[row], row);
            }

            const auto* target_row_ids = target.row_ids.data<int64_t>();
            for (uint64_t row = 0; row < target.size(); row++) {
                const auto found = source_offsets.find(target_row_ids[row]);
                if (found == source_offsets.end()) {
                    continue;
                }
                const auto source_row = found->second;
                for (const auto column : projected_cols) {
                    if (column >= source.column_count() || column >= target.column_count() ||
                        is_unprojected_placeholder(source.data[column])) {
                        continue;
                    }
                    vector::vector_ops::copy(source.data[column], target.data[column], source_row + 1, source_row, row);
                }
            }
        }

        void collect_update_target_paths(const expressions::update_expr_ptr& expression,
                                         std::vector<std::vector<uint64_t>>& target_paths) {
            if (!expression) {
                return;
            }
            if (expression->type() == expressions::update_expr_type::set) {
                const auto* set_expr = dynamic_cast<const expressions::update_expr_set_t*>(expression.get());
                if (!set_expr || set_expr->key().path().empty()) {
                    return;
                }
                std::vector<uint64_t> path{static_cast<uint64_t>(set_expr->key().path().front())};
                if (std::find(target_paths.begin(), target_paths.end(), path) == target_paths.end()) {
                    target_paths.emplace_back(std::move(path));
                }
                return;
            }
            collect_update_target_paths(expression->left(), target_paths);
            collect_update_target_paths(expression->right(), target_paths);
        }

        std::vector<std::vector<uint64_t>>
        collect_update_target_paths(const std::pmr::vector<expressions::update_expr_ptr>& updates) {
            std::vector<std::vector<uint64_t>> target_paths;
            for (const auto& update : updates) {
                collect_update_target_paths(update, target_paths);
            }
            return target_paths;
        }

        void collect_update_source_columns(const expressions::update_expr_ptr& expression,
                                           std::vector<size_t>& source_columns,
                                           size_t column_count) {
            if (!expression) {
                return;
            }

            if (expression->type() == expressions::update_expr_type::set) {
                const auto* set_expr = dynamic_cast<const expressions::update_expr_set_t*>(expression.get());
                if (set_expr && set_expr->key().path().size() > 1) {
                    const auto column = set_expr->key().path().front();
                    if (column < column_count) {
                        append_unique_column(source_columns, static_cast<size_t>(column));
                    }
                }
            } else if (expression->type() == expressions::update_expr_type::get_value) {
                const auto* get_expr = dynamic_cast<const expressions::update_expr_get_value_t*>(expression.get());
                if (get_expr && !get_expr->key().path().empty()) {
                    const auto column = get_expr->key().path().front();
                    if (column < column_count) {
                        append_unique_column(source_columns, static_cast<size_t>(column));
                    }
                }
            }

            collect_update_source_columns(expression->left(), source_columns, column_count);
            collect_update_source_columns(expression->right(), source_columns, column_count);
        }

        std::vector<size_t> update_source_columns(const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                                  size_t column_count) {
            std::vector<size_t> columns;
            for (const auto& update : updates) {
                collect_update_source_columns(update, columns, column_count);
            }
            std::sort(columns.begin(), columns.end());
            columns.erase(std::unique(columns.begin(), columns.end()), columns.end());
            return columns;
        }

        std::vector<size_t> writable_update_columns(const std::vector<size_t>& source_columns,
                                                    const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                                    size_t column_count) {
            auto columns = source_columns;
            for (const auto& path : collect_update_target_paths(updates)) {
                if (!path.empty() && path.front() < column_count) {
                    append_unique_column(columns, static_cast<size_t>(path.front()));
                }
            }
            std::sort(columns.begin(), columns.end());
            columns.erase(std::unique(columns.begin(), columns.end()), columns.end());
            return columns;
        }

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
                    const auto source_cols = update_source_columns(updates_, chunk.column_count());
                    const auto writable_cols = writable_update_columns(source_cols, updates_, chunk.column_count());
                    const bool sparse = writable_cols.size() < chunk.column_count();
                    vector::data_chunk_t out_chunk =
                        sparse ? vector::data_chunk_t(resource, types, writable_cols, chunk.size())
                               : vector::data_chunk_t(resource, types, chunk.size());
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
                        out_chunk.row_ids.data<int64_t>()[index] = chunk.row_ids.data<int64_t>()[i];
                        for (const auto k : source_cols) {
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

        const bool projected_update = has_unprojected_placeholders(output_->chunks());
        std::unique_ptr<data_chunk_t> materialized_update_chunk;
        std::unique_ptr<data_chunk_t> old_data_for_index;

        if (projected_update) {
            auto projected_chunk = combine_update_chunks(resource_, output_->chunks());
            vector_t fetch_row_ids(resource_, types::logical_type::BIGINT, projected_chunk.size());
            for (uint64_t i = 0; i < projected_chunk.size(); i++) {
                fetch_row_ids.data<int64_t>()[i] = projected_chunk.row_ids.data<int64_t>()[i];
            }

            auto [_f, ff] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::storage_fetch,
                                             ctx->session,
                                             table_oid_,
                                             std::move(fetch_row_ids),
                                             projected_chunk.size(),
                                             ctx->txn);
            auto fetched = co_await std::move(ff);
            if (!fetched) {
                mark_executed();
                co_return;
            }

            old_data_for_index = std::make_unique<data_chunk_t>(resource_, fetched->types(), fetched->size());
            fetched->copy(*old_data_for_index, 0);

            const auto projected_cols = materialized_columns(projected_chunk);
            copy_materialized_columns_by_row_id(projected_chunk, *fetched, projected_cols);
            materialized_update_chunk = std::move(fetched);
        }

        auto& out_chunk = projected_update ? *materialized_update_chunk : output_->data_chunk();
        if (out_chunk.size() == 0) {
            mark_executed();
            co_return;
        }

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
            if (old_data_for_index || (left_ && left_->output())) {
                std::unique_ptr<data_chunk_t> old_data;
                if (old_data_for_index) {
                    old_data = std::move(old_data_for_index);
                } else {
                    auto& sc = left_->output()->data_chunk();
                    old_data = std::make_unique<data_chunk_t>(resource_, sc.types(), sc.size());
                    sc.copy(*old_data, 0);
                }
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
