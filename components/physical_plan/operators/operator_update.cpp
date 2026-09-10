#include "operator_update.hpp"
#include "dml_util.hpp"
#include "join_utils.hpp"
#include <atomic>
#include <cassert>
#include <components/vector/vector_operations.hpp>

#include <components/context/context.hpp>
#include <components/context/execution_context.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>

namespace components::operators {

#ifdef DEV_MODE
    namespace {
        std::atomic<uint64_t> g_update_storage_update_sends{0};
        std::atomic<uint64_t> g_update_gather_copy_calls{0};
    } // namespace
    uint64_t update_storage_update_sends() noexcept {
        return g_update_storage_update_sends.load(std::memory_order_relaxed);
    }
    uint64_t update_gather_copy_calls() noexcept { return g_update_gather_copy_calls.load(std::memory_order_relaxed); }
#endif

    operator_update::operator_update(std::pmr::memory_resource* resource,
                                     log_t log,
                                     components::catalog::oid_t table_oid,
                                     std::pmr::vector<expressions::expression_ptr> updates,
                                     bool upsert,
                                     std::pmr::vector<projected_column_t> returning,
                                     expressions::expression_ptr expr,
                                     std::int64_t affected_bound)
        : read_write_operator_t(resource, log, operator_type::update)
        , table_oid_(table_oid)
        , updates_(std::move(updates))
        , expr_(std::move(expr))
        , condition_(expressions::classify_condition(expr_))
        , upsert_(upsert)
        , returning_(std::move(returning))
        , returning_from_chunks_(resource)
        , affected_bound_(affected_bound) {}

    namespace {
        // The value already arrives in the target's type and flat: validation spliced the cast into it.
        [[nodiscard]] core::error_t write_target(const expressions::key_t& target,
                                                 const vector::vector_t& new_values,
                                                 vector::data_chunk_t& out_chunk,
                                                 uint64_t count) {
            assert(target.path().front() != size_t(-1));
            auto* col_vec = out_chunk.at(target.path());

            if (target.path().size() > 1) {
                const vector::vector_t* parent = &out_chunk.data[target.path().front()];
                for (size_t depth = 1; depth + 1 < target.path().size(); ++depth) {
                    parent = parent->entries()[target.path()[depth]].get();
                }
                const auto element_index = target.path().back();
                if (parent->type().type() == types::logical_type::ARRAY) {
                    auto stride =
                        static_cast<const types::array_logical_type_extension*>(parent->type().extension())->size();
                    vector::vector_ops::copy_strided_target(new_values, *col_vec, count, stride, element_index);
                    return core::error_t::no_error();
                }
                if (parent->type().type() == types::logical_type::LIST) {
                    const auto* offlen = parent->data<types::list_entry_t>();
                    for (uint64_t row = 0; row < count; ++row) {
                        if (element_index >= offlen[row].length) {
                            continue;
                        }
                        vector::vector_ops::copy(new_values,
                                                 *col_vec,
                                                 row + 1,
                                                 row,
                                                 offlen[row].offset + element_index);
                    }
                    return core::error_t::no_error();
                }
            }

            const bool array_like_target = col_vec->type().type() == types::logical_type::ARRAY ||
                                           col_vec->type().type() == types::logical_type::LIST;
            if (array_like_target) {
                const vector::vector_t& elements = new_values.entry();
                auto source_slice = [&](uint64_t row) -> types::list_entry_t {
                    if (new_values.type().type() == types::logical_type::LIST) {
                        return new_values.data<types::list_entry_t>()[row];
                    }
                    auto stride =
                        static_cast<const types::array_logical_type_extension*>(new_values.type().extension())->size();
                    return types::list_entry_t{row * stride, stride};
                };

                if (col_vec->type().type() == types::logical_type::LIST) {
                    auto* row_entries = col_vec->data<types::list_entry_t>();
                    col_vec->set_list_size(0);
                    uint64_t target_offset = 0;
                    for (uint64_t row = 0; row < count; ++row) {
                        auto slice = source_slice(row);
                        if (new_values.is_null(row)) {
                            col_vec->set_null(row, true);
                            row_entries[row] = types::list_entry_t{target_offset, 0};
                            continue;
                        }
                        col_vec->append(elements, slice.offset + slice.length, slice.offset);
                        row_entries[row] = types::list_entry_t{target_offset, slice.length};
                        target_offset += slice.length;
                    }
                    col_vec->set_list_size(target_offset);
                    return core::error_t::no_error();
                }

                auto target_stride =
                    static_cast<const types::array_logical_type_extension*>(col_vec->type().extension())->size();
                auto& target_child = col_vec->entry();
                for (uint64_t row = 0; row < count; ++row) {
                    auto slice = source_slice(row);
                    if (new_values.is_null(row)) {
                        col_vec->set_null(row, true);
                        for (uint64_t j = 0; j < target_stride; ++j) {
                            target_child.set_null(row * target_stride + j, true);
                        }
                        continue;
                    }
                    uint64_t copied = std::min<uint64_t>(slice.length, target_stride);
                    if (copied > 0) {
                        vector::vector_ops::copy(elements,
                                                 target_child,
                                                 slice.offset + copied,
                                                 slice.offset,
                                                 row * target_stride);
                    }
                    for (uint64_t j = copied; j < target_stride; ++j) {
                        target_child.set_null(row * target_stride + j, true);
                    }
                }
                return core::error_t::no_error();
            }

            vector::vector_ops::copy(new_values, *col_vec, count, 0, 0);
            return core::error_t::no_error();
        }

    } // anonymous namespace

    core::error_t operator_update::apply_updates_(pipeline::context_t* pipeline_context,
                                                  vector::data_chunk_t& out_chunk,
                                                  const vector::data_chunk_t* from_chunk,
                                                  uint64_t match_count) {
        // Both sides are already aligned row-for-row, so the FROM-side merge only references them — no copy.
        const size_t right_offset = out_chunk.column_count();
        std::optional<vector::data_chunk_t> merged;
        if (from_chunk != nullptr) {
            merged.emplace(resource_, std::pmr::vector<types::complex_logical_type>{resource_}, match_count);
            merged->data.reserve(right_offset + from_chunk->column_count());
            for (const auto& column : out_chunk.data) {
                vector::vector_t vec(resource_, column.type(), match_count);
                vec.reference(column);
                merged->data.push_back(std::move(vec));
            }
            for (const auto& column : from_chunk->data) {
                vector::vector_t vec(resource_, column.type(), match_count);
                vec.reference(column);
                merged->data.push_back(std::move(vec));
            }
            merged->set_cardinality(match_count);
        }
        const vector::data_chunk_t& input = merged.has_value() ? merged.value() : out_chunk;

        if (!updates_graph_) {
            std::pmr::vector<const expressions::expression_i*> values(resource_);
            values.reserve(updates_.size());
            for (const auto& update : updates_) {
                values.push_back(update.get());
            }
            auto built = expressions::build_update_graph(resource_,
                                                         pipeline_context->parameters.parameters,
                                                         values,
                                                         input.types(),
                                                         right_offset);
            if (built.has_error()) {
                return built.error();
            }
            updates_graph_ = std::move(built.value());
        }

        auto computed = expressions::run_graph(updates_graph_.get(),
                                               pipeline_context->parameters.parameters,
                                               input,
                                               pipeline_context->execution_context);
        if (computed.has_error()) {
            return computed.error();
        }
        auto& result = computed.value();

        for (size_t i = 0; i < updates_.size(); i++) {
            result.data[i].flatten(match_count);
            if (auto error = write_target(updates_[i]->key(), result.data[i], out_chunk, match_count);
                error.contains_error()) {
                return error;
            }
        }
        return core::error_t::no_error();
    }

    void operator_update::ensure_simple_init_() {
        if (simple_init_done_) {
            return;
        }
        output_ = operators::make_operator_data(resource_, chunks_vector_t{resource_});
        simple_init_done_ = true;
    }

    core::error_t operator_update::consume_batch_(pipeline::context_t* pipeline_context,
                                                  const vector::data_chunk_t& chunk) {
        using components::vector::data_chunk_t;
        ensure_simple_init_();
        if (chunk.size() == 0) {
            return core::error_t::no_error();
        }
        auto* resource = resource_;
        auto types = chunk.types();

        if (condition_ == expressions::condition_kind::never) {
            return core::error_t::no_error();
        }
        std::optional<vector::data_chunk_t> produced;
        if (condition_ == expressions::condition_kind::computed) {
            if (!graph_) {
                auto built = expressions::build_condition_graph(resource,
                                                                pipeline_context->parameters.parameters,
                                                                expr_.get(),
                                                                types);
                if (built.has_error()) {
                    return built.error();
                }
                graph_ = std::move(built.value());
            }
            auto decided = expressions::run_graph(graph_.get(),
                                                  pipeline_context->parameters.parameters,
                                                  chunk,
                                                  pipeline_context->execution_context);
            if (decided.has_error()) {
                return decided.error();
            }
            produced = std::move(decided.value());
        }
        const vector::vector_t* decisions = produced.has_value() ? &produced->data.front() : nullptr;

        data_chunk_t out_chunk(resource, types, chunk.size());
        // Gathered with ONE indexed copy after the loop settles the row count: the cell-at-a-time overload
        // builds a fresh indexing_vector_t per call — a pmr allocation to move one value. Same trap in join_utils.
        vector::indexing_vector_t matched_indexing(resource, chunk.size());
        size_t index = 0;
        for (size_t i = 0; i < chunk.size(); ++i) {
            if (decisions != nullptr && (decisions->is_null(i) || !decisions->get_value<bool>(i))) {
                continue;
            }
            if (chunk.data.front().get_vector_type() == vector::vector_type::DICTIONARY) {
                out_chunk.row_ids.data<int64_t>()[index] =
                    static_cast<int64_t>(chunk.data.front().indexing().get_index(i));
            } else {
                out_chunk.row_ids.data<int64_t>()[index] = chunk.row_ids.data<int64_t>()[i];
            }
            matched_indexing.set_index(index, i);
            vector::validate_chunk_capacity(out_chunk, ++index);
        }
        if (index != 0) {
            for (size_t k = 0; k < chunk.column_count(); ++k) {
#ifdef DEV_MODE
                g_update_gather_copy_calls.fetch_add(1, std::memory_order_relaxed);
#endif
                vector::vector_ops::copy(chunk.data[k], out_chunk.data[k], matched_indexing, index, 0, 0);
            }
        }
        out_chunk.set_cardinality(index);
        if (index == 0) {
            return core::error_t::no_error();
        }

        // Captured BEFORE apply_updates mutates out_chunk in place — the pre-update rows for the index mirror.
        data_chunk_t old_chunk(resource, types, index);
        out_chunk.copy(old_chunk, 0);
        index_old_chunks_.emplace_back(std::move(old_chunk));

        if (auto err = apply_updates_(pipeline_context, out_chunk, nullptr, index); err.contains_error()) {
            return err;
        }
        output_->append_chunk(std::move(out_chunk));
        return core::error_t::no_error();
    }

    core::error_t operator_update::consume_join_batch_(pipeline::context_t* pipeline_context,
                                                       const vector::data_chunk_t& chunk_left,
                                                       const chunks_vector_t& right_chunks) {
        using components::vector::data_chunk_t;
        ensure_simple_init_();
        if (chunk_left.size() == 0) {
            return core::error_t::no_error();
        }
        auto* resource = resource_;
        auto types_left = chunk_left.types();
        std::pmr::vector<types::complex_logical_type> types_right(resource);
        for (const auto& rc : right_chunks) {
            if (rc.size() > 0) {
                types_right = rc.types();
                break;
            }
        }

        if (condition_ == expressions::condition_kind::never) {
            return core::error_t::no_error();
        }
        chunks_vector_t merged(resource);
        if (condition_ == expressions::condition_kind::computed) {
            if (!graph_) {
                std::pmr::vector<types::complex_logical_type> merged_types(resource);
                merged_types.reserve(types_left.size() + types_right.size());
                merged_types.insert(merged_types.end(), types_left.begin(), types_left.end());
                merged_types.insert(merged_types.end(), types_right.begin(), types_right.end());
                auto built = expressions::build_condition_graph(resource,
                                                                pipeline_context->parameters.parameters,
                                                                expr_.get(),
                                                                merged_types,
                                                                types_left.size());
                if (built.has_error()) {
                    return built.error();
                }
                graph_ = std::move(built.value());
            }
            merged.reserve(right_chunks.size());
            for (const auto& chunk_right : right_chunks) {
                merged.push_back(join_detail::merged_chunk(resource, types_left, chunk_right));
            }
        }

        data_chunk_t out_chunk(resource, types_left, chunk_left.size());
        data_chunk_t right_chunk(resource, types_right, chunk_left.size());
        // RIGHT can't gather with one indexed copy like LEFT (see consume_batch_): matched FROM rows
        // may come from different right chunks.
        vector::indexing_vector_t left_indexing(resource, chunk_left.size());
        size_t index = 0;
        for (size_t i = 0; i < chunk_left.size(); ++i) {
            // Stops once matched_total_ + this batch's index reaches affected_bound_; -1 = unbounded.
            if (affected_bound_ >= 0 && matched_total_ + index >= static_cast<uint64_t>(affected_bound_)) {
                break;
            }
            bool row_matched = false;
            for (size_t ci = 0; ci < right_chunks.size(); ++ci) {
                const auto& chunk_right = right_chunks[ci];
                if (chunk_right.size() == 0) {
                    continue;
                }
                std::optional<vector::data_chunk_t> produced;
                if (graph_) {
                    join_detail::point_at_probe_row(resource, merged[ci], chunk_left, i);
                    auto decided = expressions::run_graph(graph_.get(),
                                                          pipeline_context->parameters.parameters,
                                                          merged[ci],
                                                          pipeline_context->execution_context);
                    if (decided.has_error()) {
                        return decided.error();
                    }
                    produced = std::move(decided.value());
                }
                const vector::vector_t* decisions = produced.has_value() ? &produced->data.front() : nullptr;
                for (size_t j = 0; j < chunk_right.size(); ++j) {
                    if (decisions != nullptr && (decisions->is_null(j) || !decisions->get_value<bool>(j))) {
                        continue;
                    }
                    // Keys on the absolute row id of the matched left row, mirroring the simple path's fallback.
                    if (chunk_left.data.front().get_vector_type() == vector::vector_type::DICTIONARY) {
                        out_chunk.row_ids.data<int64_t>()[index] =
                            static_cast<int64_t>(chunk_left.data.front().indexing().get_index(i));
                    } else {
                        out_chunk.row_ids.data<int64_t>()[index] = chunk_left.row_ids.data<int64_t>()[i];
                    }
                    left_indexing.set_index(index, i);
                    for (size_t k = 0; k < chunk_right.column_count(); ++k) {
#ifdef DEV_MODE
                        g_update_gather_copy_calls.fetch_add(1, std::memory_order_relaxed);
#endif
                        vector::vector_ops::copy(chunk_right.data[k], right_chunk.data[k], j + 1, j, index);
                    }
                    ++index;
                    vector::validate_chunk_capacity(out_chunk, index);
                    vector::validate_chunk_capacity(right_chunk, index);
                    // Semi-join: a target row updates once, so stop after the first matching FROM row.
                    row_matched = true;
                    break;
                }
                if (row_matched) {
                    break;
                }
            }
        }
        if (index != 0) {
            for (size_t k = 0; k < chunk_left.column_count(); ++k) {
#ifdef DEV_MODE
                g_update_gather_copy_calls.fetch_add(1, std::memory_order_relaxed);
#endif
                vector::vector_ops::copy(chunk_left.data[k], out_chunk.data[k], left_indexing, index, 0, 0);
            }
        }
        matched_total_ += index;
        out_chunk.set_cardinality(index);
        right_chunk.set_cardinality(index);
        if (index == 0) {
            return core::error_t::no_error();
        }

        data_chunk_t old_chunk(resource, types_left, index);
        out_chunk.copy(old_chunk, 0);
        index_old_chunks_.emplace_back(std::move(old_chunk));

        if (auto err = apply_updates_(pipeline_context, out_chunk, &right_chunk, index); err.contains_error()) {
            return err;
        }
        output_->append_chunk(std::move(out_chunk));
        // Kept aligned with the updated rows so RETURNING can project joined (right-side) columns.
        if (!returning_.empty()) {
            returning_from_chunks_.emplace_back(std::move(right_chunk));
        }
        return core::error_t::no_error();
    }

    core::error_t
    operator_update::push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& /*out*/) {
        if (right_ && right_->output()) {
            return consume_join_batch_(ctx, input, right_->output()->chunks());
        }
        return consume_batch_(ctx, input);
    }

    actor_zeta::unique_future<void> operator_update::await_async_and_resume(pipeline::context_t* ctx) {
        using components::vector::data_chunk_t;
        using components::vector::vector_t;

        // Accepted into the plan but not implemented: a plain UPDATE would report SUCCESS with 0 rows instead of
        // the insert the plan declared. No SQL reaches this flag, only the logical-plan API — refuse it now.
        if (upsert_) {
            set_error(core::error_t{
                core::error_code_t::unimplemented_yet,
                std::pmr::string{"UPDATE with upsert=true: upsert semantics are not implemented — "
                                 "the plan declares an insert-or-update this engine cannot deliver",
                                 resource_}});
            mark_failed();
            co_return;
        }

        // Driven once per mid-pump buffer-full and once at the final drive; only the final call emits output.
        const bool is_final = ctx->dml_flush_is_final;

        if (output_ && output_->size() > 0) {
            components::execution_context_t exec_ctx{ctx->session,
                                                     ctx->txn,
                                                     ctx->execution_context.timezone_offset,
                                                     table_oid_};
            constexpr auto db_oid = components::catalog::well_known_oid::main_database;
            const bool mirror_index =
                table_has_indexes_ && ctx->index_address != actor_zeta::address_t::empty_address();

            // index_old_chunks_ stays in lockstep with output_->chunks() — index_old_chunks_[k] is chunk k's
            // old version. Asserted rather than silently walking left_->output(), which is empty while streaming.
            assert(index_old_chunks_.size() == output_->chunks().size());

            auto op = [&]([[maybe_unused]] std::pmr::memory_resource* res)
                -> actor_zeta::unique_future<dml_detail::flush_outcome_t> {
                // Deliberately per chunk, not hoisted. Hoisting one schema off the first non-empty
                // chunk rests on "they all agree", which nothing here establishes: chunk types come
                // from the DATA (an all-NULL segment resolves a column to NA), and data_chunk_t::copy
                // checks the column count with an assert NDEBUG strips, then skips any column whose
                // DESTINATION is NA-typed (components/vector/data_chunk.cpp:282-288). A disagreeing
                // chunk would therefore lose columns from the WAL record and the index mirror with no
                // error at all -- a silent wrong answer bought for a types() rebuild.
                auto copy_of = [this](const data_chunk_t& src) {
                    data_chunk_t dst(resource_, src.types(), src.size());
                    src.copy(dst, 0);
                    return dst;
                };

                chunks_vector_t update_data(resource_);
                std::pmr::vector<vector_t> update_row_ids(resource_);
                chunks_vector_t wal_chunks(resource_);
                std::pmr::vector<int64_t> wal_row_ids(resource_);
                chunks_vector_t idx_old(resource_);
                chunks_vector_t idx_new(resource_);
                std::pmr::vector<int64_t> idx_row_ids(resource_);

                // reserve() takes capacity to EXACTLY the requested size, so a reserve(size() + n)
                // inside the loop reallocates on every chunk -- worse than push_back's own geometric
                // growth. One exact reserve, like operator_delete.cpp:380.
                uint64_t total_row_ids = 0;
                for (const auto& probe : output_->chunks()) {
                    total_row_ids += probe.size();
                }
                wal_row_ids.reserve(total_row_ids);
                if (mirror_index) {
                    idx_row_ids.reserve(total_row_ids);
                }

                size_t out_chunk_idx = 0;
                for (auto& out_chunk : output_->chunks()) {
                    if (out_chunk.size() == 0) {
                        continue;
                    }
                    const uint64_t n = out_chunk.size();

                    vector_t row_ids(resource_, types::logical_type::BIGINT, n);
                    for (uint64_t i = 0; i < n; i++) {
                        row_ids.data<int64_t>()[i] = out_chunk.row_ids.data<int64_t>()[i];
                    }
                    update_row_ids.emplace_back(std::move(row_ids));
                    update_data.emplace_back(copy_of(out_chunk));

                    wal_chunks.emplace_back(copy_of(out_chunk));
                    for (uint64_t i = 0; i < n; i++) {
                        wal_row_ids.push_back(out_chunk.row_ids.data<int64_t>()[i]);
                    }

                    if (mirror_index) {
                        idx_old.emplace_back(std::move(index_old_chunks_[out_chunk_idx]));
                        idx_new.emplace_back(copy_of(out_chunk));
                        for (uint64_t i = 0; i < n; i++) {
                            idx_row_ids.push_back(out_chunk.row_ids.data<int64_t>()[i]);
                        }
                    }
                    ++out_chunk_idx;
                }

                if (!returning_.empty()) {
                    for (size_t i = 0; i < output_->chunks().size(); ++i) {
                        auto& out_chunk = output_->chunks()[i];
                        if (out_chunk.size() == 0) {
                            continue;
                        }
                        data_chunk_t* right_batch =
                            i < returning_from_chunks_.size() ? &returning_from_chunks_[i] : nullptr;
                        auto proj = evaluate_projection(resource_,
                                                        returning_,
                                                        &out_chunk,
                                                        ctx->parameters,
                                                        ctx->execution_context,
                                                        &returning_graph_,
                                                        right_batch);
                        if (proj.has_error()) {
                            co_return dml_detail::flush_outcome_t{proj.error()};
                        }
                        returning_accum_.emplace_back(std::move(proj.value()));
                    }
                }

#ifdef DEV_MODE
                g_update_storage_update_sends.fetch_add(1, std::memory_order_relaxed);
#endif
                auto [_u, uf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                            &services::disk::manager_disk_t::storage_update,
                                                            exec_ctx,
                                                            table_oid_,
                                                            std::move(update_row_ids),
                                                            std::move(update_data));
                auto update_result = co_await std::move(uf);
                if (update_result.has_error()) {
                    co_return dml_detail::flush_outcome_t{update_result.error()};
                }
                auto appended = update_result.value();

                // UPDATE applies to storage first, then writes its own WAL record — unlike INSERT's
                // WAL-first storage_append.
                if (ctx->wal_address != actor_zeta::address_t::empty_address()) {
                    const uint64_t wal_count = wal_row_ids.size();
                    auto [_w, wf] =
                        actor_zeta::otterbrix::send(ctx->wal_address,
                                                    &services::wal::manager_wal_replicate_t::write_physical_update,
                                                    ctx->session,
                                                    table_oid_,
                                                    std::move(wal_row_ids),
                                                    std::move(wal_chunks),
                                                    wal_count,
                                                    ctx->txn.transaction_id,
                                                    db_oid);
                    auto wal_result = co_await std::move(wf);
                    if (wal_result.has_error()) {
                        // storage_update already landed, so a WAL failure leaves table and journal disagreeing.
                        co_return dml_detail::flush_outcome_t{wal_result.error()};
                    }
                }

                if (mirror_index) {
                    auto [_ix, ixf] = actor_zeta::otterbrix::send(ctx->index_address,
                                                                  &services::index::manager_index_t::update_rows,
                                                                  exec_ctx,
                                                                  table_oid_,
                                                                  std::move(idx_old),
                                                                  std::move(idx_new),
                                                                  std::move(idx_row_ids),
                                                                  appended.start_row);
                    auto index_error = co_await std::move(ixf);
                    if (index_error.contains_error()) {
                        co_return dml_detail::flush_outcome_t{std::move(index_error), false, 0, 0};
                    }
                }

                if (returning_.empty()) {
                    affected_rows_ += appended.count;
                }

                co_return dml_detail::flush_outcome_t{core::error_t::no_error(), true, appended.start_row, appended.count};
            };

            auto outcome = co_await op(resource_);
            // output_->chunks() is safe as constraint_rows here: op only copied from it, never moved.
            auto err = dml_detail::record_flush(ctx,
                                                resource_,
                                                table_oid_,
                                                outcome,
                                                ctx->dml_has_parent_constraint,
                                                constraint_input_,
                                                output_->chunks());
            // Recorded once per txn, before the flush-error check: the storage op stamps deletes before append
            // can fail, and only a recorded marker lets storage_revert_deletes un-stamp them on abort.
            if (!delete_marker_recorded_) {
                ctx->dml_deletes.push_back(components::table::dml_delete_range_t{table_oid_, ctx->txn.transaction_id});
                delete_marker_recorded_ = true;
            }

            if (err.contains_error()) {
                set_error(err);
                mark_failed();
                co_return;
            }

            output_->chunks().clear();
            index_old_chunks_.clear();
            returning_from_chunks_.clear();
        }

        if (!is_final) {
            co_return;
        }

        // output_ was cleared per flush, so it can't double as the affected-count carrier; emit an explicit result.
        if (returning_.empty()) {
            if (affected_rows_ > 0) {
                set_output(make_operator_data(resource_,
                                              dml_detail::make_affected_count_chunks(resource_, affected_rows_, {})));
            } else {
                set_output(nullptr);
            }
        } else {
            set_output(make_operator_data(resource_, std::move(returning_accum_)));
        }
        mark_executed();
    }

} // namespace components::operators
