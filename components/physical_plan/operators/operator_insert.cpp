#include "operator_insert.hpp"

#include <atomic>

#include "dml_util.hpp"

#include <algorithm>
#include <components/context/context.hpp>
#include <components/context/execution_context.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>

namespace components::operators {

#ifdef DEV_MODE
    namespace {
        std::atomic<uint64_t> g_insert_index_mirror_sends{0};
    } // namespace
    uint64_t insert_index_mirror_sends() noexcept {
        return g_insert_index_mirror_sends.load(std::memory_order_relaxed);
    }
    void reset_insert_index_mirror_sends() noexcept { g_insert_index_mirror_sends.store(0, std::memory_order_relaxed); }
#endif

    operator_insert::operator_insert(std::pmr::memory_resource* resource,
                                     log_t log,
                                     catalog::oid_t table_oid,
                                     std::pmr::vector<projected_column_t> returning)
        : read_write_operator_t(resource, log, operator_type::insert)
        , table_oid_(table_oid)
        , returning_(std::move(returning)) {}

    core::error_t
    operator_insert::push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& /*out*/) {
        // Streaming DML sink: folds batches into a bounded accumulator; out stays empty.
        if (!output_) {
            output_ = make_operator_data(resource_, chunks_vector_t{resource_});
            modified_ = make_operator_write_data(resource());
        }
        if (input.size() > 0) {
            const uint64_t bound = std::min<uint64_t>(input.column_count(), column_bindings_.size());
            for (uint64_t i = 0; i < bound; ++i) {
                const auto& binding = column_bindings_[i];
                if (!binding.cast) {
                    input.data[i].set_type_alias(std::string(binding.target_name));
                    continue;
                }
                auto target_type = binding.target_type;
                target_type.set_alias(std::string(binding.target_name));
                vector::vector_t casted(resource_, target_type, input.size());
                auto error =
                    binding.cast(casts::cast_kind::cast, input.data[i], &casted, ctx->execution_context, input.size());
                if (error.contains_error()) {
                    return error;
                }
                input.data[i] = std::move(casted);
            }
            // DEFAULT expansion, above the journal, so storage_append/WAL/constraints see one full-width row.
            if (!fill_list_.empty() && !components::catalog::is_catalog_table(table_oid_)) {
                const uint64_t rows = input.size();
                const uint64_t capacity = input.capacity();
                input.data.reserve(input.data.size() + fill_list_.size());
                for (const auto& column : fill_list_) {
                    auto column_type = column.type;
                    column_type.set_alias(std::string{column.name.c_str()});
                    if (column.value.is_null()) {
                        vector::vector_t filled(resource_, column_type, capacity);
                        filled.validity().set_all_invalid(rows);
                        input.data.emplace_back(std::move(filled));
                        continue;
                    }
                    vector::vector_t filled(resource_, column.value, capacity);
                    filled.flatten(rows);
                    filled.set_type_alias(std::string{column.name.c_str()});
                    input.data.emplace_back(std::move(filled));
                }
            }
            output_->append_chunk(std::move(input));
        }
        return core::error_t::no_error();
    }

    actor_zeta::unique_future<void> operator_insert::await_async_and_resume(pipeline::context_t* ctx) {
        using components::vector::data_chunk_t;

        const bool is_final = ctx->dml_flush_is_final;
        components::execution_context_t exec_ctx{ctx->session,
                                                 ctx->txn,
                                                 ctx->execution_context.timezone_offset,
                                                 table_oid_};

        // WAL-first append_pg_catalog_row; must land in ctx->pg_catalog_appends for commit to publish.
        if (components::catalog::is_catalog_table(table_oid_)) {
            if (output_ && output_->size() > 0) {
                for (auto& out_chunk : output_->chunks()) {
                    if (out_chunk.size() == 0) {
                        continue;
                    }
                    data_chunk_t row(resource_, out_chunk.types(), out_chunk.size());
                    out_chunk.copy(row, 0);
                    auto [_c, cf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                                &services::disk::manager_disk_t::append_pg_catalog_row,
                                                                exec_ctx,
                                                                table_oid_,
                                                                std::move(row));
                    auto rng_r = co_await std::move(cf);
                    if (rng_r.has_error()) {
                        set_error(rng_r.error());
                        mark_failed();
                        co_return;
                    }
                    if (rng_r.value().count > 0) {
                        ctx->pg_catalog_appends.push_back(std::move(rng_r.value()));
                    }
                }
            }
            set_output(nullptr);
            mark_executed();
            co_return;
        }

        // register_collection always creates an index engine; enrich's table-has-index flag is the real signal.
        const bool mirror_index = table_has_indexes_ && ctx->index_address != actor_zeta::address_t::empty_address();

        if (output_ && output_->size() > 0) {
            auto op = [&]([[maybe_unused]] std::pmr::memory_resource* res)
                -> actor_zeta::unique_future<dml_detail::flush_outcome_t> {
                auto copy_of = [this](const data_chunk_t& src) {
                    data_chunk_t dst(resource_, src.types(), src.size());
                    src.copy(dst, 0);
                    return dst;
                };

                // Copied up front — storage_append consumes its copy, index needs the rows intact.
                // WAL is written WAL-first inside storage_append (atomic in the disk agent).
                chunks_vector_t append_data(resource_);
                chunks_vector_t idx_chunks(resource_);
                for (auto& out_chunk : output_->chunks()) {
                    if (out_chunk.size() == 0) {
                        continue;
                    }
                    append_data.emplace_back(copy_of(out_chunk));
                    if (mirror_index) {
                        idx_chunks.emplace_back(copy_of(out_chunk));
                    }
                }
                if (append_data.empty()) {
                    co_return dml_detail::flush_outcome_t{};
                }

                auto [_a, af] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                            &services::disk::manager_disk_t::storage_append,
                                                            exec_ctx,
                                                            table_oid_,
                                                            std::move(append_data));
                auto append_result = co_await std::move(af);
                if (append_result.has_error()) {
                    co_return dml_detail::flush_outcome_t{append_result.error()};
                }
                auto [start_row, count] = append_result.value();

                if (mirror_index && count > 0) {
#ifdef DEV_MODE
                    g_insert_index_mirror_sends.fetch_add(1, std::memory_order_relaxed);
#endif
                    auto [_ix, ixf] = actor_zeta::otterbrix::send(ctx->index_address,
                                                                  &services::index::manager_index_t::insert_rows,
                                                                  exec_ctx,
                                                                  table_oid_,
                                                                  std::move(idx_chunks),
                                                                  start_row,
                                                                  count);
                    auto index_error = co_await std::move(ixf);
                    if (index_error.contains_error()) {
                        co_return dml_detail::flush_outcome_t{std::move(index_error), false, 0, 0};
                    }
                }

                if (returning_.empty()) {
                    affected_rows_ += count;
                } else if (count > 0) {
                    // A point read: the reply range IS the ids just written (generated columns need it).
                    vector::vector_t row_ids(resource_, types::logical_type::BIGINT, count);
                    auto* ids = row_ids.data<int64_t>();
                    for (uint64_t i = 0; i < count; i++) {
                        ids[i] = static_cast<int64_t>(start_row + i);
                    }
                    auto [_s, sf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                                &services::disk::manager_disk_t::storage_fetch,
                                                                ctx->session,
                                                                table_oid_,
                                                                std::move(row_ids),
                                                                count,
                                                                std::vector<size_t>{},
                                                                ctx->txn,
                                                                components::table::fetch_visibility_t::SNAPSHOT,
                                                                /*limit=*/int64_t{-1},
                                                                services::disk::k_fetch_epoch_unchecked);
                    auto segments_r = co_await std::move(sf);
                    if (segments_r.has_error()) {
                        // Must fail rather than return empty RETURNING cells; the range travels with the error.
                        co_return dml_detail::flush_outcome_t{segments_r.error(),
                                                              true,
                                                              static_cast<int64_t>(start_row),
                                                              count};
                    }
                    auto segments = std::move(segments_r.value());
                    for (auto& seg : segments) {
                        if (seg.size() == 0) {
                            continue;
                        }
                        auto proj = evaluate_projection(resource_,
                                                        returning_,
                                                        &seg,
                                                        ctx->parameters,
                                                        ctx->execution_context,
                                                        &returning_graph_);
                        if (proj.has_error()) {
                            co_return dml_detail::flush_outcome_t{proj.error(),
                                                                  true,
                                                                  static_cast<int64_t>(start_row),
                                                                  count};
                        }
                        returning_accum_.emplace_back(std::move(proj.value()));
                    }
                }

                co_return dml_detail::flush_outcome_t{core::error_t::no_error(),
                                                      true,
                                                      static_cast<int64_t>(start_row),
                                                      count};
            };

            auto outcome = co_await op(resource_);
            // record_flush needs output_->chunks() from the just-flushed rows before they're cleared.
            auto err = dml_detail::record_flush(ctx,
                                                resource_,
                                                table_oid_,
                                                outcome,
                                                ctx->dml_has_parent_constraint,
                                                constraint_input_,
                                                output_->chunks());
            if (err.contains_error()) {
                set_error(err);
                mark_failed();
                co_return;
            }
            output_->chunks().clear();
        }

        if (!is_final) {
            co_return;
        }

        if (returning_.empty()) {
            if (affected_rows_ != 0) {
                set_output(make_operator_data(resource_,
                                              dml_detail::make_affected_count_chunks(resource_, affected_rows_, {})));
            } else {
                set_output(nullptr);
            }
        } else {
            if (returning_accum_.empty()) {
                // Nothing inserted, but we still have to return correct columns
                auto [_rt, rtf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                              &services::disk::manager_disk_t::storage_types,
                                                              ctx->session,
                                                              table_oid_);
                auto returning_types = co_await std::move(rtf);
                if (returning_types.has_error()) {
                    set_error(returning_types.error());
                    mark_failed();
                    co_return;
                }
                vector::data_chunk_t empty(resource_, returning_types.value(), 0);
                empty.set_cardinality(0);
                auto proj = evaluate_projection(resource_,
                                                returning_,
                                                &empty,
                                                ctx->parameters,
                                                ctx->execution_context,
                                                &returning_graph_);
                if (proj.has_error()) {
                    set_error(proj.error());
                    mark_failed();
                    co_return;
                }
                returning_accum_.emplace_back(std::move(proj.value()));
            }
            set_output(make_operator_data(resource_, std::move(returning_accum_)));
        }
        mark_executed();
    }

} // namespace components::operators
