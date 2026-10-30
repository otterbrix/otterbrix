#include "operator_insert.hpp"

#include <atomic>
#include <cassert>

#include "dml_util.hpp"
#include "scan/guard_chunk.hpp"

#include <algorithm>
#include <components/context/context.hpp>
#include <components/context/execution_context.hpp>
#include <components/vector/vector_operations.hpp>
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

    namespace {
        void put_in_target_order(vector::data_chunk_t& chunk,
                                 const logical_plan::insert_column_bindings_t& bindings,
                                 const logical_plan::insert_fill_list_t& fill) {
            const auto source_of = logical_plan::insert_target_order(chunk.resource(), bindings, fill);
            assert(source_of.size() == chunk.column_count() && "put_in_target_order: payload width is validated");
            std::vector<vector::vector_t> ordered;
            ordered.reserve(source_of.size());
            for (const auto source : source_of) {
                ordered.push_back(std::move(chunk.data[source]));
            }
            chunk.data = std::move(ordered);
        }
    } // namespace

    operator_insert::operator_insert(std::pmr::memory_resource* resource,
                                     log_t log,
                                     catalog::oid_t table_oid,
                                     table::storage_address_t address,
                                     std::pmr::string name,
                                     std::pmr::vector<catalog::oid_t> attoids,
                                     std::pmr::vector<projected_column_t> returning)
        : read_write_operator_t(resource, log, operator_type::insert)
        , table_oid_(table_oid)
        , address_(std::move(address))
        , name_(std::move(name), resource)
        , attoids_(std::move(attoids), resource)
        , returning_(std::move(returning)) {}

    std::pmr::string operator_insert::explain_label_impl() const {
        return foreign_label(resource_, address_, "Foreign Insert", name_);
    }

    actor_zeta::unique_future<core::error_t> operator_insert::open_impl(pipeline::context_t* ctx) {
        if (auto refused = dml_detail::check_constant_returning(resource_, returning_, *ctx);
            refused.contains_error()) {
            set_error(refused);
            mark_failed();
            co_return refused;
        }
        co_return core::error_t::no_error();
    }

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
                    vector::vector_t filled(resource_, column_type, capacity);
                    if (column.value.is_null(0)) {
                        filled.validity().set_all_invalid(rows);
                    } else {
                        vector::vector_ops::copy(column.value, filled, rows, 0, 0);
                    }
                    input.data.emplace_back(std::move(filled));
                }
            }
            if (!column_bindings_.empty() && !components::catalog::is_catalog_table(table_oid_)) {
                put_in_target_order(input, column_bindings_, fill_list_);
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
            auto copy_of = [this](const data_chunk_t& src) {
                data_chunk_t dst(resource_, src.types(), src.size());
                src.copy(dst, 0);
                return dst;
            };
            auto rounds = dml_detail::split_rounds(resource_, output_->chunks(), address_.max_rows_per_write());
            for (auto& round : rounds) {
                // One write message per round; the index needs the rows intact after storage_append consumed them.
                chunks_vector_t idx_chunks(resource_);
                if (mirror_index) {
                    for (const auto& chunk : round) {
                        idx_chunks.emplace_back(copy_of(chunk));
                    }
                }
                chunks_vector_t constraint_rows(resource_);
                if (ctx->dml_has_parent_constraint) {
                    for (const auto& chunk : round) {
                        constraint_rows.emplace_back(copy_of(chunk));
                    }
                }
                ctx->join_transaction(address_, table_oid_, true);
                auto [_a, af] =
                    actor_zeta::otterbrix::send<&services::disk::storage_contract::storage_append>(address_,
                                                                                                   exec_ctx,
                                                                                                   table_oid_,
                                                                                                   attoids_,
                                                                                                   std::move(round),
                                                                                                   !returning_.empty());
                auto append_result = co_await std::move(af);
                dml_detail::flush_outcome_t outcome{};
                if (append_result.has_error()) {
                    outcome.error = append_result.error();
                } else {
                    auto& receipt = append_result.value();
                    if (receipt.awaits_publish) {
                        outcome = dml_detail::flush_outcome_t{core::error_t::no_error(),
                                                              true,
                                                              receipt.start_row,
                                                              receipt.count};
                    }
                    if (mirror_index && receipt.count > 0) {
#ifdef DEV_MODE
                        g_insert_index_mirror_sends.fetch_add(1, std::memory_order_relaxed);
#endif
                        auto [_ix, ixf] = actor_zeta::otterbrix::send(ctx->index_address,
                                                                      &services::index::manager_index_t::insert_rows,
                                                                      exec_ctx,
                                                                      table_oid_,
                                                                      std::move(idx_chunks),
                                                                      static_cast<uint64_t>(receipt.start_row),
                                                                      receipt.count);
                        if (auto index_error = co_await std::move(ixf); index_error.contains_error()) {
                            outcome.error = std::move(index_error);
                        }
                    }
                    if (!outcome.error.contains_error()) {
                        written_ += receipt.count;
                        // The rows come back in the order sent: the n-th is the row numbered start_row + n, which
                        // ON CONFLICT reads to leave out the rows it removed.
                        int64_t next_row = receipt.start_row;
                        for (auto& returned : receipt.returned) {
                            if (returned.size() == 0) {
                                continue;
                            }
                            if (auto refused = check_chunk_width(resource_, returned); refused.contains_error()) {
                                outcome.error = std::move(refused);
                                break;
                            }
                            auto rows = vector::adopted(resource_, std::move(returned));
                            auto proj = evaluate_projection(resource_,
                                                            returning_,
                                                            &rows,
                                                            ctx->parameters,
                                                            ctx->execution_context,
                                                            &returning_graph_);
                            if (proj.has_error()) {
                                outcome.error = proj.error();
                                break;
                            }
                            auto* ids = proj.value().row_ids.data<int64_t>();
                            for (uint64_t row = 0; row < proj.value().size(); ++row) {
                                ids[row] = next_row++;
                            }
                            returning_accum_.emplace_back(std::move(proj.value()));
                        }
                    }
                }
                // record_flush records the appended range first, so a failed statement still reverts the rows.
                auto err = dml_detail::record_flush(ctx,
                                                    resource_,
                                                    table_oid_,
                                                    outcome,
                                                    ctx->dml_has_parent_constraint,
                                                    constraint_input_,
                                                    constraint_rows);
                if (err.contains_error()) {
                    output_->chunks().clear();
                    set_error(err);
                    mark_failed();
                    co_return;
                }
            }
            output_->chunks().clear();
        }

        if (!is_final) {
            co_return;
        }

        if (returning_.empty()) {
            set_output(nullptr);
        } else {
            if (returning_accum_.empty()) {
                // Nothing inserted, but we still have to return correct columns
                vector::data_chunk_t empty(resource_, table_types_, 0);
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
