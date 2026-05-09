#include "operator_insert.hpp"

#include <components/context/context.hpp>
#include <components/context/execution_context.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>

namespace components::operators {

    operator_insert::operator_insert(std::pmr::memory_resource* resource, log_t log, collection_full_name_t name)
        : read_write_operator_t(resource, log, operator_type::insert)
        , name_(std::move(name)) {}

    void operator_insert::on_execute_impl(pipeline::context_t* /*pipeline_context*/) {
        if (left_ && left_->output()) {
            output_ = left_->output();
            modified_ = operators::make_operator_write_data(resource());
        }
        if (output_ && output_->size() > 0 && !name_.empty()) {
            async_wait();
        }
    }

    actor_zeta::unique_future<void>
    operator_insert::await_async_and_resume(pipeline::context_t* ctx) {
        // Phase 5: side-effects previously implemented in
        // executor_t::intercept_dml_io_(::insert) are now self-contained here.
        using components::vector::data_chunk_t;

        if (!output_) {
            mark_executed();
            co_return;
        }
        auto& out_chunk = output_->data_chunk();
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, name_};

        // 1. Capture WAL data BEFORE storage_append moves the chunk.
        auto wal_data = std::make_unique<data_chunk_t>(resource_, out_chunk.types(), out_chunk.size());
        out_chunk.copy(*wal_data, 0);

        // 2. storage_append (handles schema adoption, _id dedup).
        auto data_copy = std::make_unique<data_chunk_t>(resource_, out_chunk.types(), out_chunk.size());
        out_chunk.copy(*data_copy, 0);
        auto [_a, af] = actor_zeta::send(ctx->disk_address,
                                          &services::disk::manager_disk_t::storage_append,
                                          exec_ctx,
                                          std::move(data_copy));
        auto [start_row, actual_count] = co_await std::move(af);

        if (actual_count == 0) {
            // Nothing inserted (e.g. duplicate _id). Clear output and drop WAL.
            set_output(nullptr);
            mark_executed();
            co_return;
        }

        // 3. WAL physical_insert (synchronous; flush is fire-and-forget).
        if (ctx->wal_address != actor_zeta::address_t::empty_address()) {
            auto [_w, wf] = actor_zeta::send(ctx->wal_address,
                                              &services::wal::manager_wal_replicate_t::write_physical_insert,
                                              ctx->session,
                                              std::string(name_.database),
                                              std::string(name_.collection),
                                              std::move(wal_data),
                                              static_cast<uint64_t>(start_row),
                                              actual_count,
                                              ctx->txn.transaction_id);
            auto wal_id = co_await std::move(wf);
            auto [_d, df] = actor_zeta::send(ctx->disk_address,
                                              &services::disk::manager_disk_t::flush,
                                              ctx->session,
                                              wal_id);
            ctx->add_pending_disk_future(std::move(df));
        }

        // 4. Mirror to index (txn-aware).
        if (ctx->index_address != actor_zeta::address_t::empty_address()) {
            auto idx_data = std::make_unique<data_chunk_t>(resource_, out_chunk.types(), out_chunk.size());
            out_chunk.copy(*idx_data, 0);
            auto [_ix, ixf] = actor_zeta::send(ctx->index_address,
                                                &services::index::manager_index_t::insert_rows,
                                                exec_ctx,
                                                std::move(idx_data),
                                                static_cast<uint64_t>(start_row),
                                                actual_count);
            co_await std::move(ixf);
        }

        // 5. Record swap-info on context for executor's commit-side block.
        ctx->dml_append_row_start = static_cast<int64_t>(start_row);
        ctx->dml_append_row_count = actual_count;
        ctx->dml_collection = name_;

        // 6. Build empty result chunk (output for downstream operators).
        data_chunk_t res_chunk(resource_, {}, actual_count);
        res_chunk.set_cardinality(actual_count);
        set_output(make_operator_data(resource_, std::move(res_chunk)));
        mark_executed();
    }

} // namespace components::operators
