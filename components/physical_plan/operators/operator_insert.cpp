#include "operator_insert.hpp"

#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <components/context/execution_context.hpp>

namespace components::operators {

    operator_insert::operator_insert(std::pmr::memory_resource* resource, log_t log,
                                     collection_full_name_t name)
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

    actor_zeta::unique_future<void> operator_insert::await_async_and_resume(pipeline::context_t* ctx) {
        auto& out_chunk = output_->data_chunk();
        execution_context_t exec_ctx{ctx->session, ctx->txn, name_};

        // storage_append (handles schema adoption, _id dedup)
        auto data_copy = std::make_unique<vector::data_chunk_t>(resource_, out_chunk.types(), out_chunk.size());
        out_chunk.copy(*data_copy, 0);
        auto [_a, af] = actor_zeta::send(ctx->disk_address,
            &services::disk::manager_disk_t::storage_append, exec_ctx, std::move(data_copy));
        auto [start_row, actual_count] = co_await std::move(af);

        append_row_start_ = static_cast<int64_t>(start_row);
        append_row_count_ = actual_count;

        if (actual_count == 0) {
            output_ = nullptr;
            mark_executed();
            co_return;
        }

        // Mirror to index (txn-aware)
        if (ctx->index_address != actor_zeta::address_t::empty_address()) {
            auto idx_data = std::make_unique<vector::data_chunk_t>(resource_, out_chunk.types(), out_chunk.size());
            out_chunk.copy(*idx_data, 0);
            auto [_ix, ixf] = actor_zeta::send(ctx->index_address,
                &services::index::manager_index_t::insert_rows_txn,
                exec_ctx, std::move(idx_data), static_cast<uint64_t>(start_row), actual_count);
            co_await std::move(ixf);
        }

        // Build result chunk
        vector::data_chunk_t result(resource_, {}, actual_count);
        result.set_cardinality(actual_count);
        output_ = make_operator_data(resource_, std::move(result));
        mark_executed();
        co_return;
    }

} // namespace components::operators
