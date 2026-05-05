#include "index_scan.hpp"

#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>

namespace components::operators {

    index_scan::index_scan(std::pmr::memory_resource* resource,
                           log_t log,
                           collection_full_name_t name,
                           const expressions::key_t& key,
                           const types::logical_value_t& value,
                           expressions::compare_type compare_type,
                           logical_plan::limit_t limit)
        : read_only_operator_t(resource, log, operator_type::index_scan)
        , name_(std::move(name))
        , key_(key)
        , value_(value)
        , compare_type_(compare_type)
        , limit_(limit) {}

    void index_scan::on_execute_impl(pipeline::context_t* /*pipeline_context*/) {
        if (log_.is_valid()) {
            trace(log(), "index_scan by field \"{}\"", key_.as_string());
        }
        if (name_.empty())
            return;
        async_wait();
    }

    actor_zeta::unique_future<void> index_scan::await_async_and_resume(pipeline::context_t* ctx) {
        if (log_.is_valid()) {
            trace(log(), "index_scan::await_async_and_resume on {}", name_.to_string());
        }

        if (ctx->index_address == actor_zeta::address_t::empty_address()) {
            // No index service — return empty result
            auto [_t, tf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::storage_types,
                                             ctx->session,
                                             name_);
            auto types = co_await std::move(tf);
            output_ = make_operator_data(resource_, types);
            mark_executed();
            co_return;
        }

        // Search index for matching row IDs (txn-aware visibility)
        std::pmr::vector<int64_t> row_ids_vec(resource_);
        auto [_s, sf] = actor_zeta::send(ctx->index_address,
                                         &services::index::manager_index_t::search,
                                         ctx->session,
                                         name_,
                                         index::keys_base_storage_t{{key_}},
                                         types::logical_value_t{resource_, value_},
                                         compare_type_,
                                         ctx->txn.start_time,
                                         ctx->txn.transaction_id);
        row_ids_vec = co_await std::move(sf);

        // Apply offset and limit
        size_t total = row_ids_vec.size();
        size_t offset_val = static_cast<size_t>(std::max(int64_t{0}, limit_.offset()));
        size_t start = std::min(offset_val, total);
        size_t available = total - start;
        int64_t limit_val = limit_.limit();
        size_t count = (limit_val >= 0) ? std::min(available, static_cast<size_t>(limit_val)) : available;

        if (count > 0) {
            // Build row_ids vector for fetch
            vector::vector_t row_ids(resource_, types::logical_type::BIGINT, count);
            std::memcpy(row_ids.data(), row_ids_vec.data() + start, count * sizeof(int64_t));

            // Fetch from storage
            auto [_f, ff] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::storage_fetch,
                                             ctx->session,
                                             name_,
                                             std::move(row_ids),
                                             count);
            auto data = co_await std::move(ff);

            if (data) {
                output_ = make_operator_data(resource_, split_chunk_into_batches(resource_, std::move(*data)));
            } else {
                auto [_t2, tf2] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::storage_types,
                                                   ctx->session,
                                                   name_);
                auto types = co_await std::move(tf2);
                output_ = make_operator_data(resource_, types);
            }
        } else {
            auto [_t3, tf3] = actor_zeta::send(ctx->disk_address,
                                               &services::disk::manager_disk_t::storage_types,
                                               ctx->session,
                                               name_);
            auto types = co_await std::move(tf3);
            output_ = make_operator_data(resource_, types);
        }

        mark_executed();
        co_return;
    }

} // namespace components::operators
