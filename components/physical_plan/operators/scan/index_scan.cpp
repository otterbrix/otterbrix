#include "index_scan.hpp"

#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>

namespace components::operators {

    index_scan::index_scan(std::pmr::memory_resource* resource, log_t* log,
                           collection_full_name_t name,
                           const expressions::compare_expression_ptr& expr,
                           logical_plan::limit_t limit)
        : read_only_operator_t(resource, log, operator_type::index_scan)
        , name_(std::move(name))
        , expr_(expr)
        , limit_(limit) {}

    void index_scan::on_execute_impl(pipeline::context_t* /*pipeline_context*/) {
        if (log_) {
            trace(log(), "index_scan by field \"{}\"", expr_->primary_key().as_string());
        }
        if (name_.empty()) return;
        async_wait();
    }

    actor_zeta::unique_future<void> index_scan::await_async_and_resume(pipeline::context_t* ctx) {
        if (log_) {
            trace(log(), "index_scan::await_async_and_resume on {}", name_.to_string());
        }

        if (ctx->index_address == actor_zeta::address_t::empty_address()) {
            // No index service — return empty result
            auto [_t, tf] = actor_zeta::send(ctx->disk_address,
                &services::disk::manager_disk_t::storage_types, ctx->session, name_);
            auto types = co_await std::move(tf);
            output_ = make_operator_data(resource_, types);
            mark_executed();
            co_return;
        }

        // Search index for matching row IDs
        auto [_s, sf] = actor_zeta::send(ctx->index_address,
            &services::index::manager_index_t::search,
            ctx->session, name_,
            index::keys_base_storage_t{{expr_->primary_key()}},
            types::logical_value_t{resource_, expr_->value()},
            expr_->type());
        auto row_ids_vec = co_await std::move(sf);

        // Apply limit
        size_t count = row_ids_vec.size();
        int limit_val = limit_.limit();
        if (limit_val >= 0) {
            count = std::min(count, static_cast<size_t>(limit_val));
        }

        if (count > 0) {
            // Build row_ids vector for fetch
            vector::vector_t row_ids(resource_, types::logical_type::BIGINT, count);
            for (size_t i = 0; i < count; i++) {
                row_ids.set_value(i, types::logical_value_t{resource_, row_ids_vec[i]});
            }

            // Fetch from storage
            auto [_f, ff] = actor_zeta::send(ctx->disk_address,
                &services::disk::manager_disk_t::storage_fetch, ctx->session, name_,
                std::move(row_ids), count);
            auto data = co_await std::move(ff);

            if (data) {
                output_ = make_operator_data(resource_, std::move(*data));
            } else {
                auto [_t2, tf2] = actor_zeta::send(ctx->disk_address,
                    &services::disk::manager_disk_t::storage_types, ctx->session, name_);
                auto types = co_await std::move(tf2);
                output_ = make_operator_data(resource_, types);
            }
        } else {
            auto [_t3, tf3] = actor_zeta::send(ctx->disk_address,
                &services::disk::manager_disk_t::storage_types, ctx->session, name_);
            auto types = co_await std::move(tf3);
            output_ = make_operator_data(resource_, types);
        }

        mark_executed();
        co_return;
    }

} // namespace components::operators
