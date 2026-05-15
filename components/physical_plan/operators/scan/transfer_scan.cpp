#include "transfer_scan.hpp"

#include <services/disk/manager_disk.hpp>

#include <string>
#include <utility>
#include <vector>

namespace components::operators {

    transfer_scan::transfer_scan(std::pmr::memory_resource* resource,
                                 components::catalog::oid_t table_oid,
                                 logical_plan::limit_t limit,
                                 std::vector<std::string> live_column_aliases)
        : read_only_operator_t(resource, log_t{}, operator_type::transfer_scan)
        , table_oid_(table_oid)
        , limit_(limit)
        , has_projection_(!live_column_aliases.empty()) {
        if (has_projection_) {
            live_column_aliases_.reserve(live_column_aliases.size());
            for (auto& name : live_column_aliases) {
                live_column_aliases_.insert(std::move(name));
            }
        }
    }

    void transfer_scan::on_execute_impl(pipeline::context_t* /*pipeline_context*/) {
        if (table_oid_ == components::catalog::INVALID_OID)
            return;
        async_wait();
    }

    actor_zeta::unique_future<void> transfer_scan::await_async_and_resume(pipeline::context_t* ctx) {
        int limit_val = limit_.limit();
        auto [_s, sf] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::storage_scan,
                                         ctx->session,
                                         table_oid_,
                                         std::unique_ptr<table::table_filter_t>(nullptr),
                                         limit_val,
                                         ctx->txn);
        auto data = co_await std::move(sf);

        // Project the chunk down to live columns when the plan generator
        // supplied an alias mask. For relkind='g' (dynamic schema) tables
        // row_group keeps tombstoned columns until VACUUM; resolve_table
        // operator filters them out and the planner forwards the surviving
        // alias list here. relkind='r' tables get an empty mask (no filtering).
        if (data && data->column_count() > 0 && has_projection_) {
            bool need_filter = false;
            for (const auto& col : data->data) {
                if (!col.type().has_alias() ||
                    live_column_aliases_.find(std::string(col.type().alias())) == live_column_aliases_.end()) {
                    need_filter = true;
                    break;
                }
            }
            if (need_filter) {
                std::vector<components::vector::vector_t> kept;
                kept.reserve(data->data.size());
                for (auto& col : data->data) {
                    if (col.type().has_alias() &&
                        live_column_aliases_.find(std::string(col.type().alias())) != live_column_aliases_.end()) {
                        kept.push_back(std::move(col));
                    }
                }
                data->data = std::move(kept);
            }
        }

        if (data) {
            output_ = make_operator_data(resource_, std::move(*data));
        } else {
            output_ = make_operator_data(resource_, std::pmr::vector<types::complex_logical_type>{resource_});
        }
        mark_executed();
        co_return;
    }

} // namespace components::operators