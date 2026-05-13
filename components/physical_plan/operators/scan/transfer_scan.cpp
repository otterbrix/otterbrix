#include "transfer_scan.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/types/logical_value.hpp>
#include <services/disk/manager_disk.hpp>

#include <cstdint>
#include <limits>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace components::operators {

    namespace catalog = components::catalog;

    transfer_scan::transfer_scan(std::pmr::memory_resource* resource,
                                 components::catalog::oid_t table_oid,
                                 logical_plan::limit_t limit)
        : read_only_operator_t(resource, log_t{}, operator_type::transfer_scan)
        , table_oid_(table_oid)
        , limit_(limit) {}

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

        // Group 1 (dynamic_schema_drop_column): row_group physically keeps
        // tombstoned columns until VACUUM. Project the chunk down to live
        // columns via pg_computed_column for relkind='g' tables. 'r' tables
        // pass through unchanged.
        if (data && data->column_count() > 0 &&
            ctx->disk_address != actor_zeta::address_t::empty_address()) {
            constexpr catalog::oid_t kPgClass = catalog::well_known_oid::pg_class_table;
            constexpr catalog::oid_t kPgComputedColumn = catalog::well_known_oid::pg_computed_column_table;
            components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

            types::logical_value_t toid_lv(resource_, static_cast<std::uint32_t>(table_oid_));
            auto [_pc, pcf] = actor_zeta::send(
                ctx->disk_address,
                &services::disk::manager_disk_t::read_rows_by_key,
                exec_ctx, kPgClass,
                std::vector<std::string>{"oid"},
                std::vector<types::logical_value_t>{toid_lv});
            auto pc_rows = co_await std::move(pcf);

            char relkind = 0;
            if (!pc_rows.empty() && pc_rows[0].size() >= 4 && !pc_rows[0][3].is_null()) {
                auto rk = pc_rows[0][3].value<std::string_view>();
                if (!rk.empty()) relkind = rk.front();
            }

            if (relkind == 'g') {
                auto [_cc, ccf] = actor_zeta::send(
                    ctx->disk_address,
                    &services::disk::manager_disk_t::read_rows_by_key,
                    exec_ctx, kPgComputedColumn,
                    std::vector<std::string>{"relid"},
                    std::vector<types::logical_value_t>{toid_lv});
                auto cc_rows = co_await std::move(ccf);

                // Max-version per attname; rc>0 = live.
                struct latest_t {
                    std::int64_t version{std::numeric_limits<std::int64_t>::min()};
                    std::int64_t refcount{0};
                };
                std::unordered_map<std::string, latest_t> latest;
                for (const auto& row : cc_rows) {
                    if (row.size() < 7) continue;
                    if (row[2].is_null() || row[5].is_null() || row[6].is_null()) continue;
                    std::string attname{row[2].value<std::string_view>()};
                    const auto ver = row[5].value<std::int64_t>();
                    const auto rc = row[6].value<std::int64_t>();
                    auto it = latest.find(attname);
                    if (it == latest.end() || it->second.version < ver) {
                        latest[attname] = latest_t{ver, rc};
                    }
                }
                std::unordered_set<std::string> live_names;
                for (auto& [name, lr] : latest) {
                    if (lr.refcount > 0) live_names.insert(name);
                }

                // Only filter when there's a column to drop — avoid the
                // std::move/no-replace bug from prior attempts (moving all
                // columns into `kept` while kept.size()==orig leaves
                // data->data as all moved-from vectors).
                bool need_filter = false;
                for (const auto& col : data->data) {
                    if (!col.type().has_alias() ||
                        live_names.find(std::string(col.type().alias())) == live_names.end()) {
                        need_filter = true;
                        break;
                    }
                }
                if (need_filter) {
                    std::vector<components::vector::vector_t> kept;
                    kept.reserve(data->data.size());
                    for (auto& col : data->data) {
                        if (col.type().has_alias() &&
                            live_names.find(std::string(col.type().alias())) != live_names.end()) {
                            kept.push_back(std::move(col));
                        }
                    }
                    data->data = std::move(kept);
                }
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
