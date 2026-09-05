#include "alter_validators.hpp"

#include <components/catalog/helpers.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/manager_disk.hpp>

#include <cstdint>

namespace components::operators::alter_validators {

    namespace catalog = components::catalog;

    actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<std::string>>>
    visible_column_names(std::pmr::memory_resource* resource,
                         actor_zeta::address_t disk_address,
                         components::execution_context_t exec_ctx,
                         catalog::oid_t table_oid) {
        constexpr catalog::oid_t pg_attr_oid = catalog::well_known_oid::pg_attribute_table;

        // Key-scan pg_attribute on attrelid == table_oid.
        std::pmr::vector<std::uint64_t> keys(resource);
        keys.emplace_back(catalog::pg_attribute_col::attrelid);

        auto [_h, fut] = actor_zeta::send(disk_address,
                                          &services::disk::manager_disk_t::read_chunks_by_key,
                                          exec_ctx,
                                          pg_attr_oid,
                                          std::move(keys),
                                          components::operators::make_key_chunk(resource, table_oid),
                                          std::pmr::vector<std::uint64_t>{resource});
        auto batches_r = co_await std::move(fut);
        if (batches_r.has_error()) {
            co_return batches_r.error();
        }
        auto& batches = batches_r.value();

        // pg_attribute column layout (system_table_schemas.cpp::pg_attribute_columns):
        //   [0]=attoid, [1]=attrelid, [2]=attname, [3]=atttypid, [4]=attnum,
        //   [5]=attnotnull, [6]=atthasdefault, [7]=attisdropped, [8]=atttypspec,
        //   [9]=attdefspec, [10]=added_at_commit_id, [11]=dropped_at_commit_id.
        // Keep a row only if visible to this snapshot: not tombstoned, and
        //   added_at_commit_id <= horizon AND (dropped_at == 0 OR dropped_at > horizon).
        const std::uint64_t horizon = exec_ctx.txn.snapshot_horizon;

        std::pmr::vector<std::string> out(resource);
        for (auto& chunk : batches) {
            if (chunk.column_count() < 12)
                continue;
            out.reserve(out.size() + chunk.size());
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                // attname required
                if (chunk.is_null(2, i))
                    continue;
                // attisdropped boolean tombstone (fast reject)
                if (!chunk.is_null(7, i) && chunk.get_value<bool>(7, i))
                    continue;
                if (!chunk.is_null(10, i)) {
                    const auto added_at = static_cast<std::uint64_t>(chunk.get_value<std::int64_t>(10, i));
                    if (added_at > horizon)
                        continue;
                }
                if (!chunk.is_null(11, i)) {
                    const auto dropped_at = static_cast<std::uint64_t>(chunk.get_value<std::int64_t>(11, i));
                    if (dropped_at != 0 && dropped_at <= horizon)
                        continue;
                }
                out.emplace_back(std::string(chunk.get_value<std::string_view>(2, i)));
            }
        }
        co_return out;
    }

} // namespace components::operators::alter_validators
