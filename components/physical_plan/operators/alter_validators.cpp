#include "alter_validators.hpp"

#include <components/catalog/system_table_schemas.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/manager_disk.hpp>

#include <cstdint>

namespace components::operators::alter_validators {

    namespace catalog = components::catalog;

    actor_zeta::unique_future<std::pmr::vector<std::string>>
    visible_column_names(std::pmr::memory_resource* resource,
                         actor_zeta::address_t disk_address,
                         components::execution_context_t exec_ctx,
                         catalog::oid_t table_oid) {
        constexpr catalog::oid_t pg_attr_oid = catalog::well_known_oid::pg_attribute_table;

        // Key-scan pg_attribute on attrelid == table_oid.
        components::types::logical_value_t toid_lv(resource, table_oid);
        std::pmr::vector<std::string> keys(resource);
        keys.emplace_back("attrelid");
        std::pmr::vector<components::types::logical_value_t> vals(resource);
        vals.emplace_back(toid_lv);

        auto [_h, fut] = actor_zeta::send(disk_address,
                                          &services::disk::manager_disk_t::read_chunks_by_key,
                                          exec_ctx,
                                          pg_attr_oid,
                                          std::move(keys),
                                          components::operators::make_key_chunk(resource, std::move(vals)));
        std::pmr::vector<components::vector::data_chunk_t> batches = co_await std::move(fut);

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
                auto attname = chunk.value(2, i);
                if (attname.is_null())
                    continue;
                // attisdropped boolean tombstone (fast reject)
                auto attisdropped = chunk.value(7, i);
                if (!attisdropped.is_null() && attisdropped.value<bool>())
                    continue;
                auto added_at_cell = chunk.value(10, i);
                if (!added_at_cell.is_null()) {
                    const auto added_at = static_cast<std::uint64_t>(added_at_cell.value<std::int64_t>());
                    if (added_at > horizon)
                        continue;
                }
                auto dropped_at_cell = chunk.value(11, i);
                if (!dropped_at_cell.is_null()) {
                    const auto dropped_at = static_cast<std::uint64_t>(dropped_at_cell.value<std::int64_t>());
                    if (dropped_at != 0 && dropped_at <= horizon)
                        continue;
                }
                out.emplace_back(std::string(attname.value<std::string_view>()));
            }
        }
        co_return out;
    }

    actor_zeta::unique_future<std::pmr::vector<std::pair<int, catalog::oid_t>>>
    scan_cascade_dependents(std::pmr::memory_resource* resource,
                            actor_zeta::address_t disk_address,
                            components::execution_context_t exec_ctx,
                            catalog::oid_t ref_classid,
                            catalog::oid_t ref_objid,
                            std::int32_t /*ref_objsubid*/) {
        constexpr catalog::oid_t pg_dep_oid = catalog::well_known_oid::pg_depend_table;

        // pg_depend column layout (system_table_schemas.cpp::pg_depend_columns):
        //   [0]=classid, [1]=objid, [2]=refclassid, [3]=refobjid, [4]=deptype.
        // TBD-impl: pg_depend has no refobjsubid (column-grain subobject id) yet,
        // so ref_objsubid is ignored and we return ALL dependents of refobj. This
        // is conservative: RESTRICT over-rejects, CASCADE over-drops.
        components::types::logical_value_t refcls_lv(resource, ref_classid);
        components::types::logical_value_t refobj_lv(resource, ref_objid);
        std::pmr::vector<std::string> keys(resource);
        keys.emplace_back("refclassid");
        keys.emplace_back("refobjid");
        std::pmr::vector<components::types::logical_value_t> vals(resource);
        vals.emplace_back(refcls_lv);
        vals.emplace_back(refobj_lv);

        auto [_h, fut] = actor_zeta::send(disk_address,
                                          &services::disk::manager_disk_t::read_chunks_by_key,
                                          exec_ctx,
                                          pg_dep_oid,
                                          std::move(keys),
                                          components::operators::make_key_chunk(resource, std::move(vals)));
        std::pmr::vector<components::vector::data_chunk_t> batches = co_await std::move(fut);

        std::pmr::vector<std::pair<int, catalog::oid_t>> out(resource);
        for (auto& chunk : batches) {
            if (chunk.column_count() < 5)
                continue;
            out.reserve(out.size() + chunk.size());
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                auto classid_cell = chunk.value(0, i);
                auto objid_cell = chunk.value(1, i);
                if (classid_cell.is_null() || objid_cell.is_null())
                    continue;
                const auto classid = static_cast<catalog::oid_t>(classid_cell.value<std::uint32_t>());
                const auto objid = static_cast<catalog::oid_t>(objid_cell.value<std::uint32_t>());
                out.emplace_back(static_cast<int>(classid), objid);
            }
        }
        co_return out;
    }

} // namespace components::operators::alter_validators
