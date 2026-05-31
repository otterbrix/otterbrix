// ALTER atomic validation helpers — async data-gathering layer.
// See alter_validators.hpp for the design contract.

#include "alter_validators.hpp"

#include <components/catalog/system_table_schemas.hpp>
#include <components/types/logical_value.hpp>
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
                                          &services::disk::manager_disk_t::read_rows_by_key,
                                          exec_ctx,
                                          pg_attr_oid,
                                          std::move(keys),
                                          std::move(vals));
        auto rows = co_await std::move(fut);

        // pg_attribute layout (system_table_schemas.cpp::pg_attribute_columns):
        //   [0]=attoid, [1]=attrelid, [2]=attname, [3]=atttypid, [4]=attnum,
        //   [5]=attnotnull, [6]=atthasdefault, [7]=attisdropped, [8]=atttypspec,
        //   [9]=attdefspec, [10]=added_at_commit_id, [11]=dropped_at_commit_id.
        //
        //   added_at_commit_id <= snapshot_horizon AND
        //   (dropped_at_commit_id == 0 OR dropped_at_commit_id > snapshot_horizon).
        //
        // attisdropped is the legacy boolean tombstone kept in lockstep with
        // dropped_at_commit_id > 0 (see pg_attribute_columns comment) — we skip
        // any row where either signal indicates "dropped relative to me".
        const std::uint64_t horizon = exec_ctx.txn.snapshot_horizon;

        std::pmr::vector<std::string> out(resource);
        out.reserve(rows.size());
        for (const auto& row : rows) {
            if (row.size() < 12)
                continue;
            // attname required
            if (row[2].is_null())
                continue;
            // attisdropped boolean tombstone (fast reject)
            if (!row[7].is_null() && row[7].value<bool>())
                continue;
            if (!row[10].is_null()) {
                const auto added_at = static_cast<std::uint64_t>(row[10].value<std::int64_t>());
                if (added_at > horizon)
                    continue;
            }
            // dropped_at_commit_id == 0 OR > snapshot_horizon
            if (!row[11].is_null()) {
                const auto dropped_at = static_cast<std::uint64_t>(row[11].value<std::int64_t>());
                if (dropped_at != 0 && dropped_at <= horizon)
                    continue;
            }
            out.emplace_back(std::string(row[2].value<std::string_view>()));
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

        // pg_depend layout (system_table_schemas.cpp::pg_depend_columns):
        //   [0]=classid, [1]=objid, [2]=refclassid, [3]=refobjid, [4]=deptype.
        //
        // TBD-impl: pg_depend currently does not carry refobjsubid (the
        // column-level subobject id). The ref_objsubid parameter is accepted
        // so callers can be written against the final signature, but until the
        // schema is extended we return ALL dependents of the refobj (table)
        // rather than just those tied to a specific column attnum. This is
        // conservative — RESTRICT will over-reject, CASCADE will over-drop —
        // matching the legacy operator_alter_column_drop scan that keys on
        // (refclassid=pg_attribute_table, refobjid=attoid) for column-grain
        // entries written by ADD COLUMN; whole-table dependencies still flow
        // through this helper.
        components::types::logical_value_t refcls_lv(resource, ref_classid);
        components::types::logical_value_t refobj_lv(resource, ref_objid);
        std::pmr::vector<std::string> keys(resource);
        keys.emplace_back("refclassid");
        keys.emplace_back("refobjid");
        std::pmr::vector<components::types::logical_value_t> vals(resource);
        vals.emplace_back(refcls_lv);
        vals.emplace_back(refobj_lv);

        auto [_h, fut] = actor_zeta::send(disk_address,
                                          &services::disk::manager_disk_t::read_rows_by_key,
                                          exec_ctx,
                                          pg_dep_oid,
                                          std::move(keys),
                                          std::move(vals));
        auto rows = co_await std::move(fut);

        std::pmr::vector<std::pair<int, catalog::oid_t>> out(resource);
        out.reserve(rows.size());
        for (const auto& row : rows) {
            if (row.size() < 5)
                continue;
            if (row[0].is_null() || row[1].is_null())
                continue;
            const auto classid = static_cast<catalog::oid_t>(row[0].value<std::uint32_t>());
            const auto objid = static_cast<catalog::oid_t>(row[1].value<std::uint32_t>());
            out.emplace_back(static_cast<int>(classid), objid);
        }
        co_return out;
    }

} // namespace components::operators::alter_validators
