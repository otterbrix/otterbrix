#include "operator_computed_field_unregister.hpp"

#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <services/disk/manager_disk.hpp>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace components::operators {

    namespace catalog = components::catalog;

    operator_computed_field_unregister_t::operator_computed_field_unregister_t(
        std::pmr::memory_resource* resource,
        log_t                       log,
        catalog::oid_t              table_oid,
        catalog::oid_t              attoid,
        std::string                 column_name)
        : read_write_operator_t(resource, std::move(log), operator_type::computed_field_unregister)
        , table_oid_(table_oid)
        , attoid_(attoid)
        , column_name_(std::move(column_name)) {}

    void operator_computed_field_unregister_t::on_execute_impl(pipeline::context_t* /*ctx*/) {
        async_wait();
    }

    actor_zeta::unique_future<void>
    operator_computed_field_unregister_t::await_async_and_resume(pipeline::context_t* ctx) {
        // Phase 7 #109: concurrent INSERT registering same field while ALTER DROP
        // is in flight. MVCC isolation: each txn sees its own snapshot of
        // pg_computed_column. Three orderings possible:
        //   1. ALTER commits first, INSERT sees tombstone -> register skips (refcount<=0
        //      tombstone treated as "field exists but dead"; resolver hides it).
        //      INSERT data lands in storage column 'x' but is not exposed by reader.
        //   2. INSERT commits first, ALTER tombstone applied later -> field hidden post-ALTER.
        //   3. Both commit independently — resolver max(attversion) determines visibility.
        //
        // This sometimes produces "ghost data" (storage has values for a column the
        // reader hides). VACUUM physical-compaction (P7.5b TODO) would reclaim;
        // until then, ghost data is harmless (invisible to user).
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        constexpr catalog::oid_t pg_computed_column = catalog::well_known_oid::pg_computed_column_table;

        // Phase 9.B: routing by attoid (pre-stamped by enrich_logical_plan).
        // INVALID_OID means the resolver couldn't find a live computed column —
        // treat as idempotent no-op (matches the prior attname-scan miss).
        // Group 1: planner creates this operator directly from ALTER's
        // sub-clause without an enrich pass (planner.cpp:585-598), so
        // attoid_ is INVALID by default. Fall through to the
        // pg_computed_column scan below and match by attname instead of
        // attoid (the pre-existing attoid path remains a fast path for
        // callers that do stamp it).

        // Step 1: scan pg_computed_column rows for this relid (table is small
        // per-table, perf OK), filter by attoid in-callback. Cannot read by
        // attoid alone because the same logical (relid, attoid) row may have
        // multiple version entries; we still need max(attversion).
        // pg_computed_column layout (Phase 11.F-B): 0=relid 1=attoid 2=attname
        // 3=atttypid 4=atttypspec 5=attversion 6=attrefcount.
        types::logical_value_t toid_lv(resource_, table_oid_);
        auto [_r, rf] = actor_zeta::send(
            ctx->disk_address,
            &services::disk::manager_disk_t::read_rows_by_key,
            exec_ctx, pg_computed_column,
            std::vector<std::string>{"relid"},
            std::vector<types::logical_value_t>{toid_lv});
        auto rows = co_await std::move(rf);

        // Step 2: pick the latest live row matching attoid_ (max attversion AND attrefcount > 0).
        std::int64_t   max_version  = -1;
        catalog::oid_t live_attoid  = catalog::INVALID_OID;
        catalog::oid_t live_atttypid = catalog::INVALID_OID;
        bool found_live = false;
        for (const auto& row : rows) {
            if (row.size() < 7) continue;
            if (row[1].is_null() || row[5].is_null() || row[6].is_null()) continue;
            const auto row_attoid = static_cast<catalog::oid_t>(row[1].value<std::uint32_t>());
            // Match by attoid when enrich stamped it; otherwise fall back to
            // matching by attname (column_name_).
            if (attoid_ != catalog::INVALID_OID) {
                if (row_attoid != attoid_) continue;
            } else {
                if (row[2].is_null()) continue;
                if (row[2].value<std::string_view>() != column_name_) continue;
            }
            const auto v  = row[5].value<std::int64_t>();
            const auto rc = row[6].value<std::int64_t>();
            if (rc <= 0) continue;
            if (v > max_version) {
                max_version    = v;
                live_attoid    = row_attoid;
                live_atttypid  = row[3].is_null()
                                    ? catalog::INVALID_OID
                                    : static_cast<catalog::oid_t>(row[3].value<std::uint32_t>());
                found_live = true;
            }
        }
        if (!found_live) {
            // Nothing alive to drop: idempotent no-op.
            mark_executed();
            co_return;
        }

        // Step 3: append a tombstone row (same attoid + same atttypid, version =
        // max + 1, refcount = 0). Reusing the live attoid keeps any pg_depend
        // attrefs valid; the reader filters this row out via the refcount<=0
        // gate.
        auto cc_row = catalog::build_pg_computed_column_row(
            resource_,
            table_oid_,
            live_attoid,
            column_name_,
            live_atttypid,
            max_version + 1,
            /*attrefcount=*/std::int64_t{0});
        auto [_w, wf] = actor_zeta::send(
            ctx->disk_address,
            &services::disk::manager_disk_t::append_pg_catalog_row,
            exec_ctx, pg_computed_column, std::move(cc_row));
        if (auto rng = co_await std::move(wf); rng.count > 0) {
            ctx->pg_catalog_appends.push_back(std::move(rng));
        }

        // Phase 11.G note: a previous version of this code added an immediate
        // compact_relkind_g_storage call here (drop physical columns whose
        // tombstones were just written), but the subsequent re-INSERT path
        // (dynamic_schema_re_add_after_drop) crashed row_group::append with
        // column-count mismatch because storage::drop_column doesn't fully
        // reset row_group state when called mid-pipeline. Compaction is
        // therefore deferred to operator_vacuum_t (Phase 7.5b runs the same
        // logic asynchronously). For now SELECT * on relkind='g' continues to
        // leak dropped columns until VACUUM runs — out of scope for 11.G.
        mark_executed();
    }

} // namespace components::operators
