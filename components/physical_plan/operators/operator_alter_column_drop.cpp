#include "operator_alter_column_drop.hpp"

#include "alter_validators.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/dec43_alter_validators.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <services/disk/manager_disk.hpp>

#include <cstdint>
#include <set>
#include <utility>
#include <vector>

namespace components::operators {

    namespace catalog = components::catalog;

    operator_alter_column_drop_t::operator_alter_column_drop_t(std::pmr::memory_resource* resource,
                                                               log_t log,
                                                               catalog::oid_t table_oid,
                                                               catalog::oid_t namespace_oid,
                                                               std::string column_name,
                                                               catalog::oid_t attoid,
                                                               catalog::drop_behavior_t behavior)
        // Tagged as alter_column_drop (catch-all read_write_operator_t — same
        // convention as the sibling alter_column_add / alter_column_rename
        // operators).
        : read_write_operator_t(resource, std::move(log), operator_type::alter_column_drop)
        , table_oid_(table_oid)
        , namespace_oid_(namespace_oid)
        , column_name_(std::move(column_name))
        , attoid_(attoid)
        , behavior_(behavior) {}

    void operator_alter_column_drop_t::on_execute_impl(pipeline::context_t* /*ctx*/) { async_wait(); }

    actor_zeta::unique_future<void> operator_alter_column_drop_t::await_async_and_resume(pipeline::context_t* ctx) {
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        constexpr catalog::oid_t pg_attr_oid = catalog::well_known_oid::pg_attribute_table;
        constexpr catalog::oid_t pg_dep_oid = catalog::well_known_oid::pg_depend_table;
        constexpr catalog::oid_t pg_idx_oid = catalog::well_known_oid::pg_index_table;
        constexpr catalog::oid_t pg_class_oid = catalog::well_known_oid::pg_class_table;
        constexpr catalog::oid_t pg_con_oid = catalog::well_known_oid::pg_constraint_table;

        // Step 1 — read the live pg_attribute row by attoid (keyed single-row
        // lookup). attoid_ was pre-stamped by enrich_logical_plan from the
        // resolved column metadata; if INVALID we simply no-op (matches the
        // legacy "column not found" behavior of the prior attname scan).
        if (attoid_ == catalog::INVALID_OID) {
            mark_executed();
            co_return;
        }

        components::types::logical_value_t attoid_lv(resource_, attoid_);
        std::pmr::vector<std::string> pa_keys(resource_);
        pa_keys.emplace_back("attoid");
        std::pmr::vector<components::types::logical_value_t> pa_vals(resource_);
        pa_vals.emplace_back(attoid_lv);
        auto [_pa, paf] = actor_zeta::send(ctx->disk_address,
                                           &services::disk::manager_disk_t::read_rows_by_key,
                                           exec_ctx,
                                           pg_attr_oid,
                                           std::move(pa_keys),
                                           std::move(pa_vals));
        auto attr_rows = co_await std::move(paf);

        catalog::oid_t attoid = catalog::INVALID_OID;
        std::int32_t attnum = 0;
        catalog::oid_t atttypid = catalog::INVALID_OID;
        bool att_not_null = false, att_has_default = false;
        std::string att_typspec, att_defspec;
        for (const auto& row : attr_rows) {
            if (row.size() < 10 || row[0].is_null())
                continue;
            if (!row[7].is_null() && row[7].value<bool>())
                continue; // already dropped
            attoid = static_cast<catalog::oid_t>(row[0].value<std::uint32_t>());
            atttypid =
                row[3].is_null() ? catalog::INVALID_OID : static_cast<catalog::oid_t>(row[3].value<std::uint32_t>());
            attnum = row[4].is_null() ? 0 : row[4].value<std::int32_t>();
            att_not_null = !row[5].is_null() && row[5].value<bool>();
            att_has_default = !row[6].is_null() && row[6].value<bool>();
            if (!row[8].is_null())
                att_typspec = std::string(row[8].value<std::string_view>());
            if (!row[9].is_null())
                att_defspec = std::string(row[9].value<std::string_view>());
            break;
        }
        if (attoid == catalog::INVALID_OID) {
            // Row not found (or already dropped). No-op, no error — matches the
            // legacy ddl.cpp behavior which simply `break`-ed out of the switch.
            mark_executed();
            co_return;
        }

        // Step 2 — read pg_depend for refclassid=pg_attribute, refobjid=attoid.
        components::types::logical_value_t att_cls_lv(resource_, catalog::well_known_oid::pg_attribute_table);
        components::types::logical_value_t att_oid_lv(resource_, attoid);
        std::pmr::vector<std::string> pd_keys(resource_);
        pd_keys.emplace_back("refclassid");
        pd_keys.emplace_back("refobjid");
        std::pmr::vector<components::types::logical_value_t> pd_vals(resource_);
        pd_vals.emplace_back(att_cls_lv);
        pd_vals.emplace_back(att_oid_lv);
        auto [_pd, pdf] = actor_zeta::send(ctx->disk_address,
                                           &services::disk::manager_disk_t::read_rows_by_key,
                                           exec_ctx,
                                           pg_dep_oid,
                                           std::move(pd_keys),
                                           std::move(pd_vals));
        auto dep_rows = co_await std::move(pdf);

        // dec 43 V1 Phase 2: build the dependents vector from the pg_depend rows
        // we just read, then funnel it through the pure validator. This is the
        // ABORT-on-error gate — any failure here must surface BEFORE the first
        // mutating delete/append below (atomic-rollback semantics dec 22).
        std::pmr::vector<std::pair<int, catalog::oid_t>> dependents{resource_};
        dependents.reserve(dep_rows.size());
        for (const auto& dep_row : dep_rows) {
            if (dep_row.size() < 2 || dep_row[0].is_null() || dep_row[1].is_null())
                continue;
            const auto dep_cls = static_cast<catalog::oid_t>(dep_row[0].value<std::uint32_t>());
            const auto dep_oid = static_cast<catalog::oid_t>(dep_row[1].value<std::uint32_t>());
            dependents.emplace_back(static_cast<int>(dep_cls), dep_oid);
        }
        auto ec_cascade = components::catalog::dec43::validate_cascade_dependencies(resource_, dependents);
        if (ec_cascade.contains_error()) {
            set_error(std::move(ec_cascade));
            mark_executed();
            co_return;
        }

        // Step 3 — for RESTRICT, abort if any non-internal dep exists. For CASCADE,
        // drop each dependent object.
        if (behavior_ == catalog::drop_behavior_t::restrict_) {
            if (!dependents.empty()) {
                set_error(
                    core::error_t{core::error_code_t::other_error,
                                  std::pmr::string{"DROP COLUMN RESTRICT: column has dependent objects", resource_}});
                mark_executed();
                co_return;
            }
        }

        for (const auto& dep_row : dep_rows) {
            if (dep_row.size() < 2 || dep_row[0].is_null() || dep_row[1].is_null())
                continue;
            const auto dep_cls = static_cast<catalog::oid_t>(dep_row[0].value<std::uint32_t>());
            const auto dep_oid = static_cast<catalog::oid_t>(dep_row[1].value<std::uint32_t>());
            if (dep_cls == catalog::well_known_oid::pg_class_table) {
                // Dependent index: scrub pg_index (by indexrelid=oid_col_idx 0),
                // pg_depend.objid (idx 1), pg_depend.refobjid (idx 3), pg_class.oid.
                auto [_i1, i1f] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::delete_pg_catalog_rows,
                                                   exec_ctx,
                                                   pg_idx_oid,
                                                   std::int64_t{0},
                                                   dep_oid);
                co_await std::move(i1f);
                if (ctx->txn.transaction_id != 0)
                    ctx->pg_catalog_delete_tables.insert(pg_idx_oid);
                auto [_i2, i2f] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::delete_pg_catalog_rows,
                                                   exec_ctx,
                                                   pg_dep_oid,
                                                   std::int64_t{1},
                                                   dep_oid);
                co_await std::move(i2f);
                if (ctx->txn.transaction_id != 0)
                    ctx->pg_catalog_delete_tables.insert(pg_dep_oid);
                auto [_i3, i3f] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::delete_pg_catalog_rows,
                                                   exec_ctx,
                                                   pg_dep_oid,
                                                   std::int64_t{3},
                                                   dep_oid);
                co_await std::move(i3f);
                if (ctx->txn.transaction_id != 0)
                    ctx->pg_catalog_delete_tables.insert(pg_dep_oid);
                auto [_i4, i4f] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::delete_pg_catalog_rows,
                                                   exec_ctx,
                                                   pg_class_oid,
                                                   std::int64_t{0},
                                                   dep_oid);
                co_await std::move(i4f);
                if (ctx->txn.transaction_id != 0)
                    ctx->pg_catalog_delete_tables.insert(pg_class_oid);
            } else if (dep_cls == catalog::well_known_oid::pg_constraint_table) {
                // Dependent constraint: scrub pg_constraint + pg_depend rows.
                auto [_c1, c1f] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::delete_pg_catalog_rows,
                                                   exec_ctx,
                                                   pg_con_oid,
                                                   std::int64_t{0},
                                                   dep_oid);
                co_await std::move(c1f);
                if (ctx->txn.transaction_id != 0)
                    ctx->pg_catalog_delete_tables.insert(pg_con_oid);
                auto [_c2, c2f] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::delete_pg_catalog_rows,
                                                   exec_ctx,
                                                   pg_dep_oid,
                                                   std::int64_t{1},
                                                   dep_oid);
                co_await std::move(c2f);
                if (ctx->txn.transaction_id != 0)
                    ctx->pg_catalog_delete_tables.insert(pg_dep_oid);
                auto [_c3, c3f] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::delete_pg_catalog_rows,
                                                   exec_ctx,
                                                   pg_dep_oid,
                                                   std::int64_t{3},
                                                   dep_oid);
                co_await std::move(c3f);
                if (ctx->txn.transaction_id != 0)
                    ctx->pg_catalog_delete_tables.insert(pg_dep_oid);
            }
        }

        // Step 4 — soft-delete the column: drop original pg_attribute row,
        // then append a tombstone with attisdropped=true. The tombstone keeps
        // attnum so existing rows on disk that reference this slot remain
        // self-describing for MVCC visibility.
        auto [_d, df] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::delete_pg_catalog_rows,
                                         exec_ctx,
                                         pg_attr_oid,
                                         std::int64_t{0},
                                         attoid);
        co_await std::move(df);
        if (ctx->txn.transaction_id != 0)
            ctx->pg_catalog_delete_tables.insert(pg_attr_oid);

        // Block C §3.5 dec 32 V2 OPTION X:
        // dropped_at_commit_id is stamped as 0 here (placeholder) because the
        // commit_id is not allocated until operator_commit_transaction_t calls
        // txn_manager.commit() later in the pipeline. We record a backfill
        // marker on the pipeline context; operator_commit_transaction drains
        // the marker after commit() and patches the tombstone row in place
        // (see TBD-impl note there). The tombstone's MVCC insert_id is still
        // stamped with the executing transaction_id at write and flipped to
        // commit_id by storage_publish_commits at COMMIT.
        //
        // dec 43 V1 Phase 2: cascade dependency validation already ran in
        // Phase 1 (above, BEFORE Step 3 mutations) — ABORT-on-error dec 22.

        auto tombstone = catalog::build_pg_attribute_row(resource_,
                                                         attoid,
                                                         table_oid_,
                                                         column_name_,
                                                         atttypid,
                                                         attnum,
                                                         att_not_null,
                                                         att_has_default,
                                                         /*is_dropped=*/true,
                                                         att_typspec,
                                                         att_defspec,
                                                         /*added_at_commit_id=*/0,
                                                         /*dropped_at_commit_id=*/0);
        auto [_w, wf] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::append_pg_catalog_row,
                                         exec_ctx,
                                         pg_attr_oid,
                                         std::move(tombstone));
        auto rng = co_await std::move(wf);
        // The original pg_attribute row is already deleted above (delete_pg_catalog_rows).
        // If the tombstone append silently produced 0 rows, the column is left in a
        // half-applied state — invisible to resolve_table but with no MVCC marker for
        // recovery. Surface this as a hard error rather than letting mark_executed() lie.
        if (rng.count == 0) {
            std::string msg = "operator_alter_column_drop: tombstone append produced no rows for attoid ";
            msg += std::to_string(attoid);
            set_error(core::error_t{core::error_code_t::other_error, std::pmr::string{std::move(msg), resource_}});
            mark_executed();
            co_return;
        }
        ctx->pg_catalog_appends.push_back(std::move(rng));
        // OPTION X: schedule dropped_at_commit_id backfill on the tombstone.
        // attoid is the same as the live row's attoid (identity-preserving
        // tombstone — see build_pg_attribute_row contract).
        ctx->pg_attribute_commit_id_backfills.push_back(
            components::pg_attribute_commit_id_backfill_t{
                attoid,
                components::pg_attribute_commit_id_backfill_t::kind_t::dropped_at});

        // Note: drop_column on a relkind='g' (computing) table is routed to
        // operator_computed_field_unregister_t in planner.cpp::rewrite_alter_table,
        // which clears matching pg_computed_column rows. This branch handles
        // regular (relkind='r') tables only.

        mark_executed();
    }

} // namespace components::operators
