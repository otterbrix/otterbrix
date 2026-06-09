#include "operator_alter_column_drop.hpp"

#include "alter_validators.hpp"

#include <components/catalog/alter_column_validators.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <components/vector/data_chunk.hpp>
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

        // Keyed single-row read of the live pg_attribute row. attoid_ was
        // pre-stamped by enrich_logical_plan; INVALID means "column not found",
        // so no-op.
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
                                           &services::disk::manager_disk_t::read_chunks_by_key,
                                           exec_ctx,
                                           pg_attr_oid,
                                           std::move(pa_keys),
                                           components::operators::make_key_chunk(resource_, std::move(pa_vals)));
        std::pmr::vector<components::vector::data_chunk_t> attr_batches = co_await std::move(paf);

        catalog::oid_t attoid = catalog::INVALID_OID;
        std::int32_t attnum = 0;
        catalog::oid_t atttypid = catalog::INVALID_OID;
        bool att_not_null = false, att_has_default = false;
        std::string att_typspec, att_defspec;
        for (auto& chunk : attr_batches) {
            if (chunk.column_count() < 10)
                continue;
            bool found = false;
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                auto c0 = chunk.value(0, i);
                if (c0.is_null())
                    continue;
                auto c7 = chunk.value(7, i);
                if (!c7.is_null() && c7.value<bool>())
                    continue; // already dropped
                attoid = static_cast<catalog::oid_t>(c0.value<std::uint32_t>());
                auto c3 = chunk.value(3, i);
                atttypid = c3.is_null() ? catalog::INVALID_OID : static_cast<catalog::oid_t>(c3.value<std::uint32_t>());
                auto c4 = chunk.value(4, i);
                attnum = c4.is_null() ? 0 : c4.value<std::int32_t>();
                auto c5 = chunk.value(5, i);
                att_not_null = !c5.is_null() && c5.value<bool>();
                auto c6 = chunk.value(6, i);
                att_has_default = !c6.is_null() && c6.value<bool>();
                auto c8 = chunk.value(8, i);
                if (!c8.is_null())
                    att_typspec = std::string(c8.value<std::string_view>());
                auto c9 = chunk.value(9, i);
                if (!c9.is_null())
                    att_defspec = std::string(c9.value<std::string_view>());
                found = true;
                break;
            }
            if (found)
                break;
        }
        if (attoid == catalog::INVALID_OID) {
            // Row not found or already dropped: no-op, no error.
            mark_executed();
            co_return;
        }

        // read pg_depend for refclassid=pg_attribute, refobjid=attoid.
        components::types::logical_value_t att_cls_lv(resource_, catalog::well_known_oid::pg_attribute_table);
        components::types::logical_value_t att_oid_lv(resource_, attoid);
        std::pmr::vector<std::string> pd_keys(resource_);
        pd_keys.emplace_back("refclassid");
        pd_keys.emplace_back("refobjid");
        std::pmr::vector<components::types::logical_value_t> pd_vals(resource_);
        pd_vals.emplace_back(att_cls_lv);
        pd_vals.emplace_back(att_oid_lv);
        auto [_pd, pdf] = actor_zeta::send(ctx->disk_address,
                                           &services::disk::manager_disk_t::read_chunks_by_key,
                                           exec_ctx,
                                           pg_dep_oid,
                                           std::move(pd_keys),
                                           components::operators::make_key_chunk(resource_, std::move(pd_vals)));
        std::pmr::vector<components::vector::data_chunk_t> dep_batches = co_await std::move(pdf);

        std::size_t dep_row_count = 0;
        for (const auto& chunk : dep_batches) dep_row_count += chunk.size();

        // ABORT-on-error gate: validate dependents BEFORE the first mutating
        // delete/append below, so a rejected DROP leaves the catalog untouched.
        std::pmr::vector<std::pair<int, catalog::oid_t>> dependents{resource_};
        dependents.reserve(dep_row_count);
        for (auto& chunk : dep_batches) {
            if (chunk.column_count() < 2)
                continue;
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                auto d0 = chunk.value(0, i);
                auto d1 = chunk.value(1, i);
                if (d0.is_null() || d1.is_null())
                    continue;
                const auto dep_cls = static_cast<catalog::oid_t>(d0.value<std::uint32_t>());
                const auto dep_oid = static_cast<catalog::oid_t>(d1.value<std::uint32_t>());
                dependents.emplace_back(static_cast<int>(dep_cls), dep_oid);
            }
        }
        auto ec_cascade =
            components::catalog::alter_column_validators::validate_cascade_dependencies(resource_, dependents);
        if (ec_cascade.contains_error()) {
            set_error(std::move(ec_cascade));
            mark_executed();
            co_return;
        }

        // for RESTRICT, abort if any non-internal dep exists. For CASCADE,
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

        // Collect every dependent-scrub delete across all dep_rows into one
        // batched call. dep_batches was already awaited above, so no spec here
        // depends on an intervening read; only the sends are hoisted.
        std::pmr::vector<services::disk::pg_catalog_delete_spec_t> dep_specs(resource_);
        dep_specs.reserve(dep_row_count * 4);
        for (auto& chunk : dep_batches) {
            if (chunk.column_count() < 2)
                continue;
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                auto d0 = chunk.value(0, i);
                auto d1 = chunk.value(1, i);
                if (d0.is_null() || d1.is_null())
                    continue;
                const auto dep_cls = static_cast<catalog::oid_t>(d0.value<std::uint32_t>());
                const auto dep_oid = static_cast<catalog::oid_t>(d1.value<std::uint32_t>());
                if (dep_cls == catalog::well_known_oid::pg_class_table) {
                    // Dependent index: scrub pg_index (by indexrelid=oid_col_idx 0),
                    // pg_depend.objid (idx 1), pg_depend.refobjid (idx 3), pg_class.oid.
                    dep_specs.push_back({pg_idx_oid, std::int64_t{0}, dep_oid});
                    dep_specs.push_back({pg_dep_oid, std::int64_t{1}, dep_oid});
                    dep_specs.push_back({pg_dep_oid, std::int64_t{3}, dep_oid});
                    dep_specs.push_back({pg_class_oid, std::int64_t{0}, dep_oid});
                    if (ctx->txn.transaction_id != 0) {
                        ctx->pg_catalog_delete_tables.insert(pg_idx_oid);
                        ctx->pg_catalog_delete_tables.insert(pg_dep_oid);
                        ctx->pg_catalog_delete_tables.insert(pg_class_oid);
                    }
                } else if (dep_cls == catalog::well_known_oid::pg_constraint_table) {
                    // Dependent constraint: scrub pg_constraint + pg_depend rows.
                    dep_specs.push_back({pg_con_oid, std::int64_t{0}, dep_oid});
                    dep_specs.push_back({pg_dep_oid, std::int64_t{1}, dep_oid});
                    dep_specs.push_back({pg_dep_oid, std::int64_t{3}, dep_oid});
                    if (ctx->txn.transaction_id != 0) {
                        ctx->pg_catalog_delete_tables.insert(pg_con_oid);
                        ctx->pg_catalog_delete_tables.insert(pg_dep_oid);
                    }
                }
            }
        }
        if (!dep_specs.empty()) {
            auto [_dep, depf] = actor_zeta::send(ctx->disk_address,
                                                 &services::disk::manager_disk_t::delete_pg_catalog_rows_many,
                                                 exec_ctx,
                                                 std::move(dep_specs));
            co_await std::move(depf);
        }

        // soft-delete the column: drop original pg_attribute row,
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

        // dropped_at_commit_id is placeholder-0; a backfill marker (below) patches
        // it post-commit, since the commit_id isn't allocated until COMMIT
        // (see pg_catalog_swap.hpp). The tombstone's MVCC insert_id is still the
        // executing txn_id.
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
        // The live row is already deleted above. A 0-row tombstone append leaves
        // the column half-applied (invisible to resolve_table, no MVCC marker for
        // recovery), so surface a hard error instead of letting mark_executed() lie.
        if (rng.count == 0) {
            std::string msg = "operator_alter_column_drop: tombstone append produced no rows for attoid ";
            msg += std::to_string(attoid);
            set_error(core::error_t{core::error_code_t::other_error, std::pmr::string{std::move(msg), resource_}});
            mark_executed();
            co_return;
        }
        ctx->pg_catalog_appends.push_back(std::move(rng));
        // Backfill dropped_at_commit_id on the tombstone, keyed by attoid (same
        // attoid as the live row — identity-preserving tombstone).
        ctx->pg_attribute_commit_id_backfills.push_back(components::pg_attribute_commit_id_backfill_t{
            attoid,
            components::pg_attribute_commit_id_backfill_t::kind_t::dropped_at});

        // Note: drop_column on a relkind='g' (computing) table is routed to
        // operator_computed_field_unregister_t in planner.cpp::rewrite_alter_table,
        // which clears matching pg_computed_column rows. This branch handles
        // regular (relkind='r') tables only.

        mark_executed();
    }

} // namespace components::operators
