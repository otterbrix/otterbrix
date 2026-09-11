#include "operator_alter_column_drop.hpp"

#include "alter_validators.hpp"

#include <components/catalog/alter_column_validators.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/dependency_walker.hpp>
#include <components/catalog/helpers.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/manager_disk.hpp>

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace components::operators {

    namespace catalog = components::catalog;

    operator_alter_column_drop_t::operator_alter_column_drop_t(std::pmr::memory_resource* resource,
                                                               log_t log,
                                                               catalog::oid_t table_oid,
                                                               std::string column_name,
                                                               catalog::oid_t attoid,
                                                               catalog::drop_behavior_t behavior,
                                                               bool missing_ok)
        : read_write_operator_t(resource, std::move(log), operator_type::alter_column_drop)
        , table_oid_(table_oid)
        , column_name_(std::move(column_name))
        , attoid_(attoid)
        , behavior_(behavior)
        , missing_ok_(missing_ok) {}

    actor_zeta::unique_future<void> operator_alter_column_drop_t::await_async_and_resume(pipeline::context_t* ctx) {
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        constexpr catalog::oid_t pg_attr_oid = catalog::well_known_oid::pg_attribute_table;
        constexpr catalog::oid_t pg_dep_oid = catalog::well_known_oid::pg_depend_table;
        constexpr catalog::oid_t pg_idx_oid = catalog::well_known_oid::pg_index_table;
        constexpr catalog::oid_t pg_class_oid = catalog::well_known_oid::pg_class_table;
        constexpr catalog::oid_t pg_con_oid = catalog::well_known_oid::pg_constraint_table;

        // Match by (attrelid, attname), not attoid_: node_alter_column_t::set_attoid has no callers, so keying
        // on it would silently no-op every DROP COLUMN; attoid_ is kept only as a cross-check when present.
        if (column_name_.empty()) {
            mark_executed();
            co_return;
        }

        std::pmr::vector<std::uint64_t> pa_keys(resource_);
        pa_keys.emplace_back(catalog::pg_attribute_col::attrelid);
        auto [_pa, paf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                      &services::disk::manager_disk_t::read_chunks_by_key,
                                                      exec_ctx,
                                                      pg_attr_oid,
                                                      std::move(pa_keys),
                                                      components::operators::make_key_chunk(resource_, table_oid_),
                                                      std::pmr::vector<std::uint64_t>{resource_});
        auto attr_batches_r = co_await std::move(paf);
        if (attr_batches_r.has_error()) {
            set_error(attr_batches_r.error());
            co_return;
        }
        auto& attr_batches = attr_batches_r.value();

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
                if (chunk.is_null(0, i))
                    continue;
                if (!chunk.is_null(7, i) && chunk.get_value<bool>(7, i))
                    continue;
                if (chunk.is_null(2, i))
                    continue;
                // get_value<string_view>, not chunk.value(): the latter's logical_value_t is a temporary the
                // view would outlive.
                const auto attname_cell = chunk.get_value<std::string_view>(2, i);
                if (attname_cell != column_name_)
                    continue;
                const auto row_attoid = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
                if (attoid_ != catalog::INVALID_OID && row_attoid != attoid_)
                    continue;
                attoid = row_attoid;
                atttypid = chunk.is_null(3, i) ? catalog::INVALID_OID
                                               : static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(3, i));
                attnum = chunk.is_null(4, i) ? 0 : chunk.get_value<std::int32_t>(4, i);
                att_not_null = chunk.is_null(5, i) ? false : chunk.get_value<bool>(5, i);
                att_has_default = chunk.is_null(6, i) ? false : chunk.get_value<bool>(6, i);
                if (!chunk.is_null(8, i))
                    att_typspec = std::string(chunk.get_value<std::string_view>(8, i));
                if (!chunk.is_null(9, i))
                    att_defspec = std::string(chunk.get_value<std::string_view>(9, i));
                found = true;
                break;
            }
            if (found)
                break;
        }
        if (attoid == catalog::INVALID_OID) {
            // Refuse (PostgreSQL parity), not a silent no-op — silence would report a migration success that
            // changed nothing; missing_ok_ (IF EXISTS) is the only case suppressing this refusal. relkind='g'
            // tables have no pg_attribute row and route to operator_computed_field_unregister_t instead
            // (planner.cpp::rewrite_alter_table).
            if (missing_ok_) {
                mark_executed();
                co_return;
            }
            std::pmr::vector<std::uint64_t> cl_keys(resource_);
            cl_keys.emplace_back(catalog::pg_class_col::oid);
            auto [_cl, clf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                          &services::disk::manager_disk_t::read_chunks_by_key,
                                                          exec_ctx,
                                                          pg_class_oid,
                                                          std::move(cl_keys),
                                                          components::operators::make_key_chunk(resource_, table_oid_),
                                                          std::pmr::vector<std::uint64_t>{resource_});
            auto cls_batches_r = co_await std::move(clf);
            if (cls_batches_r.has_error()) {
                set_error(cls_batches_r.error());
                co_return;
            }
            auto rel_id = alter_validators::relation_identity_of(cls_batches_r.value());
            std::string rel = std::move(rel_id.relname);
            if (rel.empty()) {
                rel = "oid ";
                rel += std::to_string(table_oid_);
            }
            std::string msg = "column \"";
            msg += column_name_;
            msg += "\" of relation \"";
            msg += rel;
            msg += "\" does not exist; use DROP COLUMN IF EXISTS to ignore it";
            set_error(core::error_t{core::error_code_t::schema_error, std::pmr::string{std::move(msg), resource_}});
            mark_executed();
            co_return;
        }

        std::pmr::vector<std::uint64_t> pd_keys(resource_);
        pd_keys.emplace_back(catalog::pg_depend_col::refclassid);
        pd_keys.emplace_back(catalog::pg_depend_col::refobjid);
        auto [_pd, pdf] = actor_zeta::otterbrix::send(
            ctx->disk_address,
            &services::disk::manager_disk_t::read_chunks_by_key,
            exec_ctx,
            pg_dep_oid,
            std::move(pd_keys),
            components::operators::make_key_chunk(resource_, catalog::well_known_oid::pg_attribute_table, attoid),
            std::pmr::vector<std::uint64_t>{resource_});
        auto dep_batches_r = co_await std::move(pdf);
        if (dep_batches_r.has_error()) {
            set_error(dep_batches_r.error());
            co_return;
        }
        auto& dep_batches = dep_batches_r.value();

        std::size_t dep_row_count = 0;
        for (const auto& chunk : dep_batches) dep_row_count += chunk.size();

        // `blocking` = FK confkey target via 'n' edges, refused under ANY behavior; `restrict_blockers` = every
        // deptype::blocks_restrict dep, refused only under RESTRICT — kept broader than `blocking` on purpose,
        // see test_drop_restrict_deptype.cpp::a_non_constraint_blocking_edge_refuses_the_column_drop.
        std::pmr::vector<catalog::oid_t> restrict_blockers{resource_};
        restrict_blockers.reserve(dep_row_count);
        std::pmr::vector<catalog::oid_t> blocking{resource_};
        for (auto& chunk : dep_batches) {
            if (chunk.column_count() <= catalog::pg_depend_col::deptype) {
                std::string msg = "alter_column_drop: pg_depend answered with ";
                msg += std::to_string(chunk.column_count());
                msg += " column(s), fewer than the ";
                msg += std::to_string(static_cast<std::size_t>(catalog::pg_depend_col::deptype) + 1);
                msg += " this build reads — the column's dependencies cannot be classified";
                set_error(core::error_t{core::error_code_t::schema_error, std::pmr::string{std::move(msg), resource_}});
                co_return;
            }
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (chunk.is_null(0, i) || chunk.is_null(1, i))
                    continue;
                const auto dep_cls = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
                const auto dep_oid = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(1, i));
                const bool deptype_null = chunk.is_null(catalog::pg_depend_col::deptype, i);
                const auto deptype_cell =
                    deptype_null ? std::string_view{}
                                 : chunk.get_value<std::string_view>(catalog::pg_depend_col::deptype, i);

                if (deptype_cell.empty() || catalog::deptype::blocks_restrict(deptype_cell[0])) {
                    restrict_blockers.push_back(dep_oid);
                }

                if (dep_cls != catalog::well_known_oid::pg_constraint_table)
                    continue;
                if (deptype_null) {
                    std::string msg = "alter_column_drop: a pg_depend row for constraint oid ";
                    msg += std::to_string(dep_oid);
                    msg += " has no readable deptype — whether it blocks dropping column \"";
                    msg += column_name_;
                    msg += "\" cannot be determined";
                    set_error(
                        core::error_t{core::error_code_t::schema_error, std::pmr::string{std::move(msg), resource_}});
                    co_return;
                }
                if (!deptype_cell.empty() && deptype_cell[0] == 'n')
                    blocking.push_back(dep_oid);
            }
        }
        if (!blocking.empty()) {
            std::string con_name;
            catalog::oid_t con_relid = catalog::INVALID_OID;
            std::pmr::vector<std::uint64_t> pc_keys(resource_);
            pc_keys.emplace_back(catalog::pg_constraint_col::oid);
            auto [_pc, pcf] =
                actor_zeta::otterbrix::send(ctx->disk_address,
                                            &services::disk::manager_disk_t::read_chunks_by_key,
                                            exec_ctx,
                                            pg_con_oid,
                                            std::move(pc_keys),
                                            components::operators::make_key_chunk(resource_, blocking.front()),
                                            std::pmr::vector<std::uint64_t>{resource_});
            auto con_batches_r = co_await std::move(pcf);
            if (con_batches_r.has_error()) {
                set_error(con_batches_r.error());
                co_return;
            }
            for (auto& chunk : con_batches_r.value()) {
                if (chunk.size() == 0 || chunk.column_count() <= catalog::pg_constraint_col::conrelid)
                    continue;
                if (!chunk.is_null(catalog::pg_constraint_col::conname, 0))
                    con_name.assign(chunk.get_value<std::string_view>(catalog::pg_constraint_col::conname, 0));
                if (!chunk.is_null(catalog::pg_constraint_col::conrelid, 0))
                    con_relid = static_cast<catalog::oid_t>(
                        chunk.get_value<std::uint32_t>(catalog::pg_constraint_col::conrelid, 0));
                break;
            }
            std::string con_table;
            if (con_relid != catalog::INVALID_OID) {
                std::pmr::vector<std::uint64_t> cl_keys(resource_);
                cl_keys.emplace_back(catalog::pg_class_col::oid);
                auto [_cl, clf] =
                    actor_zeta::otterbrix::send(ctx->disk_address,
                                                &services::disk::manager_disk_t::read_chunks_by_key,
                                                exec_ctx,
                                                pg_class_oid,
                                                std::move(cl_keys),
                                                components::operators::make_key_chunk(resource_, con_relid),
                                                std::pmr::vector<std::uint64_t>{resource_});
                auto cls_batches_r = co_await std::move(clf);
                if (cls_batches_r.has_error()) {
                    set_error(cls_batches_r.error());
                    co_return;
                }
                for (auto& chunk : cls_batches_r.value()) {
                    if (chunk.size() == 0 || chunk.column_count() <= catalog::pg_class_col::relname)
                        continue;
                    if (!chunk.is_null(catalog::pg_class_col::relname, 0))
                        con_table.assign(chunk.get_value<std::string_view>(catalog::pg_class_col::relname, 0));
                    break;
                }
            }
            if (con_name.empty()) {
                con_name = "oid ";
                con_name += std::to_string(blocking.front());
            }
            if (con_table.empty()) {
                con_table = "oid ";
                con_table += std::to_string(con_relid);
            }
            std::string msg = "cannot drop column \"";
            msg += column_name_;
            msg += "\": foreign key constraint \"";
            msg += con_name;
            msg += "\" on table \"";
            msg += con_table;
            msg += "\" references it; drop that constraint or that table first";
            set_error(core::error_t{core::error_code_t::schema_error, std::pmr::string{std::move(msg), resource_}});
            mark_executed();
            co_return;
        }

        if (catalog::refuses_on_dependency(behavior_) && !restrict_blockers.empty()) {
            std::string msg = "DROP COLUMN RESTRICT: column has dependent objects (blocking oid ";
            msg += std::to_string(static_cast<unsigned>(restrict_blockers.front()));
            msg += ")";
            set_error(core::error_t{core::error_code_t::other_error, std::pmr::string{std::move(msg), resource_}});
            mark_executed();
            co_return;
        }

        std::pmr::vector<services::disk::pg_catalog_delete_spec_t> dep_specs(resource_);
        dep_specs.reserve(dep_row_count * 4);
        for (auto& chunk : dep_batches) {
            if (chunk.column_count() < 2)
                continue;
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (chunk.is_null(0, i) || chunk.is_null(1, i))
                    continue;
                const auto dep_cls = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
                const auto dep_oid = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(1, i));
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
            auto [_dep, depf] =
                actor_zeta::otterbrix::send(ctx->disk_address,
                                            &services::disk::manager_disk_t::delete_pg_catalog_rows_many,
                                            exec_ctx,
                                            std::move(dep_specs));
            auto dep_deleted = co_await std::move(depf);
            if (dep_deleted.has_error()) {
                set_error(dep_deleted.error());
                mark_failed();
                co_return;
            }
        }

        std::pmr::vector<services::disk::pg_catalog_delete_spec_t> attr_specs(resource_);
        attr_specs.push_back({pg_attr_oid, std::int64_t{0}, attoid});
        auto [_d, df] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                    &services::disk::manager_disk_t::delete_pg_catalog_rows_many,
                                                    exec_ctx,
                                                    std::move(attr_specs));
        auto attr_deleted_r = co_await std::move(df);
        if (attr_deleted_r.has_error()) {
            set_error(attr_deleted_r.error());
            mark_failed();
            co_return;
        }
        const auto& attr_deleted = attr_deleted_r.value();
        if (attr_deleted.empty() || attr_deleted.front() == 0) {
            std::string msg = "operator_alter_column_drop: no pg_attribute row was deleted for attoid ";
            msg += std::to_string(attoid);
            msg += " — the column is still live in the catalog";
            set_error(core::error_t{core::error_code_t::other_error, std::pmr::string{std::move(msg), resource_}});
            mark_failed();
            co_return;
        }
        if (ctx->txn.transaction_id != 0)
            ctx->pg_catalog_delete_tables.insert(pg_attr_oid);

        // dropped_at_commit_id is placeholder-0, backfilled post-commit once COMMIT allocates it (see
        // pg_catalog_swap.hpp); the tombstone's MVCC insert_id is still the executing txn_id.
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
        auto [_w, wf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                    &services::disk::manager_disk_t::append_pg_catalog_row,
                                                    exec_ctx,
                                                    pg_attr_oid,
                                                    std::move(tombstone));
        auto rng_r = co_await std::move(wf);
        if (rng_r.has_error()) {
            set_error(rng_r.error());
            mark_failed();
            co_return;
        }
        auto rng = std::move(rng_r.value());
        if (rng.count == 0) {
            std::string msg = "operator_alter_column_drop: tombstone append produced no rows for attoid ";
            msg += std::to_string(attoid);
            set_error(core::error_t{core::error_code_t::other_error, std::pmr::string{std::move(msg), resource_}});
            mark_executed();
            co_return;
        }
        ctx->pg_catalog_appends.push_back(std::move(rng));
        // This operator only marks the drop: the physical column release is irreversible while the tombstone
        // is still revertable by ROLLBACK, so operator_commit_transaction_t defers it past the WAL commit marker.
        ctx->pg_attribute_commit_id_backfills.push_back(components::pg_attribute_commit_id_backfill_t{
            attoid,
            components::pg_attribute_commit_id_backfill_t::kind_t::dropped_at,
            table_oid_,
            column_name_,
            std::string{},
            components::types::complex_logical_type{}});

        mark_executed();
    }

} // namespace components::operators
