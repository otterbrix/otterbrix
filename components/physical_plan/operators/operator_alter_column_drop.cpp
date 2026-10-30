#include "operator_alter_column_drop.hpp"

#include "alter_validators.hpp"
#include "operator_dynamic_cascade_delete.hpp"

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
                                                               core::columnname_t column_name,
                                                               qualified_name_t relation,
                                                               char relkind,
                                                               catalog::oid_t attoid,
                                                               catalog::drop_behavior_t behavior,
                                                               components::table::storage_address_t address)
        : read_write_operator_t(resource, std::move(log), operator_type::alter_column_drop)
        , table_oid_(table_oid)
        , column_name_(std::move(column_name))
        , relation_(std::move(relation))
        , relkind_(relkind)
        , attoid_(attoid)
        , behavior_(behavior)
        , address_(std::move(address)) {}

    actor_zeta::unique_future<void> operator_alter_column_drop_t::await_async_and_resume(pipeline::context_t* ctx) {
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        constexpr catalog::oid_t pg_attr_oid = catalog::well_known_oid::pg_attribute_table;
        constexpr catalog::oid_t pg_dep_oid = catalog::well_known_oid::pg_depend_table;
        constexpr catalog::oid_t pg_class_oid = catalog::well_known_oid::pg_class_table;
        constexpr catalog::oid_t pg_con_oid = catalog::well_known_oid::pg_constraint_table;

        // Match by (attrelid, attname), not attoid_: node_alter_column_t::set_attoid has no callers, so keying
        // on it would silently no-op every DROP COLUMN; attoid_ is kept only as a cross-check when present.
        if (column_name_.t.empty()) {
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
                const auto attname_cell = chunk.get_value<std::string_view>(2, i);
                if (attname_cell != column_name_.t)
                    continue;
                const auto row_attoid = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
                if (attoid_ != catalog::INVALID_OID && row_attoid != attoid_)
                    continue;
                attoid = row_attoid;
                found = true;
                break;
            }
            if (found)
                break;
        }
        if (attoid == catalog::INVALID_OID) {
            // Refuse (PostgreSQL parity), not a silent no-op — silence would report a migration success that
            // changed nothing; IF EXISTS is decided by the executor before this runs. relkind='g' tables have
            // no pg_attribute row and route to operator_computed_field_unregister_t instead
            // (planner.cpp::rewrite_alter_table).
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
            msg += column_name_.t;
            msg += "\" of relation \"";
            msg += rel;
            msg += "\" does not exist; use DROP COLUMN IF EXISTS to ignore it";
            set_error(core::error_t{core::error_code_t::undefined_column, std::pmr::string{std::move(msg), resource_}});
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

        // `blocking` = FK confkey target via 'n' edges, refused under ANY behavior; every other dependent is
        // left to drop_with_dependents below.
        std::pmr::vector<catalog::oid_t> blocking{resource_};
        for (auto& chunk : dep_batches) {
            if (chunk.column_count() <= catalog::pg_depend_col::deptype) {
                std::string msg = "alter_column_drop: pg_depend answered with ";
                msg += std::to_string(chunk.column_count());
                msg += " column(s), fewer than the ";
                msg += std::to_string(static_cast<std::size_t>(catalog::pg_depend_col::deptype) + 1);
                msg += " this build reads — the column's dependencies cannot be classified";
                set_error(
                    core::error_t{core::error_code_t::internal_error, std::pmr::string{std::move(msg), resource_}});
                co_return;
            }
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (chunk.is_null(0, i) || chunk.is_null(1, i))
                    continue;
                const auto dep_cls = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
                const auto dep_oid = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(1, i));

                if (dep_cls != catalog::well_known_oid::pg_constraint_table)
                    continue;
                auto deptype = catalog::deptype_of(chunk, i);
                if (deptype.has_error()) {
                    set_error(core::error_on(resource_, deptype.error()));
                    co_return;
                }
                if (deptype.value() == catalog::deptype::normal)
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
            msg += column_name_.t;
            msg += "\": foreign key constraint \"";
            msg += con_name;
            msg += "\" on table \"";
            msg += con_table;
            msg += "\" references it; drop that constraint or that table first";
            set_error(core::error_t{core::error_code_t::dependent_objects_still_exist,
                                    std::pmr::string{std::move(msg), resource_}});
            mark_executed();
            co_return;
        }

        // What depends on the column goes the way any DROP takes its dependents: RESTRICT refuses a normal one
        // anywhere in the closure, CASCADE drops each whole (an index with its pg_index row, a view with its
        // pg_rewrite rows). The column itself is tombstoned below.
        if (auto dropped = co_await drop_with_dependents(resource_,
                                                         ctx,
                                                         pg_attr_oid,
                                                         attoid,
                                                         behavior_,
                                                         relation_,
                                                         relkind_,
                                                         column_name_,
                                                         {});
            dropped.contains_error()) {
            set_error(dropped);
            co_return;
        }

        if (auto dropped = co_await drop_column_rows(resource_, ctx, table_oid_, attoid, address_);
            dropped.contains_error()) {
            set_error(dropped);
            mark_failed();
            co_return;
        }
        mark_executed();
    }

} // namespace components::operators
