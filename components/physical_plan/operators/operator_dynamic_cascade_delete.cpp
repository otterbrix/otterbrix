#include "operator_dynamic_cascade_delete.hpp"

#include <components/catalog/cascade_planner.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/dependency_walker.hpp>
#include <components/catalog/helpers.hpp>
#include <components/context/context.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/index/manager_index.hpp>

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace components::operators {

    namespace catalog = components::catalog;

    namespace {

        inline std::uint64_t encode_key(catalog::oid_t cls, catalog::oid_t oid) noexcept {
            return (static_cast<std::uint64_t>(cls) << 32) | static_cast<std::uint64_t>(oid);
        }

        // Re-issues, per cascade-plan step, the deletes the planner would emit for explicit drops.
        struct per_step_delete_t {
            catalog::oid_t catalog_table_oid;
            std::int64_t oid_col_idx;
        };

        std::pmr::vector<per_step_delete_t> deletes_for_classid(std::pmr::memory_resource* resource,
                                                                catalog::oid_t classid) {
            using namespace catalog::well_known_oid;
            std::pmr::vector<per_step_delete_t> out(resource);
            if (classid == pg_class_table) {
                out.push_back({pg_index_table, 0});           // pg_index.indexrelid
                out.push_back({pg_index_table, 1});           // pg_index.indrelid
                out.push_back({pg_sequence_table, 0});        // pg_sequence.seqrelid
                out.push_back({pg_rewrite_table, 2});         // pg_rewrite.ev_class
                out.push_back({pg_rewrite_ref_table, 0});     // pg_rewrite_ref.ev_class
                out.push_back({pg_attribute_table, 1});       // pg_attribute.attrelid
                out.push_back({pg_computed_column_table, 0}); // pg_computed_column.relid (relkind='g' tables)
                out.push_back({pg_constraint_table, 2});      // pg_constraint.conrelid
                out.push_back({pg_constraint_table, 4});      // pg_constraint.confrelid
                out.push_back({pg_depend_table, 1});          // pg_depend.objid
                out.push_back({pg_depend_table, 3});          // pg_depend.refobjid
                out.push_back({pg_class_table, 0});           // pg_class.oid (last)
            } else if (classid == pg_constraint_table) {
                out.push_back({pg_constraint_table, 0});
                out.push_back({pg_depend_table, 1});
                out.push_back({pg_depend_table, 3});
            } else if (classid == pg_type_table) {
                out.push_back({pg_type_table, 0});
                out.push_back({pg_depend_table, 1});
                out.push_back({pg_depend_table, 3});
            } else if (classid == pg_proc_table) {
                out.push_back({pg_proc_table, 0});
                out.push_back({pg_depend_table, 1});
                out.push_back({pg_depend_table, 3});
            } else if (classid == pg_namespace_table) {
                out.push_back({pg_namespace_table, 0});
                out.push_back({pg_depend_table, 1});
                out.push_back({pg_depend_table, 3});
            } else if (classid == pg_database_table) {
                out.push_back({pg_database_table, 0});
                out.push_back({pg_depend_table, 1});
                out.push_back({pg_depend_table, 3});
            }
            return out;
        }

        // Whether the schema `namespace_oid` is of a connection's database (its pg_database.datconnection is set);
        // main's built-in schemas are not. Read once per schema and statement.
        actor_zeta::unique_future<core::result_wrapper_t<bool>>
        is_remote_namespace(std::pmr::memory_resource* resource,
                            pipeline::context_t* ctx,
                            catalog::oid_t namespace_oid,
                            std::pmr::unordered_map<catalog::oid_t, bool>& known) {
            if (catalog::is_catalog_table(namespace_oid)) {
                co_return false;
            }
            if (auto found = known.find(namespace_oid); found != known.end()) {
                co_return found->second;
            }
            auto named = co_await names_of_namespace(resource, ctx, namespace_oid);
            if (named.has_error()) {
                co_return named.error();
            }
            const bool remote = !named.value().connection.empty();
            known.emplace(namespace_oid, remote);
            co_return remote;
        }

        // The attoids of the pg_attribute rows of `relation`.
        actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<catalog::oid_t>>>
        attoids_of(std::pmr::memory_resource* resource, pipeline::context_t* ctx, catalog::oid_t relation) {
            execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
            std::pmr::vector<std::uint64_t> keys(resource);
            keys.emplace_back(catalog::pg_attribute_col::attrelid);
            std::pmr::vector<std::uint64_t> columns(resource);
            columns.emplace_back(catalog::pg_attribute_col::attoid);
            auto [_a, af] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                        &services::disk::manager_disk_t::read_chunks_by_key,
                                                        exec_ctx,
                                                        catalog::well_known_oid::pg_attribute_table,
                                                        std::move(keys),
                                                        components::operators::make_key_chunk(resource, relation),
                                                        std::move(columns));
            auto rows = co_await std::move(af);
            if (rows.has_error()) {
                co_return rows.error();
            }
            std::pmr::vector<catalog::oid_t> attoids(resource);
            for (const auto& chunk : rows.value()) {
                for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                    if (!chunk.is_null(0, i)) {
                        attoids.push_back(static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i)));
                    }
                }
            }
            co_return attoids;
        }

        // The relnamespace and relkind of a pg_class row; INVALID_OID when there is none.
        actor_zeta::unique_future<core::result_wrapper_t<std::pair<catalog::oid_t, char>>>
        namespace_and_kind(std::pmr::memory_resource* resource, pipeline::context_t* ctx, catalog::oid_t relation) {
            execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
            std::pmr::vector<std::uint64_t> keys(resource);
            keys.emplace_back(catalog::pg_class_col::oid);
            std::pmr::vector<std::uint64_t> columns(resource);
            columns.emplace_back(catalog::pg_class_col::relnamespace);
            columns.emplace_back(catalog::pg_class_col::relkind);
            auto [_c, cf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                        &services::disk::manager_disk_t::read_chunks_by_key,
                                                        exec_ctx,
                                                        catalog::well_known_oid::pg_class_table,
                                                        std::move(keys),
                                                        components::operators::make_key_chunk(resource, relation),
                                                        std::move(columns));
            auto rows = co_await std::move(cf);
            if (rows.has_error()) {
                co_return rows.error();
            }
            for (const auto& chunk : rows.value()) {
                if (chunk.size() != 0 && !chunk.is_null(0, 0)) {
                    const auto kind =
                        chunk.is_null(1, 0) ? std::string_view{"r"} : chunk.get_value<std::string_view>(1, 0);
                    co_return std::pair{static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, 0)),
                                        kind.empty() ? catalog::relkind::regular : kind[0]};
                }
            }
            co_return std::pair{catalog::INVALID_OID, catalog::relkind::regular};
        }

        // The relations a cascade plan drops: a column of one goes with it, whatever the order of the steps.
        std::pmr::vector<catalog::oid_t> dropped_relations_of(std::pmr::memory_resource* resource,
                                                              const std::pmr::vector<catalog::drop_step_t>& steps) {
            std::pmr::vector<catalog::oid_t> dropped(resource);
            for (const auto& step : steps) {
                if (step.classid == catalog::well_known_oid::pg_class_table) {
                    dropped.push_back(step.objid);
                }
            }
            return dropped;
        }

        // The table of a column a cascade reaches; none when the column goes with its dropped table (`dropped`) or has
        // none.
        actor_zeta::unique_future<core::result_wrapper_t<catalog::oid_t>>
        reached_column_table(std::pmr::memory_resource* resource,
                             pipeline::context_t* ctx,
                             catalog::oid_t attoid,
                             const std::pmr::vector<catalog::oid_t>& dropped) {
            std::pmr::vector<std::uint64_t> keys(resource);
            keys.emplace_back(catalog::pg_attribute_col::attoid);
            auto table = co_await read_oid(resource,
                                           ctx,
                                           catalog::well_known_oid::pg_attribute_table,
                                           std::move(keys),
                                           components::operators::make_key_chunk(resource, attoid),
                                           catalog::pg_attribute_col::attrelid);
            if (table.has_error()) {
                co_return table.error();
            }
            if (table.value() == catalog::INVALID_OID ||
                std::find(dropped.begin(), dropped.end(), table.value()) != dropped.end()) {
                co_return catalog::INVALID_OID;
            }
            co_return table.value();
        }

        std::string described(catalog::oid_t classid,
                              char relkind,
                              const qualified_name_t& target,
                              const core::columnname_t& column,
                              const catalog::place_t& place) {
            using namespace catalog::well_known_oid;
            const auto relation = [relkind, &target] {
                switch (relkind) {
                    case catalog::relkind::sequence:
                        return "sequence " + target.to_string();
                    case catalog::relkind::view:
                        return "view " + target.to_string();
                    case catalog::relkind::materialized_view:
                        return "materialized view " + target.to_string();
                    case catalog::relkind::macro:
                        return "function " + target.to_string();
                    default:
                        return "table " + target.to_string();
                }
            };
            switch (classid) {
                case pg_database_table:
                    return "database " + qualified_name_t{target.connection, target.database, {}, {}}.to_string();
                case pg_namespace_table: {
                    // A schema is named with its database: an empty one is the place's.
                    const bool placed = target.database.t.empty();
                    return "schema " +
                           qualified_name_t{core::connection_name_t{placed ? place.connection : target.connection.t},
                                            core::dbname_t{placed ? place.database : target.database.t},
                                            target.schema,
                                            {}}
                               .to_string();
                }
                case pg_type_table:
                    return "type " + target.to_string();
                case pg_constraint_table:
                    return "constraint " + target.to_string();
                case pg_proc_table:
                    return "function " + target.to_string();
                case pg_attribute_table:
                    return "column " + column.t + " of " + relation();
                default:
                    return relation();
            }
        }

    } // namespace

    namespace {
        // The closure of the seed in drop order, or the RESTRICT refusal.
        actor_zeta::unique_future<core::result_wrapper_t<catalog::cascade_plan_t>>
        plan_drop_of(std::pmr::memory_resource* resource,
                     pipeline::context_t* ctx,
                     catalog::oid_t seed_classid,
                     catalog::oid_t seed_objid,
                     catalog::drop_behavior_t behavior,
                     const qualified_name_t& target,
                     char relkind,
                     const core::columnname_t& column) {
            execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

            if (seed_objid == catalog::INVALID_OID) {
                // Unreachable from SQL: the executor decides a missing target before planning.
                std::fprintf(stderr,
                             "drop_with_dependents: %s has no catalog oid; the executor's target check was skipped\n",
                             described(seed_classid, relkind, target, column, ctx->place).c_str());
                assert(false && "drop_with_dependents: the target has no catalog oid");
                std::abort();
            }
            constexpr catalog::oid_t kPgDepend = catalog::well_known_oid::pg_depend_table;

            // dep_graph doubles as the visited set (present key = already expanded).
            std::pmr::unordered_map<std::uint64_t, std::pmr::vector<catalog::dependency_t>> dep_graph(resource);
            std::pmr::vector<std::uint64_t> stack(resource);
            stack.push_back(encode_key(seed_classid, seed_objid));

            while (!stack.empty()) {
                const auto k = stack.back();
                stack.pop_back();
                if (dep_graph.count(k))
                    continue;

                const auto ref_cls = static_cast<catalog::oid_t>(k >> 32);
                const auto ref_oid = static_cast<catalog::oid_t>(k & 0xFFFFFFFFu);

                std::pmr::vector<std::uint64_t> rd_keys(resource);
                rd_keys.emplace_back(catalog::pg_depend_col::refclassid);
                rd_keys.emplace_back(catalog::pg_depend_col::refobjid);
                auto [_rd, rdf] =
                    actor_zeta::otterbrix::send(ctx->disk_address,
                                                &services::disk::manager_disk_t::read_chunks_by_key,
                                                exec_ctx,
                                                kPgDepend,
                                                std::move(rd_keys),
                                                components::operators::make_key_chunk(resource, ref_cls, ref_oid),
                                                std::pmr::vector<std::uint64_t>{resource});
                auto dep_batches_r = co_await std::move(rdf);
                if (dep_batches_r.has_error()) {
                    co_return dep_batches_r.error();
                }
                auto& dep_batches = dep_batches_r.value();

                std::pmr::vector<catalog::dependency_t> deps(resource);
                for (auto& chunk : dep_batches) {
                    if (chunk.column_count() < 5)
                        continue;
                    for (uint64_t i = 0; i < chunk.size(); ++i) {
                        catalog::dependency_t d;
                        d.classid = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
                        d.objid = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(1, i));
                        auto deptype = catalog::deptype_of(chunk, i);
                        if (deptype.has_error()) {
                            co_return core::error_on(resource, deptype.error());
                        }
                        d.deptype = deptype.value();
                        deps.push_back(d);
                        stack.push_back(encode_key(d.classid, d.objid));
                    }
                }
                dep_graph.insert_or_assign(k, std::move(deps));
            }

            // RESTRICT is a gate, not a smaller drop: refuses on a normal dependent anywhere in the closure, else plans
            // CASCADE.
            auto plan = catalog::plan_drop(
                resource,
                seed_classid,
                seed_objid,
                behavior,
                [&dep_graph](std::pmr::memory_resource* mr,
                             catalog::oid_t cls,
                             catalog::oid_t oid) -> std::pmr::vector<catalog::dependency_t> {
                    auto it = dep_graph.find(encode_key(cls, oid));
                    if (it == dep_graph.end()) {
                        return std::pmr::vector<catalog::dependency_t>{mr};
                    }
                    return std::pmr::vector<catalog::dependency_t>{it->second.begin(), it->second.end(), mr};
                });
            dep_graph.clear();

            if (plan.status == catalog::ddl_status::restrict_blocked) {
                co_return catalog::dependent_objects_error(resource,
                                                           described(seed_classid, relkind, target, column, ctx->place),
                                                           plan.blocking_oid);
            }
            if (plan.status == catalog::ddl_status::cycle_detected) {
                std::string msg = "DROP: pg_depend cycle detected at oid ";
                msg += std::to_string(plan.blocking_oid);
                co_return core::error_t{core::error_code_t::internal_error, std::pmr::string{std::move(msg), resource}};
            }
            co_return std::move(plan);
        }
    } // namespace

    actor_zeta::unique_future<core::error_t>
    drop_with_dependents(std::pmr::memory_resource* resource,
                         pipeline::context_t* ctx,
                         catalog::oid_t seed_classid,
                         catalog::oid_t seed_objid,
                         catalog::drop_behavior_t behavior,
                         const qualified_name_t& target,
                         char relkind,
                         const core::columnname_t& column,
                         std::span<const logical_plan::storage_target_t> targets) {
        // The executor stamps the storages of a DROP that may reach a connection's table; one it did not stamp (a
        // table of otterbrix's own as the seed) reaches the disk only.
        const auto storage_of =
            [&targets, ctx, resource](catalog::oid_t table_oid) -> core::result_wrapper_t<table::storage_address_t> {
            if (targets.empty()) {
                return ctx->disk_storage();
            }
            for (const auto& candidate : targets) {
                if (candidate.table_oid == table_oid) {
                    return candidate.address;
                }
            }
            std::pmr::string what{"DROP reached table oid ", resource};
            what += std::to_string(static_cast<unsigned>(table_oid));
            what += " that its plan did not stamp with a storage";
            return core::error_t{core::error_code_t::internal_error, std::move(what)};
        };
        execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
        auto planned =
            co_await plan_drop_of(resource, ctx, seed_classid, seed_objid, behavior, target, relkind, column);
        if (planned.has_error()) {
            co_return planned.error();
        }
        const auto& plan = planned.value();

        // topological_drop_order already emits each object once, so a caller-side dedup is deliberately
        // omitted here.
        const auto& steps = plan.steps;

        struct pending_storage_drop_t {
            catalog::oid_t table_oid{catalog::INVALID_OID};
        };
        std::pmr::vector<pending_storage_drop_t> pending_storage_drops(resource);

        constexpr catalog::oid_t kPgClass = catalog::well_known_oid::pg_class_table;

        std::pmr::vector<catalog::oid_t> probe_oids(resource);
        for (const auto& step : steps) {
            if (step.classid != catalog::well_known_oid::pg_class_table)
                continue;
            probe_oids.push_back(step.objid);
        }
        if (!probe_oids.empty()) {
            std::pmr::vector<std::uint64_t> pc_keys(resource);
            pc_keys.emplace_back(catalog::pg_class_col::oid);
            auto [_pc, pcf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                          &services::disk::manager_disk_t::read_chunks_by_keys,
                                                          exec_ctx,
                                                          kPgClass,
                                                          std::move(pc_keys),
                                                          components::operators::make_keys_batch(resource, probe_oids),
                                                          std::pmr::vector<std::uint64_t>{resource});
            auto pc_results_r = co_await std::move(pcf);
            if (pc_results_r.has_error()) {
                co_return pc_results_r.error();
            }
            auto& pc_results = pc_results_r.value();

            for (std::size_t k = 0; k < probe_oids.size() && k < pc_results.size(); ++k) {
                const auto& pc_batches = pc_results[k];
                if (pc_batches.empty() || pc_batches[0].size() == 0 || pc_batches[0].column_count() < 4)
                    continue;

                const auto rkv = pc_batches[0].is_null(3, 0) ? std::string_view{"r"}
                                                             : pc_batches[0].get_value<std::string_view>(3, 0);
                const char relkind = rkv.empty() ? catalog::relkind::regular : rkv[0];

                if (relkind != catalog::relkind::regular && relkind != catalog::relkind::computed &&
                    relkind != catalog::relkind::materialized_view)
                    continue;
                pending_storage_drops.push_back({probe_oids[k]});
            }
        }

        struct own_row_spec_t {
            std::size_t spec_idx;
            catalog::oid_t classid;
            catalog::oid_t objid;
        };
        std::pmr::vector<own_row_spec_t> own_rows(resource);
        std::pmr::vector<services::disk::pg_catalog_delete_spec_t> catalog_specs(resource);
        // The columns of a relation that goes take their own pg_depend edges (to their types) with them.
        const auto dropped_relations = dropped_relations_of(resource, steps);
        for (const auto relation : dropped_relations) {
            auto attoids = co_await attoids_of(resource, ctx, relation);
            if (attoids.has_error()) {
                co_return attoids.error();
            }
            for (const auto attoid : attoids.value()) {
                catalog_specs.push_back({catalog::well_known_oid::pg_depend_table, std::int64_t{1}, attoid});
            }
        }
        for (const auto& step : steps) {
            for (auto& d : deletes_for_classid(resource, step.classid)) {
                if (d.catalog_table_oid == step.classid && d.oid_col_idx == 0) {
                    own_rows.push_back({catalog_specs.size(), step.classid, step.objid});
                }
                catalog_specs.push_back({d.catalog_table_oid, d.oid_col_idx, step.objid});
                if (ctx->txn.transaction_id != 0)
                    ctx->pg_catalog_delete_tables.insert(d.catalog_table_oid);
            }
        }
        if (!catalog_specs.empty()) {
            const std::size_t spec_count = catalog_specs.size();
            auto [_d, df] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                        &services::disk::manager_disk_t::delete_pg_catalog_rows_many,
                                                        exec_ctx,
                                                        std::move(catalog_specs));
            auto deleted_r = co_await std::move(df);
            // Only the own-row count is meaningful; checked before COMMIT turns the marks below into
            // an irreversible teardown.
            if (deleted_r.has_error()) {
                co_return deleted_r.error();
            }
            const auto& deleted_counts = deleted_r.value();
            if (deleted_counts.size() != spec_count) {
                std::string msg = "DROP cascade: the catalog delete answered ";
                msg += std::to_string(deleted_counts.size());
                msg += " count(s) for ";
                msg += std::to_string(spec_count);
                msg += " spec(s) — the reply cannot be matched to the plan";
                co_return core::error_t{core::error_code_t::internal_error, std::pmr::string{std::move(msg), resource}};
            }
            for (const auto& own : own_rows) {
                if (deleted_counts[own.spec_idx] != 0) {
                    continue;
                }
                std::string msg = "DROP cascade: planned object (catalog table oid ";
                msg += std::to_string(static_cast<unsigned>(own.classid));
                msg += ", oid ";
                msg += std::to_string(static_cast<unsigned>(own.objid));
                msg += ") has no catalog row to delete — the pg_depend graph names an object "
                       "the catalog does not hold";
                co_return core::error_t{core::error_code_t::internal_error, std::pmr::string{std::move(msg), resource}};
            }
        }

        // A column the DROP reaches (DROP TYPE ... CASCADE) goes as ALTER TABLE ... DROP COLUMN takes it; the seed
        // column of a DROP COLUMN is its operator's, and a dropped relation's columns went with it.
        for (const auto& step : steps) {
            if (step.classid != catalog::well_known_oid::pg_attribute_table ||
                (seed_classid == step.classid && seed_objid == step.objid)) {
                continue;
            }
            auto reached = co_await reached_column_table(resource, ctx, step.objid, dropped_relations);
            if (reached.has_error()) {
                co_return reached.error();
            }
            const auto table = reached.value();
            if (table == catalog::INVALID_OID) {
                continue;
            }
            auto storage = storage_of(table);
            if (storage.has_error()) {
                co_return storage.error();
            }
            if (auto dropped = co_await drop_column_rows(resource, ctx, table, step.objid, storage.value());
                dropped.contains_error()) {
                co_return dropped;
            }
        }

        // A storage drops its tables in the statement: one message per storage, the tables named with their
        // oids. A table of a connection the host's map lost has no storage to tell; only the catalog knew it.
        // dropped_at = txn_id, a safe upper bound for the GC horizon predicate, since the commit_id isn't known yet.
        const uint64_t dropped_at = ctx->txn.transaction_id;
        bool any_storage_drop = false;
        struct storage_drop_t {
            table::storage_address_t address;
            std::pmr::vector<catalog::oid_t> oids;
            std::pmr::vector<qualified_name_t> names;
        };
        std::pmr::vector<storage_drop_t> storage_drops(resource);
        for (auto& sd : pending_storage_drops) {
            auto storage = storage_of(sd.table_oid);
            if (storage.has_error()) {
                co_return storage.error();
            }
            const auto address = storage.value();
            if (address.empty()) {
                continue;
            }
            auto group = std::find_if(storage_drops.begin(), storage_drops.end(), [&address](const auto& candidate) {
                return candidate.address.same_storage(address);
            });
            if (group == storage_drops.end()) {
                storage_drops.push_back(storage_drop_t{address,
                                                       std::pmr::vector<catalog::oid_t>{resource},
                                                       std::pmr::vector<qualified_name_t>{resource}});
                group = std::prev(storage_drops.end());
            }
            group->oids.push_back(sd.table_oid);
            qualified_name_t name;
            for (const auto& candidate : targets) {
                if (candidate.table_oid == sd.table_oid) {
                    name = candidate.name;
                }
            }
            group->names.push_back(std::move(name));
        }
        for (auto& group : storage_drops) {
            for (const auto oid : group.oids) {
                ctx->join_transaction(group.address, oid, true);
            }
            auto [_msd, msdf] =
                actor_zeta::otterbrix::send<&services::disk::storage_contract::mark_storage_dropped_many>(group.address,
                                                                                                          exec_ctx,
                                                                                                          group.oids,
                                                                                                          group.names);
            auto marked = co_await std::move(msdf);
            if (marked.has_error()) {
                co_return marked.error();
            }
            if (!marked.value().awaits_publish) {
                if (!marked.value().rows.empty()) {
                    ctx->host_rows.push_back(std::move(marked.value().rows));
                }
                continue;
            }
            // Tombstoned for the next GC sweep, not torn down here — physical drop fires only at COMMIT, so a DROP
            // inside a txn stays revertible until then.
            std::pmr::vector<actor_zeta::unique_future<void>> index_marks(resource);
            for (const auto oid : group.oids) {
                any_storage_drop = true;
                if (ctx->txn.transaction_id != 0) {
                    ctx->dropped_storage_oids.push_back(oid);
                }
                if (ctx->index_address != actor_zeta::address_t::empty_address()) {
                    auto [_mti, mtif] =
                        actor_zeta::otterbrix::send(ctx->index_address,
                                                    &services::index::manager_index_t::mark_table_dropped,
                                                    ctx->session,
                                                    oid,
                                                    dropped_at);
                    index_marks.push_back(std::move(mtif));
                }
            }
            for (auto& f : index_marks) {
                co_await std::move(f);
            }
        }

        if (any_storage_drop && ctx->current_message_sender != actor_zeta::address_t::empty_address()) {
            constexpr uint8_t DISK_KIND = 1;
            constexpr uint8_t INDEX_KIND = 2;
            [[maybe_unused]] auto disk_mark =
                actor_zeta::otterbrix::send(ctx->current_message_sender,
                                            &services::dispatcher::manager_dispatcher_t::on_drop_resource_marked,
                                            DISK_KIND);
            [[maybe_unused]] auto index_mark =
                actor_zeta::otterbrix::send(ctx->current_message_sender,
                                            &services::dispatcher::manager_dispatcher_t::on_drop_resource_marked,
                                            INDEX_KIND);
        }

        co_return core::error_t::no_error();
    }

    actor_zeta::unique_future<core::error_t> drop_column_rows(std::pmr::memory_resource* resource,
                                                              pipeline::context_t* ctx,
                                                              catalog::oid_t table_oid,
                                                              catalog::oid_t attoid,
                                                              const table::storage_address_t& address) {
        constexpr catalog::oid_t pg_attr_oid = catalog::well_known_oid::pg_attribute_table;
        execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
        namespace col = catalog::pg_attribute_col;

        std::pmr::vector<std::uint64_t> keys(resource);
        keys.emplace_back(col::attoid);
        auto [_r, rf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                    &services::disk::manager_disk_t::read_chunks_by_key,
                                                    exec_ctx,
                                                    pg_attr_oid,
                                                    std::move(keys),
                                                    components::operators::make_key_chunk(resource, attoid),
                                                    std::pmr::vector<std::uint64_t>{resource});
        auto rows = co_await std::move(rf);
        if (rows.has_error()) {
            co_return rows.error();
        }
        bool found = false;
        std::string attname;
        std::string typspec;
        std::string defspec;
        catalog::oid_t atttypid = catalog::INVALID_OID;
        std::int32_t attnum = 0;
        bool not_null = false;
        bool has_default = false;
        for (const auto& chunk : rows.value()) {
            for (std::uint64_t i = 0; i < chunk.size() && !found; ++i) {
                if (chunk.column_count() <= col::attdefspec || chunk.is_null(col::attname, i) ||
                    (!chunk.is_null(col::attisdropped, i) && chunk.get_value<bool>(col::attisdropped, i))) {
                    continue;
                }
                found = true;
                attname.assign(chunk.get_value<std::string_view>(col::attname, i));
                atttypid = chunk.is_null(col::atttypid, i)
                               ? catalog::INVALID_OID
                               : static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(col::atttypid, i));
                attnum = chunk.is_null(col::attnum, i) ? 0 : chunk.get_value<std::int32_t>(col::attnum, i);
                not_null = !chunk.is_null(col::attnotnull, i) && chunk.get_value<bool>(col::attnotnull, i);
                has_default = !chunk.is_null(col::atthasdefault, i) && chunk.get_value<bool>(col::atthasdefault, i);
                if (!chunk.is_null(col::atttypspec, i)) {
                    typspec.assign(chunk.get_value<std::string_view>(col::atttypspec, i));
                }
                if (!chunk.is_null(col::attdefspec, i)) {
                    defspec.assign(chunk.get_value<std::string_view>(col::attdefspec, i));
                }
            }
        }
        if (!found) {
            co_return core::error_t{core::error_code_t::internal_error,
                                    std::pmr::string{"DROP COLUMN: no live pg_attribute row for attoid " +
                                                         std::to_string(attoid) + " — the column is not in the catalog",
                                                     resource}};
        }

        std::pmr::vector<services::disk::pg_catalog_delete_spec_t> specs(resource);
        specs.push_back({pg_attr_oid, std::int64_t{0}, attoid});
        specs.push_back({catalog::well_known_oid::pg_depend_table, std::int64_t{1}, attoid});
        auto [_d, df] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                    &services::disk::manager_disk_t::delete_pg_catalog_rows_many,
                                                    exec_ctx,
                                                    std::move(specs));
        auto deleted = co_await std::move(df);
        if (deleted.has_error()) {
            co_return deleted.error();
        }
        if (deleted.value().empty() || deleted.value().front() == 0) {
            co_return core::error_t{core::error_code_t::internal_error,
                                    std::pmr::string{"DROP COLUMN: no pg_attribute row was deleted for attoid " +
                                                         std::to_string(attoid) +
                                                         " — the column is still live in the catalog",
                                                     resource}};
        }
        if (ctx->txn.transaction_id != 0) {
            ctx->pg_catalog_delete_tables.insert(pg_attr_oid);
            ctx->pg_catalog_delete_tables.insert(catalog::well_known_oid::pg_depend_table);
        }
        // A table of a connection the host's map lost: only the catalog knew the column.
        if (address.empty()) {
            co_return core::error_t::no_error();
        }
        components::execution_context_t stamp_ctx{ctx->session,
                                                  ctx->txn,
                                                  ctx->execution_context.timezone_offset,
                                                  table_oid};
        ctx->join_transaction(address, table_oid, true);
        auto [_s, sf] = actor_zeta::otterbrix::send<&services::disk::storage_contract::stamp_column_dropped>(address,
                                                                                                             stamp_ctx,
                                                                                                             table_oid,
                                                                                                             attoid);
        auto stamped = co_await std::move(sf);
        if (stamped.has_error()) {
            co_return stamped.error();
        }
        if (!stamped.value().awaits_publish) {
            if (!stamped.value().rows.empty()) {
                ctx->host_rows.push_back(std::move(stamped.value().rows));
            }
            co_return core::error_t::no_error();
        }

        // dropped_at_commit_id is placeholder-0, backfilled post-commit once COMMIT allocates it (see
        // pg_catalog_swap.hpp); the tombstone's MVCC insert_id is still the executing txn_id.
        auto tombstone = catalog::build_pg_attribute_row(resource,
                                                         attoid,
                                                         table_oid,
                                                         attname,
                                                         atttypid,
                                                         attnum,
                                                         not_null,
                                                         has_default,
                                                         /*is_dropped=*/true,
                                                         typspec,
                                                         defspec,
                                                         /*added_at_commit_id=*/0,
                                                         /*dropped_at_commit_id=*/0);
        auto [_w, wf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                    &services::disk::manager_disk_t::append_pg_catalog_row,
                                                    exec_ctx,
                                                    pg_attr_oid,
                                                    std::move(tombstone));
        auto appended = co_await std::move(wf);
        if (appended.has_error()) {
            co_return appended.error();
        }
        if (appended.value().count == 0) {
            co_return core::error_t{
                core::error_code_t::internal_error,
                std::pmr::string{"DROP COLUMN: tombstone append produced no rows for attoid " + std::to_string(attoid),
                                 resource}};
        }
        ctx->pg_catalog_appends.push_back(std::move(appended.value()));
        ctx->pg_attribute_commit_id_backfills.push_back(components::pg_attribute_commit_id_backfill_t{
            attoid,
            components::pg_attribute_commit_id_backfill_t::kind_t::dropped_at,
            table_oid,
            attname,
            std::string{},
            components::types::complex_logical_type{}});
        co_return core::error_t::no_error();
    }

    actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<catalog::oid_t>>>
    drop_closure(std::pmr::memory_resource* resource,
                 pipeline::context_t* ctx,
                 catalog::oid_t seed_classid,
                 catalog::oid_t seed_objid,
                 catalog::drop_behavior_t behavior,
                 const qualified_name_t& target,
                 char relkind,
                 const core::columnname_t& column) {
        auto planned =
            co_await plan_drop_of(resource, ctx, seed_classid, seed_objid, behavior, target, relkind, column);
        if (planned.has_error()) {
            co_return planned.error();
        }
        const auto& steps = planned.value().steps;
        std::pmr::unordered_map<catalog::oid_t, bool> remote_namespaces(resource);
        std::pmr::vector<catalog::oid_t> closure(resource);
        const auto reach = [&closure](catalog::oid_t table) {
            if (std::find(closure.begin(), closure.end(), table) == closure.end()) {
                closure.push_back(table);
            }
        };
        const auto dropped_tables = dropped_relations_of(resource, steps);
        for (const auto& step : steps) {
            if (step.classid == catalog::well_known_oid::pg_class_table) {
                auto relation = co_await namespace_and_kind(resource, ctx, step.objid);
                if (relation.has_error()) {
                    co_return relation.error();
                }
                if (relation.value().second != catalog::relkind::regular) {
                    continue;
                }
                auto remote = co_await is_remote_namespace(resource, ctx, relation.value().first, remote_namespaces);
                if (remote.has_error()) {
                    co_return remote.error();
                }
                if (remote.value()) {
                    reach(step.objid);
                }
            } else if (step.classid == catalog::well_known_oid::pg_attribute_table) {
                auto reached = co_await reached_column_table(resource, ctx, step.objid, dropped_tables);
                if (reached.has_error()) {
                    co_return reached.error();
                }
                if (reached.value() == catalog::INVALID_OID) {
                    continue;
                }
                auto relation = co_await namespace_and_kind(resource, ctx, reached.value());
                if (relation.has_error()) {
                    co_return relation.error();
                }
                auto remote = co_await is_remote_namespace(resource, ctx, relation.value().first, remote_namespaces);
                if (remote.has_error()) {
                    co_return remote.error();
                }
                if (remote.value()) {
                    reach(reached.value());
                }
            }
        }
        co_return std::move(closure);
    }

    actor_zeta::unique_future<core::error_t> drop_function_rows(std::pmr::memory_resource* resource,
                                                                pipeline::context_t* ctx,
                                                                const std::pmr::vector<catalog::oid_t>& function_oids,
                                                                function_rows_drop_t mode,
                                                                catalog::drop_behavior_t behavior,
                                                                const std::string& function_name) {
        constexpr catalog::oid_t pg_proc_coll = catalog::well_known_oid::pg_proc_table;
        if (mode == function_rows_drop_t::with_dependents) {
            for (const auto oid : function_oids) {
                auto dropped = co_await drop_with_dependents(resource,
                                                             ctx,
                                                             pg_proc_coll,
                                                             oid,
                                                             behavior,
                                                             qualified_name_t{core::relname_t{function_name}},
                                                             catalog::relkind::regular,
                                                             core::columnname_t{},
                                                             {});
                if (dropped.contains_error()) {
                    co_return dropped;
                }
            }
            co_return core::error_t::no_error();
        }

        constexpr catalog::oid_t pg_depend_coll = catalog::well_known_oid::pg_depend_table;
        std::pmr::vector<services::disk::pg_catalog_delete_spec_t> specs(resource);
        specs.reserve(function_oids.size() * 2);
        for (const auto oid : function_oids) {
            specs.push_back({pg_proc_coll, std::int64_t{0}, oid});
            specs.push_back({pg_depend_coll, std::int64_t{1}, oid});
        }
        if (ctx->txn.transaction_id != 0) {
            ctx->pg_catalog_delete_tables.insert(pg_proc_coll);
            ctx->pg_catalog_delete_tables.insert(pg_depend_coll);
        }
        execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
        auto [_d, df] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                    &services::disk::manager_disk_t::delete_pg_catalog_rows_many,
                                                    exec_ctx,
                                                    std::move(specs));
        auto deleted = co_await std::move(df);
        if (deleted.has_error()) {
            co_return core::error_on(resource, deleted.error());
        }
        // Every pg_proc spec is at an even index, its own edges right after it.
        for (std::size_t i = 0; i < deleted.value().size(); i += 2) {
            if (deleted.value()[i] == 0) {
                co_return core::error_t{core::error_code_t::internal_error,
                                        std::pmr::string{"register_udf: no pg_proc row was deleted for '" +
                                                             function_name + "' — the function is still in the catalog",
                                                         resource}};
            }
        }
        co_return core::error_t::no_error();
    }

    operator_dynamic_cascade_delete_t::operator_dynamic_cascade_delete_t(
        std::pmr::memory_resource* resource,
        log_t log,
        catalog::oid_t seed_classid,
        catalog::oid_t seed_objid,
        catalog::drop_behavior_t behavior,
        qualified_name_t target,
        char relkind,
        std::pmr::vector<logical_plan::storage_target_t> targets)
        : read_write_operator_t(resource, std::move(log), operator_type::dynamic_cascade_delete)
        , seed_classid_(seed_classid)
        , seed_objid_(seed_objid)
        , behavior_(behavior)
        , target_(std::move(target))
        , relkind_(relkind)
        , targets_(std::move(targets), resource) {}

    actor_zeta::unique_future<void>
    operator_dynamic_cascade_delete_t::await_async_and_resume(pipeline::context_t* ctx) {
        auto dropped = co_await drop_with_dependents(resource_,
                                                     ctx,
                                                     seed_classid_,
                                                     seed_objid_,
                                                     behavior_,
                                                     target_,
                                                     relkind_,
                                                     core::columnname_t{},
                                                     targets_);
        if (dropped.contains_error()) {
            set_error(dropped);
            co_return;
        }
        output_ = nullptr;
        mark_executed();
    }

} // namespace components::operators
