#include "operator_dynamic_cascade_delete.hpp"

#include <components/catalog/cascade_planner.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/dependency_walker.hpp>
#include <components/catalog/helpers.hpp>
#include <components/context/context.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/index/manager_index.hpp>

#include <cstdint>
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
            }
            return out;
        }

        std::string described(catalog::oid_t classid,
                              char relkind,
                              const qualified_name_t& target,
                              const core::columnname_t& column) {
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
                case pg_namespace_table:
                    return "database " + target.to_string();
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

    actor_zeta::unique_future<core::error_t> drop_with_dependents(std::pmr::memory_resource* resource,
                                                                  pipeline::context_t* ctx,
                                                                  catalog::oid_t seed_classid,
                                                                  catalog::oid_t seed_objid,
                                                                  catalog::drop_behavior_t behavior,
                                                                  const qualified_name_t& target,
                                                                  char relkind,
                                                                  const core::columnname_t& column) {
        execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        if (seed_objid == catalog::INVALID_OID) {
            co_return core::error_t::no_error();
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
        const auto plan = catalog::plan_drop(
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
                                                       described(seed_classid, relkind, target, column),
                                                       plan.blocking_oid);
        }
        if (plan.status == catalog::ddl_status::cycle_detected) {
            std::string msg = "DROP: pg_depend cycle detected at oid ";
            msg += std::to_string(plan.blocking_oid);
            co_return core::error_t{core::error_code_t::other_error, std::pmr::string{std::move(msg), resource}};
        }

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
                                                          components::operators::make_keys_chunk(resource, probe_oids),
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
                co_return core::error_t{core::error_code_t::other_error, std::pmr::string{std::move(msg), resource}};
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
                co_return core::error_t{core::error_code_t::other_error, std::pmr::string{std::move(msg), resource}};
            }
        }

        // Tombstoned for the next GC sweep, not torn down here — physical drop fires only at COMMIT,
        // so a DROP inside a txn stays revertible until then. dropped_at = txn_id, a safe upper bound
        // for the GC horizon predicate, since the commit_id isn't known yet.
        const uint64_t dropped_at = ctx->txn.transaction_id;
        bool any_storage_drop = false;
        std::pmr::vector<actor_zeta::unique_future<void>> drop_futures(resource);
        drop_futures.reserve(pending_storage_drops.size() + 1);
        std::pmr::vector<catalog::oid_t> dropped_storage_oids(resource);
        dropped_storage_oids.reserve(pending_storage_drops.size());
        for (auto& sd : pending_storage_drops) {
            any_storage_drop = true;
            if (ctx->txn.transaction_id != 0) {
                ctx->dropped_storage_oids.push_back(sd.table_oid);
            }
            if (ctx->index_address != actor_zeta::address_t::empty_address()) {
                auto [_mti, mtif] = actor_zeta::otterbrix::send(ctx->index_address,
                                                                &services::index::manager_index_t::mark_table_dropped,
                                                                ctx->session,
                                                                sd.table_oid,
                                                                dropped_at);
                drop_futures.push_back(std::move(mtif));
            }
            dropped_storage_oids.push_back(sd.table_oid);
        }
        if (!dropped_storage_oids.empty()) {
            auto [_msd, msdf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                            &services::disk::manager_disk_t::mark_storage_dropped_many,
                                                            ctx->session,
                                                            std::move(dropped_storage_oids),
                                                            dropped_at);
            drop_futures.push_back(std::move(msdf));
        }
        for (auto& f : drop_futures) {
            co_await std::move(f);
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
                                                             core::columnname_t{});
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
                co_return core::error_t{core::error_code_t::other_error,
                                        std::pmr::string{"register_udf: no pg_proc row was deleted for '" +
                                                             function_name + "' — the function is still in the catalog",
                                                         resource}};
            }
        }
        co_return core::error_t::no_error();
    }

    operator_dynamic_cascade_delete_t::operator_dynamic_cascade_delete_t(std::pmr::memory_resource* resource,
                                                                         log_t log,
                                                                         catalog::oid_t seed_classid,
                                                                         catalog::oid_t seed_objid,
                                                                         catalog::drop_behavior_t behavior,
                                                                         qualified_name_t target,
                                                                         char relkind)
        : read_write_operator_t(resource, std::move(log), operator_type::dynamic_cascade_delete)
        , seed_classid_(seed_classid)
        , seed_objid_(seed_objid)
        , behavior_(behavior)
        , target_(std::move(target))
        , relkind_(relkind) {}

    actor_zeta::unique_future<void>
    operator_dynamic_cascade_delete_t::await_async_and_resume(pipeline::context_t* ctx) {
        auto dropped = co_await drop_with_dependents(resource_,
                                                     ctx,
                                                     seed_classid_,
                                                     seed_objid_,
                                                     behavior_,
                                                     target_,
                                                     relkind_,
                                                     core::columnname_t{});
        if (dropped.contains_error()) {
            set_error(dropped);
            co_return;
        }
        output_ = nullptr;
        mark_executed();
    }

} // namespace components::operators
