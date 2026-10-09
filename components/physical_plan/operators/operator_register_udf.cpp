#include "operator_register_udf.hpp"

#include "catalog_util.hpp"
#include "operator_dynamic_cascade_delete.hpp"
#include "single_oid_round.hpp"

#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <services/disk/manager_disk.hpp>

#include <algorithm>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace components::operators {
    namespace catalog = components::catalog;

    operator_register_udf_t::operator_register_udf_t(std::pmr::memory_resource* resource,
                                                     log_t log,
                                                     components::compute::function_ptr function,
                                                     executor_uids_t executor_uids)
        : read_only_operator_t(resource, std::move(log), operator_type::register_udf)
        , function_(std::move(function))
        , executor_uids_(std::move(executor_uids)) {}

    actor_zeta::unique_future<void> operator_register_udf_t::await_async_and_resume(pipeline::context_t* ctx) {
        if (!function_) {
            set_error(
                core::error_t{core::error_code_t::invalid_parameter,
                              std::pmr::string{"register_udf: the plan node carries no function payload", resource_}});
            mark_failed();
            co_return;
        }

        const std::string func_name{function_->name()};
        const auto signatures = proc_signatures(resource_, *function_);

        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        // 1. PostgreSQL 18 ProcedureCreate, one pg_proc row per kernel signature: a row with this name and these
        //    inputs is kept (oid, and the edges of what depends on it) and rewritten, because prouid belongs to this
        //    process; other inputs are another function, a row of its own next to the old ones. A name the engine
        //    seeded (pg_catalog) is not taken. A signature the executors already hold was refused before this runs.
        std::pmr::vector<catalog::oid_t> row_oids(signatures.size(), catalog::INVALID_OID, resource_);
        std::pmr::vector<catalog::oid_t> rewritten(resource_);
        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            auto [_rfbn, rfbnf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                              &services::disk::manager_disk_t::resolve_function_by_name,
                                                              exec_ctx,
                                                              func_name);
            auto matches_r = co_await std::move(rfbnf);
            if (matches_r.has_error()) {
                // A pg_proc read that FAILED is not "the name is free". Reporting it as one is
                // how an unreadable catalog became a duplicate row and a successful statement.
                set_error(matches_r.error());
                mark_failed();
                co_return;
            }
            const auto& matches = matches_r.value();
            if (std::any_of(matches.begin(), matches.end(), [](const auto& m) {
                    return m.namespace_oid == catalog::well_known_oid::pg_catalog_namespace;
                })) {
                set_error(core::error_t{
                    core::error_code_t::already_exists,
                    std::pmr::string{"register_udf: a function named '" + func_name + "' already exists in the catalog",
                                     resource_}});
                mark_failed();
                co_return;
            }
            for (std::size_t i = 0; i < signatures.size(); ++i) {
                const auto existing = std::find_if(matches.begin(), matches.end(), [&](const auto& m) {
                    return m.signature.proargmatchers == signatures[i].proargmatchers;
                });
                if (existing == matches.end()) {
                    continue;
                }
                if (existing->signature != signatures[i]) {
                    set_error(core::error_t{core::error_code_t::already_exists,
                                            std::pmr::string{"register_udf: cannot change return type of existing "
                                                             "function \"" +
                                                                 func_name + "\"\nHINT: Use unregister_udf first.",
                                                             resource_}});
                    mark_failed();
                    co_return;
                }
                row_oids[i] = existing->oid;
                rewritten.push_back(existing->oid);
            }
        }

        // 2. Validate the per-executor uids the dispatcher pre-collected (it already dropped any executor
        //    that errored). Empty means nothing to mirror; non-empty must agree on one non-invalid uid
        //    across all executors, or the registration is rejected.
        const auto& uids = executor_uids_;
        if (!uids.empty()) {
            const auto first_uid = uids.front();
            const bool agree = std::all_of(uids.begin(), uids.end(), [first_uid](components::compute::function_uid u) {
                return u != components::compute::invalid_function_uid && u == first_uid;
            });
            if (!agree) {
                set_error(core::error_t{core::error_code_t::function_registry_error,
                                        std::pmr::string{"register_udf: the executor registries did not agree on a "
                                                         "single valid uid for '" +
                                                             func_name + "'",
                                                         resource_}});
                mark_failed();
                co_return;
            }
        }

        // 3. Everything that can refuse (oid round, namespace lookup/resolve, pg_proc/pg_depend appends) runs
        //    here, before the dispatcher adds the function to its master registry, so a refusal leaves the
        //    master without a function the catalog has no row for.
        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            for (auto& oid : row_oids) {
                if (oid != catalog::INVALID_OID) {
                    continue;
                }
                auto [_oa, oaf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                              &services::disk::manager_disk_t::allocate_oids_batch,
                                                              std::size_t{1});
                auto allocated = co_await std::move(oaf);
                if (auto ec_oid = single_oid_from_round(resource_, std::move(allocated), "register_udf", oid);
                    ec_oid.contains_error()) {
                    set_error(std::move(ec_oid));
                    mark_failed();
                    co_return;
                }
            }

            // 4. Persist to pg_proc, attached to the first existing user namespace;
            //    if none exists, the row lives in pg_catalog.
            catalog::oid_t target_ns = catalog::well_known_oid::pg_catalog_namespace;
            {
                auto [_ln, lnf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                              &services::disk::manager_disk_t::list_namespaces,
                                                              exec_ctx);
                auto ns_names_r = co_await std::move(lnf);
                if (ns_names_r.has_error()) {
                    // Losing this lookup does not merely lose a lookup: target_ns would stay
                    // pg_catalog and the pg_proc row would be written into the WRONG namespace.
                    set_error(ns_names_r.error());
                    mark_failed();
                    co_return;
                }
                for (auto& nname : ns_names_r.value()) {
                    if (!nname.empty() && nname != "pg_catalog") {
                        auto [_rn, rnf] =
                            actor_zeta::otterbrix::send(ctx->disk_address,
                                                        &services::disk::manager_disk_t::resolve_namespace,
                                                        exec_ctx,
                                                        std::string(nname));
                        auto rns_r = co_await std::move(rnf);
                        if (rns_r.has_error()) {
                            set_error(rns_r.error());
                            mark_failed();
                            co_return;
                        }
                        if (rns_r.value().found) {
                            target_ns = rns_r.value().oid;
                            break;
                        }
                    }
                }
            }

            const std::int64_t prouid = uids.empty() ? std::int64_t{0} : static_cast<std::int64_t>(uids.front());

            if (!rewritten.empty()) {
                if (auto ec = co_await drop_function_rows(resource_,
                                                          ctx,
                                                          rewritten,
                                                          function_rows_drop_t::keep_dependents,
                                                          catalog::drop_behavior_t::restrict_,
                                                          func_name);
                    ec.contains_error()) {
                    set_error(std::move(ec));
                    mark_failed();
                    co_return;
                }
            }

            std::vector<catalog::catalog_write_t> fn_writes;
            for (std::size_t i = 0; i < signatures.size(); ++i) {
                for (auto& w : catalog::build_create_function_writes(resource_,
                                                                     func_name,
                                                                     target_ns,
                                                                     row_oids[i],
                                                                     signatures[i].pronargs,
                                                                     prouid,
                                                                     signatures[i].proargmatchers,
                                                                     signatures[i].prorettype)) {
                    fn_writes.push_back(std::move(w));
                }
            }
            // Two-phase: the pg_proc/pg_depend writes are independent (no
            // iteration consumes the previous result), so send all rows first
            // then await in order.
            std::pmr::vector<actor_zeta::unique_future<core::result_wrapper_t<components::pg_catalog_append_range_t>>>
                fn_write_futures(resource_);
            fn_write_futures.reserve(fn_writes.size());
            for (auto& w : fn_writes) {
                auto [_w, wf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                            &services::disk::manager_disk_t::append_pg_catalog_row,
                                                            exec_ctx,
                                                            w.table_oid,
                                                            std::move(w.row));
                fn_write_futures.push_back(std::move(wf));
            }
            // Drain every reply, then act: an abandoned future is a reply with nowhere to
            // land. A pg_proc row that was refused means the function does not exist, so the
            // statement must not report that it registered one.
            core::error_t append_error = core::error_t::no_error();
            for (auto& wf : fn_write_futures) {
                auto rng_r = co_await std::move(wf);
                if (rng_r.has_error()) {
                    if (!append_error.contains_error()) {
                        append_error = rng_r.error();
                    }
                    continue;
                }
                if (rng_r.value().count > 0)
                    ctx->pg_catalog_appends.push_back(std::move(rng_r.value()));
            }
            if (append_error.contains_error()) {
                set_error(std::move(append_error));
                mark_failed();
                co_return;
            }
        }

        output_ = nullptr;
        mark_executed();
    }
} // namespace components::operators
