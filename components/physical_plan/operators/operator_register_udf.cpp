#include "operator_register_udf.hpp"

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
        success_ = false;
        if (!function_) {
            set_error(
                core::error_t{core::error_code_t::invalid_parameter,
                              std::pmr::string{"register_udf: the plan node carries no function payload", resource_}});
            mark_failed();
            co_return;
        }

        const std::string func_name = function_->name();
        const auto func_signatures = function_->get_signatures();

        // Copied here, ahead of every disk step: copying is the only part of the registry mirror (at the end
        // of this coroutine) that can fail, so once it succeeds the mirror cannot, and there's no window where
        // the pg_proc row is durable but the registry refuses to hold it.
        components::compute::function_ptr registry_copy = function_->get_copy(resource_);
        if (!registry_copy) {
            set_error(core::error_t{
                core::error_code_t::function_registry_error,
                std::pmr::string{"register_udf: the function payload could not be copied for the default registry",
                                 resource_}});
            mark_failed();
            co_return;
        }

        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        // 1. Cross-namespace conflict detection: bail on any pre-existing pg_proc
        //    row with this function name, in any namespace (user or pg_catalog).
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
            if (!matches_r.value().empty()) {
                // A pg_proc row with this name already exists in SOME namespace. Name it:
                // "collision" and "the catalog write failed" are different accidents and the
                // caller has to be able to tell them apart.
                set_error(core::error_t{
                    core::error_code_t::already_exists,
                    std::pmr::string{"register_udf: a function named '" + func_name + "' already exists in the catalog",
                                     resource_}});
                mark_failed();
                co_return;
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
        //    BEFORE the registry mirror below (the operator's only mutation), so a refusal leaves nothing
        //    changed. Hoisting only the oid round would not be enough: an unreadable pg_namespace left behind
        //    it would let the mirror answer for a function the catalog has no row for.
        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            catalog::oid_t fn_oid = catalog::INVALID_OID;
            {
                auto [_oa, oaf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                              &services::disk::manager_disk_t::allocate_oids_batch,
                                                              std::size_t{1});
                auto allocated = co_await std::move(oaf);
                if (auto ec_oid = single_oid_from_round(resource_, std::move(allocated), "register_udf", fn_oid);
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

            std::int32_t pronargs =
                func_signatures.empty() ? 0 : static_cast<std::int32_t>(func_signatures.front().input_types.size());
            std::int64_t prouid = uids.empty() ? std::int64_t{0} : static_cast<std::int64_t>(uids.front());
            // Encode the first signature's per-arg matchers + output types so
            // the function registry can reconstruct real signatures across restart.
            std::string proargmatchers;
            std::string prorettype;
            if (!func_signatures.empty()) {
                std::vector<components::compute::parameter_type> parameters;
                parameters.reserve(func_signatures.front().input_types.size());
                for (auto& it : func_signatures.front().input_types) {
                    parameters.push_back(it);
                }
                proargmatchers = catalog::encode_proargmatchers(parameters);
                std::vector<components::compute::output_type> outs;
                outs.reserve(func_signatures.front().output_types.size());
                for (auto& ot : func_signatures.front().output_types) {
                    outs.push_back(ot);
                }
                prorettype = catalog::encode_prorettype(outs);
            }

            auto fn_writes = catalog::build_create_function_writes(resource_,
                                                                   func_name,
                                                                   target_ns,
                                                                   fn_oid,
                                                                   pronargs,
                                                                   prouid,
                                                                   std::move(proargmatchers),
                                                                   std::move(prorettype));
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

        // 5. Mirror into the global default registry (for validate_logical_plan's get_default() lookups),
        //    reusing the LOCAL uid — otherwise the global counter and the per-executor counters diverge, and
        //    a plan's function_uid() matches no local entry at runtime. Runs LAST on purpose: the operator's
        //    only mutation, so it happens only once every refusal above is already known.
        if (auto* def_reg = components::compute::function_registry_t::get_default()) {
            auto res = uids.empty() ? def_reg->add_function(std::move(registry_copy))
                                    : def_reg->add_function_with_uid(uids.front(), std::move(registry_copy));
            if (res.has_error()) {
                // The default registry already carries its own typed reason — pass it through
                // rather than minting a second, vaguer one.
                set_error(res.error());
                mark_failed();
                co_return;
            }
        }

        success_ = true;
        output_ = nullptr;
        mark_executed();
    }

} // namespace components::operators
