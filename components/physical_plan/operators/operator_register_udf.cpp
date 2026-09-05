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

        // The deep copy the default-registry mirror will consume is taken HERE, ahead of every
        // disk step, because taking it is the only part of that mirror which can refuse. With it
        // in hand the mirror at the end of this coroutine cannot fail, so there is no window in
        // which the pg_proc row is already durable and the registry then refuses to hold it.
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
            auto [_rfbn, rfbnf] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::resolve_function_by_name,
                                                   exec_ctx,
                                                   func_name,
                                                   std::uint64_t{0});
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

        // 2. Validate the per-executor registration uids the dispatcher
        //    pre-collected from its fan-out. The dispatcher issues the
        //    per-executor register_udf sends, co_awaits every ack, drops any
        //    executor that returned an error, and hands the resulting uids in.
        //    An empty vector means there was nothing to mirror by uid; a
        //    non-empty vector must agree on a single, non-invalid uid (the
        //    "all executors agree" invariant) or the registration is rejected.
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

        // 3. THE WHOLE DISK PROLOGUE RUNS HERE, AHEAD OF THE ONLY MUTATION THIS OPERATOR MAKES.
        //    Everything below that can refuse — the oid round, the namespace enumeration, the
        //    namespace resolve, the pg_proc/pg_depend appends — happens while nothing has been
        //    changed yet, so a refusal leaves the process exactly as it found it. Hoisting only
        //    the oid round (which is where this started) was not enough: list_namespaces and
        //    resolve_namespace stayed BEHIND the mirror, and scan_table can now refuse a catalog
        //    read outright, so an unreadable pg_namespace left the default registry answering for
        //    a function the catalog has no row for — visible to every plan-validation lookup in
        //    this process, absent from every durable record of what exists.
        //
        //    The oid round itself: a round that delivered nothing used to be consumed anyway —
        //    allocate() answers INVALID_OID — and the pg_proc row went out stamped with 0, so
        //    CREATE FUNCTION reported success over a durable function with no identity, which is
        //    what pg_depend and every later lookup key on.
        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            catalog::oid_t fn_oid = catalog::INVALID_OID;
            {
                auto [_oa, oaf] = actor_zeta::send(ctx->disk_address,
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
                auto [_ln, lnf] =
                    actor_zeta::send(ctx->disk_address, &services::disk::manager_disk_t::list_namespaces, exec_ctx);
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
                        auto [_rn, rnf] = actor_zeta::send(ctx->disk_address,
                                                           &services::disk::manager_disk_t::resolve_namespace,
                                                           exec_ctx,
                                                           std::string(nname),
                                                           std::uint64_t{0});
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
                auto [_w, wf] = actor_zeta::send(ctx->disk_address,
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

        // 5. Mirror into the global default registry so validate_logical_plan lookups (which
        //    probe get_default()) see the UDF. MUST reuse the LOCAL uid (uids.front()):
        //    otherwise the global counter (which keeps growing across tests) and the
        //    per-executor counters diverge, so a plan's function_uid() set from global matches
        //    no local entry and the predicate gets a null function pointer at runtime.
        //
        //    LAST ON PURPOSE. This is the operator's ONLY mutation, and by the time it runs
        //    every refusal is already known: the name is free, the uids agree, the identity was
        //    minted, the namespace was resolved and the pg_proc/pg_depend rows are written. The
        //    registry therefore never answers for a function the catalog does not carry. The
        //    payload was copied at the top, so nothing here can refuse either.
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
