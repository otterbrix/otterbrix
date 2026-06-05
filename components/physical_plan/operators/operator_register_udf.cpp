#include "operator_register_udf.hpp"

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

    void operator_register_udf_t::on_execute_impl(pipeline::context_t* /*ctx*/) {
        // All work is async (conflict-detection read + pg_proc writes). Defer to
        // await_async_and_resume.
        async_wait();
    }

    actor_zeta::unique_future<void> operator_register_udf_t::await_async_and_resume(pipeline::context_t* ctx) {
        success_ = false;
        if (!function_) {
            output_ = nullptr;
            mark_executed();
            co_return;
        }

        const std::string func_name = function_->name();
        const auto func_signatures = function_->get_signatures();

        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        // 1. Cross-namespace conflict detection: bail on any pre-existing pg_proc
        //    row with this function name, in any namespace (user or pg_catalog).
        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            auto [_rfbn, rfbnf] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::resolve_function_by_name,
                                                   exec_ctx,
                                                   func_name,
                                                   std::uint64_t{0});
            auto matches = co_await std::move(rfbnf);
            if (!matches.empty()) {
                output_ = nullptr;
                mark_executed();
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
                output_ = nullptr;
                mark_executed();
                co_return;
            }
        }

        // 3. Mirror into the global default registry so validate_logical_plan
        //    lookups (which probe get_default()) see the UDF. MUST reuse the
        //    LOCAL uid (uids.front()): otherwise the global counter (which keeps
        //    growing across tests) and the per-executor counters diverge, so a
        //    plan's function_uid() set from global matches no local entry and the
        //    predicate gets a null function pointer at runtime.
        if (auto* def_reg = components::compute::function_registry_t::get_default()) {
            auto res = uids.empty() ? def_reg->add_function(function_->get_copy(resource_))
                                    : def_reg->add_function_with_uid(uids.front(), function_->get_copy(resource_));
            if (res.has_error()) {
                output_ = nullptr;
                mark_executed();
                co_return;
            }
        }

        // 4. Persist to pg_proc, attached to the first existing user namespace;
        //    if none exists, the row lives in pg_catalog.
        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            catalog::oid_t target_ns = catalog::well_known_oid::pg_catalog_namespace;
            {
                auto [_ln, lnf] =
                    actor_zeta::send(ctx->disk_address, &services::disk::manager_disk_t::list_namespaces, exec_ctx);
                auto ns_names = co_await std::move(lnf);
                for (auto& nname : ns_names) {
                    if (!nname.empty() && nname != "pg_catalog") {
                        auto [_rn, rnf] = actor_zeta::send(ctx->disk_address,
                                                           &services::disk::manager_disk_t::resolve_namespace,
                                                           exec_ctx,
                                                           std::string(nname),
                                                           std::uint64_t{0});
                        auto rns = co_await std::move(rnf);
                        if (rns.found) {
                            target_ns = rns.oid;
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
                std::vector<components::compute::input_type> matchers;
                matchers.reserve(func_signatures.front().input_types.size());
                for (auto& it : func_signatures.front().input_types) {
                    matchers.push_back(it);
                }
                proargmatchers = catalog::encode_proargmatchers(matchers);
                std::vector<components::compute::output_type> outs;
                outs.reserve(func_signatures.front().output_types.size());
                for (auto& ot : func_signatures.front().output_types) {
                    outs.push_back(ot);
                }
                prorettype = catalog::encode_prorettype(outs);
            }

            auto [_oa, oaf] = actor_zeta::send(ctx->disk_address,
                                               &services::disk::manager_disk_t::allocate_oids_batch,
                                               std::size_t{1});
            catalog::oid_batch_t fn_batch;
            fn_batch.oids = co_await std::move(oaf);
            const catalog::oid_t fn_oid = fn_batch.allocate();
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
            std::pmr::vector<actor_zeta::unique_future<components::pg_catalog_append_range_t>> fn_write_futures(
                resource_);
            fn_write_futures.reserve(fn_writes.size());
            for (auto& w : fn_writes) {
                auto [_w, wf] = actor_zeta::send(ctx->disk_address,
                                                 &services::disk::manager_disk_t::append_pg_catalog_row,
                                                 exec_ctx,
                                                 w.table_oid,
                                                 std::move(w.row));
                fn_write_futures.push_back(std::move(wf));
            }
            for (auto& wf : fn_write_futures) {
                auto rng = co_await std::move(wf);
                if (rng.count > 0)
                    ctx->pg_catalog_appends.push_back(std::move(rng));
            }
        }

        success_ = true;
        output_ = nullptr;
        mark_executed();
    }

} // namespace components::operators
