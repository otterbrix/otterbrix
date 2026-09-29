#include "operator_unregister_udf.hpp"

#include "catalog_util.hpp"

#include <components/base/collection_full_name.hpp>
#include <components/compute/function.hpp>
#include <components/context/context.hpp>
#include <core/result_wrapper.hpp>
#include <services/disk/manager_disk.hpp>

#include <atomic>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace components::operators {

#ifdef DEV_MODE
    namespace {
        std::atomic<bool> g_unregister_udf_purge_refusal{false};
    } // namespace

    void dev_set_unregister_udf_purge_refusal(bool refuse) noexcept { g_unregister_udf_purge_refusal.store(refuse); }
#endif
    operator_unregister_udf_t::operator_unregister_udf_t(std::pmr::memory_resource* resource,
                                                         log_t log,
                                                         std::string function_name,
                                                         std::pmr::vector<types::complex_logical_type> inputs)
        : read_only_operator_t(resource, std::move(log), operator_type::unregister_udf)
        , function_name_(std::move(function_name))
        , inputs_(std::move(inputs)) {}

    actor_zeta::unique_future<void> operator_unregister_udf_t::await_async_and_resume(pipeline::context_t* ctx) {
        success_ = false;

        // 1. Existence check against the dispatcher's master registry; the dispatcher drops the
        //    overload from the master only once this operator succeeds.
        const auto* reg = ctx->function_registry;
        bool exists = false;
        for (auto& [n, uid] : reg->get_functions()) {
            if (n != function_name_)
                continue;
            auto* fn = reg->get_function(uid);
            if (!fn)
                continue;
            for (auto& sig : fn->get_signatures()) {
                if (sig.matches_inputs(inputs_)) {
                    exists = true;
                    break;
                }
            }
            if (exists)
                break;
        }
        if (!exists) {
            set_error(core::error_t{core::error_code_t::unrecognized_function,
                                    std::pmr::string{"unregister_udf: no overload of '" + function_name_ +
                                                         "' matching this signature is registered",
                                                     resource_}});
            mark_failed();
            co_return;
        }

        // 2. Purge pg_proc + pg_depend rows for every namespace match.
#ifdef DEV_MODE
        if (g_unregister_udf_purge_refusal.load()) {
            set_error(core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"unregister_udf: the pg_proc purge was refused (test seam)",
                                                     resource_}});
            mark_failed();
            co_return;
        }
#endif
        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
            auto [_rfbn, rfbnf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                              &services::disk::manager_disk_t::resolve_function_by_name,
                                                              exec_ctx,
                                                              function_name_);
            auto matches_r = co_await std::move(rfbnf);
            if (matches_r.has_error()) {
                // A pg_proc read that FAILED is not "there are no rows to purge": acting on it
                // would leave the catalog rows behind while reporting the drop as done.
                set_error(matches_r.error());
                mark_failed();
                co_return;
            }
            std::pmr::vector<components::catalog::oid_t> function_oids(resource_);
            for (const auto& m : matches_r.value()) {
                function_oids.push_back(m.oid);
            }
            // pg_depend rows are optional (zero deleted is healthy); pg_proc rows are not. An EMPTY spec
            // list is legitimate: a builtin or catalog-less mirror has nothing to scrub.
            std::pmr::vector<std::size_t> pg_proc_specs(resource_);
            auto specs = stage_function_deletes(resource_, ctx, function_oids, pg_proc_specs);
            if (!specs.empty()) {
                auto [_d, df] =
                    actor_zeta::otterbrix::send(ctx->disk_address,
                                                &services::disk::manager_disk_t::delete_pg_catalog_rows_many,
                                                exec_ctx,
                                                std::move(specs));
                auto deleted_r = co_await std::move(df);
                if (deleted_r.has_error()) {
                    set_error(deleted_r.error());
                    mark_failed();
                    co_return;
                }
                if (auto ec = confirm_function_deletes(resource_,
                                                       deleted_r.value(),
                                                       pg_proc_specs,
                                                       "unregister_udf",
                                                       function_name_);
                    ec.contains_error()) {
                    set_error(std::move(ec));
                    mark_failed();
                    co_return;
                }
            }
        }

        success_ = true;
        output_ = nullptr;
        mark_executed();
    }
} // namespace components::operators
