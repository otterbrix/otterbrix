#include "operator_unregister_udf.hpp"

#include "catalog_util.hpp"
#include "operator_dynamic_cascade_delete.hpp"

#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/compute/function.hpp>
#include <components/context/context.hpp>
#include <core/result_wrapper.hpp>
#include <services/disk/manager_disk.hpp>

#include <algorithm>
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
                                                         std::pmr::vector<types::complex_logical_type> inputs,
                                                         components::catalog::drop_behavior_t behavior)
        : read_only_operator_t(resource, std::move(log), operator_type::unregister_udf)
        , function_name_(std::move(function_name))
        , inputs_(std::move(inputs))
        , behavior_(behavior) {}

    actor_zeta::unique_future<void> operator_unregister_udf_t::await_async_and_resume(pipeline::context_t* ctx) {
        success_ = false;

        // 1. The overload the master registry holds, if any: its pg_proc rows are the rows of its signatures. A
        //    function a previous process registered has its rows only; they are the rows these inputs match.
        const auto* reg = ctx->function_registry;
        const components::compute::function* live = nullptr;
        for (auto& [n, uid] : reg->get_functions()) {
            if (n != function_name_)
                continue;
            auto* fn = reg->get_function(uid);
            if (!fn)
                continue;
            for (auto& sig : fn->get_signatures()) {
                if (sig.matches_inputs(inputs_)) {
                    live = fn;
                    break;
                }
            }
            if (live != nullptr)
                break;
        }

#ifdef DEV_MODE
        if (g_unregister_udf_purge_refusal.load()) {
            set_error(core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"unregister_udf: the pg_proc purge was refused (test seam)",
                                                     resource_}});
            mark_failed();
            co_return;
        }
#endif
        // 2. Drop each of those rows the way any DROP takes its dependents.
        bool found = live != nullptr;
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
            const auto live_rows = live != nullptr ? proc_signatures(resource_, *live)
                                                   : std::pmr::vector<components::catalog::proc_signature_t>{resource_};
            std::pmr::vector<components::catalog::oid_t> rows{resource_};
            for (const auto& m : matches_r.value()) {
                bool drop = false;
                if (live != nullptr) {
                    drop = std::any_of(live_rows.begin(), live_rows.end(), [&m](const auto& row) {
                        return row == m.signature;
                    });
                } else {
                    auto parameters =
                        components::catalog::decode_proargmatchers(resource_, m.signature.proargmatchers);
                    if (parameters.has_error()) {
                        set_error(parameters.error());
                        mark_failed();
                        co_return;
                    }
                    const components::compute::kernel_signature_t stored(
                        components::compute::function_type_t::vector,
                        std::move(parameters.value()),
                        std::pmr::vector<components::compute::output_type>{resource_});
                    drop = stored.matches_inputs(inputs_);
                }
                if (drop) {
                    rows.push_back(m.oid);
                }
            }
            found = found || !rows.empty();
            if (auto dropped = co_await drop_function_rows(resource_,
                                                           ctx,
                                                           rows,
                                                           function_rows_drop_t::with_dependents,
                                                           behavior_,
                                                           function_name_);
                dropped.contains_error()) {
                set_error(dropped);
                mark_failed();
                co_return;
            }
        }
        if (!found) {
            set_error(core::error_t{core::error_code_t::unrecognized_function,
                                    std::pmr::string{"unregister_udf: no overload of '" + function_name_ +
                                                         "' matching this signature is registered or in the catalog",
                                                     resource_}});
            mark_failed();
            co_return;
        }

        success_ = true;
        output_ = nullptr;
        mark_executed();
    }
} // namespace components::operators
