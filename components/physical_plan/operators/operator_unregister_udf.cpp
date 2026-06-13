#include "operator_unregister_udf.hpp"

#include <components/base/collection_full_name.hpp>
#include <components/compute/function.hpp>
#include <components/context/context.hpp>
#include <services/disk/manager_disk.hpp>

#include <cstdint>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace components::operators {

    operator_unregister_udf_t::operator_unregister_udf_t(std::pmr::memory_resource* resource,
                                                         log_t log,
                                                         std::string function_name,
                                                         std::pmr::vector<types::complex_logical_type> inputs)
        : read_only_operator_t(resource, std::move(log), operator_type::unregister_udf)
        , function_name_(std::move(function_name))
        , inputs_(std::move(inputs)) {}

    void operator_unregister_udf_t::on_execute_impl(pipeline::context_t* /*ctx*/) { async_wait(); }

    actor_zeta::unique_future<void> operator_unregister_udf_t::await_async_and_resume(pipeline::context_t* ctx) {
        success_ = false;

        // 1. Existence check via the global default registry (V4 invariant:
        //    UDFs registered through register_udf land both in per-executor
        //    registries and in the default registry; the default one is the
        //    authoritative "exists?" check at runtime).
        auto* reg = components::compute::function_registry_t::get_default();
        bool exists = false;
        if (reg) {
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
        }
        if (!exists) {
            output_ = nullptr;
            mark_executed();
            co_return;
        }

        // 2. Drop the matching overload from the default registry.
        if (reg) {
            reg->remove_function_by_signature(function_name_, inputs_);
        }

        // 3. Purge pg_proc + pg_depend rows. resolve_function_by_name returns
        //    every namespace match; drop each one.
        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
            auto [_rfbn, rfbnf] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::resolve_function_by_name,
                                                   exec_ctx,
                                                   function_name_,
                                                   std::uint64_t{0});
            auto matches = co_await std::move(rfbnf);
            constexpr components::catalog::oid_t pg_proc_coll = components::catalog::well_known_oid::pg_proc_table;
            constexpr components::catalog::oid_t pg_depend_coll = components::catalog::well_known_oid::pg_depend_table;
            // Collect every (table, col, oid) delete across all matches into one
            // batched call. matches was already awaited above, so no spec here
            // depends on an intervening read.
            std::pmr::vector<services::disk::pg_catalog_delete_spec_t> specs(resource_);
            specs.reserve(matches.size() * 3);
            for (auto& m : matches) {
                specs.push_back({pg_proc_coll, std::int64_t{0}, m.oid});
                specs.push_back({pg_depend_coll, std::int64_t{1}, m.oid});
                specs.push_back({pg_depend_coll, std::int64_t{3}, m.oid});
                if (ctx->txn.transaction_id != 0) {
                    ctx->pg_catalog_delete_tables.insert(pg_proc_coll);
                    ctx->pg_catalog_delete_tables.insert(pg_depend_coll);
                }
            }
            if (!specs.empty()) {
                auto [_d, df] = actor_zeta::send(ctx->disk_address,
                                                 &services::disk::manager_disk_t::delete_pg_catalog_rows_many,
                                                 exec_ctx,
                                                 std::move(specs));
                co_await std::move(df);
            }
        }

        success_ = true;
        output_ = nullptr;
        mark_executed();
    }

} // namespace components::operators
