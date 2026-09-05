#include "operator_unregister_udf.hpp"

#include <components/base/collection_full_name.hpp>
#include <components/compute/function.hpp>
#include <components/context/context.hpp>
#include <core/result_wrapper.hpp>
#include <services/disk/manager_disk.hpp>

#include <cstdint>
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
            set_error(core::error_t{core::error_code_t::unrecognized_function,
                                    std::pmr::string{"unregister_udf: no overload of '" + function_name_ +
                                                         "' matching this signature is registered",
                                                     resource_}});
            mark_failed();
            co_return;
        }

        // 2. Purge pg_proc + pg_depend rows. resolve_function_by_name returns every namespace match; drop
        //    each one.
        //
        //    AHEAD OF THE REGISTRY REMOVAL BELOW, and the order is the point. The removal is this
        //    operator's only mutation, and the pg_proc read here can REFUSE (scan_table answers a
        //    result_wrapper_t). Doing the removal first means an unreadable pg_proc reports DROP FUNCTION as
        //    failed while the function is already gone from the process-global registry — every lookup
        //    missing it, and its pg_proc/pg_depend rows still on the platter claiming it exists.
        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
            auto [_rfbn, rfbnf] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::resolve_function_by_name,
                                                   exec_ctx,
                                                   function_name_,
                                                   std::uint64_t{0});
            auto matches_r = co_await std::move(rfbnf);
            if (matches_r.has_error()) {
                // A pg_proc read that FAILED is not "there are no rows to purge": acting on it
                // would leave the catalog rows behind while reporting the drop as done.
                set_error(matches_r.error());
                mark_failed();
                co_return;
            }
            auto& matches = matches_r.value();
            constexpr components::catalog::oid_t pg_proc_coll = components::catalog::well_known_oid::pg_proc_table;
            constexpr components::catalog::oid_t pg_depend_coll = components::catalog::well_known_oid::pg_depend_table;
            // Collect every (table, col, oid) delete across all matches into one
            // batched call. matches was already awaited above, so no spec here
            // depends on an intervening read.
            std::pmr::vector<services::disk::pg_catalog_delete_spec_t> specs(resource_);
            specs.reserve(matches.size() * 3);
            // WHICH ZERO IS AN ERROR HERE. The pg_depend rows are optional: a function with no dependency rows
            // deletes none of them and that is healthy. The pg_proc rows are not — every one of them was just
            // READ, out of pg_proc, under this same context, which is where m.oid comes from. A pg_proc delete
            // that matched nothing has not removed the row the read had in hand, and DROP FUNCTION must not go
            // on to take the overload out of the registry over a catalog that still describes it. "The read had
            // it in hand" is an argument only because both sides share a snapshot: resolve_function_by_name
            // scans pg_proc under ctx.txn and so does the delete (agent_disk_t::delete_pg_catalog_rows_inner).
            //
            // An EMPTY spec list is the different, legitimate emptiness, and it stays silent: the registry
            // answered for this signature but pg_proc holds no row for it — a builtin, or a function mirrored
            // into the process registry without a catalog row. Nothing to scrub and nothing to refuse.
            std::pmr::vector<std::size_t> pg_proc_specs(resource_);
            pg_proc_specs.reserve(matches.size());
            for (auto& m : matches) {
                pg_proc_specs.push_back(specs.size());
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
                auto deleted_r = co_await std::move(df);
                // STILL AHEAD OF THE REGISTRY REMOVAL, which is the whole point of the order
                // this operator already keeps: a refused scrub must be known before the only
                // mutation runs, or the function is gone from every lookup in this process
                // while its pg_proc row still claims it exists.
                if (deleted_r.has_error()) {
                    set_error(deleted_r.error());
                    mark_failed();
                    co_return;
                }
                const auto& deleted = deleted_r.value();
                for (const auto i : pg_proc_specs) {
                    if (i < deleted.size() && deleted[i] == 0) {
                        set_error(core::error_t{core::error_code_t::other_error,
                                                std::pmr::string{"unregister_udf: no pg_proc row was deleted for '" +
                                                                     function_name_ +
                                                                     "' — the function is still in the catalog",
                                                                 resource_}});
                        mark_failed();
                        co_return;
                    }
                }
            }
        }

        // 3. Drop the matching overload from the default registry — the operator's ONLY mutation, and last,
        //    so it happens with every refusal already known. `reg` is non-null here: `exists` can only be
        //    set inside the `if (reg)` pre-check, and !exists already refused above.
        //
        //    THE ANSWER IS CHECKED. remove_function_by_signature says whether it removed anything, and a
        //    `false` here means the overload the pre-check matched is no longer in the registry — the
        //    registry changed between the two looks. Reporting success over an unverified removal would
        //    paper over exactly the disagreement this ordering exists to surface.
        if (reg && !reg->remove_function_by_signature(function_name_, inputs_)) {
            set_error(core::error_t{core::error_code_t::other_error,
                                    std::pmr::string{"unregister_udf: the registry no longer holds the overload of '" +
                                                         function_name_ +
                                                         "' that the pre-check matched — nothing was removed",
                                                     resource_}});
            mark_failed();
            co_return;
        }

        success_ = true;
        output_ = nullptr;
        mark_executed();
    }

} // namespace components::operators
