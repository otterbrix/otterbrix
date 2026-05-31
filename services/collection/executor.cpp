#include "executor.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/context/execution_context.hpp>
#include <components/planner/planner.hpp>
#include <components/table/transaction_manager.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/wal_sync_mode.hpp>

#include <components/logical_plan/forward.hpp>
#include <components/logical_plan/node_allocate_oids.hpp>
#include <components/logical_plan/node_alter_table.hpp>
#include <components/logical_plan/node_catalog_resolve_function.hpp>
#include <components/logical_plan/node_catalog_resolve_namespace.hpp>
#include <components/logical_plan/node_catalog_resolve_table.hpp>
#include <components/logical_plan/node_catalog_resolve_type.hpp>
#include <components/logical_plan/node_computed_field_register.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_constraint.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_create_macro.hpp>
#include <components/logical_plan/node_create_matview.hpp>
#include <components/logical_plan/node_create_sequence.hpp>
#include <components/logical_plan/node_create_type.hpp>
#include <components/logical_plan/node_create_view.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_sequence.hpp>
#include <components/logical_plan/node_set_timezone.hpp>
#include <components/logical_plan/param_storage.hpp>
// operator_{insert,delete,update}.hpp no longer included — the
// static_cast'd intercept_dml_io_ branches now live in each operator's
// await_async_and_resume so the executor only sees the base operator_t.
#include <components/physical_plan/operators/predicates/predicate.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <core/executor.hpp>
// Variant E.3 Pass 1 catalog-resolve helpers promoted to
// services::catalog_resolve so the executor can drive them without a
// dispatcher dependency. enrich_logical_plan.hpp declares
// stamp_oids_from_resolves / find_first_view_resolve / expand_view_body /
// extract_unresolved_resolves; plan_resolve_index.hpp declares
// plan_resolve_index_t / gather_plan_resolve_index. Same translation unit
// produces them (services/dispatcher/enrich_logical_plan.cpp) — already
// linked into otterbrix_services.
#include <services/dispatcher/dispatcher.hpp>
#include <services/dispatcher/enrich_logical_plan.hpp>
#include <services/dispatcher/plan_resolve_index.hpp>
#include <services/dispatcher/validate_logical_plan.hpp>
// Variant E.3 Pass 2 enrich/validate migration: post_validate_optimize +
// pg_name_to_logical_type + table_id used by the original_type switch
// copied from manager_dispatcher_t::execute_plan_impl.
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/catalog/table_id.hpp>
#include <components/planner/optimizer.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_match.hpp>

using namespace components::cursor;

namespace {

    // Walk through planner-added constraint wrapper nodes (check_constraint,
    // sequence) to find the base DML node type.
    // Needed because the planner may wrap insert/update/delete with constraint nodes,
    // changing the top-level type from insert_t to e.g. check_constraint_t.
    //
    // Dispatcher may wrap INSERT into a sequence_t when the target is a
    // relkind='g' (computing/dynamic-schema) table — e.g. sequence_t(insert,
    // computed_field_register). Recurse through that wrapper so the executor
    // still recognizes the plan as DML and runs the begin_transaction /
    // commit-side pg_catalog swap path.
    //
    // The transformer wraps DML into
    //   sequence_t(catalog_resolve_namespace_t, catalog_resolve_table_t,
    //              <dml_consumer>).
    // We skip catalog_resolve_* prefix children and probe the FIRST non-resolve
    // child for the DML test. Without this, is_dml=false → no begin_transaction
    // → operator_insert writes invisible at commit (cursor reports 0 rows on
    // subsequent SELECT).
    components::logical_plan::node_type find_effective_dml_type(const components::logical_plan::node_ptr& plan) {
        using namespace components::logical_plan;
        auto is_catalog_resolve = [](node_type t) {
            return t == node_type::catalog_resolve_namespace_t || t == node_type::catalog_resolve_table_t ||
                   t == node_type::catalog_resolve_type_t || t == node_type::catalog_resolve_function_t ||
                   t == node_type::catalog_resolve_constraint_t;
        };
        auto* n = plan.get();
        while (n) {
            switch (n->type()) {
                case node_type::check_constraint_t:
                    if (!n->children().empty()) {
                        n = n->children().front().get();
                        continue;
                    }
                    return n->type();
                case node_type::sequence_t: {
                    if (n->children().empty())
                        return n->type();
                    // Skip catalog_resolve_* prefix children — they are
                    // metadata-stamping leaves emitted by the transformer wrap;
                    // the DML consumer is the first non-resolve child.
                    const auto& kids = n->children();
                    std::size_t i = 0;
                    while (i < kids.size() && kids[i] && is_catalog_resolve(kids[i]->type())) {
                        ++i;
                    }
                    if (i >= kids.size() || !kids[i])
                        return n->type();
                    const auto consumer_type = kids[i]->type();
                    if (consumer_type == node_type::insert_t || consumer_type == node_type::update_t ||
                        consumer_type == node_type::delete_t) {
                        n = kids[i].get();
                        continue;
                    }
                    return n->type();
                }
                default:
                    return n->type();
            }
        }
        return node_type::unused;
    }

} // namespace

namespace services::collection::executor {

    plan_t::plan_t(std::stack<components::operators::operator_ptr>&& sub_plans,
                   components::logical_plan::storage_parameters parameters,
                   services::context_storage_t&& context_storage,
                   components::logical_plan::limit_t limit)
        : sub_plans(std::move(sub_plans))
        , parameters(parameters)
        , context_storage_(context_storage)
        , limit(limit) {}

    executor_t::executor_t(std::pmr::memory_resource* resource,
                           actor_zeta::address_t parent_address,
                           actor_zeta::address_t wal_address,
                           actor_zeta::address_t disk_address,
                           actor_zeta::address_t index_address,
                           components::table::transaction_manager_t* txn_manager,
                           log_t&& log)
        : actor_zeta::basic_actor<executor_t>{resource}
        , parent_address_(std::move(parent_address))
        , wal_address_(std::move(wal_address))
        , disk_address_(std::move(disk_address))
        , index_address_(std::move(index_address))
        , txn_manager_(txn_manager)
        , log_(log)
        , function_registry_(resource)
        , pending_void_(resource)
        , local_collections_(resource) {
        register_default_functions(function_registry_);
    }

    actor_zeta::behavior_t executor_t::behavior(actor_zeta::mailbox::message* msg) {
        poll_pending();
        switch (msg->command()) {
            case actor_zeta::msg_id<executor_t, &executor_t::execute_plan>: {
                co_await actor_zeta::dispatch(this, &executor_t::execute_plan, msg);
                break;
            }
            case actor_zeta::msg_id<executor_t, &executor_t::execute_plan_full>: {
                co_await actor_zeta::dispatch(this, &executor_t::execute_plan_full, msg);
                break;
            }
            case actor_zeta::msg_id<executor_t, &executor_t::register_udf>: {
                co_await actor_zeta::dispatch(this, &executor_t::register_udf, msg);
                break;
            }
            case actor_zeta::msg_id<executor_t, &executor_t::register_collection_local>: {
                co_await actor_zeta::dispatch(this, &executor_t::register_collection_local, msg);
                break;
            }
            case actor_zeta::msg_id<executor_t, &executor_t::unregister_collection_local>: {
                co_await actor_zeta::dispatch(this, &executor_t::unregister_collection_local, msg);
                break;
            }
            default:
                break;
        }
    }

    void executor_t::poll_pending() {
        for (auto it = pending_void_.begin(); it != pending_void_.end();) {
            if (it->is_ready()) {
                it = pending_void_.erase(it);
            } else {
                ++it;
            }
        }
    }

    auto executor_t::make_type() const noexcept -> const char* { return "executor"; }

    executor_t::unique_future<execute_result_t>
    executor_t::execute_plan(components::session::session_id_t session,
                             components::logical_plan::node_ptr logical_plan,
                             components::logical_plan::storage_parameters parameters,
                             services::context_storage_t context_storage,
                             components::table::transaction_data txn) {
        trace(log_, "executor::execute_plan, session: {}", session.data());

        using namespace components::logical_plan;

        // CREATE INDEX / DROP INDEX are now lowered by the planner into
        // sequence_t(primitive_write/delete × N, create_index_t/drop_index_t)
        // and run through the standard physical-plan pipeline (see
        // components/physical_plan_generator/impl/create_plan_sequence.cpp).
        // No special-case branches required here.

        // Determine if this is a DML operation.
        // find_effective_dml_type unwraps planner-added constraint wrapper nodes so
        // that is_dml is correct even when the plan is wrapped by check_constraint etc.
        const auto effective_type = find_effective_dml_type(logical_plan);
        bool is_dml = (effective_type == node_type::insert_t || effective_type == node_type::update_t ||
                       effective_type == node_type::delete_t);

        // Variant E.3 (Pass 9 dec 1) collections_ partition — Step 2 hot-path
        // probe. Resolve the routing oid the same way dispatcher.execute_plan_impl
        // does (logical_plan root, then first child for wrapper nodes), then
        // probe local_collections_. Hit  → this executor owns the table slice
        // (intra-partition DML). Miss → either cross-partition (JOIN spanning
        // executor slices), pre-DDL (CREATE …), or a no-table node (db/ns DDL).
        // For now we only trace — Step 3+ will short-circuit
        // dispatcher.collections_ reads for the hit case. Read-only probe; no
        // side effects on the plan.
        {
            components::catalog::oid_t local_routing_oid = logical_plan->table_oid();
            if (local_routing_oid == components::catalog::INVALID_OID && !logical_plan->children().empty()) {
                local_routing_oid = logical_plan->children().front()->table_oid();
            }
            if (local_routing_oid != components::catalog::INVALID_OID) {
                const auto* local_entry = find_local_collection(local_routing_oid);
                if (local_entry) {
                    trace(log_,
                          "executor::execute_plan: local_collections_ HIT oid={} ({}.{}.{})",
                          static_cast<unsigned>(local_routing_oid),
                          local_entry->database,
                          local_entry->schema,
                          local_entry->name);
                } else {
                    trace(log_,
                          "executor::execute_plan: local_collections_ MISS oid={} (cross-partition or pre-DDL)",
                          static_cast<unsigned>(local_routing_oid));
                }
            }
        }

        // Step 1: Begin transaction for DML (executor owns full lifecycle).
        // Variant E.3 Constraint #11: route through the dispatcher's
        // txn_begin_msg mailbox handler instead of dereferencing the shared
        // txn_manager_ pointer directly. The dispatcher serializes the
        // txn_manager_ mutation in its own actor context.
        components::table::transaction_data txn_data = txn;
        if (is_dml) {
            auto [_tb, tbf] = actor_zeta::send(parent_address_,
                                               &services::dispatcher::manager_dispatcher_t::txn_begin_msg,
                                               session);
            txn_data = co_await std::move(tbf);
            trace(log_, "executor::execute_plan: began txn {}", txn_data.transaction_id);
        }

        // With the transformer wrap, logical_plan may be
        // sequence_t(catalog_resolve_*, ..., <consumer>). The limit_t node
        // lives as a child of the consumer, not on the wrapping sequence_t.
        // Search via find_effective_dml_type first (already understands
        // resolve prefix), then fall back to iterating the raw children for
        // non-DML plans.
        auto limit = components::logical_plan::limit_t::unlimit();
        auto* limit_lookup_node = logical_plan.get();
        if (limit_lookup_node && limit_lookup_node->type() == components::logical_plan::node_type::sequence_t) {
            // Find first non-resolve child as the limit-carrying consumer.
            auto is_catalog_resolve = [](components::logical_plan::node_type t) {
                return t == components::logical_plan::node_type::catalog_resolve_namespace_t ||
                       t == components::logical_plan::node_type::catalog_resolve_table_t ||
                       t == components::logical_plan::node_type::catalog_resolve_type_t ||
                       t == components::logical_plan::node_type::catalog_resolve_function_t ||
                       t == components::logical_plan::node_type::catalog_resolve_constraint_t;
            };
            for (const auto& c : limit_lookup_node->children()) {
                if (c && !is_catalog_resolve(c->type())) {
                    limit_lookup_node = c.get();
                    break;
                }
            }
        }
        for (const auto& child : limit_lookup_node->children()) {
            if (child->type() == components::logical_plan::node_type::limit_t) {
                limit = static_cast<components::logical_plan::node_limit_t*>(child.get())->limit();
            }
        }

        components::operators::operator_ptr plan =
            planner::create_plan(context_storage, function_registry_, logical_plan, limit, &parameters);

        if (!plan) {
            if (is_dml) {
                // Variant E.3 Constraint #11: mailbox-route the abort.
                auto [_ta, taf] = actor_zeta::send(parent_address_,
                                                   &services::dispatcher::manager_dispatcher_t::txn_abort_msg,
                                                   session);
                co_await std::move(taf);
            }
            co_return execute_result_t{make_cursor(resource(),
                                                   core::error_t(core::error_code_t::create_physical_plan_error,
                                                                 std::pmr::string{"invalid query plan", resource()}))};
        }

        plan->set_as_root();

        auto plan_data = traverse_plan_(std::move(plan), std::move(parameters), std::move(context_storage));
        plan_data.limit = limit;

        // Step 2: Execute physical plan
        auto result = co_await execute_sub_plan_(session, std::move(plan_data), txn_data);

        if (is_dml && result.cursor->is_success()) {
            // CHECK constraint enforcement is now handled by operator_check_constraint_t
            // (planner-inserted into the physical plan). No post-execution disk round-trip needed.

            // WAL physical_{insert,update,delete} writes live in each DML
            // operator's await_async_and_resume so each DML operator is
            // self-contained (storage_append + WAL + index mirror happen
            // together inside the operator coroutine, before mark_executed).
            // The flush future is pushed onto pipeline_context.pending_disk_futures
            // and awaited by execute_sub_plan_, ordering guarantees stay identical.

            // Block C §3.5 dec 22 Central accumulation: if a BEGIN
            // operator marked this session's txn explicit, DML statements
            // park their ranges on the transaction_t and skip the per-statement
            // commit / publish / WAL barrier here. operator_commit_transaction
            // drains the parked ranges via batched storage_publish_* at COMMIT.
            auto* current_txn = txn_manager_->find_transaction(session);
            const bool is_explicit_txn = (current_txn != nullptr && current_txn->is_explicit());
            if (is_explicit_txn) {
                for (const auto& app : result.dml_appends) {
                    current_txn->accumulate_base_append(
                        components::table::dml_append_range_t{app.table_oid, app.row_start, app.row_count});
                }
                for (const auto& del : result.dml_deletes) {
                    current_txn->accumulate_base_delete(
                        components::table::dml_delete_range_t{del.table_oid, del.txn_id});
                }
                // Block C §3.5 dec 22 EXTENSION — pg_catalog accumulation for
                // explicit BEGIN..COMMIT. Per-statement fragments produce
                // pg_catalog appends/deletes (e.g. operator_computed_field_register_t,
                // operator_create_collection_t, operator_register_udf_t). For
                // implicit txns the per-statement publish runs below at
                // lines ~395-416. For explicit txns we park them on the
                // transaction_t via accumulate_pg_catalog_pending so
                // operator_commit_transaction_t drains them into a single
                // batched storage_publish_* at COMMIT, atomic with the
                // base-table DML ranges accumulated above.
                //
                // We MOVE the vectors onto the txn and return EMPTY ones to
                // the dispatcher. The dispatcher's existing merge-into-txn
                // block (services/dispatcher/dispatcher.cpp ~1458-1467) gates
                // on txn_data.transaction_id != 0 — for explicit-DML the
                // dispatcher passes txn_data{0,0} so the merge would be
                // skipped, making this in-executor accumulate the canonical
                // path. The duplicate-into-txn elsewhere is a no-op.
                std::size_t pgc_appends_n = result.pg_catalog_appends.size();
                std::size_t pgc_deletes_n = result.pg_catalog_delete_tables.size();
                current_txn->accumulate_pg_catalog_pending(
                    std::move(result.pg_catalog_appends),
                    std::move(result.pg_catalog_delete_tables));
                result.pg_catalog_appends.clear();
                result.pg_catalog_delete_tables.clear();
                // Block C §3.5 dec 32 V2 OPTION X: park pg_attribute commit_id
                // backfill markers on the txn for operator_commit_transaction
                // to drain after commit_id allocation.
                std::size_t bf_n = result.pg_attribute_commit_id_backfills.size();
                current_txn->accumulate_pg_attribute_commit_id_backfills(
                    std::move(result.pg_attribute_commit_id_backfills));
                result.pg_attribute_commit_id_backfills.clear();
                trace(log_,
                      "executor::execute_plan: explicit txn {} — accumulated {} appends, {} deletes, "
                      "{} pg_catalog appends, {} pg_catalog delete-tables, {} pg_attribute backfills (no publish)",
                      txn_data.transaction_id,
                      result.dml_appends.size(),
                      result.dml_deletes.size(),
                      pgc_appends_n,
                      pgc_deletes_n,
                      bf_n);
                // Block C §3.5 dec 32 V2 OPTION X: forward pg_attribute backfill
                // markers down. They were accumulated onto current_txn via the
                // mark below before this co_return; see drain in
                // operator_commit_transaction_t.
                co_return execute_result_t{std::move(result.cursor),
                                           std::move(result.updates),
                                           std::move(result.pg_catalog_appends),
                                           std::move(result.pg_catalog_delete_tables),
                                           std::move(result.pg_attribute_commit_id_backfills)};
            }

            // Step 4: Commit transaction (implicit / auto-commit txn path).
            // Variant E.3 Constraint #11: route through dispatcher mailbox.
            // txn_commit_msg is the low-level alloc-commit_id wrapper — it
            // does NOT run the operator pipeline / WAL / storage_publish /
            // publish() barrier. Those side-effects continue to run inline in
            // the executor's per-statement commit phase below.
            uint64_t commit_id = 0;
            {
                auto [_tc, tcf] = actor_zeta::send(parent_address_,
                                                   &services::dispatcher::manager_dispatcher_t::txn_commit_msg,
                                                   session);
                commit_id = co_await std::move(tcf);
            }
            trace(log_, "executor::execute_plan: committed txn {}, commit_id {}", txn_data.transaction_id, commit_id);

            // Step 5: Commit side-effects on storage and index.
            // Routing is by table_oid only — never read cfn from
            // logical_plan, since wrappers (sequence_t etc.) have no cfn and
            // would shadow the inner DML node's identity. Each range's table_oid
            // is stamped by the inner DML operator; it identifies the
            // storage/index target unambiguously.
            // B33: iterate ALL accumulated ranges so FK cascade DELETE on
            // multiple tables publishes each child's flip (previously only the
            // last range was published).
            if (commit_id > 0) {
                for (const auto& app : result.dml_appends) {
                    components::execution_context_t ctx{session,
                                                        txn_data,
                                                        context_storage.session_timezone,
                                                        app.table_oid};
                    auto [_ca, caf] = actor_zeta::send(disk_address_,
                                                       &disk::manager_disk_t::storage_publish_commit,
                                                       ctx,
                                                       app.table_oid,
                                                       commit_id,
                                                       app.row_start,
                                                       app.row_count);
                    co_await std::move(caf);
                    if (index_address_ != actor_zeta::address_t::empty_address()) {
                        auto [_ci, cif] = actor_zeta::send(index_address_,
                                                           &index::manager_index_t::commit_insert,
                                                           ctx,
                                                           app.table_oid,
                                                           commit_id);
                        // Block B Parallel A.B3: commit_insert now returns a
                        // typed result_t carrying its error state. Bitcask is
                        // assert+abort terminal today so failed() never trips,
                        // but the branch is plumbed in for the future C §3.1
                        // index-side abort path.
                        auto ci_result = co_await std::move(cif);
                        if (ci_result.failed()) {
                            // Block C §3.1 (Pass 9): index commit failed →
                            // propagate via cursor and route through the
                            // existing abort path below. publish() is NOT
                            // called: the commit_id stays in
                            // in_flight_commits_, so concurrent readers keep
                            // filtering it out of their snapshots. The
                            // already-allocated commit_id is effectively
                            // stranded; txn_manager_->abort(session) is a
                            // no-op here (active_ entry was removed by
                            // commit()), but the revert side-effects on
                            // storage and index still run uniformly.
                            result.cursor = make_cursor(
                                resource(),
                                core::error_t(ci_result.error.type,
                                              std::pmr::string{ci_result.error.what.c_str(), resource()}));
                            goto __block_c_3_1_abort_label;
                        }
                    }
                }
                for (const auto& del : result.dml_deletes) {
                    components::execution_context_t del_ctx{session,
                                                            txn_data,
                                                            context_storage.session_timezone,
                                                            del.table_oid};
                    auto [_cd, cdf] = actor_zeta::send(disk_address_,
                                                       &disk::manager_disk_t::storage_publish_delete,
                                                       del_ctx,
                                                       del.table_oid,
                                                       commit_id);
                    co_await std::move(cdf);
                    if (index_address_ != actor_zeta::address_t::empty_address()) {
                        auto [_cdi, cdif] = actor_zeta::send(index_address_,
                                                             &index::manager_index_t::commit_delete,
                                                             del_ctx,
                                                             del.table_oid,
                                                             commit_id);
                        // Block B Parallel A.B3: commit_delete now returns a
                        // typed result_t. Same assert+abort terminal
                        // semantics as commit_insert apply today.
                        auto cd_result = co_await std::move(cdif);
                        if (cd_result.failed()) {
                            // Block C §3.1 (Pass 9): mirror of commit_insert
                            // failure path. Goto routes execution to the
                            // shared revert + abort block; publish() stays
                            // unreached so the commit_id remains in-flight.
                            result.cursor = make_cursor(
                                resource(),
                                core::error_t(cd_result.error.type,
                                              std::pmr::string{cd_result.error.what.c_str(), resource()}));
                            goto __block_c_3_1_abort_label;
                        }
                    }
                    // Fire-and-forget auto-GC check (single per call — uses last range's ctx).
                    // Variant E.3 Constraint #11: mailbox-route the
                    // lowest_active_start_time read so the executor stays
                    // free of the shared txn_manager_ pointer.
                    auto [_tl, tlf] = actor_zeta::send(parent_address_,
                                                       &services::dispatcher::manager_dispatcher_t::txn_lowest_active_msg);
                    auto lowest = co_await std::move(tlf);
                    auto [gc_sched, gc_fut] =
                        actor_zeta::send(disk_address_, &disk::manager_disk_t::maybe_cleanup, del_ctx, lowest);
                    pending_void_.push_back(std::move(gc_fut));
                }
            }

            // Flip MVCC tags for pg_catalog rows written during this DML
            // fragment (e.g. pg_computed_column appends produced by
            // operator_computed_field_register_t). Same txn; same commit_id;
            // swap inline so the rows become visible to the next reader
            // without dispatcher round-trips. Without this block the catalog
            // rows would carry insert_id == txn_id and be permanently
            // invisible (since txn_manager has already removed the active
            // txn entry by the time of return).
            if (commit_id > 0) {
                // Block C §3.5 dec 32 V2 OPTION X: backfill pg_attribute
                // added_at_commit_id / dropped_at_commit_id BEFORE
                // storage_publish_commits so the published row already
                // carries the correct visibility endpoints.
                //
                // The manager_disk_t::update_pg_attribute_commit_id_field
                // API has landed (manager_disk_ddl.cpp:171) — it locates the
                // pg_attribute row by attoid and patches column 10 (added_at)
                // or 11 (dropped_at) with the committing commit_id. WAL-replay
                // safe: the patch is a metadata-only field update on a row the
                // CURRENT txn just wrote (insert_id == txn_id, not yet
                // committed), so no concurrent reader observes it pre-flip.
                //
                // TODO follow-up: wire the per-backfill mailbox send here so
                // the visibility endpoints are stamped before
                // storage_publish_commits. Today the markers still propagate
                // through execute_result_t / transaction_t and surface in
                // operator_commit_transaction_t for diagnostic logging;
                // OPTION Z 0-placeholder semantics remain in effect on disk.
                (void) result.pg_attribute_commit_id_backfills;

                if (!result.pg_catalog_appends.empty()) {
                    components::execution_context_t pgc_ctx{session, txn_data, {}};
                    auto [_pa, paf] = actor_zeta::send(disk_address_,
                                                       &disk::manager_disk_t::storage_publish_commits,
                                                       pgc_ctx,
                                                       commit_id,
                                                       std::move(result.pg_catalog_appends));
                    co_await std::move(paf);
                    result.pg_catalog_appends.clear();
                }
                if (!result.pg_catalog_delete_tables.empty()) {
                    components::execution_context_t pgc_ctx{session, txn_data, {}};
                    auto [_pd, pdf] = actor_zeta::send(disk_address_,
                                                       &disk::manager_disk_t::storage_publish_deletes,
                                                       pgc_ctx,
                                                       commit_id,
                                                       std::move(result.pg_catalog_delete_tables));
                    co_await std::move(pdf);
                    result.pg_catalog_delete_tables.clear();
                }
            }

            // Step 6: WAL COMMIT marker
            if (wal_address_ != actor_zeta::address_t::empty_address()) {
                // WAL workers keyed by database_oid. Single-worker model uses
                // main_database for all DML; multi-database routing follows
                // once CREATE DATABASE allocates per-namespace workers.
                constexpr auto db_oid = components::catalog::well_known_oid::main_database;
                // Block F: commit_id carried into the COMMIT record so
                // snapshot-aware replay restores published_horizon_ without
                // re-running business logic.
                auto [_wc, wcf] = actor_zeta::send(wal_address_,
                                                   &wal::manager_wal_replicate_t::commit_txn,
                                                   session,
                                                   txn_data.transaction_id,
                                                   wal::wal_sync_mode::FULL,
                                                   db_oid,
                                                   commit_id);
                co_await std::move(wcf);
            }

            // Step 7: Block E ProcArray publish barrier (Pass 9 dec 46). After WAL
            // FULL fsync + storage_publish_* + commit_insert, advance
            // published_horizon_ so the next snapshot sees this commit_id as
            // visible. Until this call, the commit_id sits in in_flight_commits_
            // and is filtered out of every concurrent reader's snapshot.
            // Variant E.3 Constraint #11: mailbox-route the publish() call.
            if (commit_id > 0) {
                auto [_tp, tpf] = actor_zeta::send(parent_address_,
                                                   &services::dispatcher::manager_dispatcher_t::txn_publish_msg,
                                                   commit_id);
                co_await std::move(tpf);
            }

            co_return execute_result_t{std::move(result.cursor),
                                       std::move(result.updates),
                                       std::move(result.pg_catalog_appends),
                                       std::move(result.pg_catalog_delete_tables),
                                       std::move(result.pg_attribute_commit_id_backfills)};

        } else if (is_dml && result.cursor->is_error()) {
        __block_c_3_1_abort_label:
            // Block C §3.1 (Pass 9): label reached both from the existing
            // operator-error abort path (cursor flipped to error inside
            // execute_sub_plan_) and from the commit-phase index failure
            // branches above. Sharing one label keeps revert+abort logic in
            // exactly one place.
            // Abort path
            trace(log_, "executor::execute_plan: DML error, aborting txn");
            // Oid-only routing on abort path; same rationale as commit path.
            // B33: revert ALL accumulated ranges (mirrors commit-phase iteration).
            for (const auto& app : result.dml_appends) {
                components::execution_context_t abort_ctx{session,
                                                          txn_data,
                                                          context_storage.session_timezone,
                                                          app.table_oid};
                auto [_ra, raf] = actor_zeta::send(disk_address_,
                                                   &disk::manager_disk_t::storage_revert_append,
                                                   abort_ctx,
                                                   app.table_oid,
                                                   app.row_start,
                                                   app.row_count);
                co_await std::move(raf);
                if (index_address_ != actor_zeta::address_t::empty_address()) {
                    auto [_ri, rif] = actor_zeta::send(index_address_,
                                                       &index::manager_index_t::revert_insert,
                                                       abort_ctx,
                                                       app.table_oid);
                    co_await std::move(rif);
                }
            }
            // Revert pg_catalog appends written in this fragment before
            // aborting the txn (otherwise the rows linger with
            // insert_id = txn_id and are unreachable). delete_tables
            // tombstones with delete_id = txn_id are reverted by storage's
            // abort path on txn_manager_->abort.
            if (!result.pg_catalog_appends.empty()) {
                components::execution_context_t pgc_ctx{session, txn_data, {}};
                auto [_pa, paf] = actor_zeta::send(disk_address_,
                                                   &disk::manager_disk_t::storage_revert_appends,
                                                   pgc_ctx,
                                                   std::move(result.pg_catalog_appends));
                co_await std::move(paf);
                result.pg_catalog_appends.clear();
            }
            result.pg_catalog_delete_tables.clear();
            // OPTION X: pg_attribute backfills are tied to the appended rows
            // that were just reverted above (same pg_catalog_appends batch),
            // so the markers are moot on abort. Drop them.
            result.pg_attribute_commit_id_backfills.clear();
            // Variant E.3 Constraint #11: mailbox-route the abort.
            {
                auto [_ta, taf] = actor_zeta::send(parent_address_,
                                                   &services::dispatcher::manager_dispatcher_t::txn_abort_msg,
                                                   session);
                co_await std::move(taf);
            }
        }

        co_return execute_result_t{std::move(result.cursor),
                                   std::move(result.updates),
                                   std::move(result.pg_catalog_appends),
                                   std::move(result.pg_catalog_delete_tables),
                                   std::move(result.pg_attribute_commit_id_backfills)};
    }

    executor_t::unique_future<execute_result_t>
    executor_t::execute_plan_full(components::session::session_id_t session,
                                   components::logical_plan::node_ptr logical_plan,
                                   components::logical_plan::storage_parameters parameters,
                                   services::context_storage_t context_storage,
                                   components::table::transaction_data txn) {
        // Variant E.3 Pass 1 catalog resolve — skeleton wiring for migration
        // from manager_dispatcher_t::execute_plan_impl. After Pass 1 completes
        // (all OIDs resolved/stamped onto the plan tree), control delegates to
        // execute_plan which drives the operator pipeline.
        //
        // 1. begin_transaction for the resolve scope. Variant E.3 Constraint
        //    #11: route through the dispatcher's txn_begin_msg mailbox
        //    handler instead of dereferencing the raw txn_manager_ pointer
        //    (which is shared with the dispatcher actor — forbidden as
        //    actor↔actor shared mutable state). The dispatcher serializes
        //    the txn_manager_.begin_transaction call in its own actor
        //    context. begin_transaction is idempotent per session (returns
        //    the existing active txn if one is already started — which is
        //    the case while the dispatcher's own Pass 1 still runs
        //    upstream). Once the dispatcher Pass 1 is retired, this call
        //    becomes the authoritative resolve-scope txn.
        components::table::transaction_data resolve_txn;
        {
            auto [_tb, tbf] = actor_zeta::send(parent_address_,
                                               &services::dispatcher::manager_dispatcher_t::txn_begin_msg,
                                               session);
            resolve_txn = co_await std::move(tbf);
        }
        trace(log_,
              "executor::execute_plan_full: Pass 1 resolve-scope txn {}, session: {}",
              resolve_txn.transaction_id,
              session.data());

        // 2. Walk logical_plan tree for catalog_resolve_*_t front-children;
        //    for each, build a tiny sub-plan, run through operator_resolve_*_t
        //    via the executor's own operator pipeline (the resolve operators
        //    talk to disk_address_ via mailbox sends — see
        //    components/physical_plan_generator/impl/create_plan_resolve_*).
        //    The operator_resolve_*_t back-pointer constructor stamps OIDs onto
        //    the SAME logical nodes that remain in the parent plan tree, so
        //    validate / enrich downstream see the stamped OIDs.
        //
        // FOR NOW: this iteration only wires the begin_transaction skeleton +
        // delegation. The dispatcher's Pass 1
        // (services/dispatcher/dispatcher.cpp lines 738-803, plus the
        // post-Pass-1 stamp_oids_from_resolves / gather_plan_resolve_index at
        // lines 808-813 and the Phase 1.5 view expansion at lines 815-869)
        // continues to run upstream and is fully responsible for OID stamping
        // before the plan reaches us here.
        //
        // Variant E.3 Pass 1 migration — blocker catalogue (all resolved):
        //   (a) RESOLVED — recursion uses `co_await this->execute_plan` for
        //       the resolve sub-plan instead of dispatcher.execute_plan_impl
        //       (which would have been a forbidden sync inter-actor call).
        //       Safe because operator_resolve_*_t only talks to disk_address_
        //       via async mailbox sends.
        //   (b) RESOLVED — Pass 1 helpers were promoted out of
        //       services::dispatcher::impl into services::catalog_resolve
        //       (enrich_logical_plan.hpp / plan_resolve_index.hpp). The
        //       executor calls catalog_resolve::{stamp_oids_from_resolves,
        //       gather_plan_resolve_index, find_first_view_resolve,
        //       expand_view_body, extract_unresolved_resolves} directly.
        //   (c) RESOLVED — Pass 1 builds a fresh, lightweight
        //       services::context_storage_t for the sub-plan so the caller's
        //       `context_storage` (std::move'd into the final delegate at the
        //       end of this method) is not consumed by the Pass 1 call.
        //   (d) RESOLVED — `resolve_txn` (begun above) is forwarded both into
        //       the Pass 1 sub-plan and into the final `execute_plan` delegate,
        //       mirroring the dispatcher's `ctx = {..., pass1_txn, ...}`
        //       propagation (dispatcher.cpp:684).
        //
        // === Variant E.3 Pass 1 partial copy (iteration 2) ===
        // Copies the Pass 1 resolve-loop body from
        // services/dispatcher/dispatcher.cpp lines ~641-692. The dispatcher
        // recursion `execute_plan_impl(...)` (dispatcher.cpp:673) is replaced
        // with an in-actor `co_await this->execute_plan(...)` call, which is
        // safe because:
        //   * The pass1 sub-plan contains only catalog_resolve_*_t children;
        //     their physical counterparts (operator_resolve_*_t) talk to
        //     disk_address_ via async mailbox sends only — no cross-actor
        //     synchronous calls, no shared mutable state (Constraint #11).
        //   * `this->execute_plan` runs in the same actor coroutine context
        //     so there is no inter-actor round-trip.
        //
        // Iteration-1 stamp+gather block (previously inlined here for the
        // smoke-test) is now performed AFTER the resolve-loop, matching the
        // dispatcher's ordering (dispatcher.cpp:697-702 runs immediately
        // after the resolve loop).
        //
        // Iteration 3 will copy the Phase 1.5 view expansion block
        // (dispatcher.cpp lines ~704-759).
        if (logical_plan && logical_plan->type() == components::logical_plan::node_type::sequence_t) {
            auto& kids = logical_plan->children();
            auto is_resolve = [](components::logical_plan::node_type t) {
                return t == components::logical_plan::node_type::catalog_resolve_namespace_t ||
                       t == components::logical_plan::node_type::catalog_resolve_table_t ||
                       t == components::logical_plan::node_type::catalog_resolve_type_t ||
                       t == components::logical_plan::node_type::catalog_resolve_function_t ||
                       t == components::logical_plan::node_type::catalog_resolve_constraint_t;
            };
            std::size_t resolve_count = 0;
            while (resolve_count < kids.size() && kids[resolve_count] &&
                   is_resolve(kids[resolve_count]->type())) {
                ++resolve_count;
            }
            if (resolve_count > 0) {
                // Build the Pass 1 sub-plan as a sequence_t containing the
                // resolve front children. operator_resolve_*_t carries a
                // raw pointer to the logical node — those are the SAME
                // node objects shared with the parent's sequence_t, so
                // OIDs stamped during Pass 1 become visible to the parent
                // plan's validate/enrich pass that follows.
                auto pass1_root = boost::intrusive_ptr<components::logical_plan::node_t>(
                    new components::logical_plan::node_sequence_t(resource()));
                for (std::size_t i = 0; i < resolve_count; ++i) {
                    pass1_root->append_child(kids[i]);
                }
                auto pass1_params = components::logical_plan::make_parameter_node(resource());
                // Iteration 2 — recursive sub-plan execute via this->execute_plan
                // (not execute_plan_impl). Build a lightweight, throw-away
                // context_storage_t so the caller's `context_storage` (which
                // is move-consumed by the final delegate at the bottom of
                // execute_plan_full) survives untouched. The fresh storage
                // carries only the bits the resolve operators read:
                // resource / log / session_timezone. known_oids /
                // table_metadata are left empty — resolves stamp directly
                // onto the logical_plan tree, not into context_storage.
                services::context_storage_t pass1_context_storage{resource(),
                                                                   log_.clone(),
                                                                   context_storage.session_timezone};
                auto pass1_result = co_await this->execute_plan(session,
                                                                 pass1_root,
                                                                 pass1_params->take_parameters(),
                                                                 std::move(pass1_context_storage),
                                                                 resolve_txn);
                if (pass1_result.cursor->is_error()) {
                    trace(log_,
                          "executor::execute_plan_full: Pass 1 resolve failed: {}",
                          pass1_result.cursor->get_error().what);
                    co_return execute_result_t{std::move(pass1_result.cursor)};
                }
                // Note: resolves stay in plan tree so validate/enrich's
                // gather walks find them. create_plan_sequence skips
                // catalog_resolve_*_t children when building the executor's
                // left-chain (they have already run in Pass 1; putting
                // them in operator_insert.left_ would corrupt insert's
                // data input — see create_plan_sequence.cpp note).
                //
                // Dispatcher mirror dispatcher.cpp:684 also does
                //   ctx = execution_context_t{session, pass1_txn, ...};
                // for its own downstream consumers. The executor's
                // downstream `execute_plan` delegate already receives
                // `resolve_txn` directly as the `txn` parameter, so no
                // ctx rebuild is required here.
            }
        }
        // Post-Pass-1 stamp + index gather — dispatcher.cpp lines ~697-702.
        // Both helpers are pure tree-walks with no actor / mailbox
        // dependency, idempotent against the dispatcher's own Pass 1 that
        // still runs upstream (stamping writes the same oid_t twice;
        // gather skips nodes whose oid is still INVALID_OID).
        //
        // local_idx is built but not yet consumed — kept as a self-check
        // until validate/enrich migrate into the executor (then they will
        // read OIDs from this idx instead of re-walking the dispatcher's
        // copy).
        if (logical_plan) {
            services::catalog_resolve::stamp_oids_from_resolves(logical_plan.get());
            services::catalog_resolve::plan_resolve_index_t local_idx;
            services::catalog_resolve::gather_plan_resolve_index(logical_plan.get(), local_idx);
            trace(log_,
                  "executor::execute_plan_full: Pass 1 stamp+gather — "
                  "ns_by_dbname={}, tbl_ns_by_qname={}, type_oid_by_qname={}, fn_oid_by_qname={}",
                  local_idx.ns_by_dbname.size(),
                  local_idx.tbl_ns_by_qname.size(),
                  local_idx.type_oid_by_qname.size(),
                  local_idx.fn_oid_by_qname.size());
            (void)local_idx;
        }
        // === Variant E.3 Pass 1 partial copy (iteration 3) ===
        // Phase 1.5 SELECT-time view expansion + Phase 1.6 fresh-resolve
        // sub-execute. Mirrors services/dispatcher/dispatcher.cpp
        // lines ~704-759. After Pass 1 stamped resolved_metadata.view_sql
        // on catalog_resolve_table_t nodes with relkind=='v', re-parse +
        // re-transform the view body and splice the resulting sub-plan
        // in place. First-iteration scope: only top-level passthrough
        // plans (`SELECT * FROM v`) — replace the entire logical_plan
        // with the sub-plan. More elaborate compositions (extra
        // filters/projections/joins on top of v) are followup #1.
        //
        // The dispatcher recursion `execute_plan_impl(...)` for the
        // sub-plan's fresh resolves (Phase 1.6) is replaced with
        // `co_await this->execute_plan(...)`, same routing pattern used
        // in iteration 2 — safe by the same reasoning (resolve sub-plan
        // contains only catalog_resolve_*_t children whose operators
        // talk to disk_address_ via async mailbox sends only;
        // Constraint #11 respected).
        if (logical_plan) {
            if (auto* view_node = services::catalog_resolve::find_first_view_resolve(logical_plan.get())) {
                auto exp = services::catalog_resolve::expand_view_body(resource(),
                                                                       view_node->resolved_metadata()->view_sql);
                if (exp.error) {
                    trace(log_, "executor::execute_plan_full: view expansion failed");
                    co_return execute_result_t{std::move(exp.error)};
                }
                if (exp.had_expansion && exp.expanded_plan) {
                    // Full plan replacement — outer is treated as
                    // trivial passthrough for first iteration. Future
                    // work (followup #1): splice sub-plan as child of
                    // outer consumer to preserve outer projections /
                    // filters.
                    logical_plan = std::move(exp.expanded_plan);

                    // Merge sub-plan's parameter bindings into the
                    // outer `parameters` storage so downstream operators
                    // see constants used in the view body (e.g.
                    // `col_b > 10`). First-iteration assumes no
                    // parameter_id collision with outer (outer is a
                    // trivial passthrough SELECT * with no own
                    // constants). The dispatcher mirror uses
                    // `params->add_parameter(...)` on the parameter_node;
                    // here we receive raw `storage_parameters` and call
                    // the free-fn overload directly.
                    if (exp.expanded_params) {
                        for (const auto& [pid, val] : exp.expanded_params->parameters().parameters) {
                            components::logical_plan::add_parameter(parameters, pid, val);
                        }
                    }

                    // === Phase 1.6: Pass 1 on sub-plan's fresh resolves ===
                    auto fresh = services::catalog_resolve::extract_unresolved_resolves(logical_plan.get());
                    if (!fresh.empty()) {
                        auto pass2_root = boost::intrusive_ptr<components::logical_plan::node_t>(
                            new components::logical_plan::node_sequence_t(resource()));
                        for (auto& n : fresh) {
                            pass2_root->append_child(n);
                        }
                        auto pass2_params = components::logical_plan::make_parameter_node(resource());
                        // Iteration-3 recursive sub-plan execute via
                        // this->execute_plan (NOT execute_plan_impl).
                        // Fresh, throw-away context_storage_t mirrors
                        // the iteration-2 Pass 1 sub-plan call — the
                        // caller's `context_storage` survives untouched
                        // for the final delegate at the bottom of
                        // execute_plan_full.
                        services::context_storage_t pass2_context_storage{resource(),
                                                                          log_.clone(),
                                                                          context_storage.session_timezone};
                        auto pass2_result = co_await this->execute_plan(session,
                                                                         pass2_root,
                                                                         pass2_params->take_parameters(),
                                                                         std::move(pass2_context_storage),
                                                                         resolve_txn);
                        if (pass2_result.cursor->is_error()) {
                            trace(log_,
                                  "executor::execute_plan_full: view sub-plan Pass 1 "
                                  "resolve failed: {}",
                                  pass2_result.cursor->get_error().what);
                            co_return execute_result_t{std::move(pass2_result.cursor)};
                        }
                    }

                    // === Phase 1.7: rebuild idx for re-validate ===
                    // After splicing, freshly-stamped OIDs on the
                    // sub-plan's resolves must be propagated onto their
                    // consumer nodes; the local_idx self-check above
                    // was built against the pre-splice plan, so rebuild
                    // it here against the new plan tree to keep the
                    // post-Pass-1 invariant (validate / enrich see
                    // consistent OIDs on consumer nodes).
                    services::catalog_resolve::stamp_oids_from_resolves(logical_plan.get());
                    services::catalog_resolve::plan_resolve_index_t post_view_idx;
                    services::catalog_resolve::gather_plan_resolve_index(logical_plan.get(), post_view_idx);
                    trace(log_,
                          "executor::execute_plan_full: Phase 1.7 post-view stamp+gather — "
                          "ns_by_dbname={}, tbl_ns_by_qname={}, type_oid_by_qname={}, fn_oid_by_qname={}",
                          post_view_idx.ns_by_dbname.size(),
                          post_view_idx.tbl_ns_by_qname.size(),
                          post_view_idx.type_oid_by_qname.size(),
                          post_view_idx.fn_oid_by_qname.size());
                    (void)post_view_idx;
                }
            }
        }
        // === Variant E.3 Pass 2 enrich/validate migration ===
        // Mirrors services/dispatcher/dispatcher.cpp lines ~782-1176:
        //   * Build the post-view dispatcher_idx (already built above as
        //     local_idx / post_view_idx — gather once more here for
        //     parity with the dispatcher ordering after view splice).
        //   * Build qualified_name_t for the effective consumer node via
        //     build_id_cfn (closure mirrors dispatcher.cpp lines 787-865).
        //   * Switch on original_type → namespace / table / type existence
        //     checks via catalog_resolve helpers (no async / no member
        //     state on the executor besides resource()).
        //   * Default branch → validate_types + validate_schema.
        //   * post_validate_optimize → enrich_plan → planner.create_plan.
        // Dispatcher branches that depend on dispatcher-only state
        // (collections_ map for drop_collection_t; default_tz_cat_ for
        // set_timezone_t) are skipped with a TODO — those paths still
        // reach the dispatcher upstream and the executor's idempotent
        // delegate to execute_plan below preserves behaviour for now.
        using node_type = components::logical_plan::node_type;
        using components::logical_plan::node_aggregate_t;
        using components::logical_plan::node_create_collection_t;
        using components::logical_plan::node_create_constraint_t;
        using components::logical_plan::node_create_database_t;
        using components::logical_plan::node_create_type_t;
        using components::logical_plan::node_match_t;
        using components::catalog::table_id;
        using components::logical_plan::constraint_kind;
        using components::types::logical_type;

        // Capture original_type before any post-validate planner rewrites.
        const node_type original_type =
            logical_plan ? services::catalog_resolve::effective_root_node(logical_plan.get())->type()
                         : node_type::unused;

        // Rebuild dispatcher_idx against the (possibly view-spliced) plan
        // tree so validate / enrich / build_id_cfn see fully-stamped OIDs.
        // Mirrors dispatcher.cpp:723-724 (and the Phase 1.7 rebuild at 778).
        services::catalog_resolve::plan_resolve_index_t dispatcher_idx;
        if (logical_plan) {
            services::catalog_resolve::stamp_oids_from_resolves(logical_plan.get());
            services::catalog_resolve::gather_plan_resolve_index(logical_plan.get(), dispatcher_idx);
        }

        // Closure mirror of dispatcher.cpp:787-865. Builds qualified_name_t
        // from the effective consumer node; nodes that don't carry user-
        // typed names pull (db, rel) from the sibling resolve_* nodes via
        // drop_target_names_from_resolves.
        const auto* plan_root_for_drop_names = logical_plan.get();
        auto build_id_cfn = [plan_root_for_drop_names](const components::logical_plan::node_t* n) -> qualified_name_t {
            using components::logical_plan::node_create_database_t;
            using components::logical_plan::node_create_macro_t;
            using components::logical_plan::node_create_sequence_t;
            using components::logical_plan::node_create_view_t;
            if (!n)
                return {};
            switch (n->type()) {
                case node_type::aggregate_t: {
                    auto* d = static_cast<const node_aggregate_t*>(n);
                    return qualified_name_t{static_cast<const std::string&>(d->dbname()),
                                            static_cast<const std::string&>(d->relname())};
                }
                case node_type::alter_column_add_t:
                case node_type::alter_column_drop_t:
                case node_type::alter_column_rename_t:
                case node_type::alter_table_t: {
                    auto names = services::catalog_resolve::drop_target_names_from_resolves(plan_root_for_drop_names);
                    return qualified_name_t{names.first, names.second};
                }
                case node_type::create_collection_t: {
                    auto* d = static_cast<const node_create_collection_t*>(n);
                    auto names = services::catalog_resolve::drop_target_names_from_resolves(plan_root_for_drop_names);
                    return qualified_name_t{names.first, static_cast<const std::string&>(d->relname())};
                }
                case node_type::create_constraint_t: {
                    auto* d = static_cast<const node_create_constraint_t*>(n);
                    return qualified_name_t{static_cast<const std::string&>(d->dbname()),
                                            static_cast<const std::string&>(d->relname())};
                }
                case node_type::create_database_t: {
                    auto* d = static_cast<const node_create_database_t*>(n);
                    return qualified_name_t{d->dbname(), std::string{}};
                }
                case node_type::create_index_t: {
                    auto names = services::catalog_resolve::drop_target_names_from_resolves(plan_root_for_drop_names);
                    return qualified_name_t{names.first, names.second};
                }
                case node_type::create_macro_t: {
                    auto* d = static_cast<const node_create_macro_t*>(n);
                    auto names = services::catalog_resolve::drop_target_names_from_resolves(plan_root_for_drop_names);
                    return qualified_name_t{names.first, d->macroname()};
                }
                case node_type::create_sequence_t: {
                    auto* d = static_cast<const node_create_sequence_t*>(n);
                    auto names = services::catalog_resolve::drop_target_names_from_resolves(plan_root_for_drop_names);
                    return qualified_name_t{names.first, d->seqname()};
                }
                case node_type::create_view_t: {
                    auto* d = static_cast<const node_create_view_t*>(n);
                    auto names = services::catalog_resolve::drop_target_names_from_resolves(plan_root_for_drop_names);
                    return qualified_name_t{names.first, d->viewname()};
                }
                case node_type::delete_t:
                case node_type::insert_t:
                case node_type::update_t:
                case node_type::drop_collection_t:
                case node_type::drop_index_t:
                case node_type::drop_macro_t:
                case node_type::drop_sequence_t:
                case node_type::drop_view_t: {
                    auto names = services::catalog_resolve::drop_target_names_from_resolves(plan_root_for_drop_names);
                    return qualified_name_t{names.first, names.second};
                }
                case node_type::drop_database_t: {
                    auto names = services::catalog_resolve::drop_target_names_from_resolves(plan_root_for_drop_names);
                    return qualified_name_t{names.first, std::string{}};
                }
                case node_type::match_t: {
                    auto* d = static_cast<const node_match_t*>(n);
                    return qualified_name_t{static_cast<const std::string&>(d->dbname()),
                                            static_cast<const std::string&>(d->relname())};
                }
                default:
                    return {};
            }
        };

        // Build identification name from the effective consumer node, not
        // the (potentially transformer-wrapping) sequence_t.
        table_id id(resource(),
                    build_id_cfn(services::catalog_resolve::effective_root_node(logical_plan.get())));
        cursor_t_ptr error;
        // Mirror dispatcher.cpp:874 — existence checks read from the
        // explicit dispatcher_idx populated above.
        switch (original_type) {
            case node_type::create_database_t:
                if (!services::dispatcher::check_namespace_exists(resource(), &dispatcher_idx, id).contains_error()) {
                    auto* d = static_cast<const node_create_database_t*>(
                        services::catalog_resolve::effective_root_node(logical_plan.get()));
                    if (d && d->if_not_exists()) {
                        error = make_cursor(resource());
                    } else {
                        error = make_cursor(resource(),
                                            core::error_t{core::error_code_t::database_already_exists,
                                                          std::pmr::string{"database already exists", resource()}});
                    }
                }
                break;
            case node_type::drop_database_t:
                if (auto err = services::dispatcher::check_namespace_exists(resource(), &dispatcher_idx, id);
                    err.contains_error()) {
                    error = make_cursor(resource(), err);
                }
                break;
            case node_type::create_collection_t: {
                if (!services::dispatcher::check_collection_exists(resource(), &dispatcher_idx, id)
                         .contains_error()) {
                    auto* cc = static_cast<const node_create_collection_t*>(
                        services::catalog_resolve::effective_root_node(logical_plan.get()));
                    if (cc && cc->if_not_exists()) {
                        error = make_cursor(resource());
                    } else {
                        error = make_cursor(resource(),
                                            core::error_t{core::error_code_t::table_already_exists,
                                                          std::pmr::string{"collection already exists", resource()}});
                    }
                } else {
                    const std::string target_db =
                        id.get_namespace().empty() ? std::string{} : std::string(id.get_namespace().front());
                    const auto str_path = services::catalog_resolve::build_type_search_path_str(target_db);
                    auto* n = static_cast<node_create_collection_t*>(
                        services::catalog_resolve::effective_root_node(logical_plan.get()));
                    for (auto& col_def : n->column_definitions()) {
                        if (col_def.type().type() == logical_type::UNKNOWN) {
                            if (col_def.type().type_name().empty()) {
                                break;
                            }
                            const auto lt =
                                components::catalog::pg_name_to_logical_type(col_def.type().type_name());
                            if (lt != logical_type::UNKNOWN) {
                                std::string alias =
                                    col_def.type().has_alias() ? col_def.type().alias() : std::string{};
                                col_def.type() = components::types::complex_logical_type{lt};
                                if (!alias.empty()) {
                                    col_def.type().set_alias(alias);
                                }
                                continue;
                            }
                            if (auto err = services::dispatcher::check_type_exists(
                                    resource(),
                                    &dispatcher_idx,
                                    col_def.type().type_name(),
                                    std::span<const std::string>(str_path));
                                err.contains_error()) {
                                error = make_cursor(resource(), err);
                            }
                            if (!error) {
                                const auto* md = services::catalog_resolve::probe_type_in_path(
                                    &dispatcher_idx,
                                    std::string_view(col_def.type().type_name()),
                                    std::span<const std::string>(str_path));
                                if (md) {
                                    std::string alias =
                                        col_def.type().has_alias() ? col_def.type().alias() : std::string{};
                                    col_def.type() = md->type;
                                    if (!alias.empty()) {
                                        col_def.type().set_alias(alias);
                                    }
                                }
                            }
                        }
                    }
                }
                break;
            }
            case node_type::drop_collection_t: {
                // Variant E.3 Section 1 cut-over: parity with dispatcher.cpp
                // drop_collection_t branch (dispatcher.cpp:895-910). The
                // dispatcher does TWO checks in order:
                //   1) collections_.count(qualified_name_t{db, rel}) — its
                //      own routing-map probe; miss → table_not_exists.
                //   2) check_collection_exists(&dispatcher_idx, id) —
                //      catalog metadata via plan-tree idx.
                //
                // The executor's analogue of collections_ is the per-actor
                // local_collections_ partition (Step 2; executor.hpp:159-186),
                // keyed by table_oid and hash-routed across executor slices.
                // Resolve (db, rel) → oid through the plan-tree idx, then
                // probe local_collections_ for the hot-path intra-partition
                // confirmation.
                //
                // Cross-partition limitation (documented): if the target oid
                // hashes to a different executor slice, find_local_collection
                // returns nullptr even when the table exists. We therefore
                // treat a local MISS as "inconclusive — fall through" rather
                // than "not exists"; the subsequent check_collection_exists
                // call resolves the catalog-level question authoritatively
                // (same error code + message as the dispatcher's
                // collections_.count() failure path, so cursor parity holds).
                // A local HIT short-circuits to skip the catalog probe
                // since the entry implies the table is owned by this slice.
                bool local_hit = false;
                if (const auto* tbl_md =
                        services::catalog_resolve::tbl_md_for(&dispatcher_idx,
                                                              id.get_namespace().empty()
                                                                  ? std::string_view{}
                                                                  : std::string_view(id.get_namespace().front()),
                                                              std::string_view(id.table_name()));
                    tbl_md && tbl_md->table_oid != components::catalog::INVALID_OID) {
                    if (find_local_collection(tbl_md->table_oid) != nullptr) {
                        local_hit = true;
                    }
                }
                if (!local_hit) {
                    if (auto err = services::dispatcher::check_collection_exists(resource(), &dispatcher_idx, id);
                        err.contains_error()) {
                        error = make_cursor(resource(), err);
                    }
                }
                break;
            }
            case node_type::create_type_t: {
                auto* n = static_cast<node_create_type_t*>(
                    services::catalog_resolve::effective_root_node(logical_plan.get()));
                components::catalog::oid_t target_ns = components::catalog::well_known_oid::public_namespace;
                const std::string default_path[] = {"public", "pg_catalog"};
                std::span<const std::string> str_path(default_path);
                if (!services::dispatcher::check_type_exists(resource(),
                                                              &dispatcher_idx,
                                                              n->type().type_name(),
                                                              str_path)
                         .contains_error()) {
                    error = make_cursor(
                        resource(),
                        core::error_t{core::error_code_t::schema_error,
                                      std::pmr::string{("type: \'" + n->type().alias() + "\' already exists").c_str(),
                                                       resource()}});
                    break;
                }
                if (n->type().type() == logical_type::STRUCT) {
                    for (auto& field : n->type().child_types()) {
                        if (field.type() == logical_type::UNKNOWN) {
                            const auto lt = components::catalog::pg_name_to_logical_type(field.type_name());
                            if (lt != logical_type::UNKNOWN) {
                                std::string alias = field.has_alias() ? field.alias() : std::string{};
                                field = components::types::complex_logical_type{lt};
                                if (!alias.empty()) {
                                    field.set_alias(alias);
                                }
                                continue;
                            }
                            if (auto err = services::dispatcher::check_type_exists(resource(),
                                                                                    &dispatcher_idx,
                                                                                    field.type_name(),
                                                                                    str_path);
                                err.contains_error()) {
                                error = make_cursor(resource(), err);
                                break;
                            }
                            const auto* md = services::catalog_resolve::probe_type_in_path(
                                &dispatcher_idx, std::string_view(field.type_name()), str_path);
                            if (md) {
                                std::string alias = field.has_alias() ? field.alias() : std::string{};
                                field = md->type;
                                if (!alias.empty()) {
                                    field.set_alias(alias);
                                }
                            }
                        }
                    }
                    if (error) {
                        break;
                    }
                }
                n->set_namespace_oid(target_ns);
                break;
            }
            case node_type::drop_type_t: {
                std::string type_name;
                if (logical_plan->type() == node_type::sequence_t) {
                    for (const auto& c : logical_plan->children()) {
                        if (c && c->type() == node_type::catalog_resolve_type_t) {
                            type_name =
                                static_cast<const components::logical_plan::node_catalog_resolve_type_t*>(c.get())
                                    ->type_name();
                            break;
                        }
                    }
                }
                const std::string default_path[] = {"public", "pg_catalog"};
                std::span<const std::string> str_path(default_path);
                if (auto err = services::dispatcher::check_type_exists(resource(),
                                                                        &dispatcher_idx,
                                                                        type_name,
                                                                        str_path);
                    err.contains_error()) {
                    error = make_cursor(resource(), err);
                }
                break;
            }
            case node_type::set_timezone_t: {
                // Variant E.3 Constraint #11: SET TIME ZONE mutates the
                // dispatcher's default_tz_cat_ (single-owner, mailbox-only)
                // and appends a ("TimeZone", <name>) row to pg_settings via
                // disk_address_. Both happen inside set_default_timezone_msg
                // on the dispatcher's actor context. The executor just
                // forwards the timezone name string and awaits the result
                // cursor; on error it's surfaced via the standard Pass 2
                // `error` channel.
                const auto& tz_node =
                    reinterpret_cast<components::logical_plan::node_set_timezone_ptr&>(logical_plan);
                std::pmr::string tz_name{tz_node->timezone_name().c_str(),
                                          tz_node->timezone_name().size(),
                                          resource()};
                auto [_stz, stzf] =
                    actor_zeta::send(parent_address_,
                                     &services::dispatcher::manager_dispatcher_t::set_default_timezone_msg,
                                     session,
                                     std::move(tz_name));
                auto tz_cursor = co_await std::move(stzf);
                if (tz_cursor && tz_cursor->is_error()) {
                    error = std::move(tz_cursor);
                }
                break;
            }
            case node_type::checkpoint_t:
            case node_type::vacuum_t:
            case node_type::create_sequence_t:
            case node_type::drop_sequence_t:
            case node_type::create_view_t:
            case node_type::drop_view_t:
            case node_type::create_macro_t:
            case node_type::drop_macro_t:
                break;
            case node_type::alter_table_t:
                // ALTER TABLE metadata already stamped on plan-tree by
                // Pass 1; nothing to verify here.
                break;
            case node_type::create_constraint_t: {
                if (auto err = services::dispatcher::check_collection_exists(resource(), &dispatcher_idx, id);
                    err.contains_error()) {
                    error = make_cursor(resource(), err);
                }
                if (!error && !id.get_namespace().empty()) {
                    auto* cstr = static_cast<node_create_constraint_t*>(
                        services::catalog_resolve::effective_root_node(logical_plan.get()));
                    if (cstr->kind() == constraint_kind::foreign_key ||
                        cstr->kind() == constraint_kind::check) {
                        const auto* tbl_local =
                            services::catalog_resolve::tbl_md_for(&dispatcher_idx,
                                                                  std::string_view(id.get_namespace().front()),
                                                                  std::string_view(id.table_name()));
                        const bool local_is_g = tbl_local && tbl_local->relkind == 'g';
                        bool ref_is_g = false;
                        if (cstr->kind() == constraint_kind::foreign_key &&
                            cstr->ref_table_oid() != components::catalog::INVALID_OID) {
                            const auto* tbl_ref =
                                services::catalog_resolve::tbl_md_for_oid(&dispatcher_idx, cstr->ref_table_oid());
                            ref_is_g = tbl_ref && tbl_ref->relkind == 'g';
                        }
                        if (cstr->kind() == constraint_kind::foreign_key && (local_is_g || ref_is_g)) {
                            error = make_cursor(
                                resource(),
                                core::error_t{
                                    core::error_code_t::schema_error,
                                    std::pmr::string{
                                        "Foreign key constraints are not supported when the referencing or "
                                        "referenced table is dynamic-schema (relkind='g'). FK enforcement "
                                        "requires stable column attoids; dynamic-schema columns may evolve. "
                                        "Convert involved tables to static schema first.",
                                        resource()}});
                        } else if (cstr->kind() == constraint_kind::check && local_is_g) {
                            error = make_cursor(
                                resource(),
                                core::error_t{
                                    core::error_code_t::schema_error,
                                    std::pmr::string{
                                        "CHECK constraints are not supported on dynamic-schema (relkind='g') "
                                        "tables. CHECK enforcement requires stable column attoids; "
                                        "dynamic-schema columns may evolve. Convert the table to static "
                                        "schema first.",
                                        resource()}});
                        }
                    }
                }
                break;
            }
            default: {
                auto vt_err = services::dispatcher::validate_types(resource(),
                                                                    &dispatcher_idx,
                                                                    logical_plan.get(),
                                                                    context_storage.session_timezone);
                if (vt_err.contains_error()) {
                    error = make_cursor(resource(), vt_err);
                } else {
                    auto schema_res = services::dispatcher::validate_schema(resource(),
                                                                              &dispatcher_idx,
                                                                              logical_plan.get(),
                                                                              parameters);
                    if (schema_res.has_error()) {
                        error = make_cursor(resource(), schema_res.error());
                    }
                }
            }
        }

        if (error) {
            trace(log_,
                  "executor::execute_plan_full: Pass 2 validation error: {}",
                  error->get_error().what);
            co_return execute_result_t{std::move(error)};
        }

        // === Destructive Pass 2 rewrites — gated ===
        // post_validate_optimize, enrich_plan, and planner.create_plan
        // mutate logical_plan in ways that are NOT safely idempotent:
        //   * post_validate_optimize rewrites node_join_t into
        //     node_hash_join_t when eligible — repeating doesn't loop but
        //     duplicates the rewrite pass.
        //   * enrich_plan stamps DML metadata onto nodes; safe to repeat
        //     for cached snapshots but still re-reads the plan-tree idx.
        //   * planner.create_plan wraps insert/update/delete in
        //     check_constraint_t / fk_check_t. Running it twice would
        //     re-wrap on top of the previous wrap — broken plan.
        // The dispatcher upstream currently runs the SAME three passes
        // (dispatcher.cpp:1164-1176) before forwarding to the executor.
        // Until the dispatcher's Pass 2 is removed, gate the executor's
        // copy off by default. The code stays compiled so the migration
        // path is wired and tested by the unit-test follow-up; flipping
        // `enable_pass2_rewrites` to true here is the final cut-over step.
        //
        // Variant E.3 Section-0 cut-over: this flag is paired with
        // `use_executor_full_pipeline` in dispatcher.cpp's anonymous
        // namespace (~line 138). See the SAFETY MATRIX comment there for
        // the four-state correctness table. Target state for the atomic
        // flip commit: both flags `true` PLUS dispatcher.cpp:543-1313
        // (Pass 1/2/3 block) deleted in the same commit, otherwise Pass
        // 1/2/3 runs twice and produces a double-wrapped plan. See
        // docs/variant-e3-cutover-checklist.md Section 0 and Section 6.
        constexpr bool enable_pass2_rewrites = true;
        if (enable_pass2_rewrites) {
            // Late logical optimization. Mirrors dispatcher.cpp:1164.
            logical_plan = components::planner::post_validate_optimize(resource(), std::move(logical_plan));

            // Enrich DML node fields with catalog metadata (NOT NULL,
            // DEFAULT, CHECK exprs). enrich reads exclusively from the
            // plan-tree idx. Mirrors dispatcher.cpp:1169 — ctx carries
            // the resolve_txn so enrich sees the same MVCC snapshot
            // Pass 1 used.
            components::execution_context_t enrich_ctx{session,
                                                       resolve_txn,
                                                       context_storage.session_timezone};
            auto ef = services::dispatcher::enrich_plan(resource(),
                                                         logical_plan,
                                                         disk_address_,
                                                         enrich_ctx,
                                                         index_address_,
                                                         &context_storage);
            co_await std::move(ef);
            // Logical plan rewrite: insert constraint wrapper nodes driven
            // by enriched fields. Mirrors dispatcher.cpp:1173-1176.
            components::planner::planner_t planner;
            logical_plan = planner.create_plan(resource(), std::move(logical_plan));

            // === Variant E.3 Pass 3: INSERT relkind='g' wrap + DDL OID-batch
            //                          allocation ===
            // Mirrors dispatcher.cpp:1076-1310. Lives inside the same
            // enable_pass2_rewrites gate because Pass 3 mutates the
            // logical_plan in ways that conflict with the dispatcher's
            // upstream Pass 3 (would double-wrap INSERT for relkind='g'
            // and re-emit DDL primitive_writes from already-rewritten
            // sequence_t roots). Final cut-over flips both passes off in
            // dispatcher and on here in a single step.

            // Option A (chosen): inline OID allocation via the same
            // pipeline-routed node_allocate_oids_t leaf the dispatcher
            // uses. Self-contained against executor_t members
            // (resource(), disk_address_, txn_manager_, log_,
            // function_registry_).
            //
            // Variant E.3 Constraint #11 note on `pctx.txn_manager =
            // txn_manager_` below: the assignment exposes the raw
            // txn_manager_ pointer to the operator pipeline (pctx /
            // pipeline_context_t). Per Constraint #11 this is NOT an
            // actor↔actor share — the operator runs SYNCHRONOUSLY inside
            // this executor's coroutine (single-threaded actor mailbox
            // semantics), so the txn_manager access happens on the same
            // logical thread as any other executor work. The
            // actor↔actor mutation hazard (executor coroutine running
            // concurrently with the dispatcher coroutine, both touching
            // txn_manager_ at once) is closed by the dispatcher-owned
            // mailbox handlers (txn_begin_msg / txn_commit_msg /
            // txn_abort_msg / txn_publish_msg / txn_lowest_active_msg)
            // which serialize mutations through the dispatcher's mailbox.
            // The cross-actor begin_transaction at the top of
            // execute_plan_full was migrated to txn_begin_msg above.
            // Option B (mailbox-send manager_dispatcher_t::allocate_oid)
            // is unnecessary for the same reason — the OID allocation
            // pipeline is driven inside this actor's coroutine.
            auto allocate_oids_inline =
                [this, session, &context_storage](std::size_t count)
                -> executor_t::unique_future<std::vector<components::catalog::oid_t>> {
                auto node = components::logical_plan::make_node_allocate_oids(resource(), count);
                components::compute::function_registry_t local_fn_registry{resource()};
                services::context_storage_t cstor{resource(),
                                                   log_.clone(),
                                                   context_storage.session_timezone};
                auto op = services::planner::create_plan(cstor,
                                                          local_fn_registry,
                                                          node,
                                                          components::logical_plan::limit_t::unlimit(),
                                                          /*params=*/nullptr);
                if (!op) {
                    co_return std::vector<components::catalog::oid_t>{};
                }
                op->set_as_root();
                components::logical_plan::storage_parameters local_params(resource());
                components::pipeline::context_t pctx{session,
                                                      actor_zeta::address_t::empty_address(),
                                                      actor_zeta::address_t::empty_address(),
                                                      &local_fn_registry,
                                                      local_params};
                pctx.disk_address = disk_address_;
                pctx.txn_manager = txn_manager_;
                pctx.txn = components::table::transaction_data{0, 0};
                op->prepare();
                op->on_execute(&pctx);
                while (!op->is_executed()) {
                    auto waiting = op->find_waiting_operator();
                    if (!waiting)
                        break;
                    co_await waiting->await_async_and_resume(&pctx);
                    op->on_execute(&pctx);
                }
                if (pctx.has_pending_disk_futures()) {
                    auto futures = pctx.take_pending_disk_futures();
                    for (auto& f : futures)
                        co_await std::move(f);
                }
                co_return node->oids();
            };

            using components::logical_plan::node_create_matview_t;
            using components::logical_plan::node_sequence_t;
            using components::catalog::relkind::computed;

            // INSERT relkind='g' wrap — mirrors dispatcher.cpp:1076-1151.
            // Wraps INSERT into sequence_t(insert, computed_field_register)
            // so pg_computed_column rows are appended inside the DML txn.
            if (original_type == node_type::insert_t &&
                disk_address_ != actor_zeta::address_t::empty_address()) {
                components::catalog::oid_t resolved_tbl_oid = components::catalog::INVALID_OID;
                bool is_computing = false;
                auto* effective_insert_node = services::catalog_resolve::effective_root_node(logical_plan.get());
                auto enriched_oid = effective_insert_node ? effective_insert_node->table_oid()
                                                          : logical_plan->table_oid();
                if (enriched_oid == components::catalog::INVALID_OID && !logical_plan->children().empty()) {
                    enriched_oid = logical_plan->children().front()->table_oid();
                }
                if (enriched_oid != components::catalog::INVALID_OID) {
                    if (const auto* tbl = services::catalog_resolve::tbl_md_for_oid(&dispatcher_idx, enriched_oid)) {
                        if (tbl->relkind == computed) {
                            is_computing = true;
                            resolved_tbl_oid = tbl->table_oid;
                        }
                    }
                }

                if (is_computing) {
                    std::vector<components::table::column_definition_t> registered_cols;
                    auto* effective_insert = services::catalog_resolve::effective_root_node(logical_plan.get());
                    if (effective_insert) {
                        for (const auto& child : effective_insert->children()) {
                            if (!child || child->type() != components::logical_plan::node_type::data_t) {
                                continue;
                            }
                            auto* data_node =
                                static_cast<const components::logical_plan::node_data_t*>(child.get());
                            const auto& chunk = data_node->data_chunk();
                            registered_cols.reserve(chunk.column_count());
                            for (size_t i = 0; i < chunk.column_count(); ++i) {
                                const auto& type = chunk.data[i].type();
                                assert(type.has_alias());
                                registered_cols.emplace_back(type.alias(), type);
                            }
                            break;
                        }
                    }

                    auto insert_names =
                        services::catalog_resolve::drop_target_names_from_resolves(logical_plan.get());
                    auto register_node = boost::intrusive_ptr(
                        new components::logical_plan::node_computed_field_register_t(
                            resource(),
                            core::dbname_t{insert_names.first},
                            core::relname_t{insert_names.second},
                            resolved_tbl_oid,
                            std::move(registered_cols)));

                    auto seq = boost::intrusive_ptr(new node_sequence_t(resource()));
                    seq->append_child(logical_plan);
                    seq->append_child(register_node);
                    logical_plan = seq;
                }
            }

            // CREATE COLLECTION → planner rewrite to
            // sequence_t(create_collection_t, primitive_write × N).
            // Mirrors dispatcher.cpp:1156-1164.
            if (original_type == node_type::create_collection_t &&
                disk_address_ != actor_zeta::address_t::empty_address()) {
                auto* cc = static_cast<node_create_collection_t*>(
                    services::catalog_resolve::effective_root_node(logical_plan.get()));
                const std::size_t need = 1 + cc->column_definitions().size();
                components::catalog::oid_batch_t oid_batch;
                oid_batch.oids = co_await allocate_oids_inline(need);
                components::planner::planner_t ddl_planner;
                logical_plan = ddl_planner.create_plan(resource(), std::move(logical_plan), std::move(oid_batch));
            }

            // CREATE DATABASE → planner rewrite into sequence_t(primitive_write
            // on pg_namespace). Mirrors dispatcher.cpp:1167-1172.
            if (original_type == node_type::create_database_t &&
                disk_address_ != actor_zeta::address_t::empty_address()) {
                components::catalog::oid_batch_t oid_batch;
                oid_batch.oids = co_await allocate_oids_inline(std::size_t{1});
                components::planner::planner_t ddl_planner;
                logical_plan = ddl_planner.create_plan(resource(), std::move(logical_plan), std::move(oid_batch));
            }

            // CREATE TYPE → planner rewrite. Mirrors dispatcher.cpp:1179-1188.
            //   STRUCT → 1 + N OIDs (pg_class.oid + N×pg_attribute.attoid).
            //   ENUM/other → 1 OID (pg_type.oid).
            if (original_type == node_type::create_type_t &&
                disk_address_ != actor_zeta::address_t::empty_address()) {
                auto* ct = static_cast<node_create_type_t*>(
                    services::catalog_resolve::effective_root_node(logical_plan.get()));
                const std::size_t need = (ct->type().type() == logical_type::STRUCT)
                                             ? std::size_t{1} + ct->type().child_types().size()
                                             : std::size_t{1};
                components::catalog::oid_batch_t oid_batch;
                oid_batch.oids = co_await allocate_oids_inline(need);
                components::planner::planner_t ddl_planner;
                logical_plan = ddl_planner.create_plan(resource(), std::move(logical_plan), std::move(oid_batch));
            }

            // CREATE SEQUENCE/VIEW/MACRO → planner rewrite to
            // sequence_t(primitive_write × N). Mirrors dispatcher.cpp:1197-1205.
            //   SEQUENCE → 1 OID; VIEW → 2 OIDs; MACRO → 2 OIDs.
            if ((original_type == node_type::create_sequence_t ||
                 original_type == node_type::create_view_t ||
                 original_type == node_type::create_macro_t) &&
                disk_address_ != actor_zeta::address_t::empty_address()) {
                const std::size_t need =
                    (original_type == node_type::create_sequence_t) ? std::size_t{1} : std::size_t{2};
                components::catalog::oid_batch_t oid_batch;
                oid_batch.oids = co_await allocate_oids_inline(need);
                components::planner::planner_t ddl_planner;
                logical_plan = ddl_planner.create_plan(resource(), std::move(logical_plan), std::move(oid_batch));
            }

            // CREATE MATERIALIZED VIEW → mv_oid + N×attoid + rule_oid = 2 + N.
            // Mirrors dispatcher.cpp:1214-1222.
            if (original_type == node_type::create_matview_t &&
                disk_address_ != actor_zeta::address_t::empty_address()) {
                auto* cm = static_cast<node_create_matview_t*>(
                    services::catalog_resolve::effective_root_node(logical_plan.get()));
                const std::size_t col_count = cm ? cm->inferred_columns().size() : std::size_t{0};
                const std::size_t need = 2 + col_count;
                components::catalog::oid_batch_t oid_batch;
                oid_batch.oids = co_await allocate_oids_inline(need);
                components::planner::planner_t ddl_planner;
                logical_plan = ddl_planner.create_plan(resource(), std::move(logical_plan), std::move(oid_batch));
            }

            // CREATE INDEX → planner rewrite to
            // sequence_t(primitive_write × N, create_index_t).
            // Mirrors dispatcher.cpp:1228-1233.
            if (original_type == node_type::create_index_t &&
                disk_address_ != actor_zeta::address_t::empty_address()) {
                components::catalog::oid_batch_t oid_batch;
                oid_batch.oids = co_await allocate_oids_inline(std::size_t{1});
                components::planner::planner_t ddl_planner;
                logical_plan = ddl_planner.create_plan(resource(), std::move(logical_plan), std::move(oid_batch));
            }

            // DROP INDEX → planner rewrite to
            // sequence_t(primitive_delete × N, drop_index_t). No OIDs.
            // Mirrors dispatcher.cpp:1237-1241.
            if (original_type == node_type::drop_index_t) {
                components::catalog::oid_batch_t oid_batch;
                components::planner::planner_t ddl_planner;
                logical_plan = ddl_planner.create_plan(resource(), std::move(logical_plan), std::move(oid_batch));
            }

            // ALTER TABLE → planner rewrite to
            // sequence_t(alter_column_{add,rename,drop}_t × N). No OID batch
            // (alter_column_add_t allocates its own attoid at execution).
            // Mirrors dispatcher.cpp:1246-1274. The re-enrich after the
            // planner stamps fresh attoids on rename / computed_field_unregister
            // primitives that did not exist before the planner ran. We use
            // the executor's resolve_txn so the pg_computed_column scan in
            // enrich's computed_field_unregister case observes the
            // INSERT-time register rows committed under that txn.
            if (original_type == node_type::alter_table_t &&
                disk_address_ != actor_zeta::address_t::empty_address()) {
                components::catalog::oid_batch_t oid_batch; // intentionally empty
                components::planner::planner_t ddl_planner;
                logical_plan = ddl_planner.create_plan(resource(), std::move(logical_plan), std::move(oid_batch));
                components::execution_context_t enriched_ctx{session,
                                                              resolve_txn,
                                                              context_storage.session_timezone};
                auto ef2 = services::dispatcher::enrich_plan(resource(),
                                                              logical_plan,
                                                              disk_address_,
                                                              enriched_ctx,
                                                              index_address_,
                                                              &context_storage);
                co_await std::move(ef2);
            }

            // DROP DATABASE / TABLE / TYPE / SEQUENCE / VIEW / MACRO →
            // planner rewrite to node_dynamic_cascade_delete_t. No OIDs.
            // Mirrors dispatcher.cpp:1281-1288.
            if ((original_type == node_type::drop_database_t ||
                 original_type == node_type::drop_collection_t ||
                 original_type == node_type::drop_type_t ||
                 original_type == node_type::drop_sequence_t ||
                 original_type == node_type::drop_view_t ||
                 original_type == node_type::drop_macro_t) &&
                disk_address_ != actor_zeta::address_t::empty_address()) {
                components::catalog::oid_batch_t oid_batch; // intentionally empty
                components::planner::planner_t ddl_planner;
                logical_plan = ddl_planner.create_plan(resource(), std::move(logical_plan), std::move(oid_batch));
            }

            // CREATE CONSTRAINT → planner rewrite to sequence_t(primitive_write
            // on pg_constraint+pg_depend). Mirrors dispatcher.cpp:1292-1310.
            // CHECK expressions are pre-validated for non-empty here so we
            // do not waste an OID on a doomed constraint.
            if (original_type == node_type::create_constraint_t &&
                disk_address_ != actor_zeta::address_t::empty_address()) {
                auto* cstr = static_cast<node_create_constraint_t*>(
                    services::catalog_resolve::effective_root_node(logical_plan.get()));
                if (cstr->kind() == constraint_kind::check && cstr->check_expr().empty()) {
                    co_return execute_result_t{make_cursor(
                        resource(),
                        core::error_t{core::error_code_t::other_error,
                                      std::pmr::string{"CHECK constraint expression is empty or contains "
                                                       "unsupported constructs (functions, subqueries, and CASE "
                                                       "expressions are not allowed; valid: comparisons, AND/OR/NOT, "
                                                       "IS NULL/IS NOT NULL, column references, and constants)",
                                                       resource()}})};
                }
                components::catalog::oid_batch_t oid_batch;
                oid_batch.oids = co_await allocate_oids_inline(std::size_t{1});
                components::planner::planner_t ddl_planner;
                logical_plan = ddl_planner.create_plan(resource(), std::move(logical_plan), std::move(oid_batch));
            }
        }
        // For now Pass 3 rewrites stay gated; the dispatcher upstream
        // continues to perform them before forwarding to execute_plan_full.
        // Flipping enable_pass2_rewrites above is the cut-over for both
        // Pass 2 and Pass 3 in a single step.
        //
        // Variant E.3 blocker (e) — RESOLVED for the cross-actor
        // boundary. The begin_transaction at the top of
        // execute_plan_full is now mailbox-routed via
        // manager_dispatcher_t::txn_begin_msg (Constraint #11). The
        // remaining `pctx.txn_manager = txn_manager_` and
        // `pipeline_context.txn_manager = txn_manager_` assignments in
        // allocate_oids_inline (Pass 3, gated) and execute_sub_plan_
        // are NOT actor↔actor shares: the operator pipeline runs
        // synchronously inside this executor's coroutine, so the
        // txn_manager touches happen on the same single-threaded actor
        // mailbox. The cross-actor mutation hazard (executor and
        // dispatcher both racing on txn_manager_) is closed by the
        // dispatcher-owned txn_*_msg handlers that serialize
        // begin/commit/abort/publish/lowest-active through the
        // dispatcher's mailbox. See the Option-A note above
        // allocate_oids_inline for the per-call-site rationale.
        trace(log_,
              "executor::execute_plan_full: Pass 1 stub — delegating to execute_plan, "
              "session: {}",
              session.data());
        // Delegate to the operator-pipeline entry. The dispatcher already
        // stamped OIDs via its own Pass 1; we forward the resolve_txn so the
        // DML/DDL operator path observes the same MVCC snapshot the resolves
        // saw. execute_plan treats `txn` as the externally-managed scope txn
        // for non-DML and begins its own for DML — both modes are correct
        // against the idempotent begin_transaction above.
        (void)txn;
        co_return co_await execute_plan(session,
                                         std::move(logical_plan),
                                         std::move(parameters),
                                         std::move(context_storage),
                                         resolve_txn);
    }

    executor_t::unique_future<std::unique_ptr<function_result_t>>
    executor_t::register_udf(components::session::session_id_t session, components::compute::function_ptr function) {
        trace(log_, "executor::register_udf, session: {}, {}", session.data(), function->name());
        std::string name = function->name();
        auto signatures = function->get_signatures();
        auto res = function_registry_.add_function(std::move(function));
        co_return std::make_unique<function_result_t>(std::move(res));
    }

    plan_t executor_t::traverse_plan_(components::operators::operator_ptr&& plan,
                                      components::logical_plan::storage_parameters&& parameters,
                                      services::context_storage_t&& context_storage) {
        std::stack<components::operators::operator_ptr> look_up;
        std::stack<components::operators::operator_ptr> sub_plans;
        look_up.push(plan);
        while (!look_up.empty()) {
            auto check_op = look_up.top();
            while (check_op->right() == nullptr) {
                check_op = check_op->left();
                if (check_op == nullptr) {
                    break;
                }
            }
            sub_plans.push(look_up.top());
            look_up.pop();
            if (check_op != nullptr) {
                look_up.push(check_op->right());
                look_up.push(check_op->left());
            }
        }

        trace(log_, "executor::subplans count {}", sub_plans.size());

        return plan_t{std::move(sub_plans), parameters, std::move(context_storage)};
    }

    executor_t::unique_future<sub_plan_result_t>
    executor_t::execute_sub_plan_(components::session::session_id_t session,
                                  plan_t plan_data,
                                  components::table::transaction_data txn) {
        cursor_t_ptr cursor;
        components::operators::operator_write_data_t::updated_types_map_t accumulated_updates(resource());
        sub_plan_result_t result_tracking;

        while (!plan_data.sub_plans.empty()) {
            auto plan = plan_data.sub_plans.top();
            trace(log_, "executor::execute_sub_plan, session: {}", session.data());

            if (!plan) {
                cursor = make_cursor(resource(),
                                     core::error_t(core::error_code_t::create_physical_plan_error,
                                                   std::pmr::string{"invalid query plan", resource()}));
                break;
            }

            components::pipeline::context_t pipeline_context{session,
                                                             address(),
                                                             parent_address_,
                                                             &function_registry_,
                                                             plan_data.parameters};
            pipeline_context.disk_address = disk_address_;
            pipeline_context.index_address = index_address_;
            pipeline_context.wal_address = wal_address_;
            pipeline_context.txn = txn;
            pipeline_context.session_tz = plan_data.context_storage_.session_timezone;
            // Block C §3.5 dec 22: pipeline operators that need to mutate the
            // global txn map (operator_begin_transaction_t marks the session's
            // txn explicit; operator_commit_transaction_t drains and publishes
            // accumulated ranges) consult ctx->txn_manager. Wire the executor's
            // manager through so SQL BEGIN/COMMIT routed via execute_plan_impl
            // works the same as the dispatcher RPC path.
            pipeline_context.txn_manager = txn_manager_;
            // VACUUM/MVCC GC threshold. operator_vacuum_t reads
            // this to gate manager_disk_t::vacuum_all + manager_index_t::
            // cleanup_all_versions. Computed up-front so the operator does not
            // need a back-channel to txn_manager_.
            pipeline_context.lowest_active_start_time = txn_manager_->lowest_active_start_time();

            // Prepare the operator tree (connects children in aggregation, etc.)
            plan->prepare();

            // Execute the plan tree (scan operators send I/O requests and enter waiting state)
            plan->on_execute(&pipeline_context);

            if (plan->has_error()) {
                cursor = make_cursor(resource(), plan->get_error());
                break;
            }

            // Await all waiting operators (multiple scans in a join, etc.)
            while (!plan->is_executed()) {
                auto waiting_op = plan->find_waiting_operator();
                if (!waiting_op) {
                    error(log_,
                          "Plan not executed and no waiting operator! session: {}, plan type: {}",
                          session.data(),
                          static_cast<int>(plan->type()));
                    assert(plan->has_error());
                    cursor = make_cursor(resource(), plan->get_error());
                    break;
                }
                trace(log_, "executor: found waiting operator, type={}", static_cast<int>(waiting_op->type()));
                // DML operators (insert/remove/update) self-contain
                // WAL + storage + index I/O inside their await_async_and_resume.
                // Every operator is dispatched uniformly via the same
                // coroutine entry point.
                co_await waiting_op->await_async_and_resume(&pipeline_context);
                // Propagate errors set during async resume (fk_check, fk_cascade,
                // DML on disk failure, etc.)
                if (waiting_op->has_error()) {
                    cursor = make_cursor(resource(), waiting_op->get_error());
                    break;
                }
                trace(log_, "executor: after await completed");
                // Re-execute: completed scan allows parent to proceed, may find next waiting scan
                plan->on_execute(&pipeline_context);
            }
            if (cursor && cursor->is_error())
                break;

            // Detect errors set asynchronously in operators (e.g. fk_cascade root with RESTRICT).
            if (plan->has_error()) {
                cursor = make_cursor(resource(), plan->get_error());
                break;
            }

            switch (plan->type()) {
                case components::operators::operator_type::insert: {
                    trace(log_, "executor::execute_plan : operators::operator_type::insert");
                    if (plan->output()) {
                        cursor = make_cursor(resource(), std::move(plan->output()->chunks()));
                    } else {
                        cursor = make_cursor(resource(), core::error_t::no_error());
                    }
                    break;
                }

                case components::operators::operator_type::remove: {
                    trace(log_, "executor::execute_plan : operators::operator_type::remove");
                    if (plan->modified()) {
                        for (auto& [key, val] : plan->modified()->updated_types_map()) {
                            accumulated_updates[key] += val;
                        }
                    }
                    if (plan->output()) {
                        cursor = make_cursor(resource(), std::move(plan->output()->chunks()));
                    } else {
                        cursor = make_cursor(resource(), core::error_t::no_error());
                    }
                    break;
                }

                case components::operators::operator_type::update: {
                    trace(log_, "executor::execute_plan : operators::operator_type::update");
                    if (plan->output()) {
                        cursor = make_cursor(resource(), std::move(plan->output()->chunks()));
                    } else {
                        cursor = make_cursor(resource(), core::error_t::no_error());
                    }
                    break;
                }

                default: {
                    trace(log_,
                          "executor::execute_plan : operator_type={}, session: {}",
                          static_cast<int>(plan->type()),
                          session.data());

                    if (plan->is_root()) {
                        if (plan->output()) {
                            auto& chunks = plan->output()->chunks();
                            // Apply post-sort limit across multi-chunk output.
                            if (plan_data.limit.limit() > 0) {
                                uint64_t remaining = static_cast<uint64_t>(plan_data.limit.limit());
                                for (auto& c : chunks) {
                                    if (remaining == 0) {
                                        c.set_cardinality(0);
                                        continue;
                                    }
                                    if (c.size() > remaining) {
                                        c.set_cardinality(remaining);
                                        remaining = 0;
                                    } else {
                                        remaining -= c.size();
                                    }
                                }
                            }
                            cursor = make_cursor(resource(), std::move(chunks));
                        } else {
                            cursor = make_cursor(resource(), core::error_t::no_error());
                        }
                    } else {
                        cursor = make_cursor(resource(), core::error_t::no_error());
                    }
                    break;
                }
            }

            if (cursor->is_error()) {
                break;
            }

            if (pipeline_context.has_pending_disk_futures()) {
                auto disk_futures = pipeline_context.take_pending_disk_futures();
                for (auto& fut : disk_futures) {
                    co_await std::move(fut);
                }
            }

            // Lift pg_catalog swap info from this fragment's pipeline
            // context into the per-call sub_plan_result_t accumulated across
            // sub-plan iterations. execute_plan then forwards them into the
            // returned execute_result_t for the dispatcher to aggregate onto
            // transaction_t.
            for (auto& a : pipeline_context.pg_catalog_appends) {
                result_tracking.pg_catalog_appends.push_back(std::move(a));
            }
            for (auto& d : pipeline_context.pg_catalog_delete_tables) {
                result_tracking.pg_catalog_delete_tables.insert(std::move(d));
            }
            // Block C §3.5 dec 32 V2 OPTION X: lift pg_attribute commit_id
            // backfill markers emitted by ALTER COLUMN ADD/DROP/RENAME so they
            // propagate alongside pg_catalog_appends down to transaction_t /
            // operator_commit_transaction.
            for (auto& bf : pipeline_context.pg_attribute_commit_id_backfills) {
                result_tracking.pg_attribute_commit_id_backfills.push_back(bf);
            }
            pipeline_context.pg_catalog_appends.clear();
            pipeline_context.pg_catalog_delete_tables.clear();
            pipeline_context.pg_attribute_commit_id_backfills.clear();

            // Lift DML swap-info recorded by operator_insert /
            // operator_delete / operator_update inside await_async_and_resume.
            // B33 (Pass 9): accumulate per-table ranges across sub-plans —
            // previously overwriting silently dropped non-last FK cascade child
            // table publishes. execute_plan iterates the vectors below to drive
            // storage_publish_commit / storage_publish_delete for each range.
            if (pipeline_context.dml_append_row_count > 0) {
                result_tracking.dml_appends.push_back({pipeline_context.dml_table_oid,
                                                       pipeline_context.dml_append_row_start,
                                                       pipeline_context.dml_append_row_count});
            }
            if (pipeline_context.dml_delete_txn_id != 0) {
                result_tracking.dml_deletes.push_back({pipeline_context.dml_table_oid,
                                                       pipeline_context.dml_delete_txn_id});
            }
            pipeline_context.dml_append_row_start = 0;
            pipeline_context.dml_append_row_count = 0;
            pipeline_context.dml_delete_txn_id = 0;
            pipeline_context.dml_table_oid = components::catalog::INVALID_OID;

            plan_data.sub_plans.pop();
        }

        trace(log_, "executor::execute_sub_plan finished, success: {}", cursor->is_success());
        result_tracking.cursor = std::move(cursor);
        result_tracking.updates = std::move(accumulated_updates);
        co_return std::move(result_tracking);
    }

    // HEAD: intercept_dml_io_ removed — DML I/O now happens inside each operator's await_async_and_resume.

    // Variant E.3 (Pass 9 dec 1) collections_ partition — Step 2.
    // dispatcher.cpp fans `register_collection_local` to executor[hash(oid) % 4]
    // after `collections_.insert(...)`. The partition invariant is enforced by
    // the dispatcher's hash routing; we do not re-validate it here. The entry
    // is a by-value POD (oid + database + schema + name) — no shared
    // collection_t pointer (constraint #11). execute_plan consults
    // find_local_collection(oid) before falling through to the cross-partition
    // path that still routes through dispatcher.collections_.
    executor_t::unique_future<void>
    executor_t::register_collection_local(components::session::session_id_t /*session*/,
                                           components::catalog::oid_t table_oid,
                                           local_collection_entry_t entry) {
        trace(log_,
              "executor::register_collection_local: oid={} db={} ns={} rel={}",
              static_cast<unsigned>(table_oid),
              entry.database,
              entry.schema,
              entry.name);
        local_collections_.insert_or_assign(table_oid, std::move(entry));
        co_return;
    }

    executor_t::unique_future<void>
    executor_t::unregister_collection_local(components::session::session_id_t /*session*/,
                                             components::catalog::oid_t table_oid) {
        trace(log_, "executor::unregister_collection_local: oid={}", static_cast<unsigned>(table_oid));
        local_collections_.erase(table_oid);
        co_return;
    }
} // namespace services::collection::executor
