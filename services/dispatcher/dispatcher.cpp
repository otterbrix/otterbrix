#include "dispatcher.hpp"
#include "catalog_view.hpp"
#include "validate_logical_plan.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/oid_batch.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/catalog/table_id.hpp>

#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_alter_column_add.hpp>
#include <components/logical_plan/node_catalog_resolve_namespace.hpp>
#include <components/logical_plan/node_catalog_resolve_table.hpp>
#include <components/logical_plan/node_alter_column_drop.hpp>
#include <components/logical_plan/node_alter_column_rename.hpp>
#include <components/logical_plan/node_alter_table.hpp>
#include <components/logical_plan/node_computed_field_register.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_constraint.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_create_macro.hpp>
#include <components/logical_plan/node_create_sequence.hpp>
#include <components/logical_plan/node_create_type.hpp>
#include <components/logical_plan/node_create_view.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_drop_collection.hpp>
#include <components/logical_plan/node_drop_database.hpp>
#include <components/logical_plan/node_drop_index.hpp>
#include <components/logical_plan/node_drop_macro.hpp>
#include <components/logical_plan/node_drop_sequence.hpp>
#include <components/logical_plan/node_drop_type.hpp>
#include <components/logical_plan/node_drop_view.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/node_abort_transaction.hpp>
#include <components/logical_plan/node_commit_transaction.hpp>
#include <components/logical_plan/node_get_schema.hpp>
#include <components/logical_plan/node_primitive_write.hpp>
#include <components/logical_plan/node_register_udf.hpp>
#include <components/logical_plan/node_sequence.hpp>
#include <components/logical_plan/node_unregister_udf.hpp>
#include <components/physical_plan/operators/operator_abort_transaction.hpp>
#include <components/physical_plan/operators/operator_commit_transaction.hpp>
#include <components/physical_plan/operators/operator_get_schema.hpp>
#include <components/physical_plan/operators/operator_register_udf.hpp>
#include <components/physical_plan/operators/operator_unregister_udf.hpp>
#include <components/physical_plan_generator/impl/create_plan_register_udf.hpp>
#include <components/context/context.hpp>

#include <core/executor.hpp>
#include <core/tracy/tracy.hpp>

#include <components/physical_plan_generator/create_plan.hpp>
#include <components/planner/optimizer.hpp>
#include <components/planner/planner.hpp>

#include "enrich_logical_plan.hpp"

#include <services/collection/context_storage.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/wal_sync_mode.hpp>

#include <boost/polymorphic_pointer_cast.hpp>

#include <set>
#include <span>
#include <string_view>
#include <vector>

using namespace components::logical_plan;
using namespace components::cursor;
using namespace components::catalog;
using namespace components::types;

namespace services::dispatcher {

    namespace catalog = components::catalog;

    namespace {

        // Build the namespace search path for type-name resolution at DDL/DML validation:
        // [target_ns, public, pg_catalog], deduplicated. target_ns is the user-specified
        // schema (CREATE TABLE mydb.foo → mydb); INVALID_OID means no qualifier given.
        // Mirrors PostgreSQL's search_path semantics for type lookup; replaces the old
        // public-only probe so non-public UDTs resolve correctly.
        std::vector<components::catalog::oid_t>
        build_type_search_path(components::catalog::oid_t target_ns) {
            std::vector<components::catalog::oid_t> path;
            if (target_ns != components::catalog::INVALID_OID &&
                target_ns != components::catalog::well_known_oid::public_namespace &&
                target_ns != components::catalog::well_known_oid::pg_catalog_namespace) {
                path.push_back(target_ns);
            }
            path.push_back(components::catalog::well_known_oid::public_namespace);
            path.push_back(components::catalog::well_known_oid::pg_catalog_namespace);
            return path;
        }

        // Probe `name` in cache across the search path. Returns first hit or nullptr.
        const resolved_type_t*
        probe_type_in_path(const catalog_view_t& view,
                            std::string_view name,
                            std::span<const components::catalog::oid_t> search_path) {
            for (auto ns : search_path) {
                if (auto* rt = view.try_get_type(ns, name)) return rt;
            }
            return nullptr;
        }

        // Phase 13 T18: When the SQL transformer wraps a DML/DDL plan in
        //   sequence_t(catalog_resolve_namespace_t, catalog_resolve_table_t, <real_root>)
        // the dispatcher needs to route based on <real_root> (insert_t, select_t, ...)
        // not the wrapping sequence_t. This helper descends a sequence_t root and
        // returns the LAST non-catalog_resolve_* child (the "consumer" node, after
        // all resolution-only prefix children). For non-sequence_t roots it returns
        // the node itself unchanged. Returns nullptr only when the input is null or
        // a sequence_t with no non-resolve children.
        //
        // Note: the planner ALSO wraps in sequence_t later (for DDL primitive_write
        // pipelines and FK/CHECK INSERT pipelines). That wrap happens AFTER
        // original_type is captured at the top of execute_plan_impl, so this helper
        // is only relevant to transformer-side wraps (Phase 13 T13 toggle).
        const components::logical_plan::node_t*
        effective_root_node(const components::logical_plan::node_t* n) {
            if (!n) return nullptr;
            if (n->type() != components::logical_plan::node_type::sequence_t) {
                return n;
            }
            using nt = components::logical_plan::node_type;
            auto is_catalog_resolve = [](nt t) {
                return t == nt::catalog_resolve_namespace_t ||
                       t == nt::catalog_resolve_table_t ||
                       t == nt::catalog_resolve_type_t ||
                       t == nt::catalog_resolve_function_t;
            };
            const auto& kids = n->children();
            // Only descend if the first child is a catalog_resolve_* — this
            // distinguishes the transformer's resolve-wrapping sequence_t from
            // the planner's DDL/DML rewrite sequence_t (which has e.g.
            // create_collection_t + primitive_write children, no resolves).
            // Without this gate, a planner-produced sequence_t would mis-route
            // to its last primitive_write child instead of being treated as
            // an opaque sequence by the caller.
            if (kids.empty() || !kids.front() || !is_catalog_resolve(kids.front()->type())) {
                return n;
            }
            // Walk children back-to-front: the real consumer is the last
            // non-resolve child. (Resolve nodes are positioned at the front
            // of the sequence by the transformer.)
            for (auto it = kids.rbegin(); it != kids.rend(); ++it) {
                if (!*it) continue;
                if (!is_catalog_resolve((*it)->type())) {
                    return it->get();
                }
            }
            // All children are catalog_resolve_* (or empty): no consumer
            // available, return the wrapper itself so callers fall through
            // to the generic execute path.
            return n;
        }

        components::logical_plan::node_type
        effective_root_type(const components::logical_plan::node_t* n) {
            auto* r = effective_root_node(n);
            return r ? r->type() : components::logical_plan::node_type::unused;
        }

        // Phase 13 Step 2: mutable-pointer overload so the various
        // static_cast<node_X_t*>(logic_plan.get()) call sites can descend
        // through the transformer's catalog_resolve_* wrapper without
        // duplicating the walk logic.
        components::logical_plan::node_t*
        effective_root_node(components::logical_plan::node_t* n) {
            return const_cast<components::logical_plan::node_t*>(
                effective_root_node(static_cast<const components::logical_plan::node_t*>(n)));
        }
    } // namespace

    manager_dispatcher_t::manager_dispatcher_t(std::pmr::memory_resource* resource_ptr,
                                               actor_zeta::scheduler_raw scheduler,
                                               log_t& log,
                                               run_fn_t run_fn)
        : actor_zeta::actor::actor_mixin<manager_dispatcher_t>()
        , resource_(resource_ptr)
        , scheduler_(scheduler)
        , log_(log.clone())
        , run_fn_(std::move(run_fn))
        , collections_(resource_ptr)
        , executors_(resource_ptr)
        , executor_addresses_(resource_ptr) {
        ZoneScoped;
        trace(log_, "manager_dispatcher_t::manager_dispatcher_t");
    }

    manager_dispatcher_t::~manager_dispatcher_t() {
        ZoneScoped;
        trace(log_, "delete manager_dispatcher_t");
    }

    auto manager_dispatcher_t::make_type() const noexcept -> const char* { return "manager_dispatcher"; }

    std::pair<bool, actor_zeta::detail::enqueue_result>
    manager_dispatcher_t::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
        std::lock_guard<std::mutex> guard(mutex_);
        current_behavior_ = behavior(msg.get());

        while (current_behavior_.is_busy()) {
            if (current_behavior_.is_awaited_ready()) {
                auto cont = current_behavior_.take_awaited_continuation();
                if (cont) {
                    cont.resume();
                }
            } else {
                run_fn_();
            }
        }

        return {false, actor_zeta::detail::enqueue_result::success};
    }

    actor_zeta::behavior_t manager_dispatcher_t::behavior(actor_zeta::mailbox::message* msg) {
        switch (msg->command()) {
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::execute_plan>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::execute_plan, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::get_schema>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::get_schema, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::register_udf>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::register_udf, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::unregister_udf>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::unregister_udf, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::begin_transaction>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::begin_transaction, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::commit_transaction>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::commit_transaction, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::abort_transaction>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::abort_transaction, msg);
                break;
            }
            default:
                break;
        }
    }


    void manager_dispatcher_t::sync(sync_pack pack) {
        constexpr static int wal_idx = 0;
        constexpr static int disk_idx = 1;
        constexpr static int index_idx = 2;
        wal_address_ = std::get<wal_idx>(pack);
        disk_address_ = std::get<disk_idx>(pack);
        index_address_ = std::get<index_idx>(pack);

        executors_.reserve(executor_pool_size_);
        executor_addresses_.reserve(executor_pool_size_);
        for (std::size_t i = 0; i < executor_pool_size_; ++i) {
            auto exec = actor_zeta::spawn<collection::executor::executor_t>(resource(),
                                                                            address(),
                                                                            wal_address_,
                                                                            disk_address_,
                                                                            index_address_,
                                                                            &txn_manager_,
                                                                            log_.clone());
            executor_addresses_.push_back(exec->address());
            executors_.push_back(std::move(exec));
        }
        trace(log_, "manager_dispatcher_t: spawned {} executors with WAL/Disk/Index addresses", executor_pool_size_);
    }

    void manager_dispatcher_t::init_from_state(std::pmr::set<collection_full_name_t> collections) {
        trace(log_, "manager_dispatcher_t::init_from_state: populating storage");
        for (const auto& full_name : collections) {
            collections_.insert(full_name);
        }
        trace(log_, "manager_dispatcher_t::init_from_state: complete - {} collections", collections_.size());
    }

    manager_dispatcher_t::unique_future<components::cursor::cursor_t_ptr>
    manager_dispatcher_t::execute_plan(components::session::session_id_t session,
                                       node_ptr plan,
                                       parameter_node_ptr params) {
        trace(log_, "manager_dispatcher_t::execute_plan session: {}, {}", session.data(), plan->to_string());

        auto params_for_wal = make_parameter_node(resource());
        params_for_wal->set_parameters(params->parameters());

        // catalog_view_t — thin wrapper over disk_address_ for ad-hoc lookups during
        // validate/enrich. No caching, no version pinning — каждое чтение pg_catalog идёт через
        // обычный SELECT с MVCC visibility.
        catalog_view_t view{disk_address_, resource()};
        components::execution_context_t ctx{session, components::table::transaction_data{0, 0}, {}};

        // Rebuild collections_ from on-disk pg_catalog so post-restart queries find user
        // collections that were re-loaded via WAL replay. Direct list_* calls bypass the
        // per-name V4 cache (cache cannot serve enumeration semantics). Clear first so that
        // dropped tables (whose pg_class rows are committed-deleted) are not left as stale
        // entries — the disk scan is authoritative when disk is enabled.
        if (disk_address_ != actor_zeta::address_t::empty_address()) {
            collections_.clear();
            auto [_ln, lnf] = actor_zeta::send(disk_address_,
                                                &disk::manager_disk_t::list_namespaces, ctx);
            auto namespaces = co_await std::move(lnf);
            for (auto& ns_name : namespaces) {
                if (ns_name.empty()) continue;
                auto* ns_e = co_await view.get_namespace(ctx, ns_name);
                if (!ns_e) continue;
                auto [_lt, ltf] = actor_zeta::send(disk_address_,
                                                    &disk::manager_disk_t::list_tables_in_namespace,
                                                    ctx, ns_e->oid);
                auto tables = co_await std::move(ltf);
                for (auto& [_oid, tname] : tables) {
                    collections_.insert(collection_full_name_t{ns_name, tname});
                }
            }
        }

        // Save original node type — used after planner rewrite to dispatch DDL/DML paths.
        // Phase 13 T18: when transformer wraps DML/DDL in
        //   sequence_t(catalog_resolve_namespace_t, catalog_resolve_table_t, <consumer>)
        // we route on <consumer>'s type, not the wrapping sequence_t. For non-wrapped
        // plans effective_root_type() is identity (n->type()), so this is a no-op for
        // the T13-toggle-OFF case and existing DDL flows (planner wraps are applied
        // AFTER this line — see notes inside the helper).
        const auto original_type = effective_root_type(plan.get());
        // Capture the drop target before the planner rewrites it into a
        // node_dynamic_cascade_delete_t (which carries only OIDs, not names).
        // Used after successful execution to clean up the in-memory collections_
        // routing map so a subsequent execute_plan does not see a stale entry.
        // Phase 13 T18: descend through transformer's sequence_t(catalog_resolve_*,
        // <drop_node>) wrapper to reach the real drop node before casting.
        std::string drop_target_database;
        collection_full_name_t drop_target_collection;
        const auto* root_for_drop = effective_root_node(plan.get());
        if (original_type == node_type::drop_database_t) {
            drop_target_database = static_cast<const node_drop_database_t*>(root_for_drop)->dbname();
        } else if (original_type == node_type::drop_collection_t) {
            auto* drop_node = static_cast<const node_drop_collection_t*>(root_for_drop);
            drop_target_collection = collection_full_name_t{drop_node->dbname(), drop_node->relname()};
        }
        auto logic_plan = std::move(plan);
        // Optimizer: constant folding, etc.
        logic_plan = components::planner::optimize(resource(), logic_plan, nullptr, params.get());

        // Phase 13 Step 3 (deferred): auto-wrap + Pass 1 prototype lived here.
        // Reverted because Pass 1's executor re-entry interacted badly with
        // the operator_insert pipeline (test_collection insert returned 6/50
        // rows). The operator_resolve_*_t back-pointer plumbing remains in
        // place (will activate once the executor re-entry is reshaped). For
        // now, enrich stamps table_oid via the existing in-memory catalog
        // cache (sync try_get_* only — no NEW async actor sends are added by
        // this session).
        // Build table_id from the plan's role-named accessors. Each derived
        // node owns a (db, rel)-shaped pair; nodes that don't (create_type_t,
        // drop_type_t, wrappers) yield empty identifiers — same outcome as
        // the previous cfn_of() default branch.
        auto build_id_cfn = [](const node_t* n) -> collection_full_name_t {
            if (!n) return {};
            switch (n->type()) {
                case node_type::aggregate_t: {
                    auto* d = static_cast<const node_aggregate_t*>(n);
                    return collection_full_name_t{d->dbname(), d->relname()};
                }
                case node_type::alter_column_add_t: {
                    auto* d = static_cast<const node_alter_column_add_t*>(n);
                    return collection_full_name_t{d->dbname(), d->relname()};
                }
                case node_type::alter_column_drop_t: {
                    auto* d = static_cast<const node_alter_column_drop_t*>(n);
                    return collection_full_name_t{d->dbname(), d->relname()};
                }
                case node_type::alter_column_rename_t: {
                    auto* d = static_cast<const node_alter_column_rename_t*>(n);
                    return collection_full_name_t{d->dbname(), d->relname()};
                }
                case node_type::alter_table_t: {
                    auto* d = static_cast<const node_alter_table_t*>(n);
                    return collection_full_name_t{d->dbname(), d->relname()};
                }
                case node_type::create_collection_t: {
                    auto* d = static_cast<const node_create_collection_t*>(n);
                    return collection_full_name_t{d->dbname(), d->relname()};
                }
                case node_type::create_constraint_t: {
                    auto* d = static_cast<const node_create_constraint_t*>(n);
                    return collection_full_name_t{d->dbname(), d->relname()};
                }
                case node_type::create_database_t: {
                    auto* d = static_cast<const node_create_database_t*>(n);
                    return collection_full_name_t{d->dbname(), std::string{}};
                }
                case node_type::create_index_t: {
                    auto* d = static_cast<const node_create_index_t*>(n);
                    return collection_full_name_t{d->dbname(), d->relname()};
                }
                case node_type::create_macro_t: {
                    auto* d = static_cast<const node_create_macro_t*>(n);
                    return collection_full_name_t{d->dbname(), d->macroname()};
                }
                case node_type::create_sequence_t: {
                    auto* d = static_cast<const node_create_sequence_t*>(n);
                    return collection_full_name_t{d->dbname(), d->seqname()};
                }
                case node_type::create_view_t: {
                    auto* d = static_cast<const node_create_view_t*>(n);
                    return collection_full_name_t{d->dbname(), d->viewname()};
                }
                case node_type::delete_t: {
                    auto* d = static_cast<const node_delete_t*>(n);
                    return collection_full_name_t{d->dbname(), d->relname()};
                }
                case node_type::drop_collection_t: {
                    auto* d = static_cast<const node_drop_collection_t*>(n);
                    return collection_full_name_t{d->dbname(), d->relname()};
                }
                case node_type::drop_database_t: {
                    auto* d = static_cast<const node_drop_database_t*>(n);
                    return collection_full_name_t{d->dbname(), std::string{}};
                }
                case node_type::drop_index_t: {
                    auto* d = static_cast<const node_drop_index_t*>(n);
                    return collection_full_name_t{d->dbname(), d->indexname()};
                }
                case node_type::drop_macro_t: {
                    auto* d = static_cast<const node_drop_macro_t*>(n);
                    return collection_full_name_t{d->dbname(), d->macroname()};
                }
                case node_type::drop_sequence_t: {
                    auto* d = static_cast<const node_drop_sequence_t*>(n);
                    return collection_full_name_t{d->dbname(), d->seqname()};
                }
                case node_type::drop_view_t: {
                    auto* d = static_cast<const node_drop_view_t*>(n);
                    return collection_full_name_t{d->dbname(), d->viewname()};
                }
                case node_type::insert_t: {
                    auto* d = static_cast<const node_insert_t*>(n);
                    return collection_full_name_t{d->dbname(), d->relname()};
                }
                case node_type::match_t: {
                    auto* d = static_cast<const node_match_t*>(n);
                    return collection_full_name_t{d->dbname(), d->relname()};
                }
                case node_type::update_t: {
                    auto* d = static_cast<const node_update_t*>(n);
                    return collection_full_name_t{d->dbname(), d->relname()};
                }
                default:
                    return {};
            }
        };
        // Phase 13 Step 2: build identification name from the effective
        // consumer node, not the (potentially transformer-wrapping) sequence_t.
        table_id id(resource(), build_id_cfn(effective_root_node(logic_plan.get())));
        cursor_t_ptr error;
        // For DDL existence checks we need the namespace cached. Pre-fetch the plan's
        // namespace if any so view.try_get_namespace inside check_*_exists hits the cache.
        if (!id.get_namespace().empty()) {
            co_await view.get_namespace(ctx, std::string(id.get_namespace().front()));
        }
        // Phase 13 Step 2: route validation by the effective consumer type,
        // not the transformer's wrapping sequence_t (which would always fall
        // into the default validate_types branch and skip DDL existence checks).
        switch (original_type) {
            case node_type::create_database_t:
                if (!check_namespace_exists(resource(), view, id)) {
                    error = make_cursor(resource(), error_code_t::database_already_exists, "database already exists");
                }
                break;
            case node_type::drop_database_t:
                error = check_namespace_exists(resource(), view, id);
                break;
            case node_type::create_collection_t: {
                // Resolve the table's target namespace once — used both to pre-load the
                // table cache (for check_collection_exists) AND as the primary search
                // namespace for column-type resolution. This is the user's "current
                // schema" for unqualified type refs in CREATE TABLE; types living there
                // resolve before public/pg_catalog fallbacks.
                components::catalog::oid_t table_ns_oid = components::catalog::INVALID_OID;
                if (!id.get_namespace().empty()) {
                    auto* ns_e = view.try_get_namespace(std::string_view(id.get_namespace().front()));
                    if (ns_e) {
                        table_ns_oid = ns_e->oid;
                        co_await view.get_table(ctx, ns_e->oid, std::string(id.table_name()));
                    }
                }
                if (!check_collection_exists(resource(), view, id)) {
                    error =
                        make_cursor(resource(), error_code_t::collection_already_exists, "collection already exists");
                } else {
                    auto type_search_path = build_type_search_path(table_ns_oid);
                    auto& n = reinterpret_cast<node_create_collection_ptr&>(logic_plan);
                    for (auto& col_def : n->column_definitions()) {
                        if (col_def.type().type() == logical_type::UNKNOWN) {
                            // Skip UDT lookup for pg_catalog built-ins that the type name
                            // resolver didn't recognise — their type_name() is empty (not
                            // a user-defined type, just an unmapped catalog alias). Only
                            // run the catalog check when the name is non-empty (real UDT).
                            if (col_def.type().type_name().empty()) {
                                break;
                            }
                            // Pre-load each UDT name (recursively, nested STRUCT children
                            // included) in every search-path namespace so check_type_exists
                            // and probe_type_in_path can serve from cache.
                            std::set<std::string> names;
                            walk_user_type_refs(col_def.type(),
                                                [&](std::string_view nm) { names.emplace(nm); });
                            for (const auto& nm : names) {
                                for (auto ns_oid : type_search_path) {
                                    co_await view.get_type(ctx, ns_oid, nm);
                                }
                            }
                            error = check_type_exists(resource(), view, col_def.type().type_name(),
                                                       std::span<const components::catalog::oid_t>(type_search_path));
                            if (!error) {
                                const auto* rt = probe_type_in_path(view,
                                                                     std::string_view(col_def.type().type_name()),
                                                                     std::span<const components::catalog::oid_t>(type_search_path));
                                if (rt) {
                                    std::string alias = col_def.type().has_alias()
                                                          ? col_def.type().alias()
                                                          : std::string{};
                                    col_def.type() = rt->type;
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
                auto* drop_node = static_cast<node_drop_collection_t*>(effective_root_node(logic_plan.get()));
                if (!collections_.count(collection_full_name_t{drop_node->dbname(), drop_node->relname()})) {
                    error = make_cursor(resource(), error_code_t::collection_not_exists,
                                        "collection does not exist");
                    break;
                }
                if (!id.get_namespace().empty()) {
                    auto* ns_e = view.try_get_namespace(std::string_view(id.get_namespace().front()));
                    if (ns_e) {
                        co_await view.get_table(ctx, ns_e->oid, std::string(id.table_name()));
                    }
                }
                error = check_collection_exists(resource(), view, id);
                break;
            }
            case node_type::create_type_t: {
                auto& n = reinterpret_cast<node_create_type_ptr&>(logic_plan);
                // Phase 9.W: node_create_type_t no longer carries a user-typed db
                // name (no dbname accessor on the derived class). Falls back to
                // public_namespace; rewrite_create_type promotes namespace_oid_
                // when set by the user via SET search_path or qualified ref.
                components::catalog::oid_t target_ns = components::catalog::well_known_oid::public_namespace;
                auto type_search_path = build_type_search_path(target_ns);
                // Pre-load the new type's name across the search path so check_type_exists
                // detects collisions in any of {target, public, pg_catalog}.
                for (auto ns_oid : type_search_path) {
                    co_await view.get_type(ctx, ns_oid, std::string(n->type().type_name()));
                }
                if (!check_type_exists(resource(), view, n->type().type_name(),
                                         std::span<const components::catalog::oid_t>(type_search_path))) {
                    error = make_cursor(resource(),
                                        error_code_t::schema_error,
                                        "type: \'" + n->type().alias() + "\' already exists");
                    break;
                }
                if (n->type().type() == logical_type::STRUCT) {
                    // Pre-load every UDT referenced by nested fields (recursive walk —
                    // covers STRUCT-in-STRUCT). Each name probed in the full search path.
                    std::set<std::string> nested_names;
                    for (const auto& field : n->type().child_types()) {
                        walk_user_type_refs(field,
                                            [&](std::string_view nm) { nested_names.emplace(nm); });
                    }
                    for (const auto& nm : nested_names) {
                        for (auto ns_oid : type_search_path) {
                            co_await view.get_type(ctx, ns_oid, nm);
                        }
                    }
                    // Resolve UNKNOWN field references in place — rewrite_create_type
                    // expects child_types() to already carry concrete definitions where
                    // available (nested STRUCT children are reduced to UNKNOWN-by-name
                    // by the planner; everything else inlines the resolved type).
                    for (auto& field : n->type().child_types()) {
                        if (field.type() == logical_type::UNKNOWN) {
                            error = check_type_exists(resource(), view, field.type_name(),
                                                       std::span<const components::catalog::oid_t>(type_search_path));
                            if (error) {
                                break;
                            }
                            const auto* rt = probe_type_in_path(view,
                                                                 std::string_view(field.type_name()),
                                                                 std::span<const components::catalog::oid_t>(type_search_path));
                            if (rt) {
                                std::string alias = field.has_alias() ? field.alias() : std::string{};
                                field = rt->type;
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
                // Hand the resolved namespace_oid to the planner — rewrite_create_type
                // uses it to build pg_class+pg_attribute (STRUCT) or pg_type (ENUM) rows.
                // The allocate_oids_batch + ddl_planner.create_plan call follows this
                // switch, mirroring the create_collection_t / create_database_t pattern.
                n->set_namespace_oid(target_ns);
                break;
            }
            case node_type::drop_type_t: {
                // Existence check + pre-load the type cache so enrich_logical_plan can
                // resolve the type_oid synchronously (Phase 2 #49). The actual catalog
                // row deletes + pg_depend cascade are emitted by the planner as a
                // node_dynamic_cascade_delete_t and run by the executor.
                // Phase 9.W: node_drop_type_t no longer carries a user-typed db
                // name (no dbname accessor on the derived class). Falls back to
                // public_namespace + the standard search path below.
                auto* n = static_cast<node_drop_type_t*>(effective_root_node(logic_plan.get()));
                components::catalog::oid_t target_ns = components::catalog::well_known_oid::public_namespace;
                auto type_search_path = build_type_search_path(target_ns);
                for (auto ns_oid : type_search_path) {
                    co_await view.get_type(ctx, ns_oid, n->name());
                }
                error = check_type_exists(resource(), view, n->name(),
                                            std::span<const components::catalog::oid_t>(type_search_path));
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
            case node_type::alter_table_t:
                break;
            case node_type::create_constraint_t: {
                // Pre-load the target table so enrich_plan can resolve attoids from
                // catalog_view cache. Same pattern as drop_collection_t above.
                if (!id.get_namespace().empty()) {
                    auto* ns_e = view.try_get_namespace(std::string_view(id.get_namespace().front()));
                    if (!ns_e) ns_e = co_await view.get_namespace(ctx, std::string(id.get_namespace().front()));
                    if (ns_e) {
                        co_await view.get_table(ctx, ns_e->oid, std::string(id.table_name()));
                        // Also pre-load the FK reference table (lives in same namespace).
                        auto* cstr = static_cast<node_create_constraint_t*>(effective_root_node(logic_plan.get()));
                        if (cstr->kind() == constraint_kind::foreign_key &&
                            !cstr->ref_relname().empty()) {
                            co_await view.get_table(ctx, ns_e->oid,
                                                     std::string(cstr->ref_relname()));
                        }
                    }
                }
                error = check_collection_exists(resource(), view, id);
                // Phase 7: reject FK / CHECK on dynamic-schema (relkind='g') tables.
                // FK / CHECK enforcement requires stable column attoids; relkind='g'
                // attoids may evolve (pg_computed_column type evolution). Full support
                // deferred — see docs/phase7-deferred-items.md §7.6. We enforce here so
                // the user gets a clear diagnostic instead of silent corruption.
                if (!error && !id.get_namespace().empty()) {
                    auto* ns_e = view.try_get_namespace(std::string_view(id.get_namespace().front()));
                    auto* cstr = static_cast<node_create_constraint_t*>(effective_root_node(logic_plan.get()));
                    if (ns_e && (cstr->kind() == constraint_kind::foreign_key ||
                                 cstr->kind() == constraint_kind::check)) {
                        const auto* tbl_local =
                            view.try_get_table(ns_e->oid, std::string_view(id.table_name()));
                        const bool local_is_g = tbl_local && tbl_local->relkind == 'g';
                        bool ref_is_g = false;
                        if (cstr->kind() == constraint_kind::foreign_key &&
                            !cstr->ref_relname().empty()) {
                            const auto* tbl_ref = view.try_get_table(
                                ns_e->oid,
                                std::string_view(cstr->ref_relname()));
                            ref_is_g = tbl_ref && tbl_ref->relkind == 'g';
                        }
                        if (cstr->kind() == constraint_kind::foreign_key &&
                            (local_is_g || ref_is_g)) {
                            error = make_cursor(
                                resource(),
                                error_code_t::schema_error,
                                "Foreign key constraints are not supported when the referencing or "
                                "referenced table is dynamic-schema (relkind='g'). FK enforcement "
                                "requires stable column attoids; dynamic-schema columns may evolve. "
                                "Convert involved tables to static schema first (see "
                                "docs/phase7-deferred-items.md section 7.6).");
                        } else if (cstr->kind() == constraint_kind::check && local_is_g) {
                            error = make_cursor(
                                resource(),
                                error_code_t::schema_error,
                                "CHECK constraints are not supported on dynamic-schema (relkind='g') "
                                "tables. CHECK enforcement requires stable column attoids; "
                                "dynamic-schema columns may evolve. Convert the table to static "
                                "schema first (see docs/phase7-deferred-items.md section 7.6).");
                        }
                    }
                }
                break;
            }
            default: {
                // Validate: pre-walks the AST collecting referenced tables — co_await
                // per-cache-miss only. view + ctx already constructed at the top of
                // execute_plan and shared with DDL branches above.
                auto [_vt, vtf] = std::pair<bool, actor_zeta::unique_future<cursor_t_ptr>>{
                    false, validate_types(resource(), view, ctx, logic_plan.get())};
                auto check_result = co_await std::move(vtf);
                if (check_result->is_error()) {
                    error = std::move(check_result);
                } else {
                    auto [_vs, vsf] = std::pair<bool, actor_zeta::unique_future<schema_result<named_schema>>>{
                        false, validate_schema(resource(), view, ctx, logic_plan.get(), params->parameters())};
                    auto schema_res = co_await std::move(vsf);
                    if (schema_res.is_error()) {
                        error = make_cursor(resource(), schema_res.error().type, schema_res.error().what);
                    }
                }
            }
        }

        if (error) {
            trace(log_, "manager_dispatcher_t::execute_plan: validation error: {}", error->get_error().what);
            co_return std::move(error);
        }

        // Enrich DML node fields with catalog metadata (NOT NULL, DEFAULT, CHECK exprs).
        // Must run after validate_schema so catalog_view cache is warm.
        {
            auto ef = enrich_plan(logic_plan, view, disk_address_, ctx, resource());
            co_await std::move(ef);
        }
        // Logical plan rewrite: insert constraint wrapper nodes driven by enriched fields.
        {
            components::planner::planner_t planner;
            logic_plan = planner.create_plan(resource(), std::move(logic_plan));
        }

        // Phase 7.4: deleted the adopt-based wrapper that promoted relkind
        // 'g'->'r' on first INSERT (broke the Mongo-style dynamic-schema
        // model where relkind='g' is the table's permanent state, with
        // pg_computed_column tracking columns dynamically). P7.2 will
        // reintroduce a relkind='g' branch here that wraps the INSERT into
        // sequence_t(insert, computed_field_register) so pg_computed_column
        // rows are appended inside the executor's DML txn. Until P7.2 lands,
        // relkind='g' INSERT falls through to the unmodified executor
        // pipeline.
        // Phase 8.F: enrich_plan has already stamped table_oid on the INSERT node
        // (and resolved relkind via the (ns_oid, name) primary index). We probe the
        // oid-keyed secondary index instead of re-running the (ns, name) resolution
        // path. Note: planner.create_plan above may have wrapped the INSERT in
        // check_constraint_t / fk_check_t, in which case table_oid_ on the root
        // wrapper is INVALID — peek at the first child the same way pool routing
        // does at line 1257. Falls back gracefully when enrich could not stamp an
        // oid (legacy / disk-disabled — INVALID_OID short-circuits the branch).
        if (original_type == node_type::insert_t &&
            disk_address_ != actor_zeta::address_t::empty_address()) {
            components::catalog::oid_t resolved_tbl_oid = components::catalog::INVALID_OID;
            bool is_computing = false;
            auto enriched_oid = logic_plan->table_oid();
            if (enriched_oid == components::catalog::INVALID_OID && !logic_plan->children().empty()) {
                enriched_oid = logic_plan->children().front()->table_oid();
            }
            if (enriched_oid != components::catalog::INVALID_OID) {
                if (auto* tbl = view.try_get_table_by_oid(enriched_oid)) {
                    if (tbl->relkind == relkind::computed) {
                        is_computing = true;
                        resolved_tbl_oid = tbl->oid;
                    }
                }
            }

            // P7.2: relkind='g' INSERT — wrap the user's INSERT plan in
            // sequence_t(insert, computed_field_register) so pg_computed_column
            // rows are appended inside the executor's DML txn (commit applies
            // the MVCC swap atomically with the data write). The table stays as
            // relkind='g' permanently — no promotion to 'r'. Column definitions
            // come from the embedded node_data_t chunk produced by the parser
            // (each vector_t::type() carries the field name as alias).
            if (is_computing) {
                std::vector<components::table::column_definition_t> registered_cols;
                for (const auto& child : logic_plan->children()) {
                    if (child->type() != components::logical_plan::node_type::data_t) {
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

                auto* insert_node = static_cast<node_insert_t*>(effective_root_node(logic_plan.get()));
                auto register_node = boost::intrusive_ptr(
                    new components::logical_plan::node_computed_field_register_t(
                        resource(),
                        insert_node->dbname(),
                        insert_node->relname(),
                        resolved_tbl_oid,
                        std::move(registered_cols)));

                auto seq = boost::intrusive_ptr(
                    new components::logical_plan::node_sequence_t(resource()));
                seq->append_child(logic_plan);
                seq->append_child(register_node);
                logic_plan = seq;
            }
        }

        // For create_collection_t: allocate OIDs then call DDL planner to produce
        // sequence_t(create_collection_t, primitive_write×N). The physical plan
        // generator maps this to operator_create_collection_t (storage + catalog writes).
        if (original_type == node_type::create_collection_t &&
            disk_address_ != actor_zeta::address_t::empty_address()) {
            auto* cc = static_cast<node_create_collection_t*>(effective_root_node(logic_plan.get()));
            const std::size_t need = 1 + cc->column_definitions().size();
            auto [_oa, oaf] = actor_zeta::send(disk_address_,
                                               &disk::manager_disk_t::allocate_oids_batch,
                                               need);
            catalog::oid_batch_t oid_batch;
            oid_batch.oids = co_await std::move(oaf);
            components::planner::planner_t ddl_planner;
            logic_plan = ddl_planner.create_plan(resource(), std::move(logic_plan), std::move(oid_batch));
        }

        // CREATE DATABASE → planner rewrite в sequence_t(primitive_write на pg_namespace).
        if (original_type == node_type::create_database_t &&
            disk_address_ != actor_zeta::address_t::empty_address()) {
            auto [_oa, oaf] = actor_zeta::send(disk_address_,
                                               &disk::manager_disk_t::allocate_oids_batch,
                                               std::size_t{1});
            catalog::oid_batch_t oid_batch;
            oid_batch.oids = co_await std::move(oaf);
            components::planner::planner_t ddl_planner;
            logic_plan = ddl_planner.create_plan(resource(), std::move(logic_plan), std::move(oid_batch));
        }

        // CREATE TYPE → planner rewrite to sequence_t(primitive_write × N).
        //   STRUCT     → (1 + N) OIDs: pg_class.oid + N×pg_attribute.attoid (composite type).
        //   ENUM/other → 1 OID: pg_type.oid.
        // Existence checks + UNKNOWN-field resolution already happened in the validation
        // switch above; namespace_oid is stored on the node for the planner to read.
        if (original_type == node_type::create_type_t &&
            disk_address_ != actor_zeta::address_t::empty_address()) {
            auto* ct = static_cast<node_create_type_t*>(effective_root_node(logic_plan.get()));
            const std::size_t need =
                (ct->type().type() == logical_type::STRUCT)
                    ? std::size_t{1} + ct->type().child_types().size()
                    : std::size_t{1};
            auto [_oa, oaf] = actor_zeta::send(disk_address_,
                                               &disk::manager_disk_t::allocate_oids_batch,
                                               need);
            catalog::oid_batch_t oid_batch;
            oid_batch.oids = co_await std::move(oaf);
            components::planner::planner_t ddl_planner;
            logic_plan = ddl_planner.create_plan(resource(), std::move(logic_plan), std::move(oid_batch));
        }

        // CREATE SEQUENCE/VIEW/MACRO → planner rewrite to sequence_t(primitive_write × N).
        //   CREATE SEQUENCE → 1 OID  (seq_oid)
        //   CREATE VIEW     → 2 OIDs (view_oid  + rule_oid)
        //   CREATE MACRO    → 2 OIDs (macro_oid + rule_oid)
        // The enrich phase has already stamped namespace_oid on the node so the
        // planner's rewrite is a pure sync transformation. After rewrite, logic_plan
        // is a sequence_t and flows through execute_plan_impl like CREATE DATABASE.
        if ((original_type == node_type::create_sequence_t ||
             original_type == node_type::create_view_t ||
             original_type == node_type::create_macro_t) &&
            disk_address_ != actor_zeta::address_t::empty_address()) {
            const std::size_t need =
                (original_type == node_type::create_sequence_t) ? std::size_t{1} : std::size_t{2};
            auto [_oa, oaf] = actor_zeta::send(disk_address_,
                                               &disk::manager_disk_t::allocate_oids_batch,
                                               need);
            catalog::oid_batch_t oid_batch;
            oid_batch.oids = co_await std::move(oaf);
            components::planner::planner_t ddl_planner;
            logic_plan = ddl_planner.create_plan(resource(), std::move(logic_plan), std::move(oid_batch));
        }

        // CREATE INDEX → planner rewrite to sequence_t(primitive_write × N, create_index_t).
        // The trailing create_index_t carries name/keys/type plus the resolved
        // namespace_oid/table_oid/index_oid so create_plan_sequence can lower it
        // to operator_create_index_metadata_t + operator_create_index_backfill_t.
        if (original_type == node_type::create_index_t &&
            disk_address_ != actor_zeta::address_t::empty_address()) {
            auto [_oa, oaf] = actor_zeta::send(disk_address_,
                                               &disk::manager_disk_t::allocate_oids_batch,
                                               std::size_t{1});
            catalog::oid_batch_t oid_batch;
            oid_batch.oids = co_await std::move(oaf);
            components::planner::planner_t ddl_planner;
            logic_plan = ddl_planner.create_plan(resource(), std::move(logic_plan), std::move(oid_batch));
        }

        // DROP INDEX → planner rewrite to sequence_t(primitive_delete × N, drop_index_t).
        // No OIDs needed; the index_oid is resolved by enrich_logical_plan.
        if (original_type == node_type::drop_index_t) {
            catalog::oid_batch_t oid_batch;
            components::planner::planner_t ddl_planner;
            logic_plan = ddl_planner.create_plan(resource(), std::move(logic_plan), std::move(oid_batch));
        }

        // ALTER TABLE → planner rewrite to sequence_t(alter_column_{add,rename,drop}_t × N).
        // No OID batch: alter_column_add_t allocates its own attoid at execution time
        // (one per ADD COLUMN clause). table_oid is resolved by enrich_logical_plan.
        if (original_type == node_type::alter_table_t &&
            disk_address_ != actor_zeta::address_t::empty_address()) {
            catalog::oid_batch_t oid_batch; // intentionally empty
            components::planner::planner_t ddl_planner;
            logic_plan = ddl_planner.create_plan(resource(), std::move(logic_plan), std::move(oid_batch));
            // Phase 9.A.0: re-run enrich over the planner-emitted sequence so the
            // freshly-constructed alter_column_rename_t / computed_field_unregister_t
            // primitives get their attoid_ resolved (they cannot be resolved before
            // the planner runs because they don't yet exist). The new explicit
            // cases in enrich_plan walk the sequence and stamp attoid via
            // catalog_view (rename: tbl->columns) or pg_computed_column scan
            // (unregister). Foundation only — operators don't read attoid_ yet
            // (that's 9.B), so this is a behavior-neutral pre-resolution.
            //
            // Phase 11.F-A: at this point the DDL transaction has not yet been
            // started (begin_transaction below at line ~924), so `ctx` carries
            // transaction_data{0, 0}. The pg_computed_column scan inside the
            // computed_field_unregister enrich case (enrich_logical_plan.cpp
            // ~411) reads with zero-txn visibility and misses the INSERT-time
            // register rows → live_attoid stays INVALID_OID → unregister
            // no-ops at execute time → no tombstone → DROP COLUMN on relkind='g'
            // never propagates (dynamic_schema_drop_column failure).
            // Begin the DDL txn here. begin_transaction is idempotent per
            // session (returns the existing active txn if one exists — see
            // components/table/transaction_manager.cpp:12), so the unchanged
            // call at line ~924 reuses this same txn.
            auto enrich_txn = txn_manager_.begin_transaction(session).data();
            components::execution_context_t enriched_ctx{session, enrich_txn, ctx.table_oid};
            auto ef2 = enrich_plan(logic_plan, view, disk_address_, enriched_ctx, resource());
            co_await std::move(ef2);
        }

        // DROP DATABASE / TABLE / TYPE / SEQUENCE / VIEW / MACRO → planner rewrite to
        // node_dynamic_cascade_delete_t. The cascade operator self-walks pg_depend at
        // runtime and performs catalog row deletes + storage drops. No OID batch is
        // needed (drops don't allocate). Resolved seed OIDs were stamped on the
        // logical drop_X node by enrich_logical_plan above. Phase 2 #49.
        if ((original_type == node_type::drop_database_t ||
             original_type == node_type::drop_collection_t ||
             original_type == node_type::drop_type_t ||
             original_type == node_type::drop_sequence_t ||
             original_type == node_type::drop_view_t ||
             original_type == node_type::drop_macro_t) &&
            disk_address_ != actor_zeta::address_t::empty_address()) {
            catalog::oid_batch_t oid_batch; // intentionally empty
            components::planner::planner_t ddl_planner;
            logic_plan = ddl_planner.create_plan(resource(), std::move(logic_plan), std::move(oid_batch));
        }

        // CREATE CONSTRAINT → planner rewrite в sequence_t(primitive_write на pg_constraint+pg_depend).
        // Resolved attoids/table_oid/ref_table_oid populated by enrich_plan above.
        if (original_type == node_type::create_constraint_t &&
            disk_address_ != actor_zeta::address_t::empty_address()) {
            // Validate CHECK expression non-empty before allocating OIDs.
            auto* cstr = static_cast<node_create_constraint_t*>(effective_root_node(logic_plan.get()));
            if (cstr->kind() == constraint_kind::check && cstr->check_expr().empty()) {
                co_return make_cursor(resource(),
                    error_code_t::other_error,
                    "CHECK constraint expression is empty or contains unsupported "
                    "constructs (functions, subqueries, and CASE expressions are not "
                    "allowed; valid: comparisons, AND/OR/NOT, IS NULL/IS NOT NULL, "
                    "column references, and constants)");
            }
            auto [_oa, oaf] = actor_zeta::send(disk_address_,
                                               &disk::manager_disk_t::allocate_oids_batch,
                                               std::size_t{1});
            catalog::oid_batch_t oid_batch;
            oid_batch.oids = co_await std::move(oaf);
            components::planner::planner_t ddl_planner;
            logic_plan = ddl_planner.create_plan(resource(), std::move(logic_plan), std::move(oid_batch));
        }

        // DDL needs a real (non-zero) txn so that mid-DDL crash → WAL replay rolls back
        // partially-written pg_catalog.* records.
        components::table::transaction_data txn_data{0, 0};
        {
            // create_collection_t/create_constraint_t are checked via original_type:
            // after the DDL planner rewrite they become sequence_t, but still need a
            // DDL txn so that append_pg_catalog_row records ranges on
            // txn_t->pg_catalog_appends and storage_commit_appends rebuilds
            // table_to_oid_ on success.
            const bool needs_ddl_txn = original_type == node_type::create_collection_t ||
                                        original_type == node_type::create_constraint_t ||
                                        original_type == node_type::create_sequence_t ||
                                        original_type == node_type::create_view_t ||
                                        original_type == node_type::create_macro_t ||
                                        original_type == node_type::create_type_t ||
                                        original_type == node_type::create_index_t ||
                                        original_type == node_type::drop_index_t ||
                                        original_type == node_type::drop_database_t ||
                                        original_type == node_type::drop_collection_t ||
                                        original_type == node_type::drop_type_t ||
                                        original_type == node_type::drop_sequence_t ||
                                        original_type == node_type::drop_view_t ||
                                        original_type == node_type::drop_macro_t ||
                                        original_type == node_type::create_database_t ||
                                        original_type == node_type::alter_table_t;
            if (needs_ddl_txn) {
                txn_data = txn_manager_.begin_transaction(session).data();
                trace(log_, "manager_dispatcher_t::execute_plan: DDL began txn {}",
                      txn_data.transaction_id);
            }
        }

        collection::executor::execute_result_t exec_result;
        // Phase 13 Step 2: route execution by the effective consumer type;
        // see comments above the validate switch.
        switch (original_type) {
            // Phase 3 #51: CHECKPOINT now runs through the executor pipeline as
            // operator_checkpoint_t (planner falls through, create_plan_checkpoint
            // builds the operator). Inline send to disk::checkpoint_all + wal trim
            // moved into operator_checkpoint_t::await_async_and_resume — the WAL
            // recovery boundary semantics (snapshot wal_max_id BEFORE checkpoint,
            // truncate_before only when min_prev > 0) are preserved verbatim.
            //
            // Phase 3 #52: VACUUM now runs through the executor pipeline as
            // operator_vacuum_t. The operator iterates pg_class (relkind 'r'/'g')
            // to discover user tables for the per-table index rebuild loop —
            // this replaces the legacy walk over `collections_`, which is being
            // removed in Phase 5. lowest_active_start_time is propagated via
            // pipeline::context_t (set by the executor from txn_manager_t).
            case node_type::alter_table_t: {
                // ALTER TABLE is normally rewritten by the planner into
                // sequence_t(alter_column_{add,rename,drop}_t × N). Reaching this
                // case means rewrite_alter_table bailed out because table_oid was
                // not resolved by enrich (table not found); return no-op success
                // and let the validate/enrich layer surface a hard error.
                //
                // Phase 13 Step 2: this switch now routes by `original_type`,
                // so we still see alter_table_t here even AFTER the planner has
                // rewritten the plan into sequence_t. Distinguish the genuine
                // bailout (logic_plan->type() still alter_table_t) from the
                // already-rewritten case (logic_plan is sequence_t) — the
                // latter must run through the executor like every other DDL.
                if (logic_plan->type() == node_type::alter_table_t) {
                    exec_result = {make_cursor(resource(), operation_status_t::success), {}};
                } else {
                    exec_result = co_await execute_plan_impl(session, logic_plan, params->take_parameters(), txn_data);
                }
                break;
            }
            default:
                exec_result = co_await execute_plan_impl(session, logic_plan, params->take_parameters(), txn_data);
                break;
        }

        // Phase 5b: hand pg_catalog swap-info up to the transaction so commit/abort
        // operators (or the inline DDL commit blocks below) can apply
        // storage_commit_appends / storage_revert_appends after txn_manager_.commit()/abort().
        // Skip txn=0 (auto-commit / bootstrap path).
        if (txn_data.transaction_id != 0) {
            if (auto* txn_t = txn_manager_.find_transaction(session)) {
                for (auto& a : exec_result.pg_catalog_appends) {
                    txn_t->pg_catalog_appends.push_back(std::move(a));
                }
                for (auto& d : exec_result.pg_catalog_delete_tables) {
                    txn_t->pg_catalog_delete_tables.insert(std::move(d));
                }
            }
        }

        auto& result = exec_result.cursor;
        trace(log_, "manager_dispatcher_t::execute_plan: result received, success: {}", result->is_success());

        if (result->is_success()) {
            // Use original_type for dispatch: planner may have wrapped DML nodes,
            // changing logic_plan->type() to a constraint wrapper type.
            const auto t = original_type;
            // ALTER TABLE flows through the executor pipeline as
            // sequence_t(alter_column_{add,rename,drop}_t × N).
            if (t == node_type::insert_t) {
                trace(log_,
                      "manager_dispatcher_t::execute_plan: DML {} completed by executor",
                      to_string(t));
                // Phase 7.4: relkind='g' INSERT is unwrapped here — P7.2 will
                // reintroduce a sequence_t(insert, computed_field_register)
                // wrapper above, so pg_computed_column appends ride the same
                // executor DML txn that commits user rows. The previous
                // adopt-based wrapper (W4-D) was removed because promoting
                // 'g'->'r' on first INSERT broke the Mongo-style dynamic
                // schema model.
                co_return result;
            }
            if (t == node_type::create_collection_t || t == node_type::create_database_t ||
                t == node_type::create_constraint_t ||
                t == node_type::create_sequence_t ||
                t == node_type::create_view_t ||
                t == node_type::create_macro_t ||
                t == node_type::create_type_t ||
                t == node_type::create_index_t ||
                t == node_type::drop_index_t ||
                t == node_type::drop_database_t ||
                t == node_type::drop_collection_t ||
                t == node_type::drop_type_t ||
                t == node_type::drop_sequence_t ||
                t == node_type::drop_view_t ||
                t == node_type::drop_macro_t ||
                t == node_type::alter_table_t) {
                // Flush + WAL commit + storage_commit_appends (flips MVCC tags).
                // Mirrors the legacy DDL commit sequence used by DROP TABLE, ALTER TABLE, etc.
                if (disk_address_ != actor_zeta::address_t::empty_address()) {
                    auto [_f, ff] = actor_zeta::send(disk_address_,
                                                      &disk::manager_disk_t::flush,
                                                      session, wal::id_t{0});
                    co_await std::move(ff);
                }
                if (wal_address_ != actor_zeta::address_t::empty_address()) {
                    // Phase 8.E: WAL routes by database_oid. Single-worker model uses
                    // main_database for all DDL in this phase.
                    constexpr auto db_oid = components::catalog::well_known_oid::main_database;
                    auto [_c, cf] = actor_zeta::send(wal_address_,
                                                      &wal::manager_wal_replicate_t::commit_txn,
                                                      session,
                                                      txn_data.transaction_id,
                                                      wal::wal_sync_mode::FULL,
                                                      db_oid);
                    co_await std::move(cf);
                }
                if (txn_data.transaction_id != 0 &&
                    disk_address_ != actor_zeta::address_t::empty_address()) {
                    // Phase 5b: snapshot accumulated pg_catalog swap-info from txn_t
                    // BEFORE commit() purges the active map, then dispatch new batched
                    // APIs after the swap point.
                    std::vector<components::pg_catalog_append_range_t> swap_appends;
                    std::set<components::catalog::oid_t>               swap_deletes;
                    if (auto* txn_t = txn_manager_.find_transaction(session)) {
                        swap_appends = std::move(txn_t->pg_catalog_appends);
                        swap_deletes = std::move(txn_t->pg_catalog_delete_tables);
                    }
                    const uint64_t commit_id = txn_manager_.commit(session);
                    if (commit_id > 0) {
                        components::execution_context_t swap_ctx{session, txn_data, {}};
                        if (!swap_appends.empty()) {
                            auto [_a, af] = actor_zeta::send(disk_address_,
                                                              &disk::manager_disk_t::storage_commit_appends,
                                                              swap_ctx, commit_id, std::move(swap_appends));
                            co_await std::move(af);
                        }
                        if (!swap_deletes.empty()) {
                            auto [_d, df] = actor_zeta::send(disk_address_,
                                                              &disk::manager_disk_t::storage_commit_deletes,
                                                              swap_ctx, commit_id, std::move(swap_deletes));
                            co_await std::move(df);
                        }
                    }
                }
                // Update routing map: for create_collection_t logic_plan is sequence_t whose
                // first child is the create_collection_t carrying the new collection name.
                // create_constraint_t and create_database_t have no collection to register.
                if (t == node_type::create_collection_t) {
                    // Phase 13 Step 2: peer through the transformer wrap
                    // (sequence_t(catalog_resolve_*, planner_sequence_t(create_collection_t, ...)))
                    // to reach the planner-produced sequence whose front child
                    // is the create_collection_t we need.
                    auto* root_after_plan = effective_root_node(logic_plan.get());
                    if (root_after_plan && !root_after_plan->children().empty()) {
                        auto* cc_child = static_cast<node_create_collection_t*>(root_after_plan->children().front().get());
                        collections_.insert(collection_full_name_t{cc_child->dbname(), cc_child->relname()});
                    }
                }
                // Drop side: the cascade operator removed pg_class/pg_namespace rows on
                // disk, but the dispatcher's in-memory collections_ map is rebuilt from
                // pg_catalog only on the *next* execute_plan. Clean up here so any
                // immediate follow-up call (in the same handler chain) does not see a
                // stale entry. Names captured before the planner rewrite. Phase 2 #49.
                if (t == node_type::drop_database_t && !drop_target_database.empty()) {
                    for (auto it = collections_.begin(); it != collections_.end();) {
                        if (it->database == drop_target_database) {
                            it = collections_.erase(it);
                        } else {
                            ++it;
                        }
                    }
                }
                if (t == node_type::drop_collection_t && !drop_target_collection.collection.empty()) {
                    collections_.erase(drop_target_collection);
                }
                co_return result;
            }
            if (t == node_type::update_t || t == node_type::delete_t) {
                co_return result;
            }
            trace(log_, "manager_dispatcher_t::execute_plan: non processed type - {}",
                  to_string(t));
        } else {
            // Executor handles abort + revert for DML errors
            trace(log_, "manager_dispatcher_t::execute_plan: error: \"{}\"", result->get_error().what);
        }

        co_return std::move(result);
    }

    manager_dispatcher_t::unique_future<bool>
    manager_dispatcher_t::register_udf(components::session::session_id_t session,
                                       components::compute::function_ptr function) {
        trace(log_, "dispatcher_t::register_udf session: {}, function name: {}", session.data(), function->name());

        // Phase 4 #55 — go through the operator pipeline. The logical leaf
        // node_register_udf_t carries the function payload; create_plan lowers
        // it to operator_register_udf_t which fans out to per-executor
        // registries, mirrors into function_registry_t::get_default(), and
        // persists pg_proc rows.
        //
        // We invoke the operator directly here (mirroring get_schema after
        // #54) rather than routing through execute_plan_impl: register_udf has
        // a custom return type (bool, not cursor) and needs the executor
        // address list which only the dispatcher has.

        // Wrap the unique_ptr function in a shared_ptr so the logical node can
        // copy without consuming. The operator deep-copies via get_copy() when
        // fanning out, leaving the shared_ptr's payload untouched for the
        // pg_proc encode step.
        std::shared_ptr<components::compute::function> shared_fn(function.release());
        auto plan = boost::intrusive_ptr(
            new components::logical_plan::node_register_udf_t(resource(), shared_fn));

        services::context_storage_t cstor{resource(), log_.clone()};
        // Build the executor fan-out callable. The dispatcher captures
        // executor_addresses_, scheduler_, and executors_ so the operator can
        // drive per-executor register_udf without needing direct scheduler
        // access. needs_sched is honoured here (matching the legacy inline
        // path) so the executor's mailbox is processed.
        auto fanout = [this](components::session::session_id_t s,
                              components::compute::function_ptr fcopy,
                              std::size_t i) -> actor_zeta::unique_future<components::compute::function_uid> {
            auto [needs_sched, future] =
                actor_zeta::otterbrix::send(executor_addresses_[i],
                                             &collection::executor::executor_t::register_udf,
                                             s,
                                             std::move(fcopy));
            if (needs_sched && executors_[i]) {
                scheduler_->enqueue(executors_[i].get());
            }
            return std::move(future);
        };
        auto op = services::planner::impl::create_plan_register_udf(
            cstor, plan, executor_addresses_.size(), std::move(fanout));
        if (!op) {
            co_return false;
        }
        op->set_as_root();

        components::logical_plan::storage_parameters params(resource());
        components::compute::function_registry_t fn_registry;
        components::pipeline::context_t pctx{session,
                                              actor_zeta::address_t::empty_address(),
                                              actor_zeta::address_t::empty_address(),
                                              &fn_registry,
                                              params};
        pctx.disk_address = disk_address_;
        pctx.txn = components::table::transaction_data{0, 0};

        op->prepare();
        op->on_execute(&pctx);
        while (!op->is_executed()) {
            auto waiting = op->find_waiting_operator();
            if (!waiting) break;
            co_await waiting->await_async_and_resume(&pctx);
            op->on_execute(&pctx);
        }
        if (pctx.has_pending_disk_futures()) {
            auto futures = pctx.take_pending_disk_futures();
            for (auto& f : futures) {
                co_await std::move(f);
            }
        }

        auto* ru = static_cast<components::operators::operator_register_udf_t*>(op.get());
        co_return ru->success();
    }

    manager_dispatcher_t::unique_future<bool>
    manager_dispatcher_t::unregister_udf(components::session::session_id_t session,
                                         std::string function_name,
                                         std::pmr::vector<complex_logical_type> inputs) {
        trace(log_, "dispatcher_t::unregister_udf: session {}, {}", session.data(), function_name);

        // Phase 4 #55 — operator-pipeline replacement. The logical leaf
        // node_unregister_udf_t carries the (name, inputs) signature; the
        // operator probes function_registry_t::get_default(), removes the
        // matching overload, and purges pg_proc + pg_depend rows.
        auto plan = boost::intrusive_ptr(
            new components::logical_plan::node_unregister_udf_t(resource(),
                                                                  std::move(function_name),
                                                                  std::move(inputs)));

        services::context_storage_t cstor{resource(), log_.clone()};
        components::compute::function_registry_t fn_registry;
        auto op = services::planner::create_plan(
            cstor, fn_registry, plan,
            components::logical_plan::limit_t::unlimit(),
            /*params=*/nullptr);
        if (!op) {
            co_return false;
        }
        op->set_as_root();

        components::logical_plan::storage_parameters params(resource());
        components::pipeline::context_t pctx{session,
                                              actor_zeta::address_t::empty_address(),
                                              actor_zeta::address_t::empty_address(),
                                              &fn_registry,
                                              params};
        pctx.disk_address = disk_address_;
        pctx.txn = components::table::transaction_data{0, 0};

        op->prepare();
        op->on_execute(&pctx);
        while (!op->is_executed()) {
            auto waiting = op->find_waiting_operator();
            if (!waiting) break;
            co_await waiting->await_async_and_resume(&pctx);
            op->on_execute(&pctx);
        }
        if (pctx.has_pending_disk_futures()) {
            auto futures = pctx.take_pending_disk_futures();
            for (auto& f : futures) {
                co_await std::move(f);
            }
        }

        auto* uu = static_cast<components::operators::operator_unregister_udf_t*>(op.get());
        co_return uu->success();
    }

    manager_dispatcher_t::unique_future<components::cursor::cursor_t_ptr>
    manager_dispatcher_t::get_schema(components::session::session_id_t session,
                                     std::pmr::vector<std::pair<database_name_t, collection_name_t>> ids) {
        trace(log_, "manager_dispatcher_t::get_schema session: {}, ids count: {}", session.data(), ids.size());

        // No disk → no pg_catalog → every id is unresolved. Mirrors the legacy
        // path where catalog_view_t reads short-circuit when disk_address_ is
        // empty (purely IN_MEMORY deployments without a backing disk actor).
        if (disk_address_ == actor_zeta::address_t::empty_address()) {
            std::pmr::vector<complex_logical_type> schemas(resource());
            schemas.reserve(ids.size());
            for (std::size_t i = 0; i < ids.size(); ++i) {
                schemas.push_back(complex_logical_type{logical_type::INVALID});
            }
            co_return make_cursor(resource(), std::move(schemas));
        }

        // Phase 4 #54 — go through the operator pipeline instead of inline
        // catalog_view_t reads. The logical leaf node_get_schema_t carries the
        // requested ids; create_plan lowers it to operator_get_schema_t which
        // self-resolves namespace / table / columns via async pg_catalog reads
        // and accumulates one complex_logical_type per id in input order.
        //
        // We invoke the operator directly here (mirroring executor's
        // execute_sub_plan_ loop) rather than routing through execute_plan_impl
        // because the get_schema cursor format is the typed-vector cursor
        // (make_cursor(resource, vector<complex_logical_type>)) — distinct from
        // the chunk-cursor format the executor produces for general plans.
        std::pmr::vector<std::pair<std::string, std::string>> id_pairs(resource());
        id_pairs.reserve(ids.size());
        for (const auto& [db, coll] : ids) {
            id_pairs.emplace_back(std::string(db), std::string(coll));
        }
        auto plan = boost::intrusive_ptr(new components::logical_plan::node_get_schema_t(
            resource(), std::move(id_pairs)));

        services::context_storage_t cstor{resource(), log_.clone()};
        components::compute::function_registry_t fn_registry;
        auto op = services::planner::create_plan(
            cstor, fn_registry, plan,
            components::logical_plan::limit_t::unlimit(),
            /*params=*/nullptr);
        if (!op) {
            // Should not happen — create_plan_get_schema is unconditional.
            co_return make_cursor(resource(),
                                   std::pmr::vector<complex_logical_type>(resource()));
        }
        op->set_as_root();

        // Build a minimal pipeline context. operator_get_schema_t only reads
        // disk_address (read_rows_by_key on pg_namespace/pg_class/pg_attribute);
        // a zero-txn matches the catalog reads the legacy path issued through
        // execution_context_t{session, {0,0}, {}}.
        components::logical_plan::storage_parameters params(resource());
        components::pipeline::context_t pctx{session,
                                              actor_zeta::address_t::empty_address(),
                                              actor_zeta::address_t::empty_address(),
                                              &fn_registry,
                                              params};
        pctx.disk_address = disk_address_;
        pctx.txn = components::table::transaction_data{0, 0};

        op->prepare();
        op->on_execute(&pctx);
        // Drive the async resume loop (the operator's only waiting state is
        // its own await_async_and_resume — there are no child operators).
        while (!op->is_executed()) {
            auto waiting = op->find_waiting_operator();
            if (!waiting) {
                break;
            }
            co_await waiting->await_async_and_resume(&pctx);
            op->on_execute(&pctx);
        }

        // Drain pending side-channel disk futures (none expected for read-only
        // get_schema, but mirrors the executor pattern for consistency).
        if (pctx.has_pending_disk_futures()) {
            auto futures = pctx.take_pending_disk_futures();
            for (auto& f : futures) {
                co_await std::move(f);
            }
        }

        auto* gs = static_cast<components::operators::operator_get_schema_t*>(op.get());
        co_return make_cursor(resource(), gs->take_schemas());
    }

    manager_dispatcher_t::unique_future<collection::executor::execute_result_t>
    manager_dispatcher_t::execute_plan_impl(components::session::session_id_t session,
                                            node_ptr logical_plan,
                                            storage_parameters parameters,
                                            components::table::transaction_data txn) {
        trace(log_,
              "manager_dispatcher_t:execute_plan_impl: node_type: {}, table_oid: {}, session: {}",
              components::logical_plan::to_string(logical_plan->type()),
              logical_plan->table_oid(),
              session.data());

        // Phase 8.B: oid-only routing. Plan generators ask context_storage_t
        // whether a given resolved table_oid is known (i.e. we have an actor
        // for it). Walk the plan, collect every table_oid stamped by enrich,
        // and forward the set to the executor. Wrapper / parser-window / DDL
        // nodes contribute INVALID_OID and are filtered.
        auto dependency_oids = logical_plan->table_oid_dependencies();
        context_storage_t collections_context_storage(resource(), log_.clone());
        for (auto oid : dependency_oids) {
            collections_context_storage.known_oids.insert(oid);
        }

        // Populate index metadata for optimizer-driven index selection.
        // Phase 8.D: keyed on table_oid (stamped by enrich_logical_plan).
        if (index_address_ != actor_zeta::address_t::empty_address()) {
            const auto tbl_oid = logical_plan->table_oid();
            if (tbl_oid != components::catalog::INVALID_OID) {
                auto [_ik, ikf] =
                    actor_zeta::send(index_address_, &index::manager_index_t::get_indexed_keys, session, tbl_oid);
                collections_context_storage.indexed_keys = co_await std::move(ikf);
            }
        }
        collections_context_storage.parameters = &parameters;

        assert(!executors_.empty());
        // Phase 8.B: oid-only pool routing. For wrapper nodes (sequence_t etc.)
        // table_oid is INVALID at the root; peek at the first child which is
        // the inner DML/DDL bearing the resolved oid. When no oid is resolvable
        // (db/ns DDL — no table involved) we route to executor[0] deterministically.
        std::size_t pool_idx = 0;
        components::catalog::oid_t routing_oid = logical_plan->table_oid();
        if (routing_oid == components::catalog::INVALID_OID && !logical_plan->children().empty()) {
            routing_oid = logical_plan->children().front()->table_oid();
        }
        if (routing_oid != components::catalog::INVALID_OID) {
            pool_idx = static_cast<std::size_t>(routing_oid) % executors_.size();
        }
        trace(log_, "manager_dispatcher_t:execute_plan_impl: calling executor[{}]", pool_idx);
        auto [needs_sched, future] = actor_zeta::otterbrix::send(executor_addresses_[pool_idx],
                                                                 &collection::executor::executor_t::execute_plan,
                                                                 session,
                                                                 logical_plan,
                                                                 parameters,
                                                                 std::move(collections_context_storage),
                                                                 txn);
        if (needs_sched && executors_[pool_idx]) {
            scheduler_->enqueue(executors_[pool_idx].get());
        }
        auto result = co_await std::move(future);

        trace(log_,
              "manager_dispatcher_t:execute_plan_impl: executor returned, success: {}",
              result.cursor->is_success());
        co_return result;
    }

    manager_dispatcher_t::unique_future<components::table::transaction_data>
    manager_dispatcher_t::begin_transaction(components::session::session_id_t session) {
        trace(log_, "manager_dispatcher_t::begin_transaction, session: {}", session.data());
        auto& txn = txn_manager_.begin_transaction(session);
        co_return txn.data();
    }

    manager_dispatcher_t::unique_future<uint64_t>
    manager_dispatcher_t::commit_transaction(components::session::session_id_t session) {
        trace(log_, "manager_dispatcher_t::commit_transaction, session: {}", session.data());

        // Phase 4 #56 — go through the operator pipeline instead of inline
        // txn_manager + disk sends. The leaf node carries no fields; the
        // operator reads txn_manager / disk_address / session off the
        // pipeline::context_t we build here. Mirrors the get_schema #54 and
        // register_udf #55 migrations.
        auto plan = boost::intrusive_ptr(
            new components::logical_plan::node_commit_transaction_t(resource()));

        services::context_storage_t cstor{resource(), log_.clone()};
        components::compute::function_registry_t fn_registry;
        auto op = services::planner::create_plan(
            cstor, fn_registry, plan,
            components::logical_plan::limit_t::unlimit(),
            /*params=*/nullptr);
        if (!op) {
            // Should not happen — create_plan_commit_transaction is unconditional.
            co_return 0;
        }
        op->set_as_root();

        components::logical_plan::storage_parameters params(resource());
        components::pipeline::context_t pctx{session,
                                              actor_zeta::address_t::empty_address(),
                                              actor_zeta::address_t::empty_address(),
                                              &fn_registry,
                                              params};
        pctx.disk_address = disk_address_;
        pctx.txn_manager = &txn_manager_;
        // txn snapshot is unused by the operator (it re-reads via find_transaction)
        // but carry a sane default so any nested debug logs see a zero value.
        pctx.txn = components::table::transaction_data{0, 0};

        op->prepare();
        op->on_execute(&pctx);
        while (!op->is_executed()) {
            auto waiting = op->find_waiting_operator();
            if (!waiting) {
                break;
            }
            co_await waiting->await_async_and_resume(&pctx);
            op->on_execute(&pctx);
        }
        if (pctx.has_pending_disk_futures()) {
            auto futures = pctx.take_pending_disk_futures();
            for (auto& f : futures) {
                co_await std::move(f);
            }
        }

        auto* commit_op =
            static_cast<components::operators::operator_commit_transaction_t*>(op.get());
        co_return commit_op->commit_id();
    }

    manager_dispatcher_t::unique_future<void>
    manager_dispatcher_t::abort_transaction(components::session::session_id_t session) {
        trace(log_, "manager_dispatcher_t::abort_transaction, session: {}", session.data());

        // Phase 4 #56 — operator-pipeline replacement (mirrors commit above).
        auto plan = boost::intrusive_ptr(
            new components::logical_plan::node_abort_transaction_t(resource()));

        services::context_storage_t cstor{resource(), log_.clone()};
        components::compute::function_registry_t fn_registry;
        auto op = services::planner::create_plan(
            cstor, fn_registry, plan,
            components::logical_plan::limit_t::unlimit(),
            /*params=*/nullptr);
        if (!op) {
            co_return;
        }
        op->set_as_root();

        components::logical_plan::storage_parameters params(resource());
        components::pipeline::context_t pctx{session,
                                              actor_zeta::address_t::empty_address(),
                                              actor_zeta::address_t::empty_address(),
                                              &fn_registry,
                                              params};
        pctx.disk_address = disk_address_;
        pctx.txn_manager = &txn_manager_;
        pctx.txn = components::table::transaction_data{0, 0};

        op->prepare();
        op->on_execute(&pctx);
        while (!op->is_executed()) {
            auto waiting = op->find_waiting_operator();
            if (!waiting) {
                break;
            }
            co_await waiting->await_async_and_resume(&pctx);
            op->on_execute(&pctx);
        }
        if (pctx.has_pending_disk_futures()) {
            auto futures = pctx.take_pending_disk_futures();
            for (auto& f : futures) {
                co_await std::move(f);
            }
        }
        co_return;
    }

    node_ptr manager_dispatcher_t::create_logic_plan(node_ptr plan) {
        // Retained for any callers outside execute_plan; the primary path now
        // calls planner_t::create_plan directly after enrich_plan.
        components::planner::planner_t planner;
        return planner.create_plan(resource(), std::move(plan));
    }


} // namespace services::dispatcher
