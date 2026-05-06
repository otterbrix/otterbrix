#include "dispatcher.hpp"
#include "catalog_view.hpp"
#include "validate_logical_plan.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/oid_batch.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/catalog/table_id.hpp>

#include <components/logical_plan/node_alter_table.hpp>
#include <components/logical_plan/node_checkpoint.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_constraint.hpp>
#include <components/logical_plan/node_create_macro.hpp>
#include <components/logical_plan/node_create_sequence.hpp>
#include <components/logical_plan/node_create_type.hpp>
#include <components/logical_plan/node_create_view.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_drop_database.hpp>
#include <components/logical_plan/node_drop_macro.hpp>
#include <components/logical_plan/node_drop_sequence.hpp>
#include <components/logical_plan/node_drop_view.hpp>
#include <components/logical_plan/node_primitive_write.hpp>
#include <components/logical_plan/node_sequence.hpp>

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
        , executor_addresses_(resource_ptr)
        , plan_cache_(resource_ptr) {
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
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::size>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::size, msg);
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
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::close_cursor>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::close_cursor, msg);
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

        // M5: pull invalidation events first so plan_cache_ is consistent with the catalog
        // snapshot we're about to populate. Overflow → clear cache; otherwise advance
        // last_seen_version_ for subsequent pin_version calls.
        co_await refresh_invalidations_(session);

        // V4: build catalog_view_t over plan_cache + disk_address. View serves DDL
        // existence checks and the validate path; collections_ rebuild uses disk's
        // list_namespaces/list_tables_in_namespace (populate-path retired).
        // Prefer the session's pinned version (snapshot isolation) over last_seen_version_.
        const auto view_version = plan_cache_.pinned_version_for(session).value_or(last_seen_version_);
        catalog_view_t view{plan_cache_, disk_address_, view_version, resource()};
        components::execution_context_t ctx{session, components::table::transaction_data{0, 0}, {}};

        // Rebuild collections_ from on-disk pg_catalog so post-restart queries find user
        // collections that were re-loaded via WAL replay. Direct list_* calls bypass the
        // per-name V4 cache (cache cannot serve enumeration semantics).
        if (disk_address_ != actor_zeta::address_t::empty_address()) {
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
        const auto original_type = plan->type();
        auto logic_plan = std::move(plan);
        // Optimizer: constant folding, etc.
        logic_plan = components::planner::optimize(resource(), logic_plan, nullptr, params.get());
        table_id id(resource(), logic_plan->collection_full_name());
        cursor_t_ptr error;
        // For DDL existence checks we need the namespace cached. Pre-fetch the plan's
        // namespace if any so view.try_get_namespace inside check_*_exists hits the cache.
        if (!id.get_namespace().empty()) {
            co_await view.get_namespace(ctx, std::string(id.get_namespace().front()));
        }
        switch (logic_plan->type()) {
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
                // Resolve the user-specified database (logic_plan->database_name()) to its
                // namespace OID early — used both for the existence-check search path AND
                // for ddl_create_type/ddl_create_table persistence below. INVALID_OID =
                // unqualified CREATE TYPE → public namespace.
                components::catalog::oid_t target_ns = components::catalog::well_known_oid::public_namespace;
                if (!logic_plan->database_name().empty() &&
                    disk_address_ != actor_zeta::address_t::empty_address()) {
                    components::execution_context_t ddl_ctx{session,
                                                              components::table::transaction_data{0, 0}, {}};
                    auto [_rn, rnf] = actor_zeta::send(disk_address_,
                                                        &disk::manager_disk_t::resolve_namespace,
                                                        ddl_ctx,
                                                        std::string(logic_plan->database_name()),
                                                        std::uint64_t{0});
                    auto rns = co_await std::move(rnf);
                    if (rns.found) target_ns = rns.oid;
                }
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
                } else {
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
                        for (auto& field : n->type().child_types()) {
                            if (field.type() == logical_type::UNKNOWN) {
                                error = check_type_exists(resource(), view, field.type_name(),
                                                           std::span<const components::catalog::oid_t>(type_search_path));
                                if (error) {
                                    break;
                                } else {
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
                        }
                        if (error) {
                            break;
                        }
                    }
                    // pg_class+pg_attribute (relkind='c' for composite STRUCT) or pg_type
                    // (typdefspec for non-struct) is the source of truth — disk's
                    // ddl_create_type/ddl_create_table handle persistence below.
                    // Persist user types — composite (STRUCT) goes through pg_class+pg_attribute
                    // (PostgreSQL-canonical, relkind='c' = composite type) to avoid msgpack
                    // roundtrip through typdefspec. ENUM and other types still use pg_type
                    // because they don't have field columns.
                    // target_ns was resolved upfront above (defaults to public for unqualified).
                    if (disk_address_ != actor_zeta::address_t::empty_address()) {
                        components::execution_context_t ddl_ctx{session,
                                                                  components::table::transaction_data{0, 0}, {}};
                        if (n->type().type() == logical_type::STRUCT) {
                            // Composite: persist as pg_class entry with relkind='d' + pg_attribute
                            // rows for each field. Mirrors PostgreSQL's storage of composite types.
                            // For fields that reference other user types, persist as UNKNOWN-by-
                            // name (using type_name as alias). populate resolves these references
                            // against pg_class entries with relkind='d' on read. This
                            // sidesteps msgpack's nested-struct roundtrip bug.
                            std::vector<components::table::column_definition_t> field_cols;
                            field_cols.reserve(n->type().child_types().size());
                            for (const auto& field : n->type().child_types()) {
                                std::string fname = field.has_alias() ? field.alias()
                                                                       : field.type_name();
                                if (field.type() == logical_type::STRUCT) {
                                    // Reduce nested STRUCT to UNKNOWN-by-name reference.
                                    auto unk = components::types::complex_logical_type::create_unknown(
                                        field.type_name(), fname);
                                    field_cols.emplace_back(fname, std::move(unk));
                                } else {
                                    field_cols.emplace_back(fname, field);
                                }
                            }
                            auto [_ct, ctf] = actor_zeta::send(disk_address_,
                                                                &disk::manager_disk_t::ddl_create_table,
                                                                ddl_ctx,
                                                                target_ns,
                                                                std::string(n->type().type_name()),
                                                                std::move(field_cols),
                                                                relkind::composite_type);
                            if (auto r = co_await std::move(ctf); r.failed())
                                co_return make_ddl_error_cursor(resource(), r);
                        } else {
                            // ENUM and other extension types — keep using pg_type with
                            // typdefspec. ENUM serialization is flat (no nested types) and
                            // round-trips correctly.
                            auto [_dt, dtf] = actor_zeta::send(disk_address_,
                                                                &disk::manager_disk_t::ddl_create_type,
                                                                ddl_ctx,
                                                                target_ns,
                                                                std::string(n->type().type_name()),
                                                                components::catalog::encode_type_spec(n->type()));
                            if (auto r = co_await std::move(dtf); r.failed())
                                co_return make_ddl_error_cursor(resource(), r);
                        }
                    }
                    co_return make_cursor(resource(), operation_status_t::success);
                }
            }
            case node_type::drop_type_t: {
                const auto& n = boost::polymorphic_pointer_downcast<node_create_type_t>(logic_plan);
                // Resolve the user-specified database to its namespace OID. Same pattern as
                // create_type — DROP TYPE mydb.foo finds foo in mydb; unqualified falls
                // back to public.
                components::catalog::oid_t target_ns = components::catalog::well_known_oid::public_namespace;
                if (!logic_plan->database_name().empty() &&
                    disk_address_ != actor_zeta::address_t::empty_address()) {
                    components::execution_context_t ddl_ctx{session,
                                                              components::table::transaction_data{0, 0}, {}};
                    auto [_rn, rnf] = actor_zeta::send(disk_address_,
                                                        &disk::manager_disk_t::resolve_namespace,
                                                        ddl_ctx,
                                                        std::string(logic_plan->database_name()),
                                                        std::uint64_t{0});
                    auto rns = co_await std::move(rnf);
                    if (rns.found) target_ns = rns.oid;
                }
                auto type_search_path = build_type_search_path(target_ns);
                for (auto ns_oid : type_search_path) {
                    co_await view.get_type(ctx, ns_oid, std::string(n->type().alias()));
                }
                error = check_type_exists(resource(), view, n->type().alias(),
                                            std::span<const components::catalog::oid_t>(type_search_path));
                if (error) {
                    break;
                } else {
                    // Mirrors create_type: pg_type is the cross-call source of truth.
                    if (disk_address_ != actor_zeta::address_t::empty_address()) {
                        components::execution_context_t ddl_ctx{session,
                                                                  components::table::transaction_data{0, 0}, {}};
                        auto [_rt, rtf] = actor_zeta::send(disk_address_,
                                                            &disk::manager_disk_t::resolve_type,
                                                            ddl_ctx,
                                                            target_ns,
                                                            std::string(n->type().type_name()),
                                                            std::uint64_t{0});
                        auto rt = co_await std::move(rtf);
                        if (rt.found) {
                            auto [_dt, dtf] = actor_zeta::send(disk_address_,
                                                                &disk::manager_disk_t::ddl_drop_type,
                                                                ddl_ctx,
                                                                rt.oid,
                                                                disk::drop_behavior_t::cascade_);
                            if (auto r = co_await std::move(dtf); r.failed())
                                co_return make_ddl_error_cursor(resource(), r);
                        }
                    }
                    co_return make_cursor(resource(), operation_status_t::success);
                }
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
            case node_type::create_constraint_t:
                break;
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

        // DDL needs a real (non-zero) txn so that mid-DDL crash → WAL replay rolls back
        // partially-written pg_catalog.* records.
        components::table::transaction_data txn_data{0, 0};
        {
            const auto t = logic_plan->type();
            const bool needs_ddl_txn = t == node_type::create_database_t || t == node_type::drop_database_t ||
                                        t == node_type::create_collection_t || t == node_type::drop_collection_t ||
                                        t == node_type::create_sequence_t || t == node_type::drop_sequence_t ||
                                        t == node_type::create_view_t || t == node_type::drop_view_t ||
                                        t == node_type::create_macro_t || t == node_type::drop_macro_t ||
                                        t == node_type::alter_table_t || t == node_type::create_constraint_t;
            if (needs_ddl_txn) {
                txn_data = txn_manager_.begin_transaction(session).data();
                trace(log_, "manager_dispatcher_t::execute_plan: DDL began txn {}",
                      txn_data.transaction_id);
            }
        }

        collection::executor::execute_result_t exec_result;
        switch (logic_plan->type()) {
            case node_type::create_database_t:
                exec_result = create_database_(logic_plan);
                break;
            case node_type::drop_database_t:
                exec_result = drop_database_(logic_plan);
                break;
            case node_type::create_collection_t:
                exec_result = create_collection_(logic_plan);
                break;
            case node_type::drop_collection_t:
                exec_result = drop_collection_(logic_plan);
                break;
            case node_type::checkpoint_t: {
                trace(log_, "manager_dispatcher_t::execute_plan: {}", to_string(logic_plan->type()));
                // Flush all dirty index btrees before table checkpoint
                if (index_address_ != actor_zeta::address_t::empty_address()) {
                    auto [_fi, fif] =
                        actor_zeta::send(index_address_, &index::manager_index_t::flush_all_indexes, session);
                    co_await std::move(fif);
                }
                // Query WAL for current max ID before checkpoint
                services::wal::id_t wal_max_id{0};
                if (wal_address_ != actor_zeta::address_t::empty_address()) {
                    auto [_wi, wif] =
                        actor_zeta::send(wal_address_, &wal::manager_wal_replicate_t::current_wal_id, session);
                    wal_max_id = co_await std::move(wif);
                }
                auto [_cp, cpf] =
                    actor_zeta::send(disk_address_, &disk::manager_disk_t::checkpoint_all, session, wal_max_id);
                auto checkpoint_wal_id = co_await std::move(cpf);
                // After checkpoint, trim old WAL segments (id=0 means no-op: IN_MEMORY tables need WAL)
                if (checkpoint_wal_id > 0 && wal_address_ != actor_zeta::address_t::empty_address()) {
                    auto [_wt, wtf] = actor_zeta::send(wal_address_,
                                                       &wal::manager_wal_replicate_t::truncate_before,
                                                       session,
                                                       checkpoint_wal_id);
                    co_await std::move(wtf);
                }
                co_return make_cursor(resource(), operation_status_t::success);
            }
            case node_type::vacuum_t: {
                trace(log_, "manager_dispatcher_t::execute_plan: {}", to_string(logic_plan->type()));
                auto lowest = txn_manager_.lowest_active_start_time();
                auto [_v, vf] = actor_zeta::send(disk_address_, &disk::manager_disk_t::vacuum_all, session, lowest);
                co_await std::move(vf);
                // Cleanup old index versions + rebuild (compact changes row positions)
                if (index_address_ != actor_zeta::address_t::empty_address()) {
                    auto [_cv, cvf] = actor_zeta::send(index_address_,
                                                       &index::manager_index_t::cleanup_all_versions,
                                                       session,
                                                       lowest);
                    co_await std::move(cvf);
                    // Rebuild indexes for each collection (compact invalidates row positions)
                    for (const auto& coll : collections_) {
                        auto [_rb, rbf] =
                            actor_zeta::send(index_address_, &index::manager_index_t::rebuild_indexes, session, coll);
                        co_await std::move(rbf);
                        // Re-populate indexes from storage
                        auto [_tr, trf] =
                            actor_zeta::send(disk_address_, &disk::manager_disk_t::storage_total_rows, session, coll);
                        auto total = co_await std::move(trf);
                        if (total > 0) {
                            auto [_ss, ssf] = actor_zeta::send(disk_address_,
                                                               &disk::manager_disk_t::storage_scan_segment,
                                                               session,
                                                               coll,
                                                               int64_t{0},
                                                               total);
                            auto scan_data = co_await std::move(ssf);
                            if (scan_data) {
                                auto count = scan_data->size();
                                auto [_ir, irf] = actor_zeta::send(index_address_,
                                                                   &index::manager_index_t::insert_rows,
                                                                   index::execution_context_t{session, txn_data, coll},
                                                                   std::move(scan_data),
                                                                   uint64_t{0},
                                                                   count);
                                co_await std::move(irf);
                            }
                        }
                    }
                }
                co_return make_cursor(resource(), operation_status_t::success);
            }
            case node_type::create_sequence_t:
            case node_type::drop_sequence_t:
            case node_type::create_view_t:
            case node_type::drop_view_t:
            case node_type::create_macro_t:
            case node_type::drop_macro_t:
            case node_type::alter_table_t:
            case node_type::create_constraint_t: {
                // DDL for sequences/views/macros/alter_table/constraints — no physical plan.
                // The real work runs in execute_ddl() after this switch.
                exec_result = {make_cursor(resource(), operation_status_t::success), {}};
                break;
            }
            default:
                exec_result = co_await execute_plan_impl(session, logic_plan, params->take_parameters(), txn_data);
                break;
        }

        auto& result = exec_result.cursor;
        trace(log_, "manager_dispatcher_t::execute_plan: result received, success: {}", result->is_success());

        if (result->is_success()) {
            // Use original_type for dispatch: planner may have wrapped DML nodes,
            // changing logic_plan->type() to a constraint wrapper type.
            const auto t = original_type;
            const bool is_ddl = t == node_type::create_database_t || t == node_type::drop_database_t ||
                                 t == node_type::create_collection_t || t == node_type::drop_collection_t ||
                                 t == node_type::create_sequence_t || t == node_type::drop_sequence_t ||
                                 t == node_type::create_view_t || t == node_type::drop_view_t ||
                                 t == node_type::create_macro_t || t == node_type::drop_macro_t ||
                                 t == node_type::alter_table_t || t == node_type::create_constraint_t;
            if (is_ddl) {
                trace(log_, "manager_dispatcher_t::execute_plan: DDL {} via execute_ddl",
                      to_string(t));
                auto ddl_cursor = co_await execute_ddl(session, logic_plan, txn_data, view,
                    ddl_context_t{disk_address_, index_address_, wal_address_, txn_manager_, collections_, resource()});
                if (!ddl_cursor->is_success())
                    co_return ddl_cursor;
                co_return result;
            }
            if (t == node_type::insert_t) {
                trace(log_,
                      "manager_dispatcher_t::execute_plan: DML {} completed by executor",
                      to_string(t));
                // V4: relkind='g' (computing/generated) check via cached table. The view's
                // try_get_table is sync — pre-load via co_await get_namespace + get_table so
                // the lookup hits cache.
                bool is_computing = false;
                if (!id.get_namespace().empty()) {
                    auto* ns_e = view.try_get_namespace(std::string_view(id.get_namespace().front()));
                    if (!ns_e) {
                        ns_e = co_await view.get_namespace(ctx, std::string(id.get_namespace().front()));
                    }
                    if (ns_e) {
                        auto* tbl = view.try_get_table(ns_e->oid, std::string_view(id.table_name()));
                        if (!tbl) {
                            tbl = co_await view.get_table(ctx, ns_e->oid, std::string(id.table_name()));
                        }
                        if (tbl && tbl->relkind == relkind::computed) {
                            is_computing = true;
                        }
                    }
                }
                if (is_computing) {
                    for (const auto& child : logic_plan->children()) {
                        if (child->type() != node_type::data_t)
                            continue;
                        auto* data_node = static_cast<const node_data_t*>(child.get());
                        std::vector<components::table::column_definition_t> columns;
                        columns.reserve(data_node->data_chunk().column_count());
                        for (size_t i = 0; i < data_node->data_chunk().column_count(); i++) {
                            const auto& type = data_node->data_chunk().data[i].type();
                            assert(type.has_alias());
                            columns.emplace_back(type.alias(), type);
                        }
                        components::execution_context_t ddl_ctx{session, txn_data,
                                                                  logic_plan->collection_full_name()};
                        auto [_rn, rnf] = actor_zeta::send(disk_address_,
                                                            &disk::manager_disk_t::resolve_namespace,
                                                            ddl_ctx,
                                                            logic_plan->database_name(),
                                                            std::uint64_t{0});
                        auto rns = co_await std::move(rnf);
                        if (!rns.found)
                            break;
                        auto [_rt, rtf] = actor_zeta::send(disk_address_,
                                                            &disk::manager_disk_t::resolve_table,
                                                            ddl_ctx,
                                                            rns.oid,
                                                            logic_plan->collection_name(),
                                                            std::uint64_t{0});
                        auto rt = co_await std::move(rtf);
                        if (!rt.found)
                            break;
                        auto [_da, daf] = actor_zeta::send(disk_address_,
                                                            &disk::manager_disk_t::ddl_adopt_computing_schema,
                                                            ddl_ctx,
                                                            rt.oid,
                                                            std::move(columns));
                        if (auto r = co_await std::move(daf); r.failed())
                            trace(log_, "ddl_adopt_computing_schema failed: status={}, blocker={}", static_cast<int>(r.status), r.blocking_oid);
                        break;
                    }
                }
                co_return result;
            }
            if (t == node_type::update_t || t == node_type::delete_t ||
                t == node_type::create_index_t || t == node_type::drop_index_t) {
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
        auto func_name = function->name();
        auto func_signatures = function->get_signatures();
        // Cross-namespace conflict detection (#41 Path 2): one resolve_function_by_name
        // scan replaces the legacy populate-iterate-namespaces loop. Returns every pg_proc
        // row matching `func_name` across all namespaces; any match (user or pg_catalog) is
        // a conflict.
        if (disk_address_ != actor_zeta::address_t::empty_address()) {
            components::execution_context_t ddl_ctx{session,
                                                      components::table::transaction_data{0, 0}, {}};
            auto [_rfbn, rfbnf] = actor_zeta::send(disk_address_,
                                                     &disk::manager_disk_t::resolve_function_by_name,
                                                     ddl_ctx,
                                                     std::string(func_name),
                                                     std::uint64_t{0});
            auto matches = co_await std::move(rfbnf);
            if (!matches.empty()) {
                co_return false;
            }
        }
        {
            // we have to send it to all executors and validate, that results are the same...
            std::pmr::vector<collection::executor::function_result_t> results(resource_);
            results.reserve(executor_pool_size_);
            for (size_t i = 0; i < executor_pool_size_; i++) {
                auto [needs_sched, future] =
                    actor_zeta::otterbrix::send(executor_addresses_[i],
                                                &collection::executor::executor_t::register_udf,
                                                session,
                                                function->get_copy());
                if (needs_sched && executors_[i]) {
                    scheduler_->enqueue(executors_[i].get());
                }
                results.emplace_back(co_await std::move(future));
            }
            // TODO: if executors return different uids once, they continue to disagree and any call to register_udf will fail
            if (std::all_of(results.begin(),
                            results.end(),
                            [first_uid = results.front()](components::compute::function_uid uid) {
                                return uid != components::compute::invalid_function_uid && uid == first_uid;
                            })) {
                // V4 lookup_function in validate_logical_plan probes
                // function_registry_t::get_default() — UDFs must live there too, not only
                // in executor-local registries. Mirror the executor add_function so the
                // dispatcher-side validate sees the UDF (matches pre-V4 visibility).
                if (auto* def_reg = components::compute::function_registry_t::get_default()) {
                    (void)def_reg->add_function(function->get_copy());
                }
                // pg_proc is the persistent source of truth. Function visibility on
                // subsequent execute_plan calls is served by catalog_view_t resolving
                // against pg_proc.
                // Persist in pg_proc — UDFs registered here are user-namespace functions.
                // We attach them to the first existing user namespace; if none exists, the
                // pg_proc row is namespaced to the well-known pg_catalog.
                if (disk_address_ != actor_zeta::address_t::empty_address()) {
                    components::execution_context_t ddl_ctx{session,
                                                              components::table::transaction_data{0, 0}, {}};
                    components::catalog::oid_t target_ns = components::catalog::well_known_oid::pg_catalog_namespace;
                    // V4: enumerate user namespaces via disk's list_namespaces (admin-path
                    // bypass of per-name cache). First non-pg_catalog namespace wins.
                    auto [_ln, lnf] = actor_zeta::send(disk_address_,
                                                        &disk::manager_disk_t::list_namespaces,
                                                        ddl_ctx);
                    auto ns_names = co_await std::move(lnf);
                    for (auto& nname : ns_names) {
                        if (!nname.empty() && nname != "pg_catalog") {
                            auto [_rn, rnf] = actor_zeta::send(disk_address_,
                                                                &disk::manager_disk_t::resolve_namespace,
                                                                ddl_ctx,
                                                                std::string(nname),
                                                                std::uint64_t{0});
                            auto rns = co_await std::move(rnf);
                            if (rns.found) {
                                target_ns = rns.oid;
                                break;
                            }
                        }
                    }
                    std::int32_t pronargs = func_signatures.empty()
                        ? 0
                        : static_cast<std::int32_t>(func_signatures.front().input_types.size());
                    std::int64_t prouid = static_cast<std::int64_t>(results.front());
                    // Encode the first signature's per-arg matcher kinds + output types so
                    // catalog_view_t can reconstruct real signatures across restart (#152).
                    // Empty signatures → empty fields → reader falls back to always_true ×
                    // pronargs / same_at(0) defaults.
                    std::string proargmatchers;
                    std::string prorettype;
                    if (!func_signatures.empty()) {
                        std::vector<components::compute::input_type> matchers;
                        matchers.reserve(func_signatures.front().input_types.size());
                        for (auto& it : func_signatures.front().input_types) {
                            matchers.push_back(it);
                        }
                        proargmatchers = components::catalog::encode_proargmatchers(matchers);
                        std::vector<components::compute::output_type> outs;
                        outs.reserve(func_signatures.front().output_types.size());
                        for (auto& ot : func_signatures.front().output_types) {
                            outs.push_back(ot);
                        }
                        prorettype = components::catalog::encode_prorettype(outs);
                    }
                    auto [_df, dff] = actor_zeta::send(disk_address_,
                                                        &disk::manager_disk_t::ddl_create_function,
                                                        ddl_ctx,
                                                        target_ns,
                                                        std::string(func_name),
                                                        pronargs,
                                                        prouid,
                                                        std::move(proargmatchers),
                                                        std::move(prorettype));
                    if (auto r = co_await std::move(dff); r.failed())
                        trace(log_, "ddl_create_function failed: status={}, blocker={}", static_cast<int>(r.status), r.blocking_oid);
                }
                co_return true;
            } else {
                co_return false;
            }
        }
    }

    manager_dispatcher_t::unique_future<bool>
    manager_dispatcher_t::unregister_udf(components::session::session_id_t session,
                                         std::string function_name,
                                         std::pmr::vector<complex_logical_type> inputs) {
        trace(log_, "dispatcher_t::unregister_udf: session {}, {}", session.data(), function_name);
        // V4: existence check via the in-process function_registry_t. Functions registered
        // through register_udf land in the default registry; pg_proc is the cross-restart
        // source of truth but at runtime the registry is authoritative for "exists?" checks.
        auto* reg = components::compute::function_registry_t::get_default();
        bool exists = false;
        if (reg) {
            for (auto& [n, uid] : reg->get_functions()) {
                if (n != function_name) continue;
                auto* fn = reg->get_function(uid);
                if (!fn) continue;
                for (auto& sig : fn->get_signatures()) {
                    if (sig.matches_inputs(inputs)) { exists = true; break; }
                }
                if (exists) break;
            }
        }
        if (exists) {
            // V4: also drop from the in-memory default registry so subsequent validate
            // lookups (which probe function_registry_t::get_default()) don't find the
            // function any more. Mirrors register_udf's add to the default registry.
            if (reg) {
                (void)reg->remove_function_by_signature(function_name, inputs);
            }
            // pg_proc is the source of truth — catalog_view_t resolves against it on the
            // next execute_plan, so no shadow write is needed.
            // Persist drop in pg_proc by name lookup. Only purges rows matching proname; if
            // the same function name lives in multiple namespaces, all are dropped.
            if (disk_address_ != actor_zeta::address_t::empty_address()) {
                components::execution_context_t ddl_ctx{session,
                                                          components::table::transaction_data{0, 0}, {}};
                // #41 Path 4: one resolve_function_by_name scan returns every pg_proc row
                // matching `function_name` across all namespaces. Drop each match.
                auto [_rfbn, rfbnf] = actor_zeta::send(disk_address_,
                                                         &disk::manager_disk_t::resolve_function_by_name,
                                                         ddl_ctx,
                                                         std::string(function_name),
                                                         std::uint64_t{0});
                auto matches = co_await std::move(rfbnf);
                for (auto& m : matches) {
                    auto [_df, dff] = actor_zeta::send(disk_address_,
                                                        &disk::manager_disk_t::ddl_drop_function,
                                                        ddl_ctx,
                                                        m.oid,
                                                        disk::drop_behavior_t::cascade_);
                    if (auto r = co_await std::move(dff); r.failed())
                        trace(log_, "ddl_drop_function failed: status={}, blocker={}", static_cast<int>(r.status), r.blocking_oid);
                }
            }
            co_return true;
        }
        co_return false;
    }

    manager_dispatcher_t::unique_future<size_t> manager_dispatcher_t::size(components::session::session_id_t session,
                                                                           std::string database_name,
                                                                           std::string collection) {
        trace(log_,
              "manager_dispatcher_t::size session:{}, database: {}, collection: {}",
              session.data(),
              database_name,
              collection);

        // V4: collection-existence check via catalog_view_t. Pre-load namespace + table so
        // try_get_* in check_collection_exists hits the cache.
        catalog_view_t view{plan_cache_, disk_address_,
                            plan_cache_.pinned_version_for(session).value_or(last_seen_version_),
                            resource()};
        components::execution_context_t ctx{session, components::table::transaction_data{0, 0}, {}};
        table_id id{resource(), {database_name, collection}};
        if (!id.get_namespace().empty()) {
            auto* ns_e = co_await view.get_namespace(ctx, std::string(id.get_namespace().front()));
            if (ns_e) {
                co_await view.get_table(ctx, ns_e->oid, std::string(id.table_name()));
            }
        }
        auto error = check_collection_exists(resource(), view, id);
        if (error) {
            co_return size_t(0);
        }

        collection_full_name_t name{database_name, collection};
        if (collections_.find(name) == collections_.end()) {
            co_return size_t(0);
        }
        // Get size from storage in manager_disk_t
        auto [_s, sf] = actor_zeta::send(disk_address_,
                                         &disk::manager_disk_t::storage_calculate_size,
                                         components::session::session_id_t{},
                                         name);
        auto sz = co_await std::move(sf);
        co_return static_cast<size_t>(sz);
    }

    manager_dispatcher_t::unique_future<components::cursor::cursor_t_ptr>
    manager_dispatcher_t::get_schema(components::session::session_id_t session,
                                     std::pmr::vector<std::pair<database_name_t, collection_name_t>> ids) {
        trace(log_, "manager_dispatcher_t::get_schema session: {}, ids count: {}", session.data(), ids.size());
        // V4: serve schema lookups via catalog_view_t. Pre-load referenced namespaces+tables
        // and read resolved_table_t::columns (attname/type per column) to build the struct
        // shape that callers expect.
        catalog_view_t view{plan_cache_, disk_address_,
                            plan_cache_.pinned_version_for(session).value_or(last_seen_version_),
                            resource()};
        components::execution_context_t ctx{session, components::table::transaction_data{0, 0}, {}};
        std::pmr::vector<complex_logical_type> schemas;
        schemas.reserve(ids.size());

        for (const auto& [db, coll] : ids) {
            table_id id(resource(), {db, coll});
            const resolved_table_t* tbl = nullptr;
            if (!id.get_namespace().empty()) {
                auto* ns_e = view.try_get_namespace(std::string_view(id.get_namespace().front()));
                if (!ns_e) {
                    ns_e = co_await view.get_namespace(ctx, std::string(id.get_namespace().front()));
                }
                if (ns_e) {
                    tbl = view.try_get_table(ns_e->oid, std::string_view(id.table_name()));
                    if (!tbl) {
                        tbl = co_await view.get_table(ctx, ns_e->oid, std::string(id.table_name()));
                    }
                }
            }
            if (tbl && tbl->relkind != relkind::computed) {
                std::vector<complex_logical_type> col_types;
                col_types.reserve(tbl->columns.size());
                for (const auto& c : tbl->columns) {
                    auto t = c.type;
                    t.set_alias(c.attname);
                    col_types.push_back(std::move(t));
                }
                schemas.push_back(complex_logical_type::create_struct("schema", col_types));
                continue;
            }
            if (tbl && tbl->relkind == relkind::computed) {
                // Computing/generated table — surface the latest column types as a "latest_types"
                // struct to mirror computed_schema::latest_types_struct().
                std::vector<complex_logical_type> col_types;
                col_types.reserve(tbl->columns.size());
                for (const auto& c : tbl->columns) {
                    auto t = c.type;
                    t.set_alias(c.attname);
                    col_types.push_back(std::move(t));
                }
                schemas.push_back(complex_logical_type::create_struct("latest_types", col_types));
                continue;
            }
            schemas.push_back(logical_type::INVALID);
        }

        co_return make_cursor(resource(), std::move(schemas));
    }

    manager_dispatcher_t::unique_future<void>
    manager_dispatcher_t::close_cursor(components::session::session_id_t /*session*/) {
        co_return;
    }

    collection::executor::execute_result_t manager_dispatcher_t::create_database_(node_ptr logical_plan) {
        trace(log_, "manager_dispatcher_t:create_database {}", logical_plan->database_name());
        return {make_cursor(resource(), operation_status_t::success), {}};
    }

    collection::executor::execute_result_t manager_dispatcher_t::drop_database_(node_ptr logical_plan) {
        trace(log_, "manager_dispatcher_t:drop_database {}", logical_plan->database_name());
        auto db_name = logical_plan->database_name();
        for (auto it = collections_.begin(); it != collections_.end();) {
            if (it->database == db_name) {
                it = collections_.erase(it);
            } else {
                ++it;
            }
        }
        return {make_cursor(resource(), operation_status_t::success), {}};
    }

    collection::executor::execute_result_t manager_dispatcher_t::create_collection_(node_ptr logical_plan) {
        trace(log_, "manager_dispatcher_t:create_collection {}", logical_plan->collection_full_name().to_string());
        collections_.insert(logical_plan->collection_full_name());
        return {make_cursor(resource(), operation_status_t::success), {}};
    }

    collection::executor::execute_result_t manager_dispatcher_t::drop_collection_(node_ptr logical_plan) {
        trace(log_, "manager_dispatcher_t:drop_collection {}", logical_plan->collection_full_name().to_string());
        collections_.erase(logical_plan->collection_full_name());
        return {make_cursor(resource(), operation_status_t::success), {}};
    }

    manager_dispatcher_t::unique_future<collection::executor::execute_result_t>
    manager_dispatcher_t::execute_plan_impl(components::session::session_id_t session,
                                            node_ptr logical_plan,
                                            storage_parameters parameters,
                                            components::table::transaction_data txn) {
        trace(log_,
              "manager_dispatcher_t:execute_plan_impl: collection: {}, session: {}",
              logical_plan->collection_full_name().to_string(),
              session.data());

        auto dependency_tree_collections_names = logical_plan->collection_dependencies();
        context_storage_t collections_context_storage(resource(), log_.clone());
        for (auto& name : dependency_tree_collections_names) {
            if (!name.empty() && collections_.count(name) > 0) {
                collections_context_storage.known_collections.insert(name);
            }
        }

        // Populate index metadata for optimizer-driven index selection
        if (index_address_ != actor_zeta::address_t::empty_address()) {
            auto coll = logical_plan->collection_full_name();
            if (!coll.empty()) {
                auto [_ik, ikf] =
                    actor_zeta::send(index_address_, &index::manager_index_t::get_indexed_keys, session, coll);
                collections_context_storage.indexed_keys = co_await std::move(ikf);
            }
        }
        collections_context_storage.parameters = &parameters;

        assert(!executors_.empty());
        auto pool_idx = collection_name_hash{}(logical_plan->collection_full_name()) % executors_.size();
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
        // Pin the session's plan-cache view to the most recently observed catalog version.
        // Subsequent execute_plan calls on this session will probe at the pinned version,
        // so DDL committed by other sessions (which bumps catalog_version_) doesn't change
        // the schema view of this in-flight txn.
        plan_cache_.pin_version(session, last_seen_version_);
        co_return txn.data();
    }

    manager_dispatcher_t::unique_future<uint64_t>
    manager_dispatcher_t::commit_transaction(components::session::session_id_t session) {
        trace(log_, "manager_dispatcher_t::commit_transaction, session: {}", session.data());
        // Capture txn_data before commit() removes the transaction from the active set.
        components::table::transaction_data txn_data{0, 0};
        if (auto* txn = txn_manager_.find_transaction(session)) {
            txn_data = txn->data();
        }
        plan_cache_.unpin_version(session);
        const uint64_t commit_id = txn_manager_.commit(session);
        // Flip MVCC state on pg_catalog rows appended under this explicit transaction.
        // Mirrors the auto-commit path in execute_ddl().
        if (txn_data.transaction_id != 0 && commit_id > 0
            && disk_address_ != actor_zeta::address_t::empty_address()) {
            components::execution_context_t cpa_ctx{session, txn_data, {}};
            auto [_cpa, cpaf] = actor_zeta::send(disk_address_,
                                                  &disk::manager_disk_t::commit_pg_catalog_appends,
                                                  cpa_ctx, commit_id);
            co_await std::move(cpaf);
        }
        co_return commit_id;
    }

    manager_dispatcher_t::unique_future<void>
    manager_dispatcher_t::abort_transaction(components::session::session_id_t session) {
        trace(log_, "manager_dispatcher_t::abort_transaction, session: {}", session.data());
        // Capture txn_data before abort() removes the transaction.
        components::table::transaction_data txn_data{0, 0};
        if (auto* txn = txn_manager_.find_transaction(session)) {
            txn_data = txn->data();
        }
        plan_cache_.unpin_version(session);
        txn_manager_.abort(session);
        // Revert any pg_catalog rows appended under this transaction (DDL rollback).
        if (txn_data.transaction_id != 0
            && disk_address_ != actor_zeta::address_t::empty_address()) {
            components::execution_context_t revert_ctx{session, txn_data, {}};
            auto [_r, rf] = actor_zeta::send(disk_address_,
                                              &disk::manager_disk_t::revert_pg_catalog_appends,
                                              revert_ctx);
            co_await std::move(rf);
        }
        co_return;
    }

    // M5: pull recent invalidation events from disk, advance last_seen_version_, and on
    // overflow reset the entire plan cache. Called at the start of every execute_plan
    // before validating + executing the plan. The disk-side ring buffer's snapshot_t carries
    // both the latest version and overflow flag; we honor the latter wholesale.
    manager_dispatcher_t::unique_future<void>
    manager_dispatcher_t::refresh_invalidations_(components::session::session_id_t session) {
        if (disk_address_ == actor_zeta::address_t::empty_address()) {
            co_return;
        }
        auto [_inv, invf] = actor_zeta::send(disk_address_,
                                              &disk::manager_disk_t::recent_invalidations_since,
                                              session,
                                              last_seen_version_);
        auto snap = co_await std::move(invf);
        if (snap.overflow) {
            // Consumer fell more than CAPACITY events behind — reset cache wholesale.
            plan_cache_.clear();
        }
        if (snap.latest_version > last_seen_version_) {
            last_seen_version_ = snap.latest_version;
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
