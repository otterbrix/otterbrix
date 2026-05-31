#include "dispatcher.hpp"
#include "plan_resolve_index.hpp"
#include "validate_logical_plan.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/oid_batch.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/catalog/table_id.hpp>

#include <components/context/context.hpp>
#include <components/logical_plan/node_abort_transaction.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_allocate_oids.hpp>
#include <components/logical_plan/node_alter_column_add.hpp>
#include <components/logical_plan/node_alter_column_drop.hpp>
#include <components/logical_plan/node_alter_column_rename.hpp>
#include <components/logical_plan/node_alter_table.hpp>
#include <components/logical_plan/node_catalog_resolve_namespace.hpp>
#include <components/logical_plan/node_catalog_resolve_table.hpp>
#include <components/logical_plan/node_catalog_resolve_type.hpp>
#include <components/logical_plan/node_commit_transaction.hpp>
#include <components/logical_plan/node_computed_field_register.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_constraint.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_create_macro.hpp>
#include <components/logical_plan/node_create_matview.hpp>
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
#include <components/logical_plan/node_get_schema.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_primitive_write.hpp>
#include <components/logical_plan/node_refresh_matview.hpp>
#include <components/logical_plan/node_register_udf.hpp>
#include <components/logical_plan/node_sequence.hpp>
#include <components/logical_plan/node_set_timezone.hpp>
#include <components/logical_plan/node_unregister_udf.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/physical_plan/operators/operator_abort_transaction.hpp>
#include <components/physical_plan/operators/operator_commit_transaction.hpp>
#include <components/physical_plan/operators/operator_get_schema.hpp>
#include <components/physical_plan/operators/operator_register_udf.hpp>
#include <components/physical_plan/operators/operator_unregister_udf.hpp>
#include <components/physical_plan_generator/impl/create_plan_register_udf.hpp>
#include <core/executor.hpp>
#include <core/tracy/tracy.hpp>

#include <components/physical_plan_generator/create_plan.hpp>
#include <components/planner/optimizer.hpp>
#include <components/planner/planner.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

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

        // Probe `name` in the plan-tree idx across the dbname search path.
        // The transformer emits resolve_type for every (dbname, name) tuple
        // we expect to find here (CREATE TABLE column UDT, CREATE TYPE
        // collision check, DROP TYPE existence check). search_dbnames
        // carries dbname strings ordered by precedence (e.g.
        // [table_dbname, "public", "pg_catalog"]).
        const components::logical_plan::resolved_type_metadata_t*
        probe_type_in_path(const impl::plan_resolve_index_t* idx,
                           std::string_view name,
                           std::span<const std::string> search_dbnames) {
            for (const auto& db : search_dbnames) {
                if (const auto* md = impl::type_md_for(idx, db, name))
                    return md;
            }
            return nullptr;
        }

        // String-based search path for plan-tree idx lookup. Deduplicates
        // entries (when target_dbname is already "public" / "pg_catalog").
        std::vector<std::string> build_type_search_path_str(std::string_view target_dbname) {
            std::vector<std::string> path;
            if (!target_dbname.empty() && target_dbname != "public" && target_dbname != "pg_catalog") {
                path.emplace_back(target_dbname);
            }
            path.emplace_back("public");
            path.emplace_back("pg_catalog");
            return path;
        }

        // When the SQL transformer wraps a DML/DDL plan in
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
        // is only relevant to transformer-side wraps.
        const components::logical_plan::node_t* effective_root_node(const components::logical_plan::node_t* n) {
            if (!n)
                return nullptr;
            if (n->type() != components::logical_plan::node_type::sequence_t) {
                return n;
            }
            using nt = components::logical_plan::node_type;
            auto is_catalog_resolve = [](nt t) {
                return t == nt::catalog_resolve_namespace_t || t == nt::catalog_resolve_table_t ||
                       t == nt::catalog_resolve_type_t || t == nt::catalog_resolve_function_t;
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
                if (!*it)
                    continue;
                if (!is_catalog_resolve((*it)->type())) {
                    return it->get();
                }
            }
            // All children are catalog_resolve_* (or empty): no consumer
            // available, return the wrapper itself so callers fall through
            // to the generic execute path.
            return n;
        }

        components::logical_plan::node_type effective_root_type(const components::logical_plan::node_t* n) {
            auto* r = effective_root_node(n);
            return r ? r->type() : components::logical_plan::node_type::unused;
        }

        // Mutable-pointer overload so the various
        // static_cast<node_X_t*>(logic_plan.get()) call sites can descend
        // through the transformer's catalog_resolve_* wrapper without
        // duplicating the walk logic.
        components::logical_plan::node_t* effective_root_node(components::logical_plan::node_t* n) {
            return const_cast<components::logical_plan::node_t*>(
                effective_root_node(static_cast<const components::logical_plan::node_t*>(n)));
        }

        // drop_* nodes no longer carry user-typed dbname/relname; their
        // sibling resolve_namespace / resolve_table nodes inside the wrapping
        // sequence_t do. Extract (db, rel) from the resolve siblings so
        // routing code that still needs names (qualified_name_t for table_id,
        // collections_ map keys, etc.) keeps working.
        std::pair<std::string, std::string>
        drop_target_names_from_resolves(const components::logical_plan::node_t* plan_root) {
            using namespace components::logical_plan;
            if (!plan_root || plan_root->type() != node_type::sequence_t) {
                return {};
            }
            std::string db;
            std::string rel;
            for (const auto& c : plan_root->children()) {
                if (!c)
                    continue;
                if (c->type() == node_type::catalog_resolve_namespace_t) {
                    auto* rn = static_cast<const node_catalog_resolve_namespace_t*>(c.get());
                    if (db.empty())
                        db = rn->dbname();
                } else if (c->type() == node_type::catalog_resolve_table_t) {
                    auto* rt = static_cast<const node_catalog_resolve_table_t*>(c.get());
                    if (db.empty())
                        db = rt->dbname();
                    if (rel.empty())
                        rel = rt->relname();
                }
            }
            return {std::move(db), std::move(rel)};
        }

        // === Phase 1.5: SELECT-time view expansion ===
        //
        // After Pass 1 stamps resolved_metadata.view_sql on catalog_resolve_table_t
        // nodes whose relkind=='v', this helper walks the plan and replaces view
        // references with sub-plans derived from the view body SQL.
        //
        // First-iteration scope: only top-level `SELECT * FROM v` style plans
        // where the outer plan is the transformer's standard wrap
        //   sequence_t(catalog_resolve_namespace, catalog_resolve_table(v), aggregate(v))
        // and the aggregate is a trivial passthrough. Complex outer queries
        // (extra WHERE/JOIN/projection on top of v) return an error suggesting
        // followups #1 — they require column-projection composition, which is
        // out of scope for this PR.

        struct view_expansion_result_t {
            bool had_expansion{false};
            components::logical_plan::node_ptr expanded_plan;
            components::logical_plan::parameter_node_ptr expanded_params;
            components::cursor::cursor_t_ptr error;
        };

        // Find the FIRST catalog_resolve_table_t with relkind='v' (and non-empty
        // view_sql) in `root`'s direct children. Returns nullptr if none.
        components::logical_plan::node_catalog_resolve_table_t*
        find_first_view_resolve(components::logical_plan::node_t* root) {
            using namespace components::logical_plan;
            if (!root || root->type() != node_type::sequence_t) {
                return nullptr;
            }
            for (auto& c : root->children()) {
                if (!c || c->type() != node_type::catalog_resolve_table_t) {
                    continue;
                }
                auto* rt = static_cast<node_catalog_resolve_table_t*>(c.get());
                const auto& md = rt->resolved_metadata();
                if (md && md->relkind == catalog::relkind::view && !md->view_sql.empty()) {
                    return rt;
                }
            }
            return nullptr;
        }

        // Parse view body SQL and transform it into a fresh logical plan.
        // The transformer is instantiated per-call (its mutable state lives on
        // the instance — re-entrant when allocated fresh per call).
        view_expansion_result_t expand_view_body(std::pmr::memory_resource* resource, const std::string& view_sql) {
            view_expansion_result_t out;
            std::pmr::monotonic_buffer_resource parser_arena(resource);
            void* parse_cell = nullptr;
            try {
                auto* parsed = raw_parser(&parser_arena, view_sql.c_str());
                if (!parsed) {
                    out.error =
                        make_cursor(resource,
                                    core::error_t(core::error_code_t::sql_parse_error,
                                                  std::pmr::string{"view body re-parse returned null", resource}));
                    return out;
                }
                parse_cell = linitial(parsed);
            } catch (const std::exception& ex) {
                out.error = make_cursor(
                    resource,
                    core::error_t(core::error_code_t::sql_parse_error, std::pmr::string{ex.what(), resource}));
                return out;
            }
            if (!parse_cell) {
                out.error = make_cursor(resource,
                                        core::error_t(core::error_code_t::sql_parse_error,
                                                      std::pmr::string{"empty view body parse", resource}));
                return out;
            }
            components::sql::transform::transformer local_transformer(resource, view_sql.c_str());
            auto tr =
                local_transformer.transform(components::sql::transform::pg_cell_to_node_cast(parse_cell)).finalize();
            if (tr.has_error()) {
                out.error = make_cursor(resource, tr.error());
                return out;
            }
            // The transformer returns a fresh plan, typically
            // sequence_t(catalog_resolve_namespace, catalog_resolve_table(t),
            //            aggregate(t, ...)). The dispatcher will run Pass 1
            //            on this sub_plan's resolves before validate_schema.
            out.had_expansion = true;
            out.expanded_plan = std::move(tr.value().node);
            out.expanded_params = std::move(tr.value().params);
            return out;
        }

        // Collect catalog_resolve_*_t nodes whose oid hasn't been stamped yet
        // (i.e. need a fresh Pass 1 round). Operates on direct children of
        // sequence_t roots; sub-plan splicing places them at the front so a
        // shallow scan suffices.
        std::vector<components::logical_plan::node_ptr>
        extract_unresolved_resolves(components::logical_plan::node_t* root) {
            using namespace components::logical_plan;
            std::vector<node_ptr> out;
            if (!root || root->type() != node_type::sequence_t) {
                return out;
            }
            for (auto& c : root->children()) {
                if (!c)
                    continue;
                const auto t = c->type();
                const bool is_resolve =
                    t == node_type::catalog_resolve_namespace_t || t == node_type::catalog_resolve_table_t ||
                    t == node_type::catalog_resolve_type_t || t == node_type::catalog_resolve_function_t ||
                    t == node_type::catalog_resolve_constraint_t;
                if (!is_resolve)
                    continue;
                if (t == node_type::catalog_resolve_table_t) {
                    auto* rt = static_cast<node_catalog_resolve_table_t*>(c.get());
                    if (rt->resolved_metadata().has_value()) {
                        continue; // already resolved (outer plan's resolve)
                    }
                } else if (t == node_type::catalog_resolve_namespace_t) {
                    auto* rn = static_cast<node_catalog_resolve_namespace_t*>(c.get());
                    if (rn->namespace_oid() != catalog::INVALID_OID) {
                        continue; // already resolved
                    }
                }
                out.push_back(c);
            }
            return out;
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

    manager_dispatcher_t::unique_future<components::cursor::cursor_t_ptr>
    manager_dispatcher_t::execute_plan(components::session::session_id_t session,
                                       node_ptr plan,
                                       parameter_node_ptr params) {
        trace(log_, "manager_dispatcher_t::execute_plan session: {}, {}", session.data(), plan->to_string());

        auto params_for_wal = make_parameter_node(resource());
        params_for_wal->set_parameters(params->parameters());

        // All catalog reads go through the plan-tree resolve idx (validate /
        // enrich / DDL paths). The collections_ map is maintained incrementally by
        // init_from_state, post-create (line ~1251), and post-drop (line
        // ~1259-1269), so re-fetching pg_namespace + pg_class on every plan
        // is unnecessary.
        components::execution_context_t ctx{session, components::table::transaction_data{0, 0}, {}};

        // Save original node type — used after planner rewrite to dispatch DDL/DML paths.
        // When transformer wraps DML/DDL in
        //   sequence_t(catalog_resolve_namespace_t, catalog_resolve_table_t, <consumer>)
        // we route on <consumer>'s type, not the wrapping sequence_t. For non-wrapped
        // plans effective_root_type() is identity (n->type()), so this is a no-op for
        // existing DDL flows (planner wraps are applied AFTER this line — see notes
        // inside the helper).
        const auto original_type = effective_root_type(plan.get());
        // Capture the drop target before the planner rewrites it into a
        // node_dynamic_cascade_delete_t (which carries only OIDs, not names).
        // Used after successful execution to clean up the in-memory collections_
        // routing map so a subsequent execute_plan does not see a stale entry.
        // Descend through transformer's sequence_t(catalog_resolve_*,
        // <drop_node>) wrapper to reach the real drop node before casting.
        std::string drop_target_database;
        qualified_name_t drop_target_collection;
        if (original_type == node_type::drop_database_t) {
            auto names = drop_target_names_from_resolves(plan.get());
            drop_target_database = std::move(names.first);
        } else if (original_type == node_type::drop_collection_t) {
            auto names = drop_target_names_from_resolves(plan.get());
            drop_target_collection = qualified_name_t{names.first, names.second};
        }
        auto logic_plan = std::move(plan);
        // Optimizer: constant folding, etc.
        logic_plan = components::planner::optimize(resource(), logic_plan, params.get());

        // Wrap the plan with catalog_resolve_namespace + catalog_resolve_table
        // for every (db, rel) pair found in the tree. Validate/enrich consume
        // OIDs through the plan-tree idx; the SQL transformer only emits
        // resolves for the outermost target (e.g. INSERT FROM SELECT wraps
        // CopyTestCollection but not the SELECT source TestCollection), so we
        // need to top-up missing tables here. For direct-API callers
        // (wrapper_dispatcher::execute_plan, find, etc.) this builds the full
        // wrap from scratch. Existing resolves in sequence_t already cover
        // their (db, rel) tuples — set-based dedup avoids re-emitting them.
        {
            // Collect resolves that already exist in the plan tree so we don't
            // re-emit them. Operates on the immediate front children of
            // sequence_t (where the transformer puts its resolves); a deeper
            // walk is unnecessary because Pass 1 only consumes front-children.
            std::set<std::string> existing_dbs;
            std::set<std::pair<std::string, std::string>> existing_tbls;
            if (logic_plan->type() == node_type::sequence_t) {
                for (const auto& c : logic_plan->children()) {
                    if (!c)
                        continue;
                    if (c->type() == node_type::catalog_resolve_namespace_t) {
                        auto* r = static_cast<const node_catalog_resolve_namespace_t*>(c.get());
                        existing_dbs.insert(r->dbname());
                    } else if (c->type() == node_type::catalog_resolve_table_t) {
                        auto* r = static_cast<const node_catalog_resolve_table_t*>(c.get());
                        existing_tbls.insert({r->dbname(), r->relname()});
                        existing_dbs.insert(r->dbname());
                    }
                }
            }
            std::set<std::string> wrap_dbs;
            std::set<std::pair<std::string, std::string>> wrap_tbls;
            auto add_dbrel = [&](std::string db, std::string rel) {
                if (db.empty())
                    return;
                wrap_dbs.insert(db);
                if (!rel.empty()) {
                    wrap_tbls.insert({std::move(db), std::move(rel)});
                }
            };
            // Iterative pre-order walk (no recursion → no std::function).
            std::vector<const node_t*> stack;
            stack.push_back(logic_plan.get());
            while (!stack.empty()) {
                const node_t* n = stack.back();
                stack.pop_back();
                if (!n)
                    continue;
                switch (n->type()) {
                    // DML consumers no longer carry (db, rel) — names
                    // for collection-set tracking come from the sibling
                    // resolve_table inside the wrapping sequence_t (the
                    // catalog_resolve_table_t branch below picks them up).
                    case node_type::insert_t:
                    case node_type::update_t:
                    case node_type::delete_t:
                        break;
                    case node_type::aggregate_t: {
                        auto* d = static_cast<const node_aggregate_t*>(n);
                        add_dbrel(static_cast<const std::string&>(d->dbname()),
                                  static_cast<const std::string&>(d->relname()));
                        break;
                    }
                    case node_type::match_t: {
                        auto* d = static_cast<const node_match_t*>(n);
                        add_dbrel(static_cast<const std::string&>(d->dbname()),
                                  static_cast<const std::string&>(d->relname()));
                        break;
                    }
                    case node_type::join_t: {
                        auto* d = static_cast<const node_join_t*>(n);
                        add_dbrel(static_cast<const std::string&>(d->dbname()),
                                  static_cast<const std::string&>(d->relname()));
                        break;
                    }
                    case node_type::create_database_t: {
                        auto* d = static_cast<const node_create_database_t*>(n);
                        if (!d->dbname().empty())
                            wrap_dbs.insert(d->dbname());
                        break;
                    }
                    // create_collection_t / create_index_t no longer
                    // carry parent dbname/relname; the transformer always wraps
                    // them with sibling catalog_resolve_namespace / resolve_table
                    // so wrap_dbs/wrap_tbls is already populated from
                    // existing_dbs/existing_tbls above.
                    // drop_database_t / drop_collection_t / drop_index_t
                    // no longer carry names; the transformer always wraps them
                    // with sibling catalog_resolve_* nodes so wrap_dbs/wrap_tbls
                    // already has the (db, rel) covered.
                    default:
                        break;
                }
                for (const auto& c : n->children()) stack.push_back(c.get());
            }
            // Drop resolves already present so we don't duplicate them.
            for (const auto& db : existing_dbs) wrap_dbs.erase(db);
            for (const auto& t : existing_tbls) wrap_tbls.erase(t);
            if (!wrap_dbs.empty() || !wrap_tbls.empty()) {
                // Collect new resolves to prepend.
                std::vector<components::logical_plan::node_ptr> new_resolves;
                std::set<std::string> resolved_dbs = existing_dbs;
                for (const auto& db : wrap_dbs) {
                    if (resolved_dbs.insert(db).second) {
                        new_resolves.push_back(
                            components::logical_plan::make_node_catalog_resolve_namespace(resource(),
                                                                                          core::dbname_t{db}));
                    }
                }
                for (const auto& [db, rel] : wrap_tbls) {
                    if (resolved_dbs.insert(db).second) {
                        new_resolves.push_back(
                            components::logical_plan::make_node_catalog_resolve_namespace(resource(),
                                                                                          core::dbname_t{db}));
                    }
                    new_resolves.push_back(
                        components::logical_plan::make_node_catalog_resolve_table(resource(),
                                                                                  core::dbname_t{db},
                                                                                  core::relname_t{rel}));
                }
                if (logic_plan->type() == node_type::sequence_t) {
                    // Splice new resolves AFTER existing leading resolve_*
                    // siblings but BEFORE the consumer node. Order matters:
                    // stamp_oids_from_resolves picks the FIRST resolve_table
                    // as the DML target — preserving original-target priority
                    // means walker-added scan resolves don't shadow it.
                    auto is_resolve_local = [](node_type t) {
                        return t == node_type::catalog_resolve_namespace_t || t == node_type::catalog_resolve_table_t ||
                               t == node_type::catalog_resolve_type_t || t == node_type::catalog_resolve_function_t ||
                               t == node_type::catalog_resolve_constraint_t;
                    };
                    auto& kids = logic_plan->children();
                    std::vector<components::logical_plan::node_ptr> merged;
                    merged.reserve(kids.size() + new_resolves.size());
                    std::size_t split = 0;
                    while (split < kids.size() && kids[split] && is_resolve_local(kids[split]->type())) {
                        merged.push_back(std::move(kids[split]));
                        ++split;
                    }
                    for (auto& r : new_resolves) merged.push_back(std::move(r));
                    for (; split < kids.size(); ++split) {
                        merged.push_back(std::move(kids[split]));
                    }
                    kids.clear();
                    for (auto& m : merged) kids.push_back(std::move(m));
                } else {
                    auto seq = boost::intrusive_ptr<components::logical_plan::node_t>(
                        new components::logical_plan::node_sequence_t(resource()));
                    for (auto& r : new_resolves) seq->append_child(std::move(r));
                    seq->append_child(std::move(logic_plan));
                    logic_plan = seq;
                }
            }
        }

        // Pass 1 — execute catalog_resolve_*_t front children
        // of the transformer's sequence_t wrap BEFORE validate. The
        // operator_resolve_*_t back-pointer constructor stamps
        // namespace_oid / table_oid on the corresponding logical nodes;
        // subsequent validate (via plan_resolve_index_t threaded through
        // explicitly) and enrich (via enrich_resolve_idx_t) read OIDs
        // from the plan tree.
        //
        // The wrap shape is sequence_t(catalog_resolve_*, ..., <consumer>)
        // and emission is unconditional in the transformer (toggle removed
        // 2026-05-13 after all four root causes were diagnosed and fixed).
        // For plans that aren't sequence_t (or whose first child isn't a
        // catalog_resolve_*), the block short-circuits with no behavioural
        // change.
        if (logic_plan->type() == node_type::sequence_t) {
            auto& kids = logic_plan->children();
            auto is_resolve = [](node_type t) {
                return t == node_type::catalog_resolve_namespace_t || t == node_type::catalog_resolve_table_t ||
                       t == node_type::catalog_resolve_type_t || t == node_type::catalog_resolve_function_t ||
                       t == node_type::catalog_resolve_constraint_t;
            };
            std::size_t resolve_count = 0;
            while (resolve_count < kids.size() && kids[resolve_count] && is_resolve(kids[resolve_count]->type())) {
                ++resolve_count;
            }
            if (resolve_count > 0) {
                // begin_transaction is idempotent per session (returns the
                // existing active txn or starts one); the later
                // begin_transaction at line ~1031 reuses this same txn.
                // Required because operator_resolve_*_t reads pg_catalog
                // tables (pg_class, pg_namespace, ...) which need MVCC
                // visibility against the active txn.
                auto pass1_txn = txn_manager_.begin_transaction(session).data();
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
                auto pass1_params = make_parameter_node(resource());
                auto pass1_result =
                    co_await execute_plan_impl(session, pass1_root, pass1_params->take_parameters(), pass1_txn);
                if (pass1_result.cursor->is_error()) {
                    trace(log_,
                          "manager_dispatcher_t::execute_plan: Pass 1 "
                          "resolve failed: {}",
                          pass1_result.cursor->get_error().what);
                    co_return std::move(pass1_result.cursor);
                }
                // Propagate the established txn into ctx so validate /
                // enrich reads (which still take `ctx`) see the same MVCC
                // snapshot the resolves saw.
                ctx = components::execution_context_t{session, pass1_txn, ctx.session_tz, ctx.table_oid};
                // Note: resolves stay in plan tree so validate/enrich's
                // gather walks find them. create_plan_sequence skips
                // catalog_resolve_*_t children when building the executor's
                // left-chain (they have already run in Pass 1; putting
                // them in operator_insert.left_ would corrupt insert's
                // data input — see create_plan_sequence.cpp note).
            }
        }
        // Pass 1 stamps OIDs on resolve_* siblings via back-pointer
        // but not on the consumer nodes. Propagate now so validate (which
        // reads node->table_oid() via tbl_md_for_oid) sees stamped OIDs.
        // Must run AFTER Pass 1 and BEFORE the dispatcher_idx build below.
        stamp_oids_from_resolves(logic_plan.get());
        // Build plan-tree idx ONCE so the DDL existence checks
        // below read ns_oid / table metadata via Pass 1 stamps. The pointer
        // is threaded explicitly into every helper (no thread_local).
        impl::plan_resolve_index_t dispatcher_idx;
        impl::gather_plan_resolve_index(logic_plan.get(), dispatcher_idx);

        // === Phase 1.5: SELECT-time view expansion ===
        // After Pass 1 stamps resolved_metadata.view_sql on catalog_resolve_table_t
        // nodes whose relkind=='v', re-parse + re-transform the view body and
        // splice the resulting sub-plan in place. First-iteration scope: only
        // top-level passthrough plans (`SELECT * FROM v`) — we replace the
        // entire logic_plan with the sub-plan. More elaborate compositions
        // (extra filters, projections, joins on top of v) are followups #1.
        if (auto* view_node = find_first_view_resolve(logic_plan.get())) {
            auto exp = expand_view_body(resource(), view_node->resolved_metadata()->view_sql);
            if (exp.error) {
                trace(log_, "manager_dispatcher_t::execute_plan: view expansion failed");
                co_return std::move(exp.error);
            }
            if (exp.had_expansion && exp.expanded_plan) {
                // Full plan replacement — outer is treated as trivial passthrough
                // for first iteration. Future: splice sub-plan as child of outer
                // consumer to preserve outer projections/filters (followups #1).
                logic_plan = std::move(exp.expanded_plan);

                // Merge sub-plan's parameter bindings into the outer params
                // node so downstream operators see constants used in the view
                // body (e.g. `col_b > 10` parameter). First-iteration assumes
                // no parameter_id collision with outer (outer is a trivial
                // passthrough SELECT * with no own constants).
                if (exp.expanded_params) {
                    for (const auto& [pid, val] : exp.expanded_params->parameters().parameters) {
                        params->add_parameter(pid, val);
                    }
                }

                // === Phase 1.6: Pass 1 on sub-plan's fresh resolves ===
                auto fresh = extract_unresolved_resolves(logic_plan.get());
                if (!fresh.empty()) {
                    auto pass2_root = boost::intrusive_ptr<components::logical_plan::node_t>(
                        new components::logical_plan::node_sequence_t(resource()));
                    for (auto& n : fresh) {
                        pass2_root->append_child(n);
                    }
                    auto pass2_params = make_parameter_node(resource());
                    auto pass2_result =
                        co_await execute_plan_impl(session, pass2_root, pass2_params->take_parameters(), ctx.txn);
                    if (pass2_result.cursor->is_error()) {
                        trace(log_,
                              "manager_dispatcher_t::execute_plan: view sub-plan Pass 1 "
                              "resolve failed: {}",
                              pass2_result.cursor->get_error().what);
                        co_return std::move(pass2_result.cursor);
                    }
                }

                // === Phase 1.7: rebuild idx for re-validate ===
                stamp_oids_from_resolves(logic_plan.get());
                dispatcher_idx = impl::plan_resolve_index_t{};
                impl::gather_plan_resolve_index(logic_plan.get(), dispatcher_idx);
            }
        }
        // Build table_id from the plan's role-named accessors. Each derived
        // node owns a (db, rel)-shaped pair; nodes that don't (create_type_t,
        // drop_type_t, wrappers) yield empty identifiers — same outcome as
        // the previous cfn_of() default branch.
        const auto* plan_root_for_drop_names = logic_plan.get();
        auto build_id_cfn = [plan_root_for_drop_names](const node_t* n) -> qualified_name_t {
            if (!n)
                return {};
            switch (n->type()) {
                case node_type::aggregate_t: {
                    auto* d = static_cast<const node_aggregate_t*>(n);
                    return qualified_name_t{static_cast<const std::string&>(d->dbname()),
                                            static_cast<const std::string&>(d->relname())};
                }
                // alter_* nodes carry no user-typed names; pull
                // (db, rel) from the sibling resolve nodes in the wrapping
                // sequence_t (transform_alter_table wraps in resolve_table).
                case node_type::alter_column_add_t:
                case node_type::alter_column_drop_t:
                case node_type::alter_column_rename_t:
                case node_type::alter_table_t: {
                    auto names = drop_target_names_from_resolves(plan_root_for_drop_names);
                    return qualified_name_t{names.first, names.second};
                }
                case node_type::create_collection_t: {
                    auto* d = static_cast<const node_create_collection_t*>(n);
                    auto names = drop_target_names_from_resolves(plan_root_for_drop_names);
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
                    auto names = drop_target_names_from_resolves(plan_root_for_drop_names);
                    return qualified_name_t{names.first, names.second};
                }
                case node_type::create_macro_t: {
                    auto* d = static_cast<const node_create_macro_t*>(n);
                    auto names = drop_target_names_from_resolves(plan_root_for_drop_names);
                    return qualified_name_t{names.first, d->macroname()};
                }
                case node_type::create_sequence_t: {
                    auto* d = static_cast<const node_create_sequence_t*>(n);
                    auto names = drop_target_names_from_resolves(plan_root_for_drop_names);
                    return qualified_name_t{names.first, d->seqname()};
                }
                case node_type::create_view_t: {
                    auto* d = static_cast<const node_create_view_t*>(n);
                    auto names = drop_target_names_from_resolves(plan_root_for_drop_names);
                    return qualified_name_t{names.first, d->viewname()};
                }
                // drop_* and DML (insert/update/delete) nodes
                // carry no user-typed names; pull (db, rel) from the sibling
                // resolve nodes in the wrapping sequence_t.
                case node_type::delete_t:
                case node_type::insert_t:
                case node_type::update_t:
                case node_type::drop_collection_t:
                case node_type::drop_index_t:
                case node_type::drop_macro_t:
                case node_type::drop_sequence_t:
                case node_type::drop_view_t: {
                    auto names = drop_target_names_from_resolves(plan_root_for_drop_names);
                    return qualified_name_t{names.first, names.second};
                }
                case node_type::drop_database_t: {
                    auto names = drop_target_names_from_resolves(plan_root_for_drop_names);
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
        // Build identification name from the effective
        // consumer node, not the (potentially transformer-wrapping) sequence_t.
        table_id id(resource(), build_id_cfn(effective_root_node(logic_plan.get())));
        cursor_t_ptr error;
        // Namespace existence is resolved via plan-tree idx
        // populated by Pass 1 (operator_resolve_namespace_t stamps ns_oid on
        // the resolve node). No async preload needed — check_namespace_exists
        // / ns_oid_for_dbname read from the explicit `dispatcher_idx`.
        switch (original_type) {
            case node_type::create_database_t:
                if (!check_namespace_exists(resource(), &dispatcher_idx, id).contains_error()) {
                    // PostgreSQL IF NOT EXISTS: DB already present -> success no-op (not an error).
                    auto* d = static_cast<const node_create_database_t*>(effective_root_node(logic_plan.get()));
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
                if (auto err = check_namespace_exists(resource(), &dispatcher_idx, id); err.contains_error()) {
                    error = make_cursor(resource(), err);
                }
                break;
            case node_type::create_collection_t: {
                // Target namespace + UDT existence both resolved via
                // plan-tree idx (no async preloads).
                if (!check_collection_exists(resource(), &dispatcher_idx, id).contains_error()) {
                    // PostgreSQL IF NOT EXISTS: table already present -> success no-op (not an error).
                    auto* cc = static_cast<const node_create_collection_t*>(effective_root_node(logic_plan.get()));
                    if (cc && cc->if_not_exists()) {
                        error = make_cursor(resource());
                    } else {
                        error = make_cursor(resource(),
                                            core::error_t{core::error_code_t::table_already_exists,
                                                          std::pmr::string{"collection already exists", resource()}});
                    }
                } else {
                    // UDT existence + resolution reads from plan-tree
                    // idx (transform_create_table emits resolve_type
                    // per column UDT). No async catalog preloads needed.
                    const std::string target_db =
                        id.get_namespace().empty() ? std::string{} : std::string(id.get_namespace().front());
                    const auto str_path = build_type_search_path_str(target_db);
                    auto* n = static_cast<node_create_collection_t*>(effective_root_node(logic_plan.get()));
                    for (auto& col_def : n->column_definitions()) {
                        if (col_def.type().type() == logical_type::UNKNOWN) {
                            if (col_def.type().type_name().empty()) {
                                break;
                            }
                            // Builtin scalars (bool, int4, …) resolve sync via
                            // pg_name_to_logical_type — the pre-M4 path handled
                            // this implicitly through view.get_type preload.
                            const auto lt = components::catalog::pg_name_to_logical_type(col_def.type().type_name());
                            if (lt != logical_type::UNKNOWN) {
                                std::string alias = col_def.type().has_alias() ? col_def.type().alias() : std::string{};
                                col_def.type() = components::types::complex_logical_type{lt};
                                if (!alias.empty()) {
                                    col_def.type().set_alias(alias);
                                }
                                continue;
                            }
                            if (auto err = check_type_exists(resource(),
                                                             &dispatcher_idx,
                                                             col_def.type().type_name(),
                                                             std::span<const std::string>(str_path));
                                err.contains_error()) {
                                error = make_cursor(resource(), err);
                            }
                            if (!error) {
                                const auto* md = probe_type_in_path(&dispatcher_idx,
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
                // Drop nodes carry no names; (db, rel) lives on the
                // sibling resolve nodes already captured in `id` above.
                if (!collections_.count(qualified_name_t{
                        id.get_namespace().empty() ? std::string{} : std::string(id.get_namespace().front()),
                        std::string(id.table_name())})) {
                    error = make_cursor(resource(),
                                        core::error_t{core::error_code_t::table_not_exists,
                                                      std::pmr::string{"collection does not exist", resource()}});
                    break;
                }
                if (auto err = check_collection_exists(resource(), &dispatcher_idx, id); err.contains_error()) {
                    error = make_cursor(resource(), err);
                }
                break;
            }
            case node_type::create_type_t: {
                // logic_plan is sequence_t(catalog_resolve_*..., create_type) —
                // descend through the wrap to reach the create_type leaf. The previous
                // reinterpret_cast<node_create_type_ptr&>(logic_plan) treated the
                // sequence_t bytes as a create_type_t, yielding garbage from n->type().
                auto* n = static_cast<node_create_type_t*>(effective_root_node(logic_plan.get()));
                // Collision detection + nested field resolution read
                // from plan-tree idx (transform_create_type / transform_create_enum_type
                // emit resolve_type per UDT name). No view.* preloads.
                components::catalog::oid_t target_ns = components::catalog::well_known_oid::public_namespace;
                const std::string default_path[] = {"public", "pg_catalog"};
                std::span<const std::string> str_path(default_path);
                // Collision check — if Pass 1 stamped a type_md for the same
                // name, the type already exists.
                if (!check_type_exists(resource(), &dispatcher_idx, n->type().type_name(), str_path).contains_error()) {
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
                            if (auto err = check_type_exists(resource(), &dispatcher_idx, field.type_name(), str_path);
                                err.contains_error()) {
                                error = make_cursor(resource(), err);
                                break;
                            }
                            const auto* md =
                                probe_type_in_path(&dispatcher_idx, std::string_view(field.type_name()), str_path);
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
                // drop_type carries no name; pull it from the sibling
                // catalog_resolve_type_t in the wrapping sequence_t.
                std::string type_name;
                if (logic_plan->type() == node_type::sequence_t) {
                    for (const auto& c : logic_plan->children()) {
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
                if (auto err = check_type_exists(resource(), &dispatcher_idx, type_name, str_path);
                    err.contains_error()) {
                    error = make_cursor(resource(), err);
                }
                break;
            }
            case node_type::set_timezone_t: {
                const auto& tz_node = reinterpret_cast<node_set_timezone_ptr&>(logic_plan);
                auto tz_err = default_tz_cat_.set_timezone(resource(), tz_node->timezone_name());
                if (!tz_err.contains_error() && disk_address_ != actor_zeta::address_t::empty_address()) {
                    const auto* settings_def = components::catalog::find_system_table("pg_settings");
                    if (settings_def) {
                        std::pmr::vector<components::types::complex_logical_type> types(resource());
                        for (const auto& col : settings_def->columns) {
                            types.push_back(col.type());
                        }
                        components::vector::data_chunk_t row(resource(), types, 1);
                        row.set_cardinality(1);
                        row.set_value(0, 0, components::types::logical_value_t(resource(), std::string{"TimeZone"}));
                        row.set_value(
                            1,
                            0,
                            components::types::logical_value_t(resource(), std::string{tz_node->timezone_name()}));
                        components::execution_context_t exec_ctx{session, {0, 0}, session_tz(session)};
                        auto [_u, uf] = actor_zeta::send(disk_address_,
                                                         &services::disk::manager_disk_t::append_pg_catalog_row,
                                                         exec_ctx,
                                                         components::catalog::well_known_oid::pg_settings_table,
                                                         std::move(row));
                        co_await std::move(uf);
                    }
                }
                co_return make_cursor(resource(), std::move(tz_err));
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
                // ALTER TABLE target metadata (ns_oid, table_oid, columns)
                // is stamped on the plan-tree resolve_table node by Pass 1
                // (transform_alter_table emits the wrap). enrich_plan reads
                // from the plan-tree idx.
                break;
            case node_type::create_constraint_t: {
                // Target + FK ref tables stamped on plan-tree by Pass 1
                // (transform_alter_table's CONSTRAINT path uses the multi-target
                // wrap so both end up in tbl_md_by_qname).
                if (auto err = check_collection_exists(resource(), &dispatcher_idx, id); err.contains_error()) {
                    error = make_cursor(resource(), err);
                }
                // Reject FK / CHECK on dynamic-schema (relkind='g') tables.
                // FK / CHECK enforcement requires stable column attoids; relkind='g'
                // attoids may evolve. We probe relkind via the plan-tree idx.
                if (!error && !id.get_namespace().empty()) {
                    auto* cstr = static_cast<node_create_constraint_t*>(effective_root_node(logic_plan.get()));
                    if (cstr->kind() == constraint_kind::foreign_key || cstr->kind() == constraint_kind::check) {
                        const auto* tbl_local = impl::tbl_md_for(&dispatcher_idx,
                                                                 std::string_view(id.get_namespace().front()),
                                                                 std::string_view(id.table_name()));
                        const bool local_is_g = tbl_local && tbl_local->relkind == 'g';
                        bool ref_is_g = false;
                        if (cstr->kind() == constraint_kind::foreign_key &&
                            cstr->ref_table_oid() != components::catalog::INVALID_OID) {
                            const auto* tbl_ref = impl::tbl_md_for_oid(&dispatcher_idx, cstr->ref_table_oid());
                            ref_is_g = tbl_ref && tbl_ref->relkind == 'g';
                        }
                        if (cstr->kind() == constraint_kind::foreign_key && (local_is_g || ref_is_g)) {
                            error = make_cursor(
                                resource(),
                                core::error_t{core::error_code_t::schema_error,
                                              std::pmr::string{
                                                  "Foreign key constraints are not supported when the referencing or "
                                                  "referenced table is dynamic-schema (relkind='g'). FK enforcement "
                                                  "requires stable column attoids; dynamic-schema columns may evolve. "
                                                  "Convert involved tables to static schema first.",
                                                  resource()}});
                        } else if (cstr->kind() == constraint_kind::check && local_is_g) {
                            error = make_cursor(
                                resource(),
                                core::error_t{core::error_code_t::schema_error,
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
                auto vt_err = validate_types(resource(), &dispatcher_idx, logic_plan.get(), session_tz(session));
                if (vt_err.contains_error()) {
                    error = make_cursor(resource(), vt_err);
                } else {
                    auto schema_res =
                        validate_schema(resource(), &dispatcher_idx, logic_plan.get(), params->parameters());
                    if (schema_res.has_error()) {
                        error = make_cursor(resource(), schema_res.error());
                    }
                }
            }
        }

        if (error) {
            trace(log_, "manager_dispatcher_t::execute_plan: validation error: {}", error->get_error().what);
            co_return std::move(error);
        }

        // Enrich DML node fields with catalog metadata (NOT NULL, DEFAULT, CHECK exprs).
        // enrich reads exclusively from the plan-tree idx.
        {
            auto ef = enrich_plan(resource(), logic_plan, disk_address_, ctx);
            co_await std::move(ef);
        }
        // Logical plan rewrite: insert constraint wrapper nodes driven by enriched fields.
        {
            components::planner::planner_t planner;
            logic_plan = planner.create_plan(resource(), std::move(logic_plan));
        }

        // enrich_plan has already stamped table_oid on the INSERT node
        // (and resolved relkind via the (ns_oid, name) primary index). We probe the
        // oid-keyed secondary index instead of re-running the (ns, name) resolution
        // path. Note: planner.create_plan above may have wrapped the INSERT in
        // check_constraint_t / fk_check_t, in which case table_oid_ on the root
        // wrapper is INVALID — peek at the first child the same way pool routing
        // does at line 1257. Falls back gracefully when enrich could not stamp an
        // oid (legacy / disk-disabled — INVALID_OID short-circuits the branch).
        if (original_type == node_type::insert_t && disk_address_ != actor_zeta::address_t::empty_address()) {
            components::catalog::oid_t resolved_tbl_oid = components::catalog::INVALID_OID;
            bool is_computing = false;
            // logic_plan may be sequence_t(catalog_resolve_*,
            // ..., insert_t). The table_oid lives on the insert_t consumer,
            // not on the wrapping sequence_t or the resolve_* prefix
            // children. Descend via effective_root_node to reach the real
            // INSERT before reading table_oid; otherwise enriched_oid stays
            // INVALID and is_computing falsely returns false, skipping the
            // register wrap for relkind='g' tables (pg_computed_column never
            // gets populated → SELECT * sees no columns).
            auto* effective_insert_node = effective_root_node(logic_plan.get());
            auto enriched_oid = effective_insert_node ? effective_insert_node->table_oid() : logic_plan->table_oid();
            if (enriched_oid == components::catalog::INVALID_OID && !logic_plan->children().empty()) {
                enriched_oid = logic_plan->children().front()->table_oid();
            }
            if (enriched_oid != components::catalog::INVALID_OID) {
                // Read relkind from plan-tree idx (Pass 1 stamps it).
                if (const auto* tbl = impl::tbl_md_for_oid(&dispatcher_idx, enriched_oid)) {
                    if (tbl->relkind == relkind::computed) {
                        is_computing = true;
                        resolved_tbl_oid = tbl->table_oid;
                    }
                }
            }

            // relkind='g' INSERT — wrap the user's INSERT plan in
            // sequence_t(insert, computed_field_register) so pg_computed_column
            // rows are appended inside the executor's DML txn (commit applies
            // the MVCC swap atomically with the data write). The table stays as
            // relkind='g' permanently — no promotion to 'r'. Column definitions
            // come from the embedded node_data_t chunk produced by the parser
            // (each vector_t::type() carries the field name as alias).
            if (is_computing) {
                std::vector<components::table::column_definition_t> registered_cols;
                // logic_plan may be
                // sequence_t(catalog_resolve_*, ..., insert_t). The data_t
                // chunk we need lives inside the insert_t consumer, not as
                // a sibling of the wrapping sequence_t. Descend through
                // effective_root_node to reach the real INSERT before
                // iterating its children.
                auto* effective_insert = effective_root_node(logic_plan.get());
                if (effective_insert) {
                    for (const auto& child : effective_insert->children()) {
                        if (!child || child->type() != components::logical_plan::node_type::data_t) {
                            continue;
                        }
                        auto* data_node = static_cast<const components::logical_plan::node_data_t*>(child.get());
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

                // insert_node carries only its OID; (db, rel) names
                // travel via the sibling resolve_table inside the wrapping
                // sequence_t — pull them from there for the register node.
                auto insert_names = drop_target_names_from_resolves(logic_plan.get());
                auto register_node = boost::intrusive_ptr(
                    new components::logical_plan::node_computed_field_register_t(resource(),
                                                                                 core::dbname_t{insert_names.first},
                                                                                 core::relname_t{insert_names.second},
                                                                                 resolved_tbl_oid,
                                                                                 std::move(registered_cols)));

                auto seq = boost::intrusive_ptr(new components::logical_plan::node_sequence_t(resource()));
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
            catalog::oid_batch_t oid_batch;
            oid_batch.oids = co_await allocate_oids_via_pipeline(session, need);
            components::planner::planner_t ddl_planner;
            logic_plan = ddl_planner.create_plan(resource(), std::move(logic_plan), std::move(oid_batch));
        }

        // CREATE DATABASE → planner rewrite to sequence_t(primitive_write on pg_namespace).
        if (original_type == node_type::create_database_t && disk_address_ != actor_zeta::address_t::empty_address()) {
            catalog::oid_batch_t oid_batch;
            oid_batch.oids = co_await allocate_oids_via_pipeline(session, std::size_t{1});
            components::planner::planner_t ddl_planner;
            logic_plan = ddl_planner.create_plan(resource(), std::move(logic_plan), std::move(oid_batch));
        }

        // CREATE TYPE → planner rewrite to sequence_t(primitive_write × N).
        //   STRUCT     → (1 + N) OIDs: pg_class.oid + N×pg_attribute.attoid (composite type).
        //   ENUM/other → 1 OID: pg_type.oid.
        // Existence checks + UNKNOWN-field resolution already happened in the validation
        // switch above; namespace_oid is stored on the node for the planner to read.
        if (original_type == node_type::create_type_t && disk_address_ != actor_zeta::address_t::empty_address()) {
            auto* ct = static_cast<node_create_type_t*>(effective_root_node(logic_plan.get()));
            const std::size_t need = (ct->type().type() == logical_type::STRUCT)
                                         ? std::size_t{1} + ct->type().child_types().size()
                                         : std::size_t{1};
            catalog::oid_batch_t oid_batch;
            oid_batch.oids = co_await allocate_oids_via_pipeline(session, need);
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
        if ((original_type == node_type::create_sequence_t || original_type == node_type::create_view_t ||
             original_type == node_type::create_macro_t) &&
            disk_address_ != actor_zeta::address_t::empty_address()) {
            const std::size_t need = (original_type == node_type::create_sequence_t) ? std::size_t{1} : std::size_t{2};
            catalog::oid_batch_t oid_batch;
            oid_batch.oids = co_await allocate_oids_via_pipeline(session, need);
            components::planner::planner_t ddl_planner;
            logic_plan = ddl_planner.create_plan(resource(), std::move(logic_plan), std::move(oid_batch));
        }

        // CREATE MATERIALIZED VIEW → standard DDL pattern (like CREATE TABLE):
        // allocate OIDs → planner stamps mv_oid + catalog_writes on the matview node →
        // physical_plan_generator produces composite operator_create_matview_t that
        // does heap+catalog+populate atomically in one async coroutine.
        // OID batch holds: mv_oid + N×attoid + rule_oid = 2 + N (where N is the
        // matview's inferred column count populated by enrich's
        // derive_matview_output_schema).
        if (original_type == node_type::create_matview_t && disk_address_ != actor_zeta::address_t::empty_address()) {
            auto* cm = static_cast<node_create_matview_t*>(effective_root_node(logic_plan.get()));
            const std::size_t col_count = cm ? cm->inferred_columns().size() : std::size_t{0};
            const std::size_t need = 2 + col_count;
            catalog::oid_batch_t oid_batch;
            oid_batch.oids = co_await allocate_oids_via_pipeline(session, need);
            components::planner::planner_t ddl_planner;
            logic_plan = ddl_planner.create_plan(resource(), std::move(logic_plan), std::move(oid_batch));
        }

        // CREATE INDEX → planner rewrite to sequence_t(primitive_write × N, create_index_t).
        // The trailing create_index_t carries name/keys/type plus the resolved
        // namespace_oid/table_oid/index_oid so create_plan_sequence can lower it
        // to operator_create_index_metadata_t + operator_create_index_backfill_t.
        if (original_type == node_type::create_index_t && disk_address_ != actor_zeta::address_t::empty_address()) {
            catalog::oid_batch_t oid_batch;
            oid_batch.oids = co_await allocate_oids_via_pipeline(session, std::size_t{1});
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
        if (original_type == node_type::alter_table_t && disk_address_ != actor_zeta::address_t::empty_address()) {
            catalog::oid_batch_t oid_batch; // intentionally empty
            components::planner::planner_t ddl_planner;
            logic_plan = ddl_planner.create_plan(resource(), std::move(logic_plan), std::move(oid_batch));
            // Re-run enrich over the planner-emitted sequence so the
            // freshly-constructed alter_column_rename_t / computed_field_unregister_t
            // primitives get their attoid_ resolved (they cannot be resolved before
            // the planner runs because they don't yet exist). The new explicit
            // cases in enrich_plan walk the sequence and stamp attoid via
            // the plan-tree resolved table metadata (rename: tbl->columns) or
            // pg_computed_column scan (unregister).
            //
            // At this point the DDL transaction has not yet been
            // started (begin_transaction below), so `ctx` carries
            // transaction_data{0, 0}. The pg_computed_column scan inside the
            // computed_field_unregister enrich case reads with zero-txn
            // visibility and misses the INSERT-time register rows → live_attoid
            // stays INVALID_OID → unregister no-ops at execute time → no
            // tombstone → DROP COLUMN on relkind='g' never propagates
            // (dynamic_schema_drop_column failure).
            // Begin the DDL txn here. begin_transaction is idempotent per
            // session (returns the existing active txn if one exists — see
            // components/table/transaction_manager.cpp:12), so the unchanged
            // call below reuses this same txn.
            auto enrich_txn = txn_manager_.begin_transaction(session).data();
            components::execution_context_t enriched_ctx{session, enrich_txn, ctx.session_tz, ctx.table_oid};
            auto ef2 = enrich_plan(resource(), logic_plan, disk_address_, enriched_ctx);
            co_await std::move(ef2);
        }

        // DROP DATABASE / TABLE / TYPE / SEQUENCE / VIEW / MACRO → planner rewrite to
        // node_dynamic_cascade_delete_t. The cascade operator self-walks pg_depend at
        // runtime and performs catalog row deletes + storage drops. No OID batch is
        // needed (drops don't allocate). Resolved seed OIDs were stamped on the
        // logical drop_X node by enrich_logical_plan above.
        if ((original_type == node_type::drop_database_t || original_type == node_type::drop_collection_t ||
             original_type == node_type::drop_type_t || original_type == node_type::drop_sequence_t ||
             original_type == node_type::drop_view_t || original_type == node_type::drop_macro_t) &&
            disk_address_ != actor_zeta::address_t::empty_address()) {
            catalog::oid_batch_t oid_batch; // intentionally empty
            components::planner::planner_t ddl_planner;
            logic_plan = ddl_planner.create_plan(resource(), std::move(logic_plan), std::move(oid_batch));
        }

        // CREATE CONSTRAINT → planner rewrite to sequence_t(primitive_write on pg_constraint+pg_depend).
        // Resolved attoids/table_oid/ref_table_oid populated by enrich_plan above.
        if (original_type == node_type::create_constraint_t &&
            disk_address_ != actor_zeta::address_t::empty_address()) {
            // Validate CHECK expression non-empty before allocating OIDs.
            auto* cstr = static_cast<node_create_constraint_t*>(effective_root_node(logic_plan.get()));
            if (cstr->kind() == constraint_kind::check && cstr->check_expr().empty()) {
                co_return make_cursor(
                    resource(),
                    core::error_t{core::error_code_t::other_error,
                                  std::pmr::string{"CHECK constraint expression is empty or contains unsupported "
                                                   "constructs (functions, subqueries, and CASE expressions are not "
                                                   "allowed; valid: comparisons, AND/OR/NOT, IS NULL/IS NOT NULL, "
                                                   "column references, and constants)",
                                                   resource()}});
            }
            catalog::oid_batch_t oid_batch;
            oid_batch.oids = co_await allocate_oids_via_pipeline(session, std::size_t{1});
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
            const bool needs_ddl_txn =
                original_type == node_type::create_collection_t || original_type == node_type::create_constraint_t ||
                original_type == node_type::create_sequence_t || original_type == node_type::create_view_t ||
                original_type == node_type::create_macro_t || original_type == node_type::create_type_t ||
                original_type == node_type::create_index_t || original_type == node_type::drop_index_t ||
                original_type == node_type::drop_database_t || original_type == node_type::drop_collection_t ||
                original_type == node_type::drop_type_t || original_type == node_type::drop_sequence_t ||
                original_type == node_type::drop_view_t || original_type == node_type::drop_macro_t ||
                original_type == node_type::create_database_t || original_type == node_type::alter_table_t ||
                original_type == node_type::create_matview_t;
            if (needs_ddl_txn) {
                txn_data = txn_manager_.begin_transaction(session).data();
                trace(log_, "manager_dispatcher_t::execute_plan: DDL began txn {}", txn_data.transaction_id);
            }
        }

        collection::executor::execute_result_t exec_result;
        // Route execution by the effective consumer type;
        // see comments above the validate switch.
        switch (original_type) {
            case node_type::alter_table_t: {
                // ALTER TABLE is normally rewritten by the planner into
                // sequence_t(alter_column_{add,rename,drop}_t × N). Reaching this
                // case means rewrite_alter_table bailed out because table_oid was
                // not resolved by enrich (table not found); return no-op success
                // and let the validate/enrich layer surface a hard error.
                //
                // This switch routes by `original_type`, so we still see
                // alter_table_t here even AFTER the planner has rewritten the
                // plan into sequence_t. Distinguish the genuine bailout
                // (logic_plan->type() still alter_table_t) from the
                // already-rewritten case (logic_plan is sequence_t) — the
                // latter must run through the executor like every other DDL.
                if (logic_plan->type() == node_type::alter_table_t) {
                    exec_result = {make_cursor(resource()), {}, {}, {}};
                } else {
                    exec_result = co_await execute_plan_impl(session, logic_plan, params->take_parameters(), txn_data);
                }
                break;
            }
            default:
                exec_result = co_await execute_plan_impl(session, logic_plan, params->take_parameters(), txn_data);
                break;
        }

        // Hand pg_catalog swap-info up to the transaction so commit/abort
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
                trace(log_, "manager_dispatcher_t::execute_plan: DML {} completed by executor", to_string(t));
                co_return result;
            }
            if (t == node_type::create_collection_t || t == node_type::create_database_t ||
                t == node_type::create_constraint_t || t == node_type::create_sequence_t ||
                t == node_type::create_view_t || t == node_type::create_macro_t || t == node_type::create_type_t ||
                t == node_type::create_index_t || t == node_type::drop_index_t || t == node_type::drop_database_t ||
                t == node_type::drop_collection_t || t == node_type::drop_type_t || t == node_type::drop_sequence_t ||
                t == node_type::drop_view_t || t == node_type::drop_macro_t || t == node_type::alter_table_t ||
                t == node_type::create_matview_t) {
                // DDL commit goes through operator_commit_transaction_t in
                // ddl-commit mode. The operator performs flush (durability
                // barrier) + wal::commit_txn + txn_manager.commit +
                // storage_commit_appends/deletes (steps 1-6). Step 7
                // (CREATE INDEX backfill commit_insert) stays inline below
                // because it depends on plan structure.
                std::uint64_t commit_id = 0;
                {
                    constexpr auto db_oid = components::catalog::well_known_oid::main_database;
                    auto ddl_commit_node =
                        boost::intrusive_ptr(new components::logical_plan::node_commit_transaction_t(resource()));
                    ddl_commit_node->set_is_ddl_commit(true);
                    ddl_commit_node->set_txn_id(txn_data.transaction_id);
                    ddl_commit_node->set_database_oid(db_oid);

                    services::context_storage_t cstor{resource(), log_.clone(), session_tz(session)};
                    components::compute::function_registry_t fn_registry{resource()};
                    auto commit_op = services::planner::create_plan(cstor,
                                                                    fn_registry,
                                                                    ddl_commit_node,
                                                                    components::logical_plan::limit_t::unlimit(),
                                                                    /*params=*/nullptr);
                    if (commit_op) {
                        commit_op->set_as_root();
                        components::logical_plan::storage_parameters cparams(resource());
                        components::pipeline::context_t pctx{session,
                                                             actor_zeta::address_t::empty_address(),
                                                             actor_zeta::address_t::empty_address(),
                                                             &fn_registry,
                                                             cparams};
                        pctx.disk_address = disk_address_;
                        pctx.wal_address = wal_address_;
                        pctx.txn_manager = &txn_manager_;
                        pctx.txn = txn_data;
                        commit_op->prepare();
                        commit_op->on_execute(&pctx);
                        while (!commit_op->is_executed()) {
                            auto waiting = commit_op->find_waiting_operator();
                            if (!waiting)
                                break;
                            co_await waiting->await_async_and_resume(&pctx);
                            commit_op->on_execute(&pctx);
                        }
                        if (pctx.has_pending_disk_futures()) {
                            auto futures = pctx.take_pending_disk_futures();
                            for (auto& f : futures) co_await std::move(f);
                        }
                        commit_id = static_cast<components::operators::operator_commit_transaction_t*>(commit_op.get())
                                        ->commit_id();
                    }
                }
                // Step 7 (only): drive index commit_insert for CREATE INDEX.
                if (commit_id > 0 && original_type == node_type::create_index_t &&
                    index_address_ != actor_zeta::address_t::empty_address()) {
                    auto* root_after_plan = effective_root_node(logic_plan.get());
                    components::catalog::oid_t indexed_tbl_oid = components::catalog::INVALID_OID;
                    if (root_after_plan && !root_after_plan->children().empty()) {
                        auto* back = root_after_plan->children().back().get();
                        if (back && back->type() == node_type::create_index_t) {
                            auto* ci = static_cast<const node_create_index_t*>(back);
                            indexed_tbl_oid = ci->table_oid();
                        }
                    }
                    if (indexed_tbl_oid != components::catalog::INVALID_OID) {
                        components::execution_context_t swap_ctx{session, txn_data, {}};
                        auto [_ci, cif] = actor_zeta::send(index_address_,
                                                           &index::manager_index_t::commit_insert,
                                                           swap_ctx,
                                                           indexed_tbl_oid,
                                                           commit_id);
                        co_await std::move(cif);
                    }
                }
                // CREATE MATERIALIZED VIEW — register routing for SELECT * FROM mv.
                // The matview heap + INSERT-SELECT populate is handled atomically by
                // operator_create_matview_t (composite physical operator); dispatcher
                // only needs to register the new collection in its routing map.
                if (t == node_type::create_matview_t) {
                    auto* mv_node = effective_root_node(logic_plan.get());
                    if (mv_node && mv_node->type() == node_type::create_matview_t) {
                        auto* cm = static_cast<const node_create_matview_t*>(mv_node);
                        auto names = drop_target_names_from_resolves(logic_plan.get());
                        collections_.insert(qualified_name_t{names.first, cm->matviewname()});
                    }
                }
                // Update routing map: for create_collection_t logic_plan is sequence_t whose
                // first child is the create_collection_t carrying the new collection name.
                // create_constraint_t and create_database_t have no collection to register.
                if (t == node_type::create_collection_t) {
                    auto* root_after_plan = effective_root_node(logic_plan.get());
                    if (root_after_plan && !root_after_plan->children().empty()) {
                        auto* cc_child =
                            static_cast<node_create_collection_t*>(root_after_plan->children().front().get());
                        auto names = drop_target_names_from_resolves(logic_plan.get());
                        collections_.insert(qualified_name_t{names.first, cc_child->relname()});
                    }
                }
                // Drop side: the cascade operator removed pg_class/pg_namespace rows on
                // disk, but the dispatcher's in-memory collections_ map is rebuilt from
                // pg_catalog only on the *next* execute_plan. Clean up here so any
                // immediate follow-up call (in the same handler chain) does not see a
                // stale entry. Names captured before the planner rewrite.
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
            trace(log_, "manager_dispatcher_t::execute_plan: non processed type - {}", to_string(t));
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

        // Go through the operator pipeline. The logical leaf
        // node_register_udf_t carries the function payload; create_plan lowers
        // it to operator_register_udf_t which fans out to per-executor
        // registries, mirrors into function_registry_t::get_default(), and
        // persists pg_proc rows.
        //
        // We invoke the operator directly here rather than routing through
        // execute_plan_impl: register_udf has a custom return type (bool, not
        // cursor) and needs the executor address list which only the
        // dispatcher has.

        // Wrap the unique_ptr function in a shared_ptr so the logical node can
        // copy without consuming. The operator deep-copies via get_copy() when
        // fanning out, leaving the shared_ptr's payload untouched for the
        // pg_proc encode step.
        std::shared_ptr<components::compute::function> shared_fn(function.release());
        auto plan = boost::intrusive_ptr(new components::logical_plan::node_register_udf_t(resource(), shared_fn));

        services::context_storage_t cstor{resource(), log_.clone(), session_tz(session)};
        // Build the executor fan-out callable. The dispatcher captures
        // executor_addresses_, scheduler_, and executors_ so the operator can
        // drive per-executor register_udf without needing direct scheduler
        // access. needs_sched is honoured here (matching the legacy inline
        // path) so the executor's mailbox is processed.
        using fanout_result_t = components::operators::operator_register_udf_t::executor_register_result_t;
        auto fanout = [this](components::session::session_id_t s,
                             components::compute::function_ptr fcopy,
                             std::size_t i) -> actor_zeta::unique_future<std::unique_ptr<fanout_result_t>> {
            auto [needs_sched, future] = actor_zeta::otterbrix::send(executor_addresses_[i],
                                                                     &collection::executor::executor_t::register_udf,
                                                                     s,
                                                                     std::move(fcopy));
            if (needs_sched && executors_[i]) {
                scheduler_->enqueue(executors_[i].get());
            }
            return std::move(future);
        };
        auto op = services::planner::impl::create_plan_register_udf(cstor,
                                                                    plan,
                                                                    executor_addresses_.size(),
                                                                    std::move(fanout));
        if (!op) {
            co_return false;
        }
        op->set_as_root();

        components::logical_plan::storage_parameters params(resource());
        components::compute::function_registry_t fn_registry{resource()};
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
            if (!waiting)
                break;
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

        // Operator-pipeline replacement. The logical leaf
        // node_unregister_udf_t carries the (name, inputs) signature; the
        // operator probes function_registry_t::get_default(), removes the
        // matching overload, and purges pg_proc + pg_depend rows.
        auto plan = boost::intrusive_ptr(
            new components::logical_plan::node_unregister_udf_t(resource(),
                                                                core::function_name_t{std::move(function_name)},
                                                                std::move(inputs)));

        services::context_storage_t cstor{resource(), log_.clone(), session_tz(session)};
        components::compute::function_registry_t fn_registry{resource()};
        auto op = services::planner::create_plan(cstor,
                                                 fn_registry,
                                                 plan,
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
            if (!waiting)
                break;
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

        // No disk → no pg_catalog → every id is unresolved (purely IN_MEMORY
        // deployments without a backing disk actor).
        if (disk_address_ == actor_zeta::address_t::empty_address()) {
            std::pmr::vector<complex_logical_type> schemas(resource());
            schemas.reserve(ids.size());
            for (std::size_t i = 0; i < ids.size(); ++i) {
                schemas.push_back(complex_logical_type{logical_type::INVALID});
            }
            co_return make_cursor(resource(), std::move(schemas));
        }

        // Go through the operator pipeline. The logical leaf
        // node_get_schema_t carries the requested ids; create_plan lowers
        // it to operator_get_schema_t which
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
        auto plan =
            boost::intrusive_ptr(new components::logical_plan::node_get_schema_t(resource(), std::move(id_pairs)));

        services::context_storage_t cstor{resource(), log_.clone(), session_tz(session)};
        components::compute::function_registry_t fn_registry{resource()};
        auto op = services::planner::create_plan(cstor,
                                                 fn_registry,
                                                 plan,
                                                 components::logical_plan::limit_t::unlimit(),
                                                 /*params=*/nullptr);
        if (!op) {
            // Should not happen — create_plan_get_schema is unconditional.
            co_return make_cursor(resource(), std::pmr::vector<complex_logical_type>(resource()));
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

        // Oid-only routing. Plan generators ask context_storage_t
        // whether a given resolved table_oid is known (i.e. we have an actor
        // for it). Walk the plan, collect every table_oid stamped by enrich,
        // and forward the set to the executor. Wrapper / parser-window / DDL
        // nodes contribute INVALID_OID and are filtered.
        auto dependency_oids = logical_plan->table_oid_dependencies();
        context_storage_t collections_context_storage(resource(), log_.clone(), session_tz(session));
        for (auto oid : dependency_oids) {
            collections_context_storage.known_oids.insert(oid);
        }
        // Forward resolve_table metadata (relkind + live columns)
        // so plan generators can build transfer_scan with the right
        // projection mask instead of inlining pg_class / pg_computed_column
        // scans. Gather a local index from the plan tree (execute_plan_impl
        // is callable from Pass 1 sub-plan execution where the caller's
        // dispatcher_idx isn't visible).
        {
            impl::plan_resolve_index_t local_idx;
            impl::gather_plan_resolve_index(logical_plan.get(), local_idx);
            for (const auto& [oid, md_ptr] : local_idx.tbl_md_by_oid) {
                collections_context_storage.table_metadata[oid] = md_ptr;
            }
        }

        // Populate index metadata for optimizer-driven index selection.
        // Keyed on table_oid (stamped by enrich_logical_plan).
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
        // Oid-only pool routing. For wrapper nodes (sequence_t etc.)
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

    manager_dispatcher_t::unique_future<std::vector<components::catalog::oid_t>>
    manager_dispatcher_t::allocate_oids_via_pipeline(components::session::session_id_t session, std::size_t count) {
        // Route OID allocation through the operator pipeline. Mirrors
        // the commit_transaction RPC pattern below.
        auto node = components::logical_plan::make_node_allocate_oids(resource(), count);

        services::context_storage_t cstor{resource(), log_.clone(), session_tz(session)};
        components::compute::function_registry_t fn_registry{resource()};
        auto op = services::planner::create_plan(cstor,
                                                 fn_registry,
                                                 node,
                                                 components::logical_plan::limit_t::unlimit(),
                                                 /*params=*/nullptr);
        if (!op) {
            co_return std::vector<components::catalog::oid_t>{};
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
            if (!waiting)
                break;
            co_await waiting->await_async_and_resume(&pctx);
            op->on_execute(&pctx);
        }
        if (pctx.has_pending_disk_futures()) {
            auto futures = pctx.take_pending_disk_futures();
            for (auto& f : futures) co_await std::move(f);
        }
        co_return node->oids();
    }

    manager_dispatcher_t::unique_future<uint64_t>
    manager_dispatcher_t::commit_transaction(components::session::session_id_t session) {
        trace(log_, "manager_dispatcher_t::commit_transaction, session: {}", session.data());

        // Go through the operator pipeline instead of inline
        // txn_manager + disk sends. The leaf node carries no fields; the
        // operator reads txn_manager / disk_address / session off the
        // pipeline::context_t we build here.
        auto plan = boost::intrusive_ptr(new components::logical_plan::node_commit_transaction_t(resource()));

        services::context_storage_t cstor{resource(), log_.clone(), session_tz(session)};
        components::compute::function_registry_t fn_registry{resource()};
        auto op = services::planner::create_plan(cstor,
                                                 fn_registry,
                                                 plan,
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

        auto* commit_op = static_cast<components::operators::operator_commit_transaction_t*>(op.get());
        co_return commit_op->commit_id();
    }

    manager_dispatcher_t::unique_future<void>
    manager_dispatcher_t::abort_transaction(components::session::session_id_t session) {
        trace(log_, "manager_dispatcher_t::abort_transaction, session: {}", session.data());

        // Operator-pipeline replacement (mirrors commit above).
        auto plan = boost::intrusive_ptr(new components::logical_plan::node_abort_transaction_t(resource()));

        services::context_storage_t cstor{resource(), log_.clone(), session_tz(session)};
        components::compute::function_registry_t fn_registry{resource()};
        auto op = services::planner::create_plan(cstor,
                                                 fn_registry,
                                                 plan,
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
