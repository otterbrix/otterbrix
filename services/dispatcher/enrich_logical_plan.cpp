// Logical plan enrichment.
//
// Runs after SQL parsing and before physical plan generation. Uses
// catalog_view_t (a per-execute_plan reader over pg_catalog) to annotate
// DML nodes with the data they need at execution time:
//   INSERT  — not_null_cols, outgoing FK references, CHECK expressions
//   UPDATE  — not_null_cols, outgoing FK references
//   DELETE  — referencing FKs (for CASCADE / SET NULL / SET DEFAULT)
//   CREATE  — namespace_oid (for catalog registration)
//
// No disk I/O of its own: all catalog reads go through catalog_view_t, which
// fetches pg_catalog rows from the disk actor on demand and memoizes them
// for the duration of a single execute_plan call.

#include "enrich_logical_plan.hpp"

#include "catalog_view.hpp"
#include "resolve_type.hpp"

#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_alter_table.hpp>
#include <components/logical_plan/node_catalog_resolve_namespace.hpp>
#include <components/logical_plan/node_catalog_resolve_table.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_constraint.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_create_macro.hpp>
#include <components/logical_plan/node_create_sequence.hpp>
#include <components/logical_plan/node_create_view.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_drop_collection.hpp>
#include <components/logical_plan/node_drop_database.hpp>
#include <components/logical_plan/node_drop_index.hpp>
#include <components/logical_plan/node_drop_macro.hpp>
#include <components/logical_plan/node_drop_sequence.hpp>
#include <components/logical_plan/node_drop_type.hpp>
#include <components/logical_plan/node_drop_view.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_having.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/logical_plan/node_update.hpp>

#include <limits>
#include <queue>
#include <string>
#include <unordered_map>

namespace services::dispatcher {

    namespace {

        void fill_not_null(const resolved_table_t& tbl,
                           std::vector<std::string>& out,
                           bool include_with_defaults) {
            for (const auto& col : tbl.columns) {
                if (col.attnotnull && (include_with_defaults || !col.atthasdefault)) {
                    out.push_back(col.attname);
                }
            }
        }

        void enrich_insert_sync(components::logical_plan::node_insert_t* node, catalog_view_t& view) {
            const auto* ns = view.try_get_namespace(node->dbname());
            if (!ns) return;
            const auto* tbl = view.try_get_table(ns->oid, node->relname());
            if (!tbl) return;
            std::vector<std::string> nn;
            fill_not_null(*tbl, nn, /*include_with_defaults=*/false);
            node->set_not_null_cols(std::move(nn));
        }

        void enrich_update_sync(components::logical_plan::node_update_t* node, catalog_view_t& view) {
            const auto* ns = view.try_get_namespace(node->dbname());
            if (!ns) return;
            const auto* tbl = view.try_get_table(ns->oid, node->relname());
            if (!tbl) return;
            std::vector<std::string> nn;
            fill_not_null(*tbl, nn, /*include_with_defaults=*/true);
            node->set_not_null_cols(std::move(nn));
        }

        void enrich_create_collection_sync(components::logical_plan::node_create_collection_t* node,
                                           catalog_view_t& view) {
            const auto* ns = view.try_get_namespace(node->dbname());
            if (!ns) return;
            node->set_namespace_oid(ns->oid);
        }

        // Phase 13 Step 3 — walk the plan tree, harvest namespace_oid /
        // table_oid stamped by operator_resolve_*_t (Pass 1) into a flat
        // hashmap. Empty when the plan has no resolve wrap (DDL paths,
        // disk-less harnesses). The caller (enrich_plan top-level) builds
        // this once and threads the const-ptr through recursive calls.
        void gather_enrich_resolve_idx(components::logical_plan::node_t* root,
                                        enrich_resolve_idx_t& out) {
            using namespace components::logical_plan;
            if (!root) return;
            std::queue<node_t*> q;
            q.push(root);
            while (!q.empty()) {
                auto* n = q.front();
                q.pop();
                switch (n->type()) {
                    case node_type::catalog_resolve_namespace_t: {
                        auto* rn = static_cast<node_catalog_resolve_namespace_t*>(n);
                        if (rn->namespace_oid() != components::catalog::INVALID_OID) {
                            out.ns_by_dbname[rn->dbname()] = rn->namespace_oid();
                        }
                        break;
                    }
                    case node_type::catalog_resolve_table_t: {
                        auto* rt = static_cast<node_catalog_resolve_table_t*>(n);
                        if (rt->namespace_oid() != components::catalog::INVALID_OID) {
                            out.ns_by_dbname[rt->dbname()] = rt->namespace_oid();
                        }
                        if (rt->table_oid() != components::catalog::INVALID_OID) {
                            std::string key;
                            key.reserve(rt->dbname().size() + 1 + rt->relname().size());
                            key.append(rt->dbname()).push_back('|');
                            key.append(rt->relname());
                            out.tbl_oid_by_qname[std::move(key)] = rt->table_oid();
                        }
                        break;
                    }
                    default:
                        break;
                }
                for (const auto& c : n->children()) {
                    if (c) q.push(c.get());
                }
            }
        }

        // Phase 13 Step 3 — name→OID lookup via plan-tree index (Pass 1
        // results). Returns INVALID_OID on miss; the caller decides whether
        // a miss is fatal (DML/SELECT routing relies on the OID being
        // present; DDL paths today fall back to async view.get_*).
        components::catalog::oid_t lookup_table_oid(const enrich_resolve_idx_t* idx,
                                                      std::string_view db,
                                                      std::string_view rel) {
            if (!idx) return components::catalog::INVALID_OID;
            std::string key;
            key.reserve(db.size() + 1 + rel.size());
            key.append(db).push_back('|');
            key.append(rel);
            auto it = idx->tbl_oid_by_qname.find(key);
            return it != idx->tbl_oid_by_qname.end() ? it->second : components::catalog::INVALID_OID;
        }

    } // anonymous namespace

    actor_zeta::unique_future<void>
    enrich_plan(components::logical_plan::node_ptr root,
                catalog_view_t& view,
                actor_zeta::address_t disk_address,
                components::execution_context_t ctx,
                std::pmr::memory_resource* resource,
                const enrich_resolve_idx_t* idx) {
        using namespace components::logical_plan;
        if (!root) co_return;
        // Phase 13 Step 3 — top-level entry: gather plan-tree resolve index
        // once, then re-enter with the gathered pointer. Recursive callers
        // already supply a non-null `idx` so this gather runs at most once
        // per public enrich_plan call.
        if (idx == nullptr) {
            enrich_resolve_idx_t local_idx;
            gather_enrich_resolve_idx(root.get(), local_idx);
            co_await enrich_plan(root, view, disk_address, ctx, resource, &local_idx);
            co_return;
        }
        // Stamp table_oid for any DML/SELECT consumer that carries (db, rel).
        // operator_resolve_*_t (Pass 1) populated the index; if the lookup
        // misses we leave table_oid INVALID_OID — caller (executor) will then
        // surface a "table not found" rather than silently routing to oid=0.
        {
            std::string_view db;
            std::string_view rel;
            switch (root->type()) {
                case node_type::insert_t: {
                    auto* d = static_cast<node_insert_t*>(root.get());
                    db = d->dbname(); rel = d->relname();
                    break;
                }
                case node_type::update_t: {
                    auto* d = static_cast<node_update_t*>(root.get());
                    db = d->dbname(); rel = d->relname();
                    break;
                }
                case node_type::delete_t: {
                    auto* d = static_cast<node_delete_t*>(root.get());
                    db = d->dbname(); rel = d->relname();
                    break;
                }
                case node_type::aggregate_t: {
                    auto* d = static_cast<node_aggregate_t*>(root.get());
                    db = d->dbname(); rel = d->relname();
                    break;
                }
                case node_type::match_t: {
                    auto* d = static_cast<node_match_t*>(root.get());
                    db = d->dbname(); rel = d->relname();
                    break;
                }
                case node_type::group_t: {
                    auto* d = static_cast<node_group_t*>(root.get());
                    db = d->dbname(); rel = d->relname();
                    break;
                }
                case node_type::sort_t: {
                    auto* d = static_cast<node_sort_t*>(root.get());
                    db = d->dbname(); rel = d->relname();
                    break;
                }
                case node_type::join_t: {
                    auto* d = static_cast<node_join_t*>(root.get());
                    db = d->dbname(); rel = d->relname();
                    break;
                }
                case node_type::limit_t: {
                    auto* d = static_cast<node_limit_t*>(root.get());
                    db = d->dbname(); rel = d->relname();
                    break;
                }
                case node_type::having_t: {
                    auto* d = static_cast<node_having_t*>(root.get());
                    db = d->dbname(); rel = d->relname();
                    break;
                }
                default:
                    break;
            }
            if (!db.empty() && !rel.empty()) {
                auto resolved_oid = lookup_table_oid(idx, db, rel);
                if (resolved_oid == components::catalog::INVALID_OID) {
                    // Phase 13 Step 3 (transitional) — Pass 1 not yet wired
                    // into execute_plan; plan-tree index is empty for both
                    // SQL T13-off paths and the logical_plan API. Fall back
                    // to the existing in-memory cache via sync try_get_*. No
                    // async actor send is added by this session — that's the
                    // "no fallback" guardrail. When tables haven't been
                    // pre-loaded into the cache, the stamp stays INVALID_OID
                    // and operator_insert / transfer_scan correctly surface
                    // the routing miss instead of silently no-op'ing.
                    const auto* ns = view.try_get_namespace(db);
                    if (ns) {
                        const auto* tbl = view.try_get_table(ns->oid, rel);
                        if (tbl) resolved_oid = tbl->oid;
                    }
                }
                if (resolved_oid != components::catalog::INVALID_OID) {
                    root->set_table_oid(resolved_oid);
                }
            }
            // DELETE FROM tableA USING tableB also needs the USING-side table_oid
            // stamped — operator_delete builds two full_scans (primary + USING)
            // and the USING scan otherwise gets INVALID_OID hardcoded in
            // create_plan_delete.cpp.
            if (root->type() == node_type::delete_t) {
                auto* del = static_cast<node_delete_t*>(root.get());
                const auto& db_from = del->dbname_from();
                const auto& rel_from = del->relname_from();
                if (!db_from.empty() && !rel_from.empty() &&
                    del->table_oid_from() == components::catalog::INVALID_OID) {
                    auto from_oid = lookup_table_oid(idx, db_from, rel_from);
                    if (from_oid == components::catalog::INVALID_OID) {
                        const auto* fns = view.try_get_namespace(db_from);
                        if (fns) {
                            const auto* ftbl = view.try_get_table(fns->oid, rel_from);
                            if (ftbl) from_oid = ftbl->oid;
                        }
                    }
                    if (from_oid != components::catalog::INVALID_OID) {
                        del->set_table_oid_from(from_oid);
                    }
                }
            }
        }
        switch (root->type()) {
        case node_type::insert_t: {
            auto* node = static_cast<node_insert_t*>(root.get());
            enrich_insert_sync(node, view);
            const auto tbl_oid = node->table_oid();
            if (tbl_oid != components::catalog::INVALID_OID) {
                auto fks = co_await view.get_fks_outgoing(ctx, tbl_oid);
                // Resolve child column names → positions in the INSERT chunk.
                const auto& kt = node->key_translation();
                for (auto& fk : fks) {
                    for (const auto& col_name : fk.child_col_names) {
                        std::size_t pos = std::numeric_limits<std::size_t>::max();
                        for (std::size_t i = 0; i < kt.size(); ++i) {
                            if (kt[i].as_string() == col_name) { pos = i; break; }
                        }
                        fk.child_col_indices.push_back(pos);
                    }
                }
                node->set_outgoing_fks(std::move(fks));
                auto checks = co_await view.get_check_exprs(ctx, tbl_oid);
                node->set_check_exprs(std::move(checks));
            }
            break;
        }
        case node_type::update_t: {
            auto* node = static_cast<node_update_t*>(root.get());
            enrich_update_sync(node, view);
            const auto tbl_oid = node->table_oid();
            if (tbl_oid != components::catalog::INVALID_OID) {
                auto fks = co_await view.get_fks_outgoing(ctx, tbl_oid);
                node->set_outgoing_fks(std::move(fks));
            }
            break;
        }
        case node_type::delete_t: {
            auto* node = static_cast<node_delete_t*>(root.get());
            // M3 (2026-05-13): name→OID side-channel for the target table
            // removed. Pass 1 stamped node->table_oid() via
            // operator_resolve_table_t back-pointer; validate_schema warmed
            // catalog_view's tbl_by_oid_ secondary index. Sync probe hits
            // and the prior view.get_namespace + view.get_table coroutine
            // pair is no longer load-bearing.
            const resolved_table_t* tbl =
                (node->table_oid() != components::catalog::INVALID_OID)
                    ? view.try_get_table_by_oid(node->table_oid())
                    : nullptr;
            if (tbl) {
                const auto tbl_oid = tbl->oid;
                auto fks = co_await view.get_fks_referencing(ctx, tbl_oid);
                // Resolve parent column names → positions in the deleted-row chunk.
                // The chunk has all parent table columns in attnum (schema) order.
                for (auto& fk : fks) {
                    for (const auto& col_name : fk.parent_col_names) {
                        std::size_t pos = std::numeric_limits<std::size_t>::max();
                        for (std::size_t i = 0; i < tbl->columns.size(); ++i) {
                            if (tbl->columns[i].attname == col_name) { pos = i; break; }
                        }
                        fk.parent_col_indices.push_back(pos);
                    }
                    // Resolve child table schema indices for SET NULL / SET DEFAULT.
                    // Child tables aren't covered by validate's prewalk (they're
                    // discovered at enrich time via pg_constraint scan), so we
                    // warm the cache here with the async path. Required for
                    // CASCADE / SET NULL / SET DEFAULT to know which child
                    // columns to update.
                    const std::string& child_ns_name =
                        fk.child_database.empty() ? fk.child_schema : fk.child_database;
                    const auto* child_ns = view.try_get_namespace(child_ns_name);
                    if (!child_ns)
                        child_ns = co_await view.get_namespace(ctx, child_ns_name);
                    if (child_ns) {
                        const auto* child_tbl =
                            view.try_get_table(child_ns->oid, fk.child_collection_name);
                        if (!child_tbl)
                            child_tbl = co_await view.get_table(
                                ctx, child_ns->oid, std::string(fk.child_collection_name));
                        if (child_tbl) {
                            for (const auto& col_name : fk.child_col_names) {
                                std::size_t pos = std::numeric_limits<std::size_t>::max();
                                std::string def_spec;
                                for (std::size_t i = 0; i < child_tbl->columns.size(); ++i) {
                                    if (child_tbl->columns[i].attname == col_name) {
                                        pos      = i;
                                        def_spec = child_tbl->columns[i].attdefspec;
                                        break;
                                    }
                                }
                                fk.child_col_schema_indices.push_back(pos);
                                fk.child_col_default_specs.push_back(std::move(def_spec));
                            }
                        }
                    }
                }
                node->set_referencing_fks(std::move(fks));
            }
            break;
        }
        case node_type::create_collection_t: {
            auto* node = static_cast<node_create_collection_t*>(root.get());
            enrich_create_collection_sync(node, view);
            co_await resolve_column_definitions(node->column_definitions(), view, ctx);
            break;
        }
        case node_type::create_sequence_t: {
            auto* node = static_cast<node_create_sequence_t*>(root.get());
            const std::string& ns_name = node->dbname();
            const auto* ns = view.try_get_namespace(ns_name);
            if (ns) node->set_namespace_oid(ns->oid);
            break;
        }
        case node_type::create_view_t: {
            auto* node = static_cast<node_create_view_t*>(root.get());
            const std::string& ns_name = node->dbname();
            const auto* ns = view.try_get_namespace(ns_name);
            if (ns) node->set_namespace_oid(ns->oid);
            break;
        }
        case node_type::create_macro_t: {
            auto* node = static_cast<node_create_macro_t*>(root.get());
            const std::string& ns_name = node->dbname();
            const auto* ns = view.try_get_namespace(ns_name);
            if (ns) node->set_namespace_oid(ns->oid);
            break;
        }
        case node_type::create_index_t: {
            // Resolve namespace + table OIDs and column attoids so the planner can
            // synchronously call build_create_index_writes. The index_oid is allocated
            // by the dispatcher and stamped on the node before the planner runs.
            auto* node = static_cast<node_create_index_t*>(root.get());
            const std::string& ns_name = node->dbname();
            const auto* ns = view.try_get_namespace(ns_name);
            if (!ns) break;
            node->set_namespace_oid(ns->oid);
            const auto* tbl = view.try_get_table(ns->oid, node->relname());
            if (!tbl) break;
            node->set_table_oid(tbl->oid);

            // Resolve key column names → attoids (preserving column order).
            // Phase 9.G: node_create_index_t no longer stores column_names; the planner
            // reads names from keys() directly. We only stamp attoids + indkey here.
            std::vector<components::catalog::oid_t> col_attoids;
            std::string indkey;
            col_attoids.reserve(node->keys().size());
            for (std::size_t i = 0; i < node->keys().size(); ++i) {
                const std::string cn = node->keys()[i].as_string();
                components::catalog::oid_t attoid = components::catalog::INVALID_OID;
                for (const auto& ci : tbl->columns) {
                    if (ci.attname == cn) { attoid = ci.attoid; break; }
                }
                col_attoids.push_back(attoid);
                if (i) indkey += ",";
                indkey += std::to_string(attoid);
            }
            node->set_column_attoids(std::move(col_attoids));
            node->set_indkey(std::move(indkey));
            break;
        }
        case node_type::drop_index_t: {
            // Phase 11.D: resolve the TARGET table directly via node->relname().
            // Resolve index_oid separately by node->indexname() — indexes live
            // in pg_class with relkind='i' as their own entries.
            // Validate's gather_cfn_deps for drop_index_t collects (db,
            // indexname) only, so the parent table cache stays cold. Use
            // async fallback here to warm both sides.
            auto* node = static_cast<node_drop_index_t*>(root.get());
            const std::string& ns_name = node->dbname();
            const auto* ns = view.try_get_namespace(ns_name);
            if (!ns) ns = co_await view.get_namespace(ctx, ns_name);
            if (!ns) break;
            node->set_namespace_oid(ns->oid);
            const auto* tbl = view.try_get_table(ns->oid, node->relname());
            if (!tbl) tbl = co_await view.get_table(ctx, ns->oid, node->relname());
            if (tbl) node->set_table_oid(tbl->oid);
            const auto* idx = view.try_get_table(ns->oid, node->indexname());
            if (!idx) idx = co_await view.get_table(ctx, ns->oid, node->indexname());
            if (idx) node->set_index_oid(idx->oid);
            break;
        }
        case node_type::create_constraint_t: {
            auto* node = static_cast<node_create_constraint_t*>(root.get());
            const std::string& ns_name = node->dbname();
            const auto* ns = view.try_get_namespace(ns_name);
            if (!ns) break;
            const auto* tbl = view.try_get_table(ns->oid, node->relname());
            if (!tbl) break;
            node->set_table_oid(tbl->oid);

            // Resolve local (child) column names → attoids.
            std::vector<components::catalog::oid_t> fk_attoids;
            for (const auto& col_name : node->local_col_names()) {
                for (const auto& ci : tbl->columns) {
                    if (ci.attname == col_name) {
                        fk_attoids.push_back(ci.attoid);
                        break;
                    }
                }
            }
            node->set_fk_col_attoids(std::move(fk_attoids));

            // FK only — resolve referenced table + parent column attoids.
            if (node->kind() == constraint_kind::foreign_key && !node->ref_relname().empty()) {
                // ref_dbname() may be empty → fall back to the same namespace as the
                // constrained table (mirrors prior cfn-based behavior).
                const std::string& ref_ns_name =
                    node->ref_dbname().empty() ? ns_name : node->ref_dbname();
                const auto* ref_ns = view.try_get_namespace(ref_ns_name);
                if (ref_ns) {
                    const auto* rrt = view.try_get_table(ref_ns->oid, node->ref_relname());
                    if (rrt) {
                        node->set_ref_table_oid(rrt->oid);
                        std::vector<components::catalog::oid_t> ref_attoids;
                        for (const auto& col_name : node->ref_col_names()) {
                            for (const auto& ci : rrt->columns) {
                                if (ci.attname == col_name) {
                                    ref_attoids.push_back(ci.attoid);
                                    break;
                                }
                            }
                        }
                        node->set_ref_col_attoids(std::move(ref_attoids));
                    }
                }
            }
            break;
        }
        case node_type::alter_table_t: {
            // Stamp table_oid so the planner's rewrite_alter_table can build per-clause
            // primitives (alter_column_add_t / alter_column_rename_t) without re-resolving.
            // Also stamp relkind so the planner can route DROP COLUMN on relkind='g'
            // (dynamic-schema) tables through computed_field_unregister instead of the
            // static-schema pg_attribute tombstone path. P7.3.
            auto* node = static_cast<node_alter_table_t*>(root.get());
            const std::string& ns_name = node->dbname();
            const auto* ns = view.try_get_namespace(ns_name);
            if (!ns) break;
            const auto* tbl = view.try_get_table(ns->oid, node->relname());
            if (tbl) {
                node->set_table_oid(tbl->oid);
                node->set_relkind(tbl->relkind);
            }
            break;
        }
        case node_type::drop_database_t: {
            // Resolve the namespace OID so the planner can seed a
            // node_dynamic_cascade_delete_t at (pg_namespace, ns_oid). Phase 2 #49.
            auto* node = static_cast<node_drop_database_t*>(root.get());
            const std::string ns_name = node->dbname();
            if (ns_name.empty()) break;
            const auto* ns = view.try_get_namespace(ns_name);
            if (ns) node->set_namespace_oid(ns->oid);
            break;
        }
        case node_type::drop_collection_t: {
            // Resolve namespace + table OIDs so the planner can seed a
            // node_dynamic_cascade_delete_t at (pg_class, table_oid). Phase 2 #49.
            // Computing tables (relkind='g') resolve identically — the cascade
            // operator's drop_storage call handles both 'r' and 'g'.
            auto* node = static_cast<node_drop_collection_t*>(root.get());
            const std::string& ns_name = node->dbname();
            const auto* ns = view.try_get_namespace(ns_name);
            if (!ns) break;
            node->set_namespace_oid(ns->oid);
            const auto* tbl = view.try_get_table(ns->oid, node->relname());
            if (tbl) node->set_table_oid(tbl->oid);
            break;
        }
        case node_type::drop_type_t: {
            // Resolve the type OID. Phase 9.W: node_drop_type_t no longer carries a
            // user-typed db/namespace, so we resolve against public_namespace only.
            // The dispatcher pre-loads the type cache for the standard search path
            // (see dispatcher.cpp drop_type_t handler), so this lookup will hit when
            // the type lives in any reachable namespace.
            // Note: only ENUM-style types live in pg_type; composite types are stored
            // in pg_class with relkind='c' and aren't reached by this seed. Mirrors
            // the legacy ddl.cpp drop_type behavior (which used resolve_type +
            // pg_type/pg_depend deletes — composite types were never dropped).
            auto* node = static_cast<node_drop_type_t*>(root.get());
            const components::catalog::oid_t target_ns =
                components::catalog::well_known_oid::public_namespace;
            const auto* rt = view.try_get_type(target_ns, node->name());
            if (rt) node->set_type_oid(rt->oid);
            break;
        }
        case node_type::drop_sequence_t: {
            // Sequences are pg_class entries with relkind='S'. catalog_view's
            // try_get_table also covers them (the cache is keyed on (ns, name)
            // regardless of relkind).
            auto* node = static_cast<node_drop_sequence_t*>(root.get());
            const std::string& ns_name = node->dbname();
            const auto* ns = view.try_get_namespace(ns_name);
            if (!ns) break;
            const auto* tbl = view.try_get_table(ns->oid, node->seqname());
            if (tbl) node->set_relation_oid(tbl->oid);
            break;
        }
        case node_type::drop_view_t: {
            // Views are pg_class entries with relkind='v'.
            auto* node = static_cast<node_drop_view_t*>(root.get());
            const std::string& ns_name = node->dbname();
            const auto* ns = view.try_get_namespace(ns_name);
            if (!ns) break;
            const auto* tbl = view.try_get_table(ns->oid, node->viewname());
            if (tbl) node->set_relation_oid(tbl->oid);
            break;
        }
        case node_type::drop_macro_t: {
            // Macros are pg_class entries with relkind='m' (macro).
            auto* node = static_cast<node_drop_macro_t*>(root.get());
            const std::string& ns_name = node->dbname();
            const auto* ns = view.try_get_namespace(ns_name);
            if (!ns) break;
            const auto* tbl = view.try_get_table(ns->oid, node->macroname());
            if (tbl) node->set_relation_oid(tbl->oid);
            break;
        }
        default:
            break;
        }
        // Phase 13 Step 3 — recurse into ALL children after the per-type
        // enrichment, regardless of which case ran. DML cases (insert/update/
        // delete) own a match_t / data_t child that itself carries (db, rel)
        // and needs its own table_oid stamp for create_plan_match / scan
        // operators to route to the right storage. The previous pattern (each
        // case's `break;` exits the function without descending) left those
        // sub-nodes at INVALID_OID — DELETE WHERE was then a no-op.
        for (auto& child : root->children()) {
            if (!child) continue;
            co_await enrich_plan(child, view, disk_address, ctx, resource, idx);
        }
    }

} // namespace services::dispatcher