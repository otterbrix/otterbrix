// Logical plan enrichment.
//
// Runs after SQL parsing and before physical plan generation. Reads the
// plan-tree resolve idx (populated by Pass 1's operator_resolve_*_t) to
// annotate DML nodes with the data they need at execution time:
//   INSERT  — not_null_cols, outgoing FK references, CHECK expressions
//   UPDATE  — not_null_cols, outgoing FK references
//   DELETE  — referencing FKs (for CASCADE / SET NULL / SET DEFAULT)
//   CREATE  — namespace_oid (for catalog registration)
//
// No disk I/O of its own — all catalog data comes from the plan-tree idx, so
// all catalog metadata comes from the resolve idx materialized in-plan by
// Pass 1.

#include "enrich_logical_plan.hpp"

#include "plan_resolve_index.hpp"
#include "resolve_type.hpp"

#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_alter_table.hpp>
#include <components/logical_plan/node_catalog_resolve_constraint.hpp>
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

        // M4.D helpers: probe enrich_resolve_idx_t (plan-tree resolves
        // stamped by operator_resolve_*_t Pass 1). All catalog reads in
        // enrich flow through these.
        components::catalog::oid_t lookup_ns_oid_local(const enrich_resolve_idx_t* idx,
                                                         std::string_view db) {
            if (!idx) return components::catalog::INVALID_OID;
            auto it = idx->ns_by_dbname.find(std::string(db));
            return it != idx->ns_by_dbname.end() ? it->second : components::catalog::INVALID_OID;
        }

        const components::logical_plan::resolved_table_metadata_t*
        lookup_table_md_local(const enrich_resolve_idx_t* idx,
                               std::string_view db,
                               std::string_view rel) {
            if (!idx) return nullptr;
            std::string key;
            key.reserve(db.size() + 1 + rel.size());
            key.append(db).push_back('|');
            key.append(rel);
            auto it = idx->tbl_md_by_qname.find(key);
            return it != idx->tbl_md_by_qname.end() ? it->second : nullptr;
        }

        const components::logical_plan::resolved_table_metadata_t*
        lookup_table_md_by_oid_local(const enrich_resolve_idx_t* idx,
                                      components::catalog::oid_t oid) {
            if (!idx) return nullptr;
            auto it = idx->tbl_md_by_oid.find(oid);
            return it != idx->tbl_md_by_oid.end() ? it->second : nullptr;
        }

        const components::logical_plan::resolved_type_metadata_t*
        lookup_type_md_local(const enrich_resolve_idx_t* idx,
                              std::string_view dbname,
                              std::string_view type_name) {
            if (!idx) return nullptr;
            std::string key;
            key.reserve(dbname.size() + 1 + type_name.size());
            key.append(dbname.data(), dbname.size()).push_back('|');
            key.append(type_name.data(), type_name.size());
            auto it = idx->type_md_by_qname.find(key);
            return it != idx->type_md_by_qname.end() ? it->second : nullptr;
        }

        void fill_not_null(const components::logical_plan::resolved_table_metadata_t& md,
                           std::vector<std::string>& out,
                           bool include_with_defaults) {
            for (const auto& col : md.columns) {
                if (col.attnotnull && (include_with_defaults || !col.atthasdefault)) {
                    out.push_back(col.attname);
                }
            }
        }

        void enrich_insert_sync(components::logical_plan::node_insert_t* node,
                                const enrich_resolve_idx_t* idx) {
            const auto* md = lookup_table_md_local(idx, node->dbname(), node->relname());
            if (!md) return;
            std::vector<std::string> nn;
            fill_not_null(*md, nn, /*include_with_defaults=*/false);
            node->set_not_null_cols(std::move(nn));
        }

        void enrich_update_sync(components::logical_plan::node_update_t* node,
                                const enrich_resolve_idx_t* idx) {
            const auto* md = lookup_table_md_local(idx, node->dbname(), node->relname());
            if (!md) return;
            std::vector<std::string> nn;
            fill_not_null(*md, nn, /*include_with_defaults=*/true);
            node->set_not_null_cols(std::move(nn));
        }

        void enrich_create_collection_sync(components::logical_plan::node_create_collection_t* node,
                                           const enrich_resolve_idx_t* idx) {
            const auto ns_oid = lookup_ns_oid_local(idx, node->dbname());
            if (ns_oid == components::catalog::INVALID_OID) return;
            node->set_namespace_oid(ns_oid);
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
                            out.tbl_oid_by_qname[key] = rt->table_oid();
                            // M4.C: stamp the full metadata pointer too when
                            // operator_resolve_table_t populated it.
                            if (rt->resolved_metadata().has_value()) {
                                const auto* md_ptr = &rt->resolved_metadata().value();
                                out.tbl_md_by_qname[std::move(key)] = md_ptr;
                                out.tbl_md_by_oid[rt->table_oid()]  = md_ptr;
                            }
                        }
                        break;
                    }
                    case node_type::catalog_resolve_constraint_t: {
                        auto* cr = static_cast<node_catalog_resolve_constraint_t*>(n);
                        if (!cr->target()) break;
                        const auto& md = cr->target()->resolved_metadata();
                        if (!md.has_value() ||
                            md->table_oid == components::catalog::INVALID_OID) {
                            break;
                        }
                        using direction_t = node_catalog_resolve_constraint_t::direction_t;
                        if (cr->direction() == direction_t::outgoing) {
                            out.outgoing_fks_by_oid[md->table_oid] = cr->fks();
                            out.check_exprs_by_oid[md->table_oid]  = cr->check_exprs();
                        } else {
                            out.referencing_fks_by_oid[md->table_oid] = cr->fks();
                        }
                        break;
                    }
                    case node_type::catalog_resolve_type_t: {
                        auto* tr = static_cast<node_catalog_resolve_type_t*>(n);
                        if (!tr->resolved_metadata().has_value()) break;
                        std::string key;
                        key.reserve(tr->dbname().size() + 1 + tr->type_name().size());
                        key.append(tr->dbname()).push_back('|');
                        key.append(tr->type_name());
                        out.type_md_by_qname[std::move(key)] =
                            &tr->resolved_metadata().value();
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

        // Name→OID lookup via plan-tree index (Pass 1 results). Returns
        // INVALID_OID on miss; the caller decides whether a miss is fatal.
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
            co_await enrich_plan(root, disk_address, ctx, resource, &local_idx);
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
                    if (from_oid != components::catalog::INVALID_OID) {
                        del->set_table_oid_from(from_oid);
                    }
                }
            }
        }
        switch (root->type()) {
        case node_type::insert_t: {
            auto* node = static_cast<node_insert_t*>(root.get());
            enrich_insert_sync(node, idx);
            const auto tbl_oid = node->table_oid();
            // M4.D: FK + CHECK populated by operator_resolve_constraint_t
            // (Pass 1, direction=outgoing) and gathered into idx. No catalog
            // probe here — pure plan-tree read.
            if (tbl_oid != components::catalog::INVALID_OID && idx) {
                if (auto it = idx->outgoing_fks_by_oid.find(tbl_oid);
                    it != idx->outgoing_fks_by_oid.end()) {
                    auto fks = it->second;
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
                }
                if (auto it = idx->check_exprs_by_oid.find(tbl_oid);
                    it != idx->check_exprs_by_oid.end()) {
                    node->set_check_exprs(it->second);
                }
            }
            break;
        }
        case node_type::update_t: {
            auto* node = static_cast<node_update_t*>(root.get());
            enrich_update_sync(node, idx);
            const auto tbl_oid = node->table_oid();
            if (tbl_oid != components::catalog::INVALID_OID && idx) {
                if (auto it = idx->outgoing_fks_by_oid.find(tbl_oid);
                    it != idx->outgoing_fks_by_oid.end()) {
                    node->set_outgoing_fks(it->second);
                }
            }
            break;
        }
        case node_type::delete_t: {
            auto* node = static_cast<node_delete_t*>(root.get());
            // M4.D: parent table metadata + referencing FK rows are both stamped
            // by Pass 1 (operator_resolve_table_t + operator_resolve_constraint_t,
            // direction=referencing). Descendant child column positions and
            // defspecs are pre-populated by the resolve_constraint operator
            // itself — see operator_resolve_constraint.cpp.
            const auto* tbl =
                (node->table_oid() != components::catalog::INVALID_OID)
                    ? lookup_table_md_by_oid_local(idx, node->table_oid())
                    : nullptr;
            if (tbl && idx) {
                const auto tbl_oid = tbl->table_oid;
                if (auto it = idx->referencing_fks_by_oid.find(tbl_oid);
                    it != idx->referencing_fks_by_oid.end()) {
                    auto fks = it->second;
                    // Resolve parent column positions in the parent table's
                    // attnum-ordered columns (used by operator_fk_cascade
                    // SET NULL / SET DEFAULT to locate FK cols in a fetched
                    // parent row).
                    for (auto& fk : fks) {
                        for (const auto& col_name : fk.parent_col_names) {
                            std::size_t pos = std::numeric_limits<std::size_t>::max();
                            for (std::size_t i = 0; i < tbl->columns.size(); ++i) {
                                if (tbl->columns[i].attname == col_name) { pos = i; break; }
                            }
                            fk.parent_col_indices.push_back(pos);
                        }
                    }
                    node->set_referencing_fks(std::move(fks));
                }
            }
            break;
        }
        case node_type::create_collection_t: {
            auto* node = static_cast<node_create_collection_t*>(root.get());
            enrich_create_collection_sync(node, idx);
            // M4.H: resolve_column_definitions now takes an explicit plan-tree
            // idx. enrich's `enrich_resolve_idx_t` is a different shape; build
            // a small plan_resolve_index_t locally from the same root tree so
            // UDT columns get resolved without thread_local state.
            impl::plan_resolve_index_t local_plan_idx;
            impl::gather_plan_resolve_index(root.get(), local_plan_idx);
            resolve_column_definitions(node->column_definitions(), &local_plan_idx);
            break;
        }
        case node_type::create_sequence_t: {
            auto* node = static_cast<node_create_sequence_t*>(root.get());
            const auto ns_oid = lookup_ns_oid_local(idx, node->dbname());
            if (ns_oid != components::catalog::INVALID_OID) {
                node->set_namespace_oid(ns_oid);
            }
            break;
        }
        case node_type::create_view_t: {
            auto* node = static_cast<node_create_view_t*>(root.get());
            const auto ns_oid = lookup_ns_oid_local(idx, node->dbname());
            if (ns_oid != components::catalog::INVALID_OID) {
                node->set_namespace_oid(ns_oid);
            }
            break;
        }
        case node_type::create_macro_t: {
            auto* node = static_cast<node_create_macro_t*>(root.get());
            const auto ns_oid = lookup_ns_oid_local(idx, node->dbname());
            if (ns_oid != components::catalog::INVALID_OID) {
                node->set_namespace_oid(ns_oid);
            }
            break;
        }
        case node_type::create_index_t: {
            // Resolve namespace + table OIDs and column attoids so the planner can
            // synchronously call build_create_index_writes. The index_oid is allocated
            // by the dispatcher and stamped on the node before the planner runs.
            // M4.E: reads from plan-tree idx (transformer wraps create_index with
            // catalog_resolve_table for the target table).
            auto* node = static_cast<node_create_index_t*>(root.get());
            const auto ns_oid = lookup_ns_oid_local(idx, node->dbname());
            if (ns_oid == components::catalog::INVALID_OID) break;
            node->set_namespace_oid(ns_oid);
            const auto* tbl = lookup_table_md_local(idx, node->dbname(), node->relname());
            if (!tbl) break;
            node->set_table_oid(tbl->table_oid);

            // Resolve key column names → attoids (preserving column order).
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
            // M4.E: transformer wraps drop_index with two catalog_resolve_table
            // children — one for the parent table relname, one for the index
            // relname (indexes live in pg_class with relkind='i'). Both end up
            // keyed on the same (dbname|name) tuple in tbl_md_by_qname.
            auto* node = static_cast<node_drop_index_t*>(root.get());
            const auto ns_oid = lookup_ns_oid_local(idx, node->dbname());
            if (ns_oid == components::catalog::INVALID_OID) break;
            node->set_namespace_oid(ns_oid);
            const auto* tbl = lookup_table_md_local(idx, node->dbname(), node->relname());
            if (tbl) node->set_table_oid(tbl->table_oid);
            const auto* idx_md = lookup_table_md_local(idx, node->dbname(), node->indexname());
            if (idx_md) node->set_index_oid(idx_md->table_oid);
            break;
        }
        case node_type::create_constraint_t: {
            // M4.E: idx provides ns/table metadata for the target. The FK
            // reference table (when constraint is FK) needs a separate
            // resolve_table emitted by the transformer.
            auto* node = static_cast<node_create_constraint_t*>(root.get());
            const std::string& ns_name = node->dbname();
            const auto* tbl = lookup_table_md_local(idx, ns_name, node->relname());
            if (!tbl) break;
            node->set_table_oid(tbl->table_oid);

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
                const std::string& ref_ns_name =
                    node->ref_dbname().empty() ? ns_name : node->ref_dbname();
                const auto* rrt = lookup_table_md_local(idx, ref_ns_name, node->ref_relname());
                if (rrt) {
                    node->set_ref_table_oid(rrt->table_oid);
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
            break;
        }
        case node_type::alter_table_t: {
            auto* node = static_cast<node_alter_table_t*>(root.get());
            const auto* tbl = lookup_table_md_local(idx, node->dbname(), node->relname());
            if (tbl) {
                node->set_table_oid(tbl->table_oid);
                node->set_relkind(tbl->relkind);
            }
            break;
        }
        case node_type::drop_database_t: {
            auto* node = static_cast<node_drop_database_t*>(root.get());
            const auto ns_oid = lookup_ns_oid_local(idx, node->dbname());
            if (ns_oid != components::catalog::INVALID_OID) {
                node->set_namespace_oid(ns_oid);
            }
            break;
        }
        case node_type::drop_collection_t: {
            auto* node = static_cast<node_drop_collection_t*>(root.get());
            const auto ns_oid = lookup_ns_oid_local(idx, node->dbname());
            if (ns_oid == components::catalog::INVALID_OID) break;
            node->set_namespace_oid(ns_oid);
            const auto* tbl = lookup_table_md_local(idx, node->dbname(), node->relname());
            if (tbl) node->set_table_oid(tbl->table_oid);
            break;
        }
        case node_type::drop_type_t: {
            // M4.F: read type_oid from plan-tree idx (resolve_type emitted by
            // transform_table OBJECT_TYPE path with dbname="public", matching
            // the legacy public_namespace default).
            auto* node = static_cast<node_drop_type_t*>(root.get());
            const auto* rt = lookup_type_md_local(idx, "public", node->name());
            if (rt) node->set_type_oid(rt->type_oid);
            break;
        }
        case node_type::drop_sequence_t: {
            // Sequences are pg_class entries with relkind='S'; resolve_table
            // operator's pg_class scan covers them regardless of relkind.
            auto* node = static_cast<node_drop_sequence_t*>(root.get());
            const auto* tbl = lookup_table_md_local(idx, node->dbname(), node->seqname());
            if (tbl) node->set_relation_oid(tbl->table_oid);
            break;
        }
        case node_type::drop_view_t: {
            auto* node = static_cast<node_drop_view_t*>(root.get());
            const auto* tbl = lookup_table_md_local(idx, node->dbname(), node->viewname());
            if (tbl) node->set_relation_oid(tbl->table_oid);
            break;
        }
        case node_type::drop_macro_t: {
            auto* node = static_cast<node_drop_macro_t*>(root.get());
            const auto* tbl = lookup_table_md_local(idx, node->dbname(), node->macroname());
            if (tbl) node->set_relation_oid(tbl->table_oid);
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
            co_await enrich_plan(child, disk_address, ctx, resource, idx);
        }
    }

} // namespace services::dispatcher