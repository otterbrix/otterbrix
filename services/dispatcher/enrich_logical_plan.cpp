// Phase 1.5-A: logical plan enrichment.
//
// Runs after SQL parsing (Phase 1) and before physical plan generation (Phase 2).
// Reads the catalog_view (cached pg_catalog snapshot) and annotates DML nodes with
// the data they need at execution time:
//   INSERT  — not_null_cols, outgoing FK references, CHECK expressions
//   UPDATE  — not_null_cols, outgoing FK references
//   DELETE  — referencing FKs (for CASCADE / SET NULL / SET DEFAULT)
//   CREATE  — namespace_oid (for catalog registration)
//
// No disk I/O of its own: all catalog reads go through catalog_view_t which
// transparently fetches and caches pg_catalog rows from the disk actor.

#include "enrich_logical_plan.hpp"

#include "catalog_view.hpp"
#include "resolve_type.hpp"

#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_update.hpp>

#include <limits>

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
            const auto& coll = node->collection_full_name();
            const auto* ns = view.try_get_namespace(coll.database.empty() ? coll.schema : coll.database);
            if (!ns) return;
            const auto* tbl = view.try_get_table(ns->oid, coll.collection);
            if (!tbl) return;
            std::vector<std::string> nn;
            fill_not_null(*tbl, nn, /*include_with_defaults=*/false);
            node->set_not_null_cols(std::move(nn));
        }

        void enrich_update_sync(components::logical_plan::node_update_t* node, catalog_view_t& view) {
            const auto& coll = node->collection_full_name();
            const auto* ns = view.try_get_namespace(coll.database.empty() ? coll.schema : coll.database);
            if (!ns) return;
            const auto* tbl = view.try_get_table(ns->oid, coll.collection);
            if (!tbl) return;
            std::vector<std::string> nn;
            fill_not_null(*tbl, nn, /*include_with_defaults=*/true);
            node->set_not_null_cols(std::move(nn));
        }

        void enrich_create_collection_sync(components::logical_plan::node_create_collection_t* node,
                                           catalog_view_t& view) {
            const auto& coll = node->collection_full_name();
            const auto* ns = view.try_get_namespace(coll.database.empty() ? coll.schema : coll.database);
            if (!ns) return;
            node->set_namespace_oid(ns->oid);
        }

        // Returns the table OID for a DML node by looking up namespace + table in the cache.
        components::catalog::oid_t resolve_table_oid(const collection_full_name_t& coll,
                                                       catalog_view_t& view) {
            const auto* ns = view.try_get_namespace(coll.database.empty() ? coll.schema : coll.database);
            if (!ns) return components::catalog::INVALID_OID;
            const auto* tbl = view.try_get_table(ns->oid, coll.collection);
            if (!tbl) return components::catalog::INVALID_OID;
            return tbl->oid;
        }

    } // anonymous namespace

    actor_zeta::unique_future<void>
    enrich_plan(components::logical_plan::node_ptr root,
                catalog_view_t& view,
                actor_zeta::address_t /*disk_address*/,
                components::execution_context_t ctx,
                std::pmr::memory_resource* /*resource*/) {
        using namespace components::logical_plan;
        if (!root) co_return;
        switch (root->type()) {
        case node_type::insert_t: {
            auto* node = static_cast<node_insert_t*>(root.get());
            enrich_insert_sync(node, view);
            const auto tbl_oid = resolve_table_oid(node->collection_full_name(), view);
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
            const auto tbl_oid = resolve_table_oid(node->collection_full_name(), view);
            if (tbl_oid != components::catalog::INVALID_OID) {
                auto fks = co_await view.get_fks_outgoing(ctx, tbl_oid);
                node->set_outgoing_fks(std::move(fks));
            }
            break;
        }
        case node_type::delete_t: {
            auto* node = static_cast<node_delete_t*>(root.get());
            const auto& coll = node->collection_full_name();
            // Use async fallback so FK enrichment works even on a cold cache.
            const std::string& del_ns_name = coll.database.empty() ? coll.schema : coll.database;
            const auto* ns = view.try_get_namespace(del_ns_name);
            if (!ns) ns = co_await view.get_namespace(ctx, del_ns_name);
            const resolved_table_t* tbl = nullptr;
            if (ns) {
                tbl = view.try_get_table(ns->oid, coll.collection);
                if (!tbl) tbl = co_await view.get_table(ctx, ns->oid, std::string(coll.collection));
            }
            if (ns && tbl) {
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
                    const std::string& child_ns_name = fk.child_database.empty() ? fk.child_schema : fk.child_database;
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
        default:
            break;
        }
    }

} // namespace services::dispatcher