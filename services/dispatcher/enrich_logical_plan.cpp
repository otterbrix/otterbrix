#include "enrich_logical_plan.hpp"

#include "catalog_view.hpp"

#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_update.hpp>

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
            const auto* ns = view.try_get_namespace(coll.database);
            if (!ns) return;
            const auto* tbl = view.try_get_table(ns->oid, coll.collection);
            if (!tbl) return;
            std::vector<std::string> nn;
            fill_not_null(*tbl, nn, /*include_with_defaults=*/false);
            node->set_not_null_cols(std::move(nn));
        }

        void enrich_update_sync(components::logical_plan::node_update_t* node, catalog_view_t& view) {
            const auto& coll = node->collection_full_name();
            const auto* ns = view.try_get_namespace(coll.database);
            if (!ns) return;
            const auto* tbl = view.try_get_table(ns->oid, coll.collection);
            if (!tbl) return;
            std::vector<std::string> nn;
            fill_not_null(*tbl, nn, /*include_with_defaults=*/true);
            node->set_not_null_cols(std::move(nn));
        }

        void enrich_create_collection(components::logical_plan::node_create_collection_t* node,
                                      catalog_view_t& view) {
            const auto& coll = node->collection_full_name();
            const auto* ns = view.try_get_namespace(coll.database);
            if (!ns) return;
            node->set_namespace_oid(ns->oid);
        }

        // Returns the table OID for a DML node by looking up namespace + table in the cache.
        components::catalog::oid_t resolve_table_oid(const components::base::collection_full_name_t& coll,
                                                       catalog_view_t& view) {
            const auto* ns = view.try_get_namespace(coll.database);
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
                node->set_outgoing_fks(std::move(fks));
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
            const auto tbl_oid = resolve_table_oid(node->collection_full_name(), view);
            if (tbl_oid != components::catalog::INVALID_OID) {
                auto fks = co_await view.get_fks_referencing(ctx, tbl_oid);
                node->set_referencing_fks(std::move(fks));
            }
            break;
        }
        case node_type::create_collection_t:
            enrich_create_collection(static_cast<node_create_collection_t*>(root.get()), view);
            break;
        default:
            break;
        }
    }

} // namespace services::dispatcher