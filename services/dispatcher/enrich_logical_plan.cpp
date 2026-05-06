#include "enrich_logical_plan.hpp"

#include "catalog_view.hpp"

#include <components/logical_plan/node_create_collection.hpp>
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

        void enrich_insert(components::logical_plan::node_insert_t* node, catalog_view_t& view) {
            const auto& coll = node->collection_full_name();
            const auto* ns = view.try_get_namespace(coll.database);
            if (!ns) return;
            const auto* tbl = view.try_get_table(ns->oid, coll.collection);
            if (!tbl) return;
            std::vector<std::string> nn;
            fill_not_null(*tbl, nn, /*include_with_defaults=*/false);
            node->set_not_null_cols(std::move(nn));
        }

        void enrich_update(components::logical_plan::node_update_t* node, catalog_view_t& view) {
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
            // Resolve the namespace OID from the database name so the planner can build
            // pg_depend rows without an async disk round-trip.
            const auto* ns = view.try_get_namespace(coll.database);
            if (!ns) return;
            node->set_namespace_oid(ns->oid);
        }

    } // anonymous namespace

    actor_zeta::unique_future<void>
    enrich_plan(components::logical_plan::node_ptr root,
                catalog_view_t& view,
                actor_zeta::address_t /*disk_address*/,
                components::execution_context_t /*ctx*/,
                std::pmr::memory_resource* /*resource*/) {
        using namespace components::logical_plan;
        if (!root) co_return;
        switch (root->type()) {
        case node_type::insert_t:
            enrich_insert(static_cast<node_insert_t*>(root.get()), view);
            break;
        case node_type::update_t:
            enrich_update(static_cast<node_update_t*>(root.get()), view);
            break;
        case node_type::create_collection_t:
            enrich_create_collection(static_cast<node_create_collection_t*>(root.get()), view);
            break;
        default:
            break;
        }
    }

} // namespace services::dispatcher