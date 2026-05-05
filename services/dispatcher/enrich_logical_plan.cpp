#include "enrich_logical_plan.hpp"

#include "catalog_view.hpp"

#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_update.hpp>

namespace services::dispatcher {

    namespace {

        // Fill NOT NULL metadata from the cached resolved_table_t.
        void fill_column_meta(const resolved_table_t& tbl,
                              std::vector<std::string>& nn_out,
                              bool include_all_notnull = false) {
            for (const auto& col : tbl.columns) {
                if (col.attnotnull && (include_all_notnull || !col.atthasdefault)) {
                    nn_out.push_back(col.attname);
                }
            }
        }

        void enrich_insert_sync(components::logical_plan::node_insert_t* node,
                                catalog_view_t& view) {
            const auto& coll = node->collection_full_name();
            const auto* ns = view.try_get_namespace(coll.database);
            if (!ns) return;
            const auto* tbl = view.try_get_table(ns->oid, coll.collection);
            if (!tbl) return;

            std::vector<std::string> nn_cols;
            fill_column_meta(*tbl, nn_cols, /*include_all_notnull=*/false);
            node->set_not_null_cols(std::move(nn_cols));
        }

        void enrich_update_sync(components::logical_plan::node_update_t* node,
                                catalog_view_t& view) {
            const auto& coll = node->collection_full_name();
            const auto* ns = view.try_get_namespace(coll.database);
            if (!ns) return;
            const auto* tbl = view.try_get_table(ns->oid, coll.collection);
            if (!tbl) return;

            std::vector<std::string> nn_cols;
            fill_column_meta(*tbl, nn_cols, /*include_all_notnull=*/true);
            node->set_not_null_cols(std::move(nn_cols));
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
            enrich_insert_sync(static_cast<node_insert_t*>(root.get()), view);
            break;
        case node_type::update_t:
            enrich_update_sync(static_cast<node_update_t*>(root.get()), view);
            break;
        case node_type::delete_t:
            break;
        default:
            break;
        }
    }

} // namespace services::dispatcher