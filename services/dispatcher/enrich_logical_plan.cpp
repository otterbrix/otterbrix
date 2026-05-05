#include "enrich_logical_plan.hpp"

#include "catalog_view.hpp"

#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_update.hpp>

#include <services/disk/manager_disk.hpp>

namespace services::dispatcher {

    namespace {

        // Fill NOT NULL / DEFAULT metadata from the cached resolved_table_t.
        // No disk round-trip — columns are already in catalog_view after validate_schema.
        void fill_column_meta(const resolved_table_t& tbl,
                              std::vector<std::string>& nn_out,
                              bool& has_def_out,
                              bool include_all_notnull = false) {
            for (const auto& col : tbl.columns) {
                if (col.attnotnull && (include_all_notnull || !col.atthasdefault)) {
                    nn_out.push_back(col.attname);
                }
                if (col.atthasdefault) has_def_out = true;
            }
        }

        // Fetch CHECK exprs for a collection via the existing actor message.
        actor_zeta::unique_future<std::vector<std::string>>
        fetch_check_exprs(actor_zeta::address_t disk_address,
                          components::execution_context_t ctx,
                          const components::logical_plan::collection_full_name_t& coll) {
            auto [_c, ckf] = actor_zeta::send(
                disk_address,
                &disk::manager_disk_t::get_check_constraints,
                ctx,
                components::logical_plan::collection_full_name_t{
                    std::string(coll.database_name()),
                    std::string(coll.collection_name())});
            auto infos = co_await std::move(ckf);
            std::vector<std::string> exprs;
            exprs.reserve(infos.size());
            for (const auto& c : infos) exprs.push_back(c.conexpr);
            co_return exprs;
        }

        actor_zeta::unique_future<void>
        enrich_insert(components::logical_plan::node_insert_t* node,
                      catalog_view_t& view,
                      actor_zeta::address_t disk_address,
                      components::execution_context_t ctx) {
            const auto& coll = node->collection_full_name();
            const auto* ns = view.try_get_namespace(coll.database_name());
            if (!ns) co_return;
            const auto* tbl = view.try_get_table(ns->oid, coll.collection_name());
            if (!tbl) co_return;

            std::vector<std::string> nn_cols;
            bool has_def = false;
            fill_column_meta(*tbl, nn_cols, has_def, /*include_all_notnull=*/false);
            node->set_not_null_cols(std::move(nn_cols));
            node->set_has_defaults(has_def);

            auto exprs = co_await fetch_check_exprs(disk_address, ctx, coll);
            node->set_check_exprs(std::move(exprs));
            // outgoing_fks left empty — FK enforcement stays in disk until Etap 5.
        }

        actor_zeta::unique_future<void>
        enrich_update(components::logical_plan::node_update_t* node,
                      catalog_view_t& view,
                      actor_zeta::address_t disk_address,
                      components::execution_context_t ctx) {
            const auto& coll = node->collection_full_name();
            const auto* ns = view.try_get_namespace(coll.database_name());
            if (!ns) co_return;
            const auto* tbl = view.try_get_table(ns->oid, coll.collection_name());
            if (!tbl) co_return;

            std::vector<std::string> nn_cols;
            bool has_def = false;
            fill_column_meta(*tbl, nn_cols, has_def, /*include_all_notnull=*/true);
            node->set_not_null_cols(std::move(nn_cols));

            auto exprs = co_await fetch_check_exprs(disk_address, ctx, coll);
            node->set_check_exprs(std::move(exprs));
            // outgoing_fks left empty — FK enforcement stays in disk until Etap 5.
        }

        // DELETE: referencing_fks left empty until Etap 5.
        actor_zeta::unique_future<void>
        enrich_delete(components::logical_plan::node_delete_t* /*node*/,
                      catalog_view_t& /*view*/,
                      actor_zeta::address_t /*disk_address*/,
                      components::execution_context_t /*ctx*/) {
            // Referencing FKs will be filled in Etap 5 when FK enforcement moves from disk.
            co_return;
        }

    } // anonymous namespace

    actor_zeta::unique_future<void>
    enrich_plan(components::logical_plan::node_ptr root,
                catalog_view_t& view,
                actor_zeta::address_t disk_address,
                components::execution_context_t ctx,
                std::pmr::memory_resource* /*resource*/) {
        using namespace components::logical_plan;
        if (!root) co_return;

        switch (root->type()) {
        case node_type::insert_t:
            co_await enrich_insert(static_cast<node_insert_t*>(root.get()),
                                    view, disk_address, ctx);
            break;
        case node_type::update_t:
            co_await enrich_update(static_cast<node_update_t*>(root.get()),
                                    view, disk_address, ctx);
            break;
        case node_type::delete_t:
            co_await enrich_delete(static_cast<node_delete_t*>(root.get()),
                                    view, disk_address, ctx);
            break;
        default:
            break;
        }
    }

} // namespace services::dispatcher
