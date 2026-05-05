#include "planner.hpp"

#include <catalog/check_predicate_compiler.hpp>
#include <catalog/ddl_metadata_builder.hpp>
#include <catalog/system_table_schemas.hpp>

#include <logical_plan/node_check_constraint.hpp>
#include <logical_plan/node_default_apply.hpp>
#include <logical_plan/node_delete.hpp>
#include <logical_plan/node_fk_cascade.hpp>
#include <logical_plan/node_fk_check.hpp>
#include <logical_plan/node_insert.hpp>
#include <logical_plan/node_not_null_check.hpp>
#include <logical_plan/node_sequence.hpp>
#include <logical_plan/node_update.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>

namespace components::planner {

    namespace {
        using node_ptr = logical_plan::node_ptr;

        // =========================================================================
        // rewrite_insert: wraps with fk_check → check_constraint → not_null_check
        //                 → default_apply (order: outermost applied last)
        // Metadata is already in node fields from the dispatcher's enrich pass.
        // =========================================================================

        node_ptr rewrite_insert(std::pmr::memory_resource* r, node_ptr node) {
            auto* ins = static_cast<logical_plan::node_insert_t*>(node.get());
            node_ptr current = node;

            // FK parent-existence checks.
            for (const auto& fk : ins->outgoing_fks()) {
                auto fk_node = boost::intrusive_ptr(new logical_plan::node_fk_check_t(
                    r, ins->collection_full_name(), fk));
                fk_node->append_child(current);
                current = std::move(fk_node);
            }

            // CHECK constraint evaluation.
            for (const auto& expr : ins->check_exprs()) {
                auto pred = catalog::compile_check(r, expr, {});
                auto chk = boost::intrusive_ptr(new logical_plan::node_check_constraint_t(
                    r, ins->collection_full_name(), std::move(pred), expr));
                chk->append_child(current);
                current = std::move(chk);
            }

            // NOT NULL check.
            if (!ins->not_null_cols().empty()) {
                auto nn = boost::intrusive_ptr(new logical_plan::node_not_null_check_t(
                    r, ins->collection_full_name(),
                    std::vector<std::string>(ins->not_null_cols())));
                nn->append_child(current);
                current = std::move(nn);
            }

            // DEFAULT apply.
            if (ins->has_defaults()) {
                std::vector<logical_plan::node_default_apply_t::default_entry_t> defs;
                auto da = boost::intrusive_ptr(new logical_plan::node_default_apply_t(
                    r, ins->collection_full_name(), std::move(defs)));
                da->append_child(current);
                current = std::move(da);
            }

            return current;
        }

        // =========================================================================
        // rewrite_update: same as insert, without DEFAULT apply
        // =========================================================================

        node_ptr rewrite_update(std::pmr::memory_resource* r, node_ptr node) {
            auto* upd = static_cast<logical_plan::node_update_t*>(node.get());
            node_ptr current = node;

            for (const auto& fk : upd->outgoing_fks()) {
                auto fk_node = boost::intrusive_ptr(new logical_plan::node_fk_check_t(
                    r, upd->collection_full_name(), fk));
                fk_node->append_child(current);
                current = std::move(fk_node);
            }

            for (const auto& expr : upd->check_exprs()) {
                auto pred = catalog::compile_check(r, expr, {});
                auto chk = boost::intrusive_ptr(new logical_plan::node_check_constraint_t(
                    r, upd->collection_full_name(), std::move(pred), expr));
                chk->append_child(current);
                current = std::move(chk);
            }

            if (!upd->not_null_cols().empty()) {
                auto nn = boost::intrusive_ptr(new logical_plan::node_not_null_check_t(
                    r, upd->collection_full_name(),
                    std::vector<std::string>(upd->not_null_cols())));
                nn->append_child(current);
                current = std::move(nn);
            }

            return current;
        }

        // =========================================================================
        // rewrite_delete: node_sequence(delete + fk_cascade × N)
        // =========================================================================

        node_ptr rewrite_delete(std::pmr::memory_resource* r, node_ptr node) {
            auto* del = static_cast<logical_plan::node_delete_t*>(node.get());
            if (del->referencing_fks().empty()) return node;

            auto seq = boost::intrusive_ptr(new logical_plan::node_sequence_t(
                r, del->collection_full_name()));
            seq->append_child(node);
            for (const auto& fk : del->referencing_fks()) {
                auto casc = boost::intrusive_ptr(new logical_plan::node_fk_cascade_t(
                    r, del->collection_full_name(), fk));
                seq->append_child(std::move(casc));
            }
            return seq;
        }

        // =========================================================================
        // walk — recursive dispatch
        // =========================================================================

        node_ptr walk(std::pmr::memory_resource* r, node_ptr node) {
            using namespace logical_plan;
            switch (node->type()) {
            case node_type::insert_t:
                return rewrite_insert(r, node);
            case node_type::update_t:
                return rewrite_update(r, node);
            case node_type::delete_t:
                return rewrite_delete(r, node);
            default:
                for (auto& child : node->children()) {
                    child = walk(r, child);
                }
                return node;
            }
        }

    } // anonymous namespace

    auto planner_t::create_plan(std::pmr::memory_resource* resource,
                                 logical_plan::node_ptr node)
        -> logical_plan::node_ptr {
        return walk(resource, std::move(node));
    }

} // namespace components::planner
