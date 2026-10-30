#include "index_selection.hpp"

#include <components/logical_plan/node_match.hpp>

namespace components::planner::optimizer {

    namespace {
        namespace lp = components::logical_plan;
        namespace ce = components::expressions;

        bool is_range(ce::compare_type compare) noexcept {
            return compare == ce::compare_type::lt || compare == ce::compare_type::lte ||
                   compare == ce::compare_type::gt || compare == ce::compare_type::gte;
        }

        bool an_index_reads(ce::compare_type compare) noexcept {
            switch (compare) {
                case ce::compare_type::eq:
                case ce::compare_type::lt:
                case ce::compare_type::lte:
                case ce::compare_type::gt:
                case ce::compare_type::gte:
                case ce::compare_type::is_null:
                case ce::compare_type::is_not_null:
                    return true;
                default:
                    return false;
            }
        }

        void stamp(lp::node_match_t& match, const lp::index_facts_t& indexes) {
            const auto oid = match.table_oid();
            if (oid == components::catalog::INVALID_OID || match.expressions().empty()) {
                return;
            }
            const auto& where = match.expressions().front();
            if (!where || where->group() != ce::expression_group::compare) {
                return;
            }
            const auto& compare = static_cast<const ce::compare_expression_t&>(*where);
            if (compare.is_union() || compare.do_not_fold() || !an_index_reads(compare.type())) {
                return;
            }
            bool key_on_left = true;
            if (ce::is_key(compare.left()) && ce::is_parameter(compare.right())) {
                key_on_left = true;
            } else if (ce::is_parameter(compare.left()) && ce::is_key(compare.right())) {
                key_on_left = false;
            } else {
                return;
            }
            const auto& key = ce::as_key(key_on_left ? compare.left() : compare.right());
            if (!indexes.has_index_on(oid, key)) {
                return;
            }
            if (is_range(compare.type()) && !indexes.has_index_on_with_other_type(oid, key, lp::index_type::hashed)) {
                return;
            }
            const auto read = key_on_left ? compare.type() : mirror_compare(compare.type());
            match.set_index(
                lp::index_choice_t{key_on_left, read, indexes.preferred_index_type_for_compare(oid, key, read)});
        }

        void walk(const lp::node_ptr& node, const lp::index_facts_t& indexes) {
            if (!node) {
                return;
            }
            if (node->type() == lp::node_type::match_t) {
                stamp(static_cast<lp::node_match_t&>(*node), indexes);
            }
            for (const auto& child : node->children()) {
                walk(child, indexes);
            }
        }
    } // namespace

    ce::compare_type mirror_compare(ce::compare_type compare) noexcept {
        switch (compare) {
            case ce::compare_type::lt:
                return ce::compare_type::gt;
            case ce::compare_type::lte:
                return ce::compare_type::gte;
            case ce::compare_type::gt:
                return ce::compare_type::lt;
            case ce::compare_type::gte:
                return ce::compare_type::lte;
            default:
                return compare;
        }
    }

    void select_indexes(const lp::node_ptr& root, const lp::index_facts_t& indexes) { walk(root, indexes); }

} // namespace components::planner::optimizer
