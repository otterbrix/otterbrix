#include "drop_redundant_distinct.hpp"

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_select.hpp>

namespace components::planner::optimizer {

    namespace {
        namespace lp = components::logical_plan;
        namespace ce = components::expressions;

        // Number of leading GROUP BY key columns of `group`, and 0 when the group is
        // not a clean all-group_field grouping. Aggregate expressions occupy no slot in
        // the group KEY schema (the frame the projection resolves get_field columns
        // against), so they are skipped. A get_field / arithmetic scalar group entry
        // would break the "keys are the contiguous {0..G-1} output ordinals" invariant,
        // so its presence forces a conservative bail (return 0 => caller keeps DISTINCT).
        // G==0 (a scalar aggregate with no explicit GROUP BY keys) also returns 0: such
        // a DISTINCT is left untouched, matching "never touch a DISTINCT with no GROUP BY".
        size_t clean_group_key_count(const lp::node_group_t* group) {
            size_t keys = 0;
            for (const auto& expr : group->expressions()) {
                if (expr->group() == ce::expression_group::aggregate) {
                    continue; // no key-schema slot
                }
                if (expr->group() != ce::expression_group::scalar) {
                    return 0; // unexpected group entry — bail
                }
                const auto* scalar = static_cast<const ce::scalar_expression_t*>(expr.get());
                if (scalar->type() != ce::scalar_type::group_field) {
                    return 0; // get_field / arithmetic key — output frame not clean, bail
                }
                ++keys;
            }
            return keys;
        }

        // Collect the group-output column ordinals a projection references through plain
        // get_field columns with a single-element resolved path. validate_schema resolved
        // these keys against the group-output frame (group KEY columns first, then the
        // aggregate columns), so a projected group key resolves to exactly its group-key
        // ordinal. Non-column / multi-path / unresolved projection entries are skipped:
        // they cannot cover a group key, so skipping them only makes the rule MORE
        // conservative (never falsely clears).
        void collect_projected_positions(const lp::node_t* select, std::pmr::vector<size_t>& out) {
            for (const auto& expr : select->expressions()) {
                if (expr->group() != ce::expression_group::scalar) {
                    continue;
                }
                const auto* scalar = static_cast<const ce::scalar_expression_t*>(expr.get());
                if (scalar->type() != ce::scalar_type::get_field) {
                    continue;
                }
                // The resolved key lives in the first param when present (get_field(name, key)),
                // else in the expression key — mirroring validate_schema's own read.
                const ce::key_t* key = nullptr;
                if (scalar->params().empty()) {
                    key = &scalar->key();
                } else if (ce::is_key(scalar->params().front())) {
                    key = &ce::as_key(scalar->params().front());
                } else {
                    continue; // parameter_id / nested expression — not a base column
                }
                if (key->path().size() == 1) {
                    out.push_back(key->path().front());
                }
            }
        }

        bool covers_all_keys(size_t key_count, const std::pmr::vector<size_t>& positions) {
            for (size_t k = 0; k < key_count; ++k) {
                bool found = false;
                for (size_t pos : positions) {
                    if (pos == k) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return false;
                }
            }
            return true;
        }

        // Clear a redundant DISTINCT on `node` when it is a DISTINCT aggregate whose
        // GROUP BY keys are a subset of the projected / ON columns. No-op otherwise.
        void try_clear_distinct(const lp::node_ptr& node) {
            if (node->type() != lp::node_type::aggregate_t) {
                return;
            }
            auto* aggregate = static_cast<lp::node_aggregate_t*>(node.get());
            if (!aggregate->is_distinct()) {
                return;
            }

            const lp::node_group_t* group = nullptr;
            const lp::node_t* select = nullptr;
            for (const auto& child : node->children()) {
                if (child->type() == lp::node_type::group_t) {
                    group = static_cast<const lp::node_group_t*>(child.get());
                } else if (child->type() == lp::node_type::select_t) {
                    select = child.get();
                }
            }
            if (group == nullptr) {
                return; // no GROUP BY -> never touch DISTINCT
            }

            const size_t key_count = clean_group_key_count(group);
            if (key_count == 0) {
                return; // scalar aggregate (implicit group) or unclean group shape
            }

            std::pmr::vector<size_t> positions{node->resource()};
            if (aggregate->distinct_on_keys().empty()) {
                // Plain DISTINCT: the projection must cover every group key.
                if (select == nullptr) {
                    return;
                }
                collect_projected_positions(select, positions);
            } else {
                // DISTINCT ON (cols): the ON columns must cover every group key. Their
                // paths were resolved against the same group-output frame as the keys.
                positions.reserve(aggregate->distinct_on_keys().size());
                for (const auto& on_key : aggregate->distinct_on_keys()) {
                    if (!on_key.path().empty()) {
                        positions.push_back(on_key.path().front());
                    }
                }
            }

            if (covers_all_keys(key_count, positions)) {
                aggregate->set_distinct(false);
                // Drop the now-dead ON key list so the plan renders / lowers as a plain
                // grouped aggregate with no DISTINCT layer.
                aggregate->set_distinct_on_keys(std::pmr::vector<ce::key_t>{node->resource()});
            }
        }

        void walk(const lp::node_ptr& node) {
            if (!node) {
                return;
            }
            for (auto& child : node->children()) {
                walk(child);
            }
            try_clear_distinct(node);
        }
    } // namespace

    logical_plan::node_ptr drop_redundant_distinct(std::pmr::memory_resource* /*resource*/,
                                                   logical_plan::node_ptr root) {
        walk(root);
        return root;
    }

} // namespace components::planner::optimizer
