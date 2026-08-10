#include "drop_redundant_distinct.hpp"

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_select.hpp>

#include <algorithm>
#include <limits>

namespace components::planner::optimizer {

    namespace {
        namespace lp = components::logical_plan;
        namespace ce = components::expressions;

        constexpr size_t not_a_column = std::numeric_limits<size_t>::max();

        // The resolved key a scalar entry reads, or nullptr when it reads no plain column.
        // The key lives in the first param when present (get_field(name, key)), else in the
        // expression key — mirroring validate_schema's own read.
        const ce::key_t* column_of(const ce::scalar_expression_t* scalar) {
            if (scalar->params().empty()) {
                return &scalar->key();
            }
            if (ce::is_key(scalar->params().front())) {
                return &ce::as_key(scalar->params().front());
            }
            return nullptr; // parameter_id / nested expression — not a base column
        }

        // A group's expression list holds BOTH its reductions and its target list: a key is
        // grouped ON as a group_field and, where the target list names it, emitted AGAIN as a
        // get_field. So the two are read out separately.
        //   `keys`    — one input-column ordinal per grouping key.
        //   `outputs` — one entry per emitted column IN OUTPUT ORDER (which is the target list's
        //               order, NOT keys-then-aggregates): the input ordinal it names, or
        //               not_a_column for an aggregate / computed column that names none.
        // False means the grouping is not plain columns (a computed or unresolved key), and the
        // caller keeps the DISTINCT rather than reasoning about a frame it cannot address.
        bool read_group_shape(const lp::node_group_t* group,
                              std::pmr::vector<size_t>& keys,
                              std::pmr::vector<size_t>& outputs) {
            // Aggregates the validator appended for a HAVING sit at the TAIL and are reduced but
            // never emitted, so they are not part of the projection (create_plan_group draws the
            // same line) — counting one as an emitted column could credit it with naming a key.
            const size_t emitted_end = group->expressions().size() - group->internal_aggregate_count;
            size_t index = 0;
            for (const auto& expr : group->expressions()) {
                const bool emitted = index++ < emitted_end;
                const bool is_scalar = expr->group() == ce::expression_group::scalar;
                const auto* scalar = is_scalar ? static_cast<const ce::scalar_expression_t*>(expr.get()) : nullptr;
                if (scalar != nullptr && scalar->type() == ce::scalar_type::group_field) {
                    if (scalar->key().path().size() != 1) {
                        return false; // nested / computed grouping key
                    }
                    keys.push_back(scalar->key().path().front());
                    continue;
                }
                if (!emitted) {
                    continue; // a HAVING helper — reduced, never projected
                }
                // Everything else is an emitted column.
                const ce::key_t* column = nullptr;
                if (scalar != nullptr && scalar->type() == ce::scalar_type::get_field) {
                    column = column_of(scalar);
                }
                outputs.push_back(column != nullptr && column->path().size() == 1 ? column->path().front()
                                                                                  : not_a_column);
            }
            return !keys.empty();
        }

        // The output position that emits grouping key `key_column`, or not_a_column when the
        // target list never names it.
        size_t output_position_of(size_t key_column, const std::pmr::vector<size_t>& outputs) {
            for (size_t position = 0; position < outputs.size(); ++position) {
                if (outputs[position] == key_column) {
                    return position;
                }
            }
            return not_a_column;
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
            for (const auto& child : node->children()) {
                if (child->type() == lp::node_type::group_t) {
                    group = static_cast<const lp::node_group_t*>(child.get());
                }
            }
            if (group == nullptr) {
                return; // no GROUP BY -> never touch DISTINCT
            }

            std::pmr::vector<size_t> keys{node->resource()};
            std::pmr::vector<size_t> outputs{node->resource()};
            if (!read_group_shape(group, keys, outputs)) {
                return; // scalar aggregate (implicit group) or unclean group shape
            }

            // One row per group is emitted, so a DISTINCT can only ever remove a row when two
            // groups collapse to the same output — which cannot happen once every grouping key is
            // emitted. Plain DISTINCT therefore asks whether the target list NAMES every key;
            // DISTINCT ON asks whether its own columns do, so each key's output position (the
            // frame validate_schema resolved the ON keys against) has to appear among them.
            std::pmr::vector<size_t> on_positions{node->resource()};
            const bool distinct_on = !aggregate->distinct_on_keys().empty();
            if (distinct_on) {
                on_positions.reserve(aggregate->distinct_on_keys().size());
                for (const auto& on_key : aggregate->distinct_on_keys()) {
                    if (!on_key.path().empty()) {
                        on_positions.push_back(on_key.path().front());
                    }
                }
            }

            bool covered = true;
            for (size_t key_column : keys) {
                const size_t position = output_position_of(key_column, outputs);
                if (position == not_a_column) {
                    covered = false;
                    break;
                }
                if (distinct_on &&
                    std::find(on_positions.begin(), on_positions.end(), position) == on_positions.end()) {
                    covered = false;
                    break;
                }
            }

            if (covered) {
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
