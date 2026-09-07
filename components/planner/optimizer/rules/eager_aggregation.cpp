#include "eager_aggregation.hpp"

#include <algorithm>
#include <string>
#include <vector>

#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_join.hpp>

namespace components::planner::optimizer {

    namespace {
        namespace ce = components::expressions;
        namespace lp = components::logical_plan;

        lp::node_ptr find_child(const lp::node_ptr& n, lp::node_type t) {
            for (const auto& c : n->children()) {
                if (c && c->type() == t) {
                    return c;
                }
            }
            return nullptr;
        }

        // Anything beyond a plain single-table scan would change semantics if grouped over.
        bool is_bare_table_source(const lp::node_ptr& n) {
            if (!n || n->type() != lp::node_type::aggregate_t) {
                return false;
            }
            if (n->table_oid() == components::catalog::INVALID_OID) {
                return false;
            }
            if (static_cast<const lp::node_aggregate_t*>(n.get())->is_distinct()) {
                return false;
            }
            for (const auto& c : n->children()) {
                if (!c || c->type() != lp::node_type::match_t) {
                    return false;
                }
            }
            return true;
        }

        bool single_col(const ce::key_t& k, size_t& out) {
            if (k.path().size() != 1) {
                return false;
            }
            out = k.path()[0];
            return true;
        }

        ce::key_t local_key(std::pmr::memory_resource* resource, const ce::key_t& src, size_t local_idx) {
            // Copy must land on `resource`: the result belongs to the new partial node, not `src`'s.
            ce::key_t k{src, resource};
            std::pmr::vector<size_t> p{resource};
            p.push_back(local_idx);
            k.set_path(std::move(p));
            return k;
        }

        void set_key_path(std::pmr::memory_resource* resource, ce::key_t& k, size_t idx) {
            std::pmr::vector<size_t> p{resource};
            p.push_back(idx);
            k.set_path(std::move(p));
        }

        // Rewrites `agg` in place if it is an eager-aggregation candidate; a no-op otherwise.
        void try_eager(std::pmr::memory_resource* resource, const lp::node_ptr& agg) {
            if (agg->type() != lp::node_type::aggregate_t) {
                return;
            }
            if (agg->children().empty()) {
                return;
            }
            const lp::node_ptr& source = agg->children()[0];
            if (!source || source->type() != lp::node_type::join_t) {
                return;
            }
            auto* join = static_cast<lp::node_join_t*>(source.get());
            // Inner + single equi-key hash join only: nested-loop has no single join column to add,
            // and outer joins need row-preservation reasoning not attempted here.
            if (join->type() != lp::join_type::inner) {
                return;
            }
            if (join->algo() != lp::node_join_t::join_algo::hash) {
                return;
            }
            if (join->children().size() != 2) {
                return;
            }
            const lp::node_ptr& lc = join->children()[0];
            const lp::node_ptr& rc = join->children()[1];
            if (!lc || !rc || !lc->has_output_types() || !rc->has_output_types()) {
                return;
            }
            const size_t left_width = lc->output_types().size();

            lp::node_ptr group = find_child(agg, lp::node_type::group_t);
            if (!group) {
                return;
            }
            // HAVING would need to run above the final merge; skip (conservative).
            if (find_child(agg, lp::node_type::having_t)) {
                return;
            }
            // A residual WHERE above the join (e.g. t1.a > t2.b, unpushed) reads the join's merged
            // column space; the partial splice would re-point it at the wrong columns, so skip.
            if (find_child(agg, lp::node_type::match_t)) {
                return;
            }
            auto* group_node = static_cast<lp::node_group_t*>(group.get());
            if (group_node->internal_aggregate_count != 0) {
                return;
            }

            std::vector<ce::scalar_expression_t*> keys;
            std::vector<size_t> key_merged;
            // A group emits its target list, so a key it names appears twice: `SELECT g, MIN(x)
            // GROUP BY g` has a group_field for the reduction and a get_field that reduces nothing
            // but moves with the splice, so its path is rewritten too rather than refusing the rule;
            // build_pushed_spec treats a repeated key the same way.
            std::vector<ce::key_t*> key_outputs;
            std::vector<size_t> key_output_merged;
            std::vector<ce::aggregate_expression_t*> aggs;
            std::vector<size_t> agg_merged;
            for (const auto& e : group_node->expressions()) {
                if (e->group() == ce::expression_group::scalar) {
                    auto* s = static_cast<ce::scalar_expression_t*>(e.get());
                    if (s->type() == ce::scalar_type::get_field) {
                        if (!s->params().empty() && !ce::is_key(s->params().front())) {
                            return; // a computed output, not a plain reference
                        }
                        ce::key_t& field = s->params().empty() ? s->key() : ce::as_key(s->params().front());
                        size_t m = 0;
                        if (!single_col(field, m)) {
                            return;
                        }
                        key_outputs.push_back(&field);
                        key_output_merged.push_back(m);
                        continue;
                    }
                    if (s->type() != ce::scalar_type::group_field) {
                        return; // coalesce / case_when / arithmetic key -> not handled
                    }
                    size_t m = 0;
                    if (!single_col(s->key(), m)) {
                        return;
                    }
                    keys.push_back(s);
                    key_merged.push_back(m);
                } else if (e->group() == ce::expression_group::aggregate) {
                    auto* a = static_cast<ce::aggregate_expression_t*>(e.get());
                    if (a->is_distinct() || !a->is_mergeable()) {
                        return;
                    }
                    const auto& fn = a->function_name();
                    if (fn != "min" && fn != "max") {
                        return; // only duplication-insensitive aggregates
                    }
                    if (a->params().size() != 1 || !ce::is_key(a->params()[0])) {
                        return; // multi-arg / expression / COUNT(*) -> not handled
                    }
                    size_t m = 0;
                    if (!single_col(ce::as_key(a->params()[0]), m)) {
                        return;
                    }
                    aggs.push_back(a);
                    agg_merged.push_back(m);
                } else {
                    return;
                }
            }
            if (keys.empty() || aggs.empty()) {
                return;
            }
            // Every bare column the target list emits must be one of the reduction keys: nothing
            // else survives at group cardinality.
            std::vector<size_t> key_output_of(key_outputs.size());
            for (size_t i = 0; i < key_outputs.size(); ++i) {
                const auto named = std::find(key_merged.begin(), key_merged.end(), key_output_merged[i]);
                if (named == key_merged.end()) {
                    return;
                }
                key_output_of[i] = static_cast<size_t>(named - key_merged.begin());
            }

            // Every group key and every aggregate argument must live on one side of the join.
            bool all_left = true;
            bool all_right = true;
            for (size_t m : key_merged) {
                if (m < left_width) {
                    all_right = false;
                } else {
                    all_left = false;
                }
            }
            for (size_t m : agg_merged) {
                if (m < left_width) {
                    all_right = false;
                } else {
                    all_left = false;
                }
            }
            if (all_left == all_right) {
                return; // mixed (cross-side) or degenerate
            }
            const bool pushed_left = all_left;
            const size_t base = pushed_left ? 0 : left_width;
            const size_t pushed_idx = pushed_left ? 0 : 1;

            const lp::node_ptr& pushed = join->children()[pushed_idx];
            if (!is_bare_table_source(pushed)) {
                return;
            }
            auto* pushed_agg = static_cast<lp::node_aggregate_t*>(pushed.get());

            const size_t join_key_local = pushed_left ? join->left_col() : join->right_col();
            if (join_key_local >= pushed->output_types().size()) {
                return; // defensive: unexpected stamp
            }
            // Every referenced column must resolve inside the pushed side's stamped width (re-stamped
            // below by that local index).
            for (size_t m : key_merged) {
                if (m - base >= pushed->output_types().size()) {
                    return;
                }
            }
            for (size_t m : agg_merged) {
                if (m - base >= pushed->output_types().size()) {
                    return;
                }
            }

            // Partial output layout: [group keys, join key (if new), aggregates].
            auto partial_group = lp::make_node_group(resource, pushed_agg->dbname(), pushed_agg->relname());
            std::vector<size_t> key_partial_pos(keys.size());
            size_t next_pos = 0;
            bool join_key_covered = false;
            size_t join_key_pos = 0;
            // A grouping key is not an output column on its own, so it must be named as well as
            // grouped on, or the partial emits only its aggregates and the join probes a missing
            // column; staged on `resource` so nothing lands on the process default allocator.
            std::vector<ce::key_t> emitted_keys;
            for (size_t i = 0; i < keys.size(); ++i) {
                const size_t local = key_merged[i] - base;
                auto key = local_key(resource, keys[i]->key(), local);
                partial_group->append_expression(
                    ce::make_scalar_expression(resource, ce::scalar_type::group_field, key));
                emitted_keys.emplace_back(key, resource);
                key_partial_pos[i] = next_pos;
                if (local == join_key_local) {
                    join_key_covered = true;
                    join_key_pos = next_pos;
                }
                ++next_pos;
            }
            if (!join_key_covered) {
                // Adds the join key as an extra grouping column so a partial group maps 1:1 to it
                // (inner-join then reduce commutes).
                ce::key_t jk{resource, std::string{pushed->output_types()[join_key_local].alias()}};
                set_key_path(resource, jk, join_key_local);
                partial_group->append_expression(
                    ce::make_scalar_expression(resource, ce::scalar_type::group_field, jk));
                emitted_keys.emplace_back(jk, resource);
                join_key_pos = next_pos;
                ++next_pos;
            }
            for (const auto& emitted : emitted_keys) {
                partial_group->append_expression(
                    ce::make_scalar_expression(resource, ce::scalar_type::get_field, emitted));
            }
            const size_t num_keys = next_pos;
            std::vector<size_t> agg_partial_pos(aggs.size());
            for (size_t i = 0; i < aggs.size(); ++i) {
                const size_t local = agg_merged[i] - base;
                auto pagg = ce::make_aggregate_expression(resource,
                                                          aggs[i]->function_name(),
                                                          aggs[i]->key(),
                                                          local_key(resource, ce::as_key(aggs[i]->params()[0]), local));
                pagg->add_function_uid(aggs[i]->function_uid());
                // This rule runs after validation, so nothing else stamps the expression's type and
                // the graph builder rejects an unstamped aggregate; copying the final's result type
                // is safe since MIN(MIN)=MIN/MAX(MAX)=MAX over the same column.
                pagg->set_result_type(aggs[i]->result_type());
                pagg->set_mergeable(true);
                pagg->set_distinct(false);
                partial_group->append_expression(pagg);
                agg_partial_pos[i] = num_keys + i;
            }

            for (size_t i = 0; i < keys.size(); ++i) {
                set_key_path(resource, keys[i]->key(), base + key_partial_pos[i]);
            }
            // A target-list reference reads the key it names, so it follows it to the same column.
            for (size_t i = 0; i < key_outputs.size(); ++i) {
                set_key_path(resource, *key_outputs[i], base + key_partial_pos[key_output_of[i]]);
            }
            for (size_t i = 0; i < aggs.size(); ++i) {
                // The function stays; only the argument moves onto the partial extremum column.
                set_key_path(resource, ce::as_key(aggs[i]->params()[0]), base + agg_partial_pos[i]);
            }

            pushed->append_child(partial_group);

            // Re-stamps the pushed node's output to the true partial layout: it still carries the base
            // table's full column list, and both lowerings (create_plan_aggregate/operator_hash_group,
            // the pushed reduce spec) treat that stamp as authoritative, mistyping columns otherwise.
            {
                const auto& base_types = pushed->output_types();
                std::pmr::vector<components::types::complex_logical_type> partial_types{resource};
                partial_types.reserve(num_keys + aggs.size());
                for (size_t i = 0; i < keys.size(); ++i) {
                    partial_types.push_back(base_types[key_merged[i] - base]);
                }
                if (!join_key_covered) {
                    partial_types.push_back(base_types[join_key_local]);
                }
                for (size_t i = 0; i < aggs.size(); ++i) {
                    // MIN/MAX preserve their argument's type; the column is named after the partial
                    // aggregate's alias.
                    auto t = base_types[agg_merged[i] - base];
                    t.set_alias(aggs[i]->key().as_string());
                    partial_types.push_back(std::move(t));
                }
                pushed->set_output_types(std::move(partial_types));
            }

            if (pushed_left) {
                join->set_equi_columns(join_key_pos, join->right_col());
            } else {
                join->set_equi_columns(join->left_col(), join_key_pos);
            }
            // Keeps the ON condition truthful: the pushed-side key now sits at its partial-output
            // position; the other side is untouched.
            if (!join->expressions().empty() && join->expressions()[0] &&
                join->expressions()[0]->group() == ce::expression_group::compare) {
                auto* cmp = static_cast<ce::compare_expression_t*>(join->expressions()[0].get());
                const ce::side_t want = pushed_left ? ce::side_t::left : ce::side_t::right;
                if (ce::is_key(cmp->left()) && ce::as_key(cmp->left()).side() == want) {
                    set_key_path(resource, ce::as_key(cmp->left()), join_key_pos);
                } else if (ce::is_key(cmp->right()) && ce::as_key(cmp->right()).side() == want) {
                    set_key_path(resource, ce::as_key(cmp->right()), join_key_pos);
                }
            }
        }

        void walk(std::pmr::memory_resource* resource, const lp::node_ptr& node) {
            if (!node) {
                return;
            }
            for (const auto& child : node->children()) {
                walk(resource, child);
            }
            try_eager(resource, node);
        }
    } // namespace

    logical_plan::node_ptr eager_aggregation(std::pmr::memory_resource* resource, logical_plan::node_ptr root) {
        walk(resource, root);
        return root;
    }

} // namespace components::planner::optimizer
