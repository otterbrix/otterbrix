#include "pushdown_filter.hpp"

#include "conjunct_utils.hpp"

#include <optional>
#include <set>
#include <string>
#include <vector>

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_select.hpp>
#include <components/logical_plan/node_sort.hpp>

namespace components::planner::optimizer {

    namespace {

        using namespace components::expressions;
        using namespace components::logical_plan;

        // db identity of a node.
        // match_t and aggregate_t carry a table name;
        // joins, functions and sub-aggregates return empty identifiers
        std::pair<core::dbname_t, core::relname_t> node_cfn(const node_ptr& n) {
            if (!n) {
                return {core::dbname_t{}, core::relname_t{}};
            }
            switch (n->type()) {
                case node_type::match_t: {
                    auto* m = static_cast<const node_match_t*>(n.get());
                    return {core::dbname_t{m->dbname()}, core::relname_t{m->relname()}};
                }
                case node_type::aggregate_t: {
                    auto* a = static_cast<const node_aggregate_t*>(n.get());
                    return {a->dbname(), a->relname()};
                }
                default:
                    return {core::dbname_t{}, core::relname_t{}};
            }
        }

        std::set<std::string> collect_referenced_columns(const expression_ptr& expr);

        void extract_from_param(const param_storage& param, std::set<std::string>& result) {
            if (std::holds_alternative<key_t>(param)) {
                result.insert(std::get<key_t>(param).as_string());
            } else if (std::holds_alternative<expression_ptr>(param)) {
                auto cols = collect_referenced_columns(std::get<expression_ptr>(param));
                result.insert(cols.begin(), cols.end());
            }
        }

        std::set<std::string> collect_referenced_columns(const expression_ptr& expr) {
            std::set<std::string> result;
            if (!expr) {
                return result;
            }

            switch (expr->group()) {
                case expression_group::compare: {
                    auto* cmp = static_cast<compare_expression_t*>(expr.get());
                    if (is_union_compare_condition(cmp->type())) {
                        for (const auto& child : cmp->children()) {
                            auto cols = collect_referenced_columns(child);
                            result.insert(cols.begin(), cols.end());
                        }
                    } else {
                        extract_from_param(cmp->left(), result);
                        extract_from_param(cmp->right(), result);
                    }
                    break;
                }
                case expression_group::scalar: {
                    auto* sc = static_cast<scalar_expression_t*>(expr.get());
                    for (const auto& param : sc->params()) {
                        extract_from_param(param, result);
                    }
                    break;
                }
                case expression_group::aggregate: {
                    auto* agg = static_cast<aggregate_expression_t*>(expr.get());
                    for (const auto& param : agg->params()) {
                        extract_from_param(param, result);
                    }
                    break;
                }
                case expression_group::sort: {
                    auto* srt = static_cast<sort_expression_t*>(expr.get());
                    result.insert(srt->key().as_string());
                    break;
                }
                case expression_group::function: {
                    auto* fn = static_cast<function_expression_t*>(expr.get());
                    for (const auto& arg : fn->args()) {
                        extract_from_param(arg, result);
                    }
                    break;
                }
                default:
                    break;
            }
            return result;
        }

        // Re-localize a conjunct's column keys from the join's MERGED coordinate space
        // to the right child's LOCAL space when a right-side single-table filter is
        // pushed below the join. A key's merged path()[0] equals its local index only
        // for the left prefix ([0, left_width)); a right-side column sits at
        // left_width + local, so pushing it into the right child unchanged leaves an
        // out-of-range column index. Subtract left_width from the leading path element
        // (deeper elements index nested struct fields and stay put). The new path is
        // built on `resource` — never set_path({...}), which pulls the default resource
        // (rule 14). The left bucket needs no rewrite (merged == local there) and the
        // residual keeps its merged paths (it evaluates over the join's merged output).
        void relocalize_key_path(key_t& k, size_t left_width, std::pmr::memory_resource* resource) {
            const auto& old_path = k.path();
            if (old_path.empty()) {
                return;
            }
            std::pmr::vector<size_t> p{resource};
            p.reserve(old_path.size());
            p.push_back(old_path[0] - left_width);
            for (size_t i = 1; i < old_path.size(); ++i) {
                p.push_back(old_path[i]);
            }
            k.set_path(std::move(p));
        }

        void relocalize_keys(const expression_ptr& expr, size_t left_width, std::pmr::memory_resource* resource);

        void relocalize_param(param_storage& param, size_t left_width, std::pmr::memory_resource* resource) {
            if (is_key(param)) {
                relocalize_key_path(as_key(param), left_width, resource);
            } else if (is_expr(param)) {
                relocalize_keys(as_expr(param), left_width, resource);
            }
        }

        // Mirrors collect_referenced_columns exactly: it visits precisely the keys that
        // classify a conjunct as right-side, so every such key is re-localized here.
        void relocalize_keys(const expression_ptr& expr, size_t left_width, std::pmr::memory_resource* resource) {
            if (!expr) {
                return;
            }
            switch (expr->group()) {
                case expression_group::compare: {
                    auto* cmp = static_cast<compare_expression_t*>(expr.get());
                    if (is_union_compare_condition(cmp->type())) {
                        for (auto& child : cmp->children()) {
                            relocalize_keys(child, left_width, resource);
                        }
                    } else {
                        relocalize_param(cmp->left(), left_width, resource);
                        relocalize_param(cmp->right(), left_width, resource);
                    }
                    break;
                }
                case expression_group::scalar: {
                    auto* sc = static_cast<scalar_expression_t*>(expr.get());
                    for (auto& param : sc->params()) {
                        relocalize_param(param, left_width, resource);
                    }
                    break;
                }
                case expression_group::aggregate: {
                    auto* agg = static_cast<aggregate_expression_t*>(expr.get());
                    for (auto& param : agg->params()) {
                        relocalize_param(param, left_width, resource);
                    }
                    break;
                }
                case expression_group::sort: {
                    auto* srt = static_cast<sort_expression_t*>(expr.get());
                    relocalize_key_path(srt->key(), left_width, resource);
                    break;
                }
                case expression_group::function: {
                    auto* fn = static_cast<function_expression_t*>(expr.get());
                    for (auto& arg : fn->args()) {
                        relocalize_param(arg, left_width, resource);
                    }
                    break;
                }
                default:
                    break;
            }
        }

        bool filter_supported_through_identity_select(const node_select_t& sel,
                                                      const std::set<std::string>& filter_cols,
                                                      const std::set<std::string>& input_cols) {
            const auto& exprs = sel.expressions();
            const size_t hidden = sel.internal_aggregate_count;
            if (exprs.size() < hidden) {
                return false;
            }
            const size_t visible = exprs.size() - hidden;

            for (const auto& col : filter_cols) {
                if (input_cols.find(col) == input_cols.end()) {
                    return false;
                }
                bool ok_for_col = false;
                for (size_t i = 0; i < visible; ++i) {
                    const auto& expr = exprs[i];
                    if (expr->group() != expression_group::scalar) {
                        return false;
                    }
                    auto* sc = static_cast<scalar_expression_t*>(expr.get());
                    if (sc->type() != scalar_type::get_field) {
                        continue;
                    }
                    const std::string out = sc->key().as_string();
                    if (out != col) {
                        continue;
                    }
                    if (sc->params().empty()) {
                        ok_for_col = true;
                        break;
                    }
                    if (sc->params().size() == 1 && std::holds_alternative<key_t>(sc->params().front())) {
                        const auto& in_key = std::get<key_t>(sc->params().front());
                        if (in_key.as_string() == col) {
                            ok_for_col = true;
                            break;
                        }
                    }
                }
                if (!ok_for_col) {
                    return false;
                }
            }
            return true;
        }

        void collect_subtree_columns(const node_ptr& node, std::set<std::string>& cols) {
            if (!node) {
                return;
            }
            // Single canonical source (rule 6): a node's own validate_schema-stamped
            // output_types() carries its VISIBLE column names (aliases) — this is the
            // set a predicate above the node references, and it is stamped for disk
            // scans (aggregate_t{db,rel}), in-memory data_t, subquery and join nodes
            // alike. Read it directly. Only when a node is UNstamped (an
            // optimizer-synthesized wrapper — e.g. the aggregate this rule's own join
            // branch appends) do we recurse into its children to the first stamped
            // node. Never recurse straight to a leaf: a renamed side (`… AS x`) carries
            // the pre-rename name at the leaf, which would mis-bucket a predicate on `x`.
            if (node->has_output_types()) {
                for (const auto& t : node->output_types()) {
                    if (!t.alias().empty()) {
                        cols.insert(t.alias());
                    }
                }
                return;
            }
            for (const auto& child : node->children()) {
                collect_subtree_columns(child, cols);
            }
        }

        size_t type_width(const components::types::complex_logical_type& t) { return t.size(); }

        const node_data_t* find_data_node(const node_ptr& node) {
            if (!node) {
                return nullptr;
            }
            if (node->type() == node_type::data_t) {
                return static_cast<const node_data_t*>(node.get());
            }
            for (const auto& child : node->children()) {
                if (auto* found = find_data_node(child)) {
                    return found;
                }
            }
            return nullptr;
        }

        // nullopt = no data node in the subtree
        // otherwise = the summed byte width of all columns.
        std::optional<size_t> estimate_row_width(const node_ptr& node) {
            const node_data_t* data = find_data_node(node);
            if (!data) {
                return std::nullopt;
            }
            size_t width = 0;
            for (const auto& t : data->data_chunk().types()) {
                width += type_width(t);
            }
            return width;
        }

        // nullopt = width can't be estimated (computed/constant column)
        // otherwise = summed width.
        std::optional<size_t> estimate_projection_width(const node_select_t& sel, const node_ptr& subtree) {
            const node_data_t* data = find_data_node(subtree);
            if (!data) {
                return std::nullopt;
            }
            const auto& types = data->data_chunk().types();
            const auto& exprs = sel.expressions();
            const size_t hidden = sel.internal_aggregate_count;
            if (exprs.size() < hidden) {
                return std::nullopt;
            }
            const size_t visible = exprs.size() - hidden;
            size_t width = 0;
            for (size_t i = 0; i < visible; ++i) {
                const auto& expr = exprs[i];
                if (expr->group() != expression_group::scalar) {
                    return std::nullopt;
                }
                auto* sc = static_cast<scalar_expression_t*>(expr.get());
                if (sc->type() != scalar_type::get_field) {
                    return std::nullopt;
                }
                const std::string out_name = sc->key().as_string();
                bool found = false;
                for (const auto& t : types) {
                    if (t.alias() == out_name) {
                        width += type_width(t);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return std::nullopt;
                }
            }
            return width;
        }

        node_ptr pushdown_filter_impl(std::pmr::memory_resource* resource, node_ptr node) {
            if (!node) {
                return node;
            }

            for (size_t i = 0; i < node->children().size(); ++i) {
                auto& child = node->children()[i];
                auto optimized = pushdown_filter_impl(resource, child);
                if (optimized != child) {
                    node->children()[i] = optimized;
                }
            }

            if (node->type() != node_type::aggregate_t) {
                return node;
            }

            auto* agg = static_cast<node_aggregate_t*>(node.get());
            // child[0] = data source, child[1..] = pipeline operations; <2 means nothing to rewrite.
            if (agg->children().size() < 2) {
                return node;
            }

            node_ptr match_child = nullptr;
            node_ptr group_child = nullptr;
            node_ptr sort_child = nullptr;

            for (size_t i = 1; i < agg->children().size(); ++i) {
                auto& c = agg->children()[i];
                if (c->type() == node_type::match_t && !match_child) {
                    match_child = c;
                }
                if (c->type() == node_type::group_t) {
                    group_child = c;
                }
                if (c->type() == node_type::sort_t) {
                    sort_child = c;
                }
            }

            // Only a match child is required to attempt a push. A group_t/sort_t above
            // the join no longer bails: pushing a single-table filter below the join
            // under GROUP BY/ORDER BY + a projection is now driven end-to-end. That
            // shape wraps the join's probe child in a streaming filter over its scan
            // source (a 2-operator sub-plan); the executor's streaming driver
            // (executor.cpp execute_pipeline) was taught to treat the TOPMOST executed
            // operator — not a contiguous bottom prefix — as the materialized sub-plan
            // boundary, so the filtered probe rows now reach the group/sort instead of
            // the drained scan source being re-driven to 0 rows (repro:
            // test_batch_execution "join + WHERE with UDF batch predicate",
            // test_column_projection "inner JOIN + GROUP BY with WHERE on non-select
            // column").
            //
            // The aggregate-SOURCE branch below keeps its original !group_child &&
            // !sort_child guard: only the join-source branch is validated under
            // grouping/sort here.
            if (!match_child) {
                return node;
            }

            auto source = agg->children()[0];

            if (source->type() == node_type::aggregate_t && !group_child && !sort_child) {
                auto* source_agg = static_cast<node_aggregate_t*>(source.get());
                bool source_has_sort = false;
                bool source_has_group = false;
                bool source_has_match = false;
                bool source_has_select = false;
                for (size_t i = 1; i < source_agg->children().size(); ++i) {
                    if (source_agg->children()[i]->type() == node_type::sort_t)
                        source_has_sort = true;
                    if (source_agg->children()[i]->type() == node_type::group_t)
                        source_has_group = true;
                    if (source_agg->children()[i]->type() == node_type::match_t)
                        source_has_match = true;
                    if (source_agg->children()[i]->type() == node_type::select_t)
                        source_has_select = true;
                }

                if (source_has_sort && !source_has_group && !source_has_match) {
                    source_agg->append_child(match_child);
                    return pushdown_filter_impl(resource, source);
                }

                if (source_has_select && !source_has_sort && !source_has_group && !source_has_match) {
                    node_ptr src_select_wrapped = nullptr;
                    for (size_t i = 1; i < source_agg->children().size(); ++i) {
                        if (source_agg->children()[i]->type() == node_type::select_t) {
                            src_select_wrapped = source_agg->children()[i];
                            break;
                        }
                    }
                    if (src_select_wrapped && !match_child->expressions().empty()) {
                        auto filter_cols = collect_referenced_columns(match_child->expressions()[0]);
                        std::set<std::string> input_cols;
                        collect_subtree_columns(source_agg->children()[0], input_cols);
                        auto* src_select = static_cast<node_select_t*>(src_select_wrapped.get());
                        if (filter_supported_through_identity_select(*src_select, filter_cols, input_cols)) {
                            auto width_full = estimate_row_width(source_agg->children()[0]);
                            auto width_proj = estimate_projection_width(*src_select, source_agg->children()[0]);
                            // Veto only if both widths are known and the projection is strictly narrower than its input.
                            bool cost_ok = !width_full || !width_proj || *width_full <= *width_proj;
                            if (cost_ok) {
                                source_agg->append_child(match_child);
                                return pushdown_filter_impl(resource, source);
                            }
                        }
                    }
                }

                if (source_has_group && !source_has_sort && !source_has_match) {
                    node_ptr src_group = nullptr;
                    for (size_t i = 1; i < source_agg->children().size(); ++i) {
                        if (source_agg->children()[i]->type() == node_type::group_t) {
                            src_group = source_agg->children()[i];
                            break;
                        }
                    }
                    bool is_projection = true;
                    std::set<std::string> output_cols;
                    for (const auto& expr : src_group->expressions()) {
                        if (expr->group() == expression_group::aggregate) {
                            is_projection = false;
                            break;
                        }
                        if (expr->group() == expression_group::scalar) {
                            auto* sc = static_cast<scalar_expression_t*>(expr.get());
                            output_cols.insert(sc->key().as_string());
                        }
                    }

                    if (is_projection && !match_child->expressions().empty()) {
                        auto filter_cols = collect_referenced_columns(match_child->expressions()[0]);
                        bool subset = std::includes(output_cols.begin(),
                                                    output_cols.end(),
                                                    filter_cols.begin(),
                                                    filter_cols.end());
                        if (subset) {
                            source_agg->append_child(match_child);
                            return pushdown_filter_impl(resource, source);
                        }
                    }
                }

                if (source_has_group && !source_has_sort && !source_has_match) {
                    node_ptr src_group = nullptr;
                    for (size_t i = 1; i < source_agg->children().size(); ++i) {
                        if (source_agg->children()[i]->type() == node_type::group_t) {
                            src_group = source_agg->children()[i];
                            break;
                        }
                    }
                    std::set<std::string> group_keys;
                    for (const auto& expr : src_group->expressions()) {
                        if (expr->group() == expression_group::scalar) {
                            auto* sc = static_cast<scalar_expression_t*>(expr.get());
                            if (sc->type() == scalar_type::group_field) {
                                group_keys.insert(sc->key().as_string());
                            }
                        }
                    }
                    if (!group_keys.empty() && !match_child->expressions().empty()) {
                        auto conjuncts = split_conjuncts(resource, match_child->expressions()[0]);
                        std::pmr::vector<expression_ptr> pushable{resource}, residual{resource};
                        for (const auto& conj : conjuncts) {
                            auto cols = collect_referenced_columns(conj);
                            if (!cols.empty() &&
                                std::includes(group_keys.begin(), group_keys.end(), cols.begin(), cols.end())) {
                                pushable.push_back(conj);
                            } else {
                                residual.push_back(conj);
                            }
                        }
                        if (!pushable.empty()) {
                            auto [m_db, m_rel] = node_cfn(match_child);
                            source_agg->append_child(
                                make_node_match(resource, m_db, m_rel, rebuild_conjunction(resource, pushable)));
                            auto residual_expr = rebuild_conjunction(resource, residual);
                            if (!residual_expr) {
                                return pushdown_filter_impl(resource, source);
                            }
                            match_child->expressions()[0] = residual_expr;
                            node->children()[0] = pushdown_filter_impl(resource, source);
                            return node;
                        }
                    }
                }
            }

            if (source->type() == node_type::join_t) {
                auto* join = static_cast<node_join_t*>(source.get());
                // a join is binary: child[0] = left input, child[1] = right input.
                if (join->children().size() >= 2 && !match_child->expressions().empty()) {
                    std::set<std::string> left_cols, right_cols;
                    collect_subtree_columns(join->children()[0], left_cols);
                    collect_subtree_columns(join->children()[1], right_cols);

                    // Width of the left child's output = the merged prefix that the left
                    // columns occupy. A right-side column's merged path()[0] is
                    // left_width + its local index, so pushing a right-side filter into
                    // the right child requires subtracting left_width (relocalize_keys).
                    // Read it from the child's stamped output_types() — reliable for a
                    // validator-stamped scan/cross join AND a promoted inner join (which
                    // promote_cross_join now stamps). MUST be captured BEFORE the left
                    // bucket wraps children()[0] in an unstamped aggregate below.
                    const bool left_width_known = join->children()[0]->has_output_types();
                    const size_t left_width =
                        left_width_known ? join->children()[0]->output_types().size() : 0;

                    // Only push below a row-preserving side of an outer join
                    // Left preserves left, right preserves right, full preserves none, inner/cross preserve both.
                    const auto jt = join->type();
                    const bool can_push_left =
                        jt == join_type::inner || jt == join_type::cross || jt == join_type::left;
                    // A right-side push also needs a known left_width to re-localize the
                    // conjunct; without it, keep the conjunct in the residual (safe no-op,
                    // rules 2/9) rather than push an out-of-range merged path.
                    const bool can_push_right =
                        (jt == join_type::inner || jt == join_type::cross || jt == join_type::right) &&
                        left_width_known;

                    auto conjuncts = split_conjuncts(resource, match_child->expressions()[0]);
                    std::pmr::vector<expression_ptr> left_bucket{resource}, right_bucket{resource}, residual{resource};
                    for (const auto& conj : conjuncts) {
                        auto cols = collect_referenced_columns(conj);
                        bool in_left = !cols.empty() &&
                                       std::includes(left_cols.begin(), left_cols.end(), cols.begin(), cols.end());
                        bool in_right = !cols.empty() &&
                                        std::includes(right_cols.begin(), right_cols.end(), cols.begin(), cols.end());
                        if (in_left && !in_right && can_push_left) {
                            left_bucket.push_back(conj);
                        } else if (in_right && !in_left && can_push_right) {
                            right_bucket.push_back(conj);
                        } else {
                            residual.push_back(conj);
                        }
                    }

                    if (!left_bucket.empty() || !right_bucket.empty()) {
                        auto [m_db, m_rel] = node_cfn(match_child);
                        if (!left_bucket.empty()) {
                            auto [l_db, l_rel] = node_cfn(join->children()[0]);
                            auto new_agg = make_node_aggregate(resource, l_db, l_rel);
                            new_agg->append_child(join->children()[0]);
                            new_agg->append_child(
                                make_node_match(resource, m_db, m_rel, rebuild_conjunction(resource, left_bucket)));
                            join->children()[0] = boost::static_pointer_cast<node_t>(new_agg);
                        }
                        if (!right_bucket.empty()) {
                            auto [r_db, r_rel] = node_cfn(join->children()[1]);
                            // Re-localize each pushed right-side conjunct from the join's
                            // merged coordinates to the right child's local space (subtract
                            // left_width). Each conjunct lands in exactly one bucket, so the
                            // right-bucket entries are not shared with the residual or the
                            // left bucket — mutating their keys in place is safe. The nested
                            // recursion below re-applies this at each deeper level with that
                            // level's own left_width, so a deep left-then-right push
                            // localizes step by step.
                            for (const auto& conj : right_bucket) {
                                relocalize_keys(conj, left_width, resource);
                            }
                            auto new_agg = make_node_aggregate(resource, r_db, r_rel);
                            new_agg->append_child(join->children()[1]);
                            new_agg->append_child(
                                make_node_match(resource, m_db, m_rel, rebuild_conjunction(resource, right_bucket)));
                            join->children()[1] = boost::static_pointer_cast<node_t>(new_agg);
                        }
                        auto residual_expr = rebuild_conjunction(resource, residual);
                        if (!residual_expr) {
                            // All conjuncts pushed below the join → the match is empty.
                            // Drop ONLY the (now-empty) match child, NOT the enclosing
                            // aggregate: returning `source` here would discard node's
                            // group_t/sort_t — reachable now that Stage 3 lifted the
                            // group/sort bail for this join branch (e.g. SSB
                            // `SUM(...) ... GROUP BY ... ORDER BY` whose WHERE, after
                            // promote moved the equi onto the join ON, is entirely
                            // single-table filters). Preserve the aggregate; recurse
                            // into the pushed join. The match sits at index >= 1, the
                            // join at index 0, so erasing it does not shift index 0.
                            auto& agg_children = node->children();
                            for (size_t i = 0; i < agg_children.size(); ++i) {
                                if (agg_children[i] == match_child) {
                                    agg_children.erase(agg_children.begin() + static_cast<std::ptrdiff_t>(i));
                                    break;
                                }
                            }
                            auto pushed_source = pushdown_filter_impl(resource, source);
                            // If the aggregate now wraps ONLY the join (its match was the
                            // sole pipeline stage), it is a redundant pass-through — expose
                            // the pushed join directly (the canonical minimal plan the
                            // unit tests assert). Keep the aggregate only when a group_t/
                            // sort_t (or other pipeline stage) still needs it (the SSB
                            // SUM/GROUP BY/ORDER BY case, which then keeps its residual).
                            if (node->children().size() == 1) {
                                return pushed_source;
                            }
                            node->children()[0] = pushed_source;
                            return node;
                        }
                        match_child->expressions()[0] = residual_expr;
                        node->children()[0] = pushdown_filter_impl(resource, source);
                        return node;
                    }
                }
            }

            return node;
        }

    } // anonymous namespace

    logical_plan::node_ptr pushdown_filter(std::pmr::memory_resource* resource, logical_plan::node_ptr node) {
        return pushdown_filter_impl(resource, std::move(node));
    }

} // namespace components::planner::optimizer
