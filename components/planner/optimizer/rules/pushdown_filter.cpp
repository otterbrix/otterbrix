#include "pushdown_filter.hpp"

#include "conjunct_utils.hpp"

#include <optional>
#include <set>
#include <string>
#include <vector>

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/cast_expression.hpp>
#include <components/expressions/clone_expression.hpp>
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
#include <components/logical_plan/node_union.hpp>

namespace components::planner::optimizer {

    namespace {

        using namespace components::expressions;
        using namespace components::logical_plan;

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

        // Single traversal ridden by every collector below, so their key sets stay identical by construction.
        template<typename Fn>
        void for_each_referenced_key(const expression_ptr& expr, Fn&& fn);

        template<typename Fn>
        void for_each_key_in_param(param_storage& param, Fn&& fn) {
            if (is_key(param)) {
                fn(as_key(param));
            } else if (is_expr(param)) {
                for_each_referenced_key(as_expr(param), fn);
            }
        }

        template<typename Fn>
        void for_each_referenced_key(const expression_ptr& expr, Fn&& fn) {
            if (!expr) {
                return;
            }
            switch (expr->group()) {
                case expression_group::compare: {
                    auto* cmp = static_cast<compare_expression_t*>(expr.get());
                    if (is_union_compare_condition(cmp->type())) {
                        for (auto& child : cmp->children()) {
                            for_each_referenced_key(child, fn);
                        }
                    } else {
                        for_each_key_in_param(cmp->left(), fn);
                        for_each_key_in_param(cmp->right(), fn);
                    }
                    break;
                }
                case expression_group::scalar: {
                    auto* sc = static_cast<scalar_expression_t*>(expr.get());
                    for (auto& param : sc->params()) {
                        for_each_key_in_param(param, fn);
                    }
                    break;
                }
                case expression_group::aggregate: {
                    auto* agg = static_cast<aggregate_expression_t*>(expr.get());
                    for (auto& param : agg->params()) {
                        for_each_key_in_param(param, fn);
                    }
                    break;
                }
                case expression_group::sort: {
                    for_each_key_in_param(static_cast<sort_expression_t*>(expr.get())->operand(), fn);
                    break;
                }
                case expression_group::function: {
                    auto* func = static_cast<function_expression_t*>(expr.get());
                    for (auto& arg : func->args()) {
                        for_each_key_in_param(arg, fn);
                    }
                    break;
                }
                case expression_group::cast: {
                    for_each_key_in_param(static_cast<cast_expression_t*>(expr.get())->child(), fn);
                    break;
                }
                default:
                    break;
            }
        }

        std::set<std::string> collect_referenced_columns(const expression_ptr& expr) {
            std::set<std::string> result;
            for_each_referenced_key(expr, [&](const key_t& k) { result.insert(k.as_string()); });
            return result;
        }

        // side() can't classify a join-WHERE key: validate_schema stamps side=left on every unqualified
        // key, so an unqualified right-side column can carry side=left with a right-range path()[0].
        void collect_referenced_path_roots(const expression_ptr& expr,
                                           std::vector<size_t>& roots,
                                           bool& has_key,
                                           bool& has_unstamped) {
            for_each_referenced_key(expr, [&](const key_t& k) {
                has_key = true;
                if (k.path().empty()) {
                    has_unstamped = true; // an unvalidated plan — caller falls back to names
                } else {
                    roots.push_back(k.path()[0]);
                }
            });
        }

        enum class conj_side
        {
            left_side,
            right_side,
            unclassified
        };

        // Primary path handles a bare column name that also exists on the other side (e.g. t1.id vs t2.id).
        conj_side classify_conjunct(const expression_ptr& conj,
                                    size_t left_width,
                                    bool left_width_known,
                                    const std::set<std::string>& left_cols,
                                    const std::set<std::string>& right_cols) {
            std::vector<size_t> roots;
            bool has_key = false;
            bool has_unstamped = false;
            collect_referenced_path_roots(conj, roots, has_key, has_unstamped);

            if (left_width_known && has_key && !has_unstamped) {
                bool any_left = false;
                bool any_right = false;
                for (size_t r : roots) {
                    if (r < left_width) {
                        any_left = true;
                    } else {
                        any_right = true;
                    }
                }
                if (any_left && !any_right) {
                    return conj_side::left_side;
                }
                if (any_right && !any_left) {
                    return conj_side::right_side;
                }
                return conj_side::unclassified; // straddles both sides
            }

            // Fallback for plans validate_schema has not stamped.
            auto cols = collect_referenced_columns(conj);
            bool in_left = !cols.empty() && std::includes(left_cols.begin(), left_cols.end(), cols.begin(), cols.end());
            bool in_right =
                !cols.empty() && std::includes(right_cols.begin(), right_cols.end(), cols.begin(), cols.end());
            if (in_left && !in_right) {
                return conj_side::left_side;
            }
            if (in_right && !in_left) {
                return conj_side::right_side;
            }
            return conj_side::unclassified;
        }

        // Subtracts left_width from the leading path element only (deeper elements are nested struct
        // fields). Build the new path on k.resource(): set_path move-assigns and keeps the target's allocator.
        void relocalize_key_path(key_t& k, size_t left_width) {
            const auto& old_path = k.path();
            if (old_path.empty()) {
                return;
            }
            std::pmr::vector<size_t> p{k.resource()};
            p.reserve(old_path.size());
            p.push_back(old_path[0] - left_width);
            for (size_t i = 1; i < old_path.size(); ++i) {
                p.push_back(old_path[i]);
            }
            k.set_path(std::move(p));
        }

        void relocalize_keys(const expression_ptr& expr, size_t left_width) {
            for_each_referenced_key(expr, [&](key_t& k) { relocalize_key_path(k, left_width); });
        }

        // source is nullptr for a name-matching but computed/renamed (non-identity) output.
        struct identity_probe_t {
            bool name_match;
            const key_t* source;
        };

        identity_probe_t probe_identity_output(const scalar_expression_t* sc, const std::string& col) {
            if (sc->type() != scalar_type::get_field || sc->key().as_string() != col) {
                return {false, nullptr};
            }
            const key_t* in = nullptr;
            if (sc->params().empty()) {
                in = &sc->key();
            } else if (sc->params().size() == 1 && is_key(sc->params().front())) {
                in = &as_key(sc->params().front());
            }
            if (in == nullptr || in->as_string() != col) {
                return {true, nullptr};
            }
            return {true, in};
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
                    auto probe = probe_identity_output(static_cast<const scalar_expression_t*>(expr.get()), col);
                    if (probe.source != nullptr) {
                        ok_for_col = true;
                        break;
                    }
                }
                if (!ok_for_col) {
                    return false;
                }
            }
            return true;
        }

        // Predicate evaluation reads columns by path index against the base scan chunk, so a push is sound
        // only when a column's body-output ordinal equals its base index (`SELECT a,b FROM t(a,b,c)`).
        bool select_prefix_identity_for(const node_select_t& sel, const std::set<std::string>& cols) {
            const auto& exprs = sel.expressions();
            const size_t hidden = sel.internal_aggregate_count;
            if (exprs.size() < hidden) {
                return false;
            }
            const size_t visible = exprs.size() - hidden;
            for (const auto& col : cols) {
                bool ok = false;
                for (size_t p = 0; p < visible; ++p) {
                    const auto& e = exprs[p];
                    if (e->group() != expression_group::scalar) {
                        continue;
                    }
                    auto probe = probe_identity_output(static_cast<const scalar_expression_t*>(e.get()), col);
                    if (!probe.name_match) {
                        continue;
                    }
                    // The first name match decides, and is identity only if its stamped path equals p.
                    if (probe.source != nullptr && probe.source->path().size() == 1 && probe.source->path()[0] == p) {
                        ok = true;
                    }
                    break;
                }
                if (!ok) {
                    return false;
                }
            }
            return true;
        }

        void collect_subtree_columns(const node_ptr& node, std::set<std::string>& cols) {
            if (!node) {
                return;
            }
            // Never recurse straight to a leaf — a renamed side (`… AS x`) carries the pre-rename name.
            if (node->has_output_types()) {
                for (const auto& t : node->output_types()) {
                    // alias() asserts on an alias-less (projected-constant) column; guard with has_alias().
                    if (t.has_alias()) {
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

        // nullopt when the width can't be estimated (computed/constant column); otherwise summed width.
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
                    if (t.has_alias() && t.alias() == out_name) {
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

        // A WHERE predicate on one equi-join key also holds on its partner — sound only for INNER/CROSS
        // (a null-padded outer row's partner is NULL, so deriving `partner OP c` would wrongly drop it).
        bool is_transportable_compare(compare_type t) {
            switch (t) {
                case compare_type::eq:
                case compare_type::ne:
                case compare_type::gt:
                case compare_type::lt:
                case compare_type::gte:
                case compare_type::lte:
                    return true;
                default:
                    return false;
            }
        }

        // ON keys are stamped SIDE-LOCAL: the right key's merged index is left_width + its local index.
        struct equi_pair_t {
            key_t left_key;
            key_t right_key;
            size_t left_merged;
            size_t right_merged;
        };

        // `resource` is explicit because key_t's plain copy ctor lands an un-placed copy on the process default.
        std::optional<equi_pair_t>
        equi_pair_from_conjunct(std::pmr::memory_resource* resource, const expression_ptr& on_conj, size_t left_width) {
            if (!on_conj || on_conj->group() != expression_group::compare) {
                return std::nullopt;
            }
            auto* cmp = static_cast<compare_expression_t*>(on_conj.get());
            if (cmp->type() != compare_type::eq || !is_key(cmp->left()) || !is_key(cmp->right())) {
                return std::nullopt;
            }
            const auto& a = as_key(cmp->left());
            const auto& b = as_key(cmp->right());
            if (a.path().size() != 1 || b.path().size() != 1) {
                return std::nullopt;
            }
            if (a.side() == side_t::left && b.side() == side_t::right) {
                return equi_pair_t{key_t{a, resource}, key_t{b, resource}, a.path()[0], left_width + b.path()[0]};
            }
            if (a.side() == side_t::right && b.side() == side_t::left) {
                return equi_pair_t{key_t{b, resource}, key_t{a, resource}, b.path()[0], left_width + a.path()[0]};
            }
            return std::nullopt;
        }

        std::pmr::vector<equi_pair_t>
        collect_equi_pairs(std::pmr::memory_resource* resource, const expression_ptr& on_expr, size_t left_width) {
            std::pmr::vector<equi_pair_t> pairs{resource};
            for (const auto& c : split_conjuncts(resource, on_expr)) {
                if (auto p = equi_pair_from_conjunct(resource, c, left_width)) {
                    pairs.push_back(std::move(*p));
                }
            }
            return pairs;
        }

        // `col` points into the conjunct (valid while it lives).
        struct key_const_conj_t {
            const key_t* col;
            core::parameter_id_t param;
            bool key_on_left;
            compare_type op;
        };

        std::optional<key_const_conj_t> as_key_const_conjunct(const expression_ptr& conj) {
            if (!conj || conj->group() != expression_group::compare) {
                return std::nullopt;
            }
            auto* cmp = static_cast<compare_expression_t*>(conj.get());
            if (!is_transportable_compare(cmp->type())) {
                return std::nullopt;
            }
            if (is_key(cmp->left()) && is_parameter(cmp->right())) {
                const auto& k = as_key(cmp->left());
                if (k.path().size() != 1) {
                    return std::nullopt;
                }
                return key_const_conj_t{&k, as_parameter(cmp->right()), true, cmp->type()};
            }
            if (is_parameter(cmp->left()) && is_key(cmp->right())) {
                const auto& k = as_key(cmp->right());
                if (k.path().size() != 1) {
                    return std::nullopt;
                }
                return key_const_conj_t{&k, as_parameter(cmp->left()), false, cmp->type()};
            }
            return std::nullopt;
        }

        bool conjunct_set_has(const std::pmr::vector<expression_ptr>& conjuncts,
                              size_t merged_idx,
                              compare_type op,
                              core::parameter_id_t param) {
            for (const auto& c : conjuncts) {
                auto kc = as_key_const_conjunct(c);
                if (kc && kc->op == op && kc->param == param && kc->col->path()[0] == merged_idx) {
                    return true;
                }
            }
            return false;
        }

        // One pass (no fixpoint) over the original WHERE conjuncts.
        void derive_transitive_conjuncts(std::pmr::memory_resource* resource,
                                         const std::pmr::vector<equi_pair_t>& pairs,
                                         std::pmr::vector<expression_ptr>& conjuncts) {
            if (pairs.empty()) {
                return;
            }
            std::pmr::vector<expression_ptr> derived{resource};
            const size_t n = conjuncts.size();
            for (size_t i = 0; i < n; ++i) {
                auto kc = as_key_const_conjunct(conjuncts[i]);
                if (!kc) {
                    continue;
                }
                const size_t m = kc->col->path()[0];
                for (const auto& pr : pairs) {
                    const key_t* partner_on_key = nullptr;
                    size_t partner_merged = 0;
                    if (m == pr.left_merged) {
                        partner_on_key = &pr.right_key;
                        partner_merged = pr.right_merged;
                    } else if (m == pr.right_merged) {
                        partner_on_key = &pr.left_key;
                        partner_merged = pr.left_merged;
                    } else {
                        continue;
                    }
                    if (partner_merged == m) {
                        continue; // degenerate self-equi
                    }
                    if (conjunct_set_has(conjuncts, partner_merged, kc->op, kc->param) ||
                        conjunct_set_has(derived, partner_merged, kc->op, kc->param)) {
                        continue; // partner predicate already present — avoid duplicates
                    }
                    // Built on partner.resource(), never a plain copy. Verified zero extra allocation in
                    // test_pushdown_key_arena.cpp ("derivation_allocates_on_the_named_arena").
                    key_t partner{*partner_on_key, resource};
                    std::pmr::vector<size_t> p{partner.resource()};
                    p.push_back(partner_merged);
                    partner.set_path(std::move(p));
                    derived.push_back(kc->key_on_left ? make_compare_expression(resource, kc->op, partner, kc->param)
                                                      : make_compare_expression(resource, kc->op, kc->param, partner));
                }
            }
            conjuncts.insert(conjuncts.end(), derived.begin(), derived.end());
        }

        // Collapsing would drop payload living only on the aggregate node (DISTINCT/result_alias);
        // read_cap is skipped — it is stamped only by the later pushdown_limit rule.
        bool aggregate_is_passthrough(const node_aggregate_t& agg) {
            return !agg.is_distinct() && agg.distinct_on_keys().empty() && agg.result_alias().empty() &&
                   agg.projected_cols().empty();
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
            // child[0] = data source, child[1..] = pipeline operations.
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

            // A group_t/sort_t above the join does not block the push: execute_pipeline treats the topmost
            // executed operator as the materialized sub-plan boundary (repro: test_batch_execution "join +
            // WHERE with UDF batch predicate", test_column_projection "inner JOIN + GROUP BY with WHERE on
            // non-select column").
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
                            // Veto only if both widths are known and the projection is narrower.
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
                if (join->children().size() >= 2 && !match_child->expressions().empty()) {
                    std::set<std::string> left_cols, right_cols;
                    collect_subtree_columns(join->children()[0], left_cols);
                    collect_subtree_columns(join->children()[1], right_cols);

                    // Must be captured before the left bucket wraps children()[0] in an unstamped aggregate.
                    const bool left_width_known = join->children()[0]->has_output_types();
                    const size_t left_width = left_width_known ? join->children()[0]->output_types().size() : 0;

                    // Only push below a row-preserving side (full preserves neither).
                    const auto jt = join->type();
                    const bool can_push_left =
                        jt == join_type::inner || jt == join_type::cross || jt == join_type::left;
                    const bool can_push_right =
                        (jt == join_type::inner || jt == join_type::cross || jt == join_type::right) &&
                        left_width_known;

                    auto conjuncts = split_conjuncts(resource, match_child->expressions()[0]);

                    if ((jt == join_type::inner || jt == join_type::cross) && left_width_known &&
                        !join->expressions().empty()) {
                        auto pairs = collect_equi_pairs(resource, join->expressions().front(), left_width);
                        derive_transitive_conjuncts(resource, pairs, conjuncts);
                    }

                    std::pmr::vector<expression_ptr> left_bucket{resource}, right_bucket{resource}, residual{resource};
                    for (const auto& conj : conjuncts) {
                        conj_side cs = classify_conjunct(conj, left_width, left_width_known, left_cols, right_cols);
                        if (cs == conj_side::left_side && can_push_left) {
                            left_bucket.push_back(conj);
                        } else if (cs == conj_side::right_side && can_push_right) {
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
                            // Each conjunct lands in exactly one bucket, so mutating keys in place is safe.
                            for (const auto& conj : right_bucket) {
                                relocalize_keys(conj, left_width);
                            }
                            auto new_agg = make_node_aggregate(resource, r_db, r_rel);
                            new_agg->append_child(join->children()[1]);
                            new_agg->append_child(
                                make_node_match(resource, m_db, m_rel, rebuild_conjunction(resource, right_bucket)));
                            join->children()[1] = boost::static_pointer_cast<node_t>(new_agg);
                        }
                        auto residual_expr = rebuild_conjunction(resource, residual);
                        if (!residual_expr) {
                            // Drop only the match child: returning `source` would discard group_t/sort_t.
                            auto& agg_children = node->children();
                            for (size_t i = 0; i < agg_children.size(); ++i) {
                                if (agg_children[i] == match_child) {
                                    agg_children.erase(agg_children.begin() + static_cast<std::ptrdiff_t>(i));
                                    break;
                                }
                            }
                            auto pushed_source = pushdown_filter_impl(resource, source);
                            // See aggregate_is_passthrough for what a collapse here would drop.
                            if (node->children().size() == 1 && aggregate_is_passthrough(*agg)) {
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

            if (source->type() == node_type::union_t) {
                // Sound for both set-op kinds (UNION ALL duplicates, UNION dedups above the union). Union
                // columns are positional, so a name-based match key is pushable only when every branch
                // exposes it at that position; each branch gets its own deep copy (keys relocalize in place).
                if (source->children().size() >= 2 && !match_child->expressions().empty() &&
                    source->has_output_types()) {
                    const auto& u_types = source->output_types();

                    auto union_pos_of = [&](const std::string& name) -> std::optional<size_t> {
                        std::optional<size_t> found;
                        for (size_t i = 0; i < u_types.size(); ++i) {
                            if (!u_types[i].has_alias()) {
                                continue;
                            }
                            if (u_types[i].alias() == name) {
                                if (found) {
                                    return std::nullopt; // ambiguous
                                }
                                found = i;
                            }
                        }
                        return found;
                    };
                    auto branch_identity = [](const node_ptr& branch, const std::string& name, size_t pos) {
                        if (!branch || !branch->has_output_types()) {
                            return false;
                        }
                        const auto& b = branch->output_types();
                        return pos < b.size() && b[pos].has_alias() && b[pos].alias() == name;
                    };

                    auto conjuncts = split_conjuncts(resource, match_child->expressions()[0]);
                    std::pmr::vector<expression_ptr> pushable{resource}, residual{resource};
                    for (const auto& conj : conjuncts) {
                        auto cols = collect_referenced_columns(conj);
                        bool ok = !cols.empty();
                        for (const auto& col : cols) {
                            auto pos = union_pos_of(col);
                            if (!pos) {
                                ok = false;
                                break;
                            }
                            for (const auto& branch : source->children()) {
                                if (!branch_identity(branch, col, *pos)) {
                                    ok = false;
                                    break;
                                }
                            }
                            if (!ok) {
                                break;
                            }
                        }
                        (ok ? pushable : residual).push_back(conj);
                    }

                    if (!pushable.empty()) {
                        auto filter_cols = collect_referenced_columns(rebuild_conjunction(resource, pushable));
                        for (auto& branch : source->children()) {
                            auto [b_db, b_rel] = node_cfn(branch);
                            // Prefer pushing below an identity projection so it rides the branch's disk scan.
                            node_select_t* branch_select = nullptr;
                            if (branch->type() == node_type::aggregate_t &&
                                branch->table_oid() != components::catalog::INVALID_OID) {
                                for (const auto& c : branch->children()) {
                                    if (c->type() == node_type::select_t) {
                                        branch_select = static_cast<node_select_t*>(c.get());
                                        break;
                                    }
                                }
                            }
                            std::set<std::string> branch_out;
                            collect_subtree_columns(branch, branch_out);
                            const bool push_below_projection =
                                branch_select != nullptr &&
                                filter_supported_through_identity_select(*branch_select, filter_cols, branch_out);

                            std::pmr::vector<expression_ptr> branch_pushed{resource};
                            branch_pushed.reserve(pushable.size());
                            for (const auto& conj : pushable) {
                                branch_pushed.push_back(clone_expression(resource, conj));
                            }
                            auto pushed_match =
                                make_node_match(resource, b_db, b_rel, rebuild_conjunction(resource, branch_pushed));
                            if (push_below_projection) {
                                // Inherit the branch's table_oid so create_plan_match binds to that table.
                                pushed_match->set_table_oid(branch->table_oid());
                                static_cast<node_aggregate_t*>(branch.get())->append_child(pushed_match);
                                branch = pushdown_filter_impl(resource, branch);
                            } else {
                                auto new_agg = make_node_aggregate(resource, b_db, b_rel);
                                new_agg->append_child(branch);
                                new_agg->append_child(pushed_match);
                                branch = pushdown_filter_impl(resource, boost::static_pointer_cast<node_t>(new_agg));
                            }
                        }
                        auto residual_expr = rebuild_conjunction(resource, residual);
                        if (!residual_expr) {
                            // Mirrors the join branch above: collapse only when aggregate_is_passthrough holds.
                            auto& agg_children = node->children();
                            for (size_t i = 0; i < agg_children.size(); ++i) {
                                if (agg_children[i] == match_child) {
                                    agg_children.erase(agg_children.begin() + static_cast<std::ptrdiff_t>(i));
                                    break;
                                }
                            }
                            if (node->children().size() == 1 && aggregate_is_passthrough(*agg)) {
                                return source;
                            }
                            return node;
                        }
                        match_child->expressions()[0] = residual_expr;
                        return node;
                    }
                }
            }

            return node;
        }

        // Runs before pushdown_filter_impl: that rule synthesizes join wrappers this pass would otherwise
        // fuse into their scans, altering the join EXPLAIN shape.
        node_ptr pushdown_cte_filter_impl(std::pmr::memory_resource* resource, node_ptr node) {
            if (!node) {
                return node;
            }
            for (size_t i = 0; i < node->children().size(); ++i) {
                auto& child = node->children()[i];
                auto optimized = pushdown_cte_filter_impl(resource, child);
                if (optimized != child) {
                    node->children()[i] = optimized;
                }
            }

            if (node->type() != node_type::aggregate_t || node->children().size() < 2) {
                return node;
            }
            auto* agg = static_cast<node_aggregate_t*>(node.get());

            // The consumer's WHERE (first match child). child[0] is the FROM source.
            node_ptr match_child = nullptr;
            for (size_t i = 1; i < agg->children().size(); ++i) {
                if (agg->children()[i]->type() == node_type::match_t) {
                    match_child = agg->children()[i];
                    break;
                }
            }
            if (!match_child || match_child->expressions().empty()) {
                return node;
            }

            auto source = agg->children()[0];
            // A recursive-CTE reference lowers to an empty-identity aggregate with no oid and is skipped.
            if (source->type() != node_type::aggregate_t || source->table_oid() == components::catalog::INVALID_OID) {
                return node;
            }
            auto* body = static_cast<node_aggregate_t*>(source.get());

            // LIMIT, GROUP BY, HAVING and DISTINCT are hard stops; a bare SORT is fine (row-preserving).
            node_ptr body_select = nullptr;
            node_ptr body_match = nullptr;
            bool body_blocked = body->is_distinct();
            for (const auto& c : body->children()) {
                switch (c->type()) {
                    case node_type::limit_t:
                    case node_type::group_t:
                    case node_type::having_t:
                        body_blocked = true;
                        break;
                    case node_type::select_t:
                        body_select = c;
                        break;
                    case node_type::match_t:
                        body_match = c;
                        break;
                    default:
                        break;
                }
            }
            if (body_blocked) {
                return node;
            }

            auto* sel = body_select ? static_cast<node_select_t*>(body_select.get()) : nullptr;
            // With no projection the body output is the base scan, so every column is prefix-identity.
            std::set<std::string> base_cols;
            if (!sel && source->has_output_types()) {
                for (const auto& t : source->output_types()) {
                    if (t.has_alias()) {
                        base_cols.insert(t.alias());
                    }
                }
            }

            auto conjuncts = split_conjuncts(resource, match_child->expressions()[0]);
            std::pmr::vector<expression_ptr> pushable{resource}, residual{resource};
            for (const auto& conj : conjuncts) {
                auto cols = collect_referenced_columns(conj);
                bool ok = !cols.empty();
                if (ok) {
                    ok = sel ? select_prefix_identity_for(*sel, cols)
                             : std::includes(base_cols.begin(), base_cols.end(), cols.begin(), cols.end());
                }
                (ok ? pushable : residual).push_back(conj);
            }
            if (pushable.empty()) {
                return node;
            }

            auto [m_db, m_rel] = node_cfn(source);
            auto pushed_expr = rebuild_conjunction(resource, pushable);
            if (body_match) {
                // create_plan_aggregate builds one match_op, so merge rather than add a second match child.
                auto existing = split_conjuncts(resource, body_match->expressions()[0]);
                existing.push_back(pushed_expr);
                body_match->expressions()[0] = rebuild_conjunction(resource, existing);
            } else {
                auto pushed_match = make_node_match(resource, m_db, m_rel, pushed_expr);
                pushed_match->set_table_oid(source->table_oid());
                body->append_child(pushed_match);
            }

            auto residual_expr = rebuild_conjunction(resource, residual);
            if (!residual_expr) {
                // Whole WHERE pushed into the body -> drop the (now-empty) consumer match child.
                auto& cc = node->children();
                for (size_t i = 0; i < cc.size(); ++i) {
                    if (cc[i] == match_child) {
                        cc.erase(cc.begin() + static_cast<std::ptrdiff_t>(i));
                        break;
                    }
                }
                if (node->children().size() == 1 && aggregate_is_passthrough(*agg)) {
                    return source;
                }
                return node;
            }
            match_child->expressions()[0] = residual_expr;
            return node;
        }

    } // anonymous namespace

    logical_plan::node_ptr pushdown_filter(std::pmr::memory_resource* resource, logical_plan::node_ptr node) {
        return pushdown_filter_impl(resource, std::move(node));
    }

    logical_plan::node_ptr pushdown_cte_filter(std::pmr::memory_resource* resource, logical_plan::node_ptr node) {
        return pushdown_cte_filter_impl(resource, std::move(node));
    }

} // namespace components::planner::optimizer
