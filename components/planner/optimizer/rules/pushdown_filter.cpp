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

        // The ONE traversal of an expression's referenced column keys: compare
        // operands (recursing through union connectives and nested expressions),
        // scalar / aggregate params, sort keys and function args. Every collector
        // below rides it, so the key set that NAMES a conjunct's columns, the set
        // that CLASSIFIES its side and the set that gets RE-LOCALIZED are identical
        // by construction. Fn is invoked as fn(key_t&) for every referenced key.
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
                    fn(static_cast<sort_expression_t*>(expr.get())->key());
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

        // --- side classification by the validator's stamped merged path -----------
        //
        // A join's WHERE keys are stamped by validate_schema against the join's MERGED
        // schema: a left-child column sits in the merged prefix [0, left_width), a
        // right-child column in [left_width, left_width + right_width). So path()[0]
        // alone tells which side a column belongs to — the SAME range test
        // promote_cross_join uses. side() cannot be used: the validator stamps
        // side=left on EVERY unqualified join-WHERE key (it resolves them against the
        // merged schema), so an unqualified right-side column carries side=left with a
        // right-range path. Correctness of the range test rests on the validator
        // rejecting genuinely ambiguous duplicate bare names, so a resolvable name maps
        // to exactly one merged column (validate_logical_plan.cpp).
        //
        // Gathers the merged path root of every key the conjunct references — the
        // same key set collect_referenced_columns names (both ride
        // for_each_referenced_key); a key lacking a stamped path flips has_unstamped.
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

        // Classify a single-table conjunct to a join side. Primary: the validator's
        // stamped merged path roots (path()[0] < left_width => left child, else right).
        // This routes a conjunct whose bare column name ALSO exists on the other side
        // (e.g. t1.id when t2 also has id) — the name-based test below cannot, because
        // the name is a subset of BOTH sides' alias sets, so it always fell to residual.
        // Fallback (an unvalidated plan whose keys carry no path, or an unknown
        // left_width): the original alias-subset test. A conjunct that references BOTH
        // sides (or no column) is unclassified => residual.
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

            // Name-based fallback (validate_schema has not stamped paths on these keys).
            auto cols = collect_referenced_columns(conj);
            bool in_left =
                !cols.empty() && std::includes(left_cols.begin(), left_cols.end(), cols.begin(), cols.end());
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

        // Re-localize a conjunct's column keys from the join's MERGED coordinate space
        // to the right child's LOCAL space when a right-side single-table filter is
        // pushed below the join. A key's merged path()[0] equals its local index only
        // for the left prefix ([0, left_width)); a right-side column sits at
        // left_width + local, so pushing it into the right child unchanged leaves an
        // out-of-range column index. Subtract left_width from the leading path element
        // (deeper elements index nested struct fields and stay put). The new path is
        // built on `resource` — never set_path({...}), which pulls the default resource.
        // The left bucket needs no rewrite (merged == local there) and the
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

        // Re-localizes every key the pushed conjunct references — the same key set
        // the side classifier saw (both ride for_each_referenced_key), so every
        // right-side key is rewritten.
        void relocalize_keys(const expression_ptr& expr, size_t left_width, std::pmr::memory_resource* resource) {
            for_each_referenced_key(expr, [&](key_t& k) { relocalize_key_path(k, left_width, resource); });
        }

        // --- identity-projection resolution ----------------------------------------
        //
        // Both pushdown paths that route a filter THROUGH a projection (the
        // identity-select consumer branch and the CTE-body prefix test below) share
        // this single resolver of "is this output column an identity projection".
        //
        // Probe one visible projection output against a filter column `col`:
        //   name_match — the output is a get_field NAMED col;
        //   source     — non-null iff that output is an IDENTITY of col: its
        //                base-source key (the sole key param when renamed /
        //                explicit, else the expression key itself) carries the
        //                SAME name. validate_schema stamped the source key's
        //                path()[0] to the incoming column index.
        // A name-matching but computed/renamed output yields {true, nullptr}.
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
                    // A name-matching but non-identity output is skipped: a later
                    // output may still expose `col` identically (position is
                    // irrelevant here — the filter evaluates by NAME above the
                    // projection).
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

        // --- push a WHERE into an inlined single-table sub-plan / CTE body ---------
        //
        // A non-recursive CTE reference (and a plain FROM-subquery) is INLINED by the
        // SQL transformer as a source aggregate BOUND TO ITS BASE TABLE
        // (table_oid() != INVALID_OID) whose pipeline stages (select / sort / its own
        // WHERE) sit at children[0..] with the scan IMPLICIT in the aggregate identity
        // — there is NO separate data child at index 0. The generic aggregate-source
        // branch handles only the OTHER shape (a sub-query over an in-memory data /
        // non-scan child at index 0, pipeline at [1..]); its loops start at i=1 and
        // miss this one. create_plan_aggregate lowers a table-scan aggregate on the
        // canonical `base -> match -> group -> sort -> select` chain, so a match child
        // of the body aggregate lands AT the base scan (a full_scan predicate = disk
        // pushdown + column pruning) — exactly the goal.
        //
        // A pushed conjunct's keys keep their paths, stamped in the body's OUTPUT
        // coordinates. Predicate evaluation reads columns BY PATH INDEX against the
        // BASE scan chunk (predicates::create_value_getter -> chunk.at(path)), so the
        // push is sound ONLY when a referenced column's body-output ordinal already
        // equals its base column index — a LEADING-PREFIX IDENTITY projection like
        // `SELECT a, b FROM t(a,b,c)`. A reorder (`SELECT b, a`) or rename (`a AS x`)
        // fails this test and the conjunct stays above the body (residual). `SELECT *`
        // (no projection) is trivially prefix-identity: the body output IS the base
        // scan in base order.
        //
        // `sel` is the body's projection. Returns true iff EVERY column in `cols` is a
        // position-preserving identity output of `sel`.
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
                        continue; // output at position p is not this column
                    }
                    // The FIRST name match decides. Position-preserving iff the
                    // stamped base column index equals the output ordinal p; a
                    // computed / renamed output (no source) or an unstamped (empty)
                    // path cannot be proven safe -> not identity.
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
            // Single canonical source: a node's own validate_schema-stamped
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
                    // A projected constant is stamped alias-less (no type extension);
                    // complex_logical_type::alias() asserts on that, so guard with
                    // has_alias(). Such a column can never match a predicate name.
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
                    // alias() asserts on an alias-less (extension-free) column type.
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

        // --- transitive equi-predicate propagation --------------------------------
        //
        // Given an equi-join `... ON a.x = b.y` and a WHERE predicate on ONE of the
        // join-key columns, the SAME predicate holds on the equality partner: on a
        // matched row a.x == b.y, so `a.x OP c` <=> `b.y OP c`. Synthesizing the
        // partner predicate here (BEFORE bucketing) lets the existing merged-path
        // bucketer route it to the OTHER side's scan (the classic star-schema win: a
        // literal on the fact join key reaches every joined dimension).
        //
        // Soundness rests on INNER/CROSS-only: on a null-padded outer side a preserved
        // row has partner == NULL, so `partner OP c` would wrongly drop it — the caller
        // gates the whole derivation on the join being inner/cross.

        // Only these comparison ops transport across an equality. IS NULL / IS NOT NULL
        // (excluded — they do not transport through `=`), LIKE/regex, ANY/ALL and the
        // union connectives are all rejected: they are not simple key-vs-const
        // predicates or do not preserve under substitution of an equal value.
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

        // An equi-join key pair carried on the join ON condition. The ON keys are
        // stamped SIDE-LOCAL (validate_key resolves each against its own side's schema;
        // promote_cross_join re-localizes the same way), so the left key's local index
        // equals its MERGED index (the left child spans the merged prefix) and the
        // right key's merged index is left_width + its local index. We keep each side's
        // full key (name + side + local path) so a synthesized partner predicate NAMES
        // the partner column and rides the existing merged-path bucketer + relocalizer
        // unchanged: a right partner is stamped at its merged index and relocalize_keys
        // later subtracts left_width back to the right-local index.
        struct equi_pair_t {
            key_t left_key;
            key_t right_key;
            size_t left_merged;
            size_t right_merged;
        };

        // Extract an equi-pair from one ON conjunct `eq(key, key)` with both operands a
        // single top-level column on opposite sides. Mirrors hash_join's
        // detect_equi_columns (side-local paths + side()), returning the pair in the
        // join's merged coordinate space. nullopt for anything else (non-eq, const
        // operand, nested-field path, same-side).
        std::optional<equi_pair_t> equi_pair_from_conjunct(const expression_ptr& on_conj, size_t left_width) {
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
                return equi_pair_t{a, b, a.path()[0], left_width + b.path()[0]};
            }
            if (a.side() == side_t::right && b.side() == side_t::left) {
                return equi_pair_t{b, a, b.path()[0], left_width + a.path()[0]};
            }
            return std::nullopt;
        }

        std::pmr::vector<equi_pair_t>
        collect_equi_pairs(std::pmr::memory_resource* resource, const expression_ptr& on_expr, size_t left_width) {
            std::pmr::vector<equi_pair_t> pairs{resource};
            for (const auto& c : split_conjuncts(resource, on_expr)) {
                if (auto p = equi_pair_from_conjunct(c, left_width)) {
                    pairs.push_back(std::move(*p));
                }
            }
            return pairs;
        }

        // A WHERE conjunct of the transportable shape `key OP param` / `param OP key`,
        // where key is a single top-level column and the other operand a bound
        // parameter. `col` points into the conjunct (valid while it lives).
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

        // Does the conjunct set already assert `<merged column> OP param`? Used to
        // suppress a duplicate derivation (the partner filter was written explicitly).
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

        // One pass (no fixpoint) over the ORIGINAL WHERE conjuncts: for each
        // transportable `key OP param` whose key is an equi-pair column, synthesize the
        // same predicate on the partner column (in merged coordinates) and append it.
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
                    // Copy the ON partner key (name + side) and stamp its MERGED path so
                    // the existing bucketer routes it and relocalize_keys localizes it
                    // below the partner scan. Reuse the SAME parameter (no value clone).
                    key_t partner = *partner_on_key;
                    std::pmr::vector<size_t> p{resource};
                    p.push_back(partner_merged);
                    partner.set_path(std::move(p));
                    derived.push_back(kc->key_on_left
                                          ? make_compare_expression(resource, kc->op, partner, kc->param)
                                          : make_compare_expression(resource, kc->op, kc->param, partner));
                }
            }
            conjuncts.insert(conjuncts.end(), derived.begin(), derived.end());
        }

        // A consumer aggregate whose WHERE was fully pushed down may be collapsed
        // into its sole remaining child ONLY when the node itself carries no
        // semantics of its own. node_aggregate_t payload that would be silently
        // dropped by a collapse: the DISTINCT / DISTINCT ON dedup lives on the
        // aggregate node (not on any child), result_alias names a FROM-subquery's
        // output, and projected_cols is a scan-projection annotation (column_pruning
        // runs after this rule, so it is normally empty here — checked anyway).
        // read_cap is likewise stamped only by the later pushdown_limit rule.
        // Pipeline stages (group/sort/select/limit children) keep the node at
        // children().size() > 1, which every collapse site already checks.
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
                    // conjunct; without it, keep the conjunct in the residual (safe
                    // no-op) rather than push an out-of-range merged path.
                    const bool can_push_right =
                        (jt == join_type::inner || jt == join_type::cross || jt == join_type::right) &&
                        left_width_known;

                    auto conjuncts = split_conjuncts(resource, match_child->expressions()[0]);

                    // Transitive equi-predicate propagation. INNER/CROSS only: on a
                    // null-padded outer side the partner is NULL, so deriving a partner
                    // predicate would wrongly drop preserved rows. Needs a known
                    // left_width to place the partner in merged coordinates (and to
                    // interpret the side-local ON right-key path). Synthesized conjuncts
                    // are appended to `conjuncts` so the bucketing below routes each to
                    // its partner side exactly like an explicit single-table filter.
                    if ((jt == join_type::inner || jt == join_type::cross) && left_width_known &&
                        !join->expressions().empty()) {
                        auto pairs = collect_equi_pairs(resource, join->expressions().front(), left_width);
                        derive_transitive_conjuncts(resource, pairs, conjuncts);
                    }

                    std::pmr::vector<expression_ptr> left_bucket{resource}, right_bucket{resource}, residual{resource};
                    for (const auto& conj : conjuncts) {
                        // Classify by the validator's stamped merged path (side-based),
                        // falling back to alias names only when the plan is unvalidated.
                        // The row-preserving outer-join guard (can_push_left /
                        // can_push_right) is applied UNCHANGED on top of the result, so a
                        // filter on a null-padded side still stays in the residual.
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
                            // group_t/sort_t — reachable now that the group/sort bail
                            // was lifted for this join branch (e.g. SSB
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
                            // sole pipeline stage) AND carries no payload of its own
                            // (DISTINCT/DISTINCT ON/result_alias live on the aggregate
                            // node), it is a redundant pass-through — expose the pushed
                            // join directly (the canonical minimal plan the unit tests
                            // assert). Keep the aggregate when a group_t/sort_t (or other
                            // pipeline stage) still needs it (the SSB SUM/GROUP BY/ORDER
                            // BY case, which then keeps its residual) or when it carries
                            // a dedup/naming payload a collapse would silently drop.
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
                // Push the WHERE below a UNION / UNION ALL by cloning it above EACH branch.
                // A union is N-ary (>= 2 branch subplans). Both set-op kinds are safe
                // targets: UNION ALL is pure duplication; plain UNION dedups ABOVE the
                // union, so a row survives the outer filter iff it survived the same
                // filter inside its branch — pushing the identical predicate into every
                // branch preserves both membership and the dedup result.
                //
                // Union output columns are POSITIONAL: output column i is branch column i
                // (validate_schema derives the union schema from the LEFT branch, and the
                // set operation aligns operands by position). The match keys above the
                // union reference the union output columns by NAME; map each name to its
                // union output position, then require EVERY branch to expose the SAME name
                // at that SAME position (an identity mapping). When a branch reorders or
                // renames a referenced position, a shared pushed predicate would target
                // the wrong branch column — so that conjunct is NOT cleanly mappable and
                // stays in the residual above the union (correct, just not pushed),
                // mirroring the join branch's residual bucket. Each branch receives its
                // OWN deep copy of the pushed conjuncts: the per-branch recursion mutates
                // pushed keys in place (relocalize_keys rewrites a right-side key's path
                // when the branch wraps a join), so sharing leaves across branches would
                // leak one branch's re-localized paths into the next branch's filter.
                if (source->children().size() >= 2 && !match_child->expressions().empty() &&
                    source->has_output_types()) {
                    const auto& u_types = source->output_types();

                    // name -> unique union output position (nullopt if absent or duplicated).
                    // An alias-less output column (a projected constant carries no type
                    // extension, and alias() asserts on that) can never match a WHERE
                    // column name — skip it.
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
                    // A branch exposes `name` identically iff its stamped output alias at
                    // the union position equals `name` (alias-less => no match, guarded).
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
                            // Prefer pushing the predicate INTO the branch's own single-table
                            // aggregate, directly below an IDENTITY projection, so it rides the
                            // branch's disk scan (create_plan_match lowers a table-bound match
                            // over a plain compare to a full_scan). Only safe when the branch is
                            // a single-table aggregate whose projection maps each pushed column
                            // identically (output name == input name): the match keys reference
                            // the union output name, which then equals the scan's column name.
                            // Otherwise (a renaming/computed projection, a join/nested-union
                            // source, or a raw scan) wrap the branch in a filter aggregate — the
                            // predicate is applied above the branch output (in-memory Filter),
                            // still correct.
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

                            // Deep-copy the pushed conjuncts for THIS branch (see the
                            // sharing rationale above): the recursion below may
                            // relocalize the copy's keys in place.
                            std::pmr::vector<expression_ptr> branch_pushed{resource};
                            branch_pushed.reserve(pushable.size());
                            for (const auto& conj : pushable) {
                                branch_pushed.push_back(clone_expression(resource, conj));
                            }
                            auto pushed_match =
                                make_node_match(resource, b_db, b_rel, rebuild_conjunction(resource, branch_pushed));
                            if (push_below_projection) {
                                // Inherit the branch's already-resolved table_oid so
                                // create_plan_match binds the pushed match to the branch table
                                // (enrich also re-stamps it from the copied (db, rel) — same oid).
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
                            // Whole WHERE pushed into every branch → the outer match is empty.
                            // Drop the (now-empty) match child. Keep the enclosing aggregate
                            // if it still carries other pipeline stages (group/sort/select)
                            // OR its own payload (DISTINCT/DISTINCT ON/result_alias — the
                            // dedup lives on the aggregate node, so collapsing would drop
                            // it); otherwise expose the pushed union directly (the minimal
                            // plan), mirroring the join branch.
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

        // --- predicate pushdown INTO an inlined single-table CTE / sub-query body ----
        //
        // Runs as its OWN pass, BEFORE pushdown_filter, on the ORIGINAL tree. Deliberately
        // separate from pushdown_filter_impl: that rule, while pushing a filter below a
        // JOIN, synthesizes `aggregate{ scan, match }` wrappers and recurses into them —
        // and this logic would then fuse those join-branch matches into their scans,
        // altering the join EXPLAIN shape (and destabilizing the join lowering). Running
        // first sidesteps that entirely: at this point no join wrappers exist, and the
        // only source shape targeted here (a table-scan aggregate: table_oid stamped,
        // scan implicit, pipeline at children[0..]) is DISJOINT from the join / union /
        // in-memory-subquery shapes pushdown_filter handles.
        //
        // See select_prefix_identity_for above for why only a LEADING-PREFIX IDENTITY
        // projection is pushable (predicate reads columns by PATH INDEX against the base
        // scan).
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
            // Only an inlined single-table body: a table-scan aggregate (resolved oid, scan
            // implicit). A recursive-CTE reference lowers to an empty-identity aggregate over a
            // node_recursive_cte (no oid) and is therefore left untouched here.
            if (source->type() != node_type::aggregate_t ||
                source->table_oid() == components::catalog::INVALID_OID) {
                return node;
            }
            auto* body = static_cast<node_aggregate_t*>(source.get());

            // LIMIT/OFFSET, GROUP BY, HAVING and DISTINCT are HARD stops: pushing a filter below
            // a LIMIT changes which rows survive (`ORDER BY .. LIMIT` then WHERE != WHERE then
            // LIMIT); below a GROUP/HAVING it would run pre-aggregation against post-aggregation
            // columns; DISTINCT ON dedups below the projection. Be conservative — leave the body
            // untouched (correct, just not pushed) when any is present. A bare SORT is fine: it is
            // row-preserving and the pushed match lands below it (base -> match -> sort).
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
            // With no projection the body output IS the base scan in base order, so every base
            // column is prefix-identity; take the base column set from the stamped output_types().
            std::set<std::string> base_cols;
            if (!sel && source->has_output_types()) {
                for (const auto& t : source->output_types()) {
                    if (t.has_alias()) { // alias() asserts on an alias-less column type
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
                // create_plan_aggregate builds ONE match_op (the last match child wins), so MERGE
                // into the body's own WHERE rather than adding a second match child.
                auto existing = split_conjuncts(resource, body_match->expressions()[0]);
                existing.push_back(pushed_expr);
                body_match->expressions()[0] = rebuild_conjunction(resource, existing);
            } else {
                auto pushed_match = make_node_match(resource, m_db, m_rel, pushed_expr);
                // Inherit the body table's resolved oid so create_plan_match binds the pushed match
                // to the base scan (a plain compare lowers to a full_scan predicate).
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
                // A bare wrapper { body } is a redundant pass-through -> expose the body
                // directly. The consumer's own payload (DISTINCT/DISTINCT ON/result_alias)
                // lives on the aggregate node itself — never collapse it away.
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
