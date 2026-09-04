#include "promote_cross_join.hpp"

#include "conjunct_utils.hpp"

#include <memory_resource>

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_select.hpp>
#include <components/logical_plan/node_sort.hpp>

namespace components::planner::optimizer {

    namespace {

        using namespace components::expressions;
        using namespace components::logical_plan;

        // A conjunct qualifies as a promotable join key iff it is an eq(key, key)
        // whose two single-element paths straddle the join boundary: one path in the
        // left range [0, left_width), the other in the right range
        // [left_width, left_width + right_width). Both keys are stamped side=left by
        // the validator (unqualified names over the merged schema), so we cannot use
        // side_t here — we classify by the merged-relative path index. Correctness of
        // the range test rests on the validator rejecting ambiguous duplicate names
        // (validate_logical_plan.cpp), so a name resolves to exactly one merged column.
        bool is_boundary_equi(const expression_ptr& expr, size_t left_width, size_t right_width) {
            if (!expr || expr->group() != expression_group::compare) {
                return false;
            }
            auto* cmp = static_cast<compare_expression_t*>(expr.get());
            if (cmp->type() != compare_type::eq) {
                return false;
            }
            if (!is_key(cmp->left()) || !is_key(cmp->right())) {
                return false;
            }
            const auto& kl = as_key(cmp->left());
            const auto& kr = as_key(cmp->right());
            if (kl.path().size() != 1 || kr.path().size() != 1) {
                return false;
            }
            const size_t pl = kl.path()[0];
            const size_t pr = kr.path()[0];
            const size_t total = left_width + right_width;
            const bool l_left = pl < left_width;
            const bool l_right = pl >= left_width && pl < total;
            const bool r_left = pr < left_width;
            const bool r_right = pr >= left_width && pr < total;
            return (l_left && r_right) || (l_right && r_left);
        }

        // Promote every CROSS join in a (left-deep / bushy) join subtree to an INNER
        // join, claiming one straddling equi conjunct per join. `conjuncts` are in the
        // OUTER-merged coordinate space (paths stamped by the validator against the
        // whole join's merged schema). For any single join, the left child spans the
        // merged prefix [0, left_width), so a key's merged path equals its column index
        // within the left child; the right child spans [left_width, left_width+right_width)
        // contiguously, so a right-range key re-localizes to mergedIdx - left_width. That
        // holds recursively (a nested join's own merged schema is exactly this join's
        // left-child prefix), so each nested join classifies its two keys against ITS OWN
        // children widths — not the outer schema.
        //
        // Widths are read from the ORIGINAL stamped children BEFORE promoting them: a
        // promoted inner join is a fresh node with no output_types(), but it shares the
        // cross join's schema, so the captured widths stay valid.
        //
        // Returns the (possibly new) subtree root. A leaf / non-join subtree, an
        // unstamped or non-cross join, or a join with no straddling equi is returned
        // unchanged (kept as-is), never throwing — optimizer rules have no error channel.
        // Each claimed conjunct is flagged so a later join cannot re-claim
        // it and so the caller can keep the unclaimed ones as the residual match.
        node_ptr promote_join_subtree(std::pmr::memory_resource* resource,
                                      node_ptr node,
                                      const std::pmr::vector<expression_ptr>& conjuncts,
                                      std::pmr::vector<char>& claimed,
                                      bool& any_claimed) {
            if (!node || node->type() != node_type::join_t) {
                return node; // leaf scan / non-join subtree
            }
            auto* join = static_cast<node_join_t*>(node.get());
            if (join->children().size() < 2) {
                return node;
            }

            // Capture this join's boundary from the intact stamped children first.
            const bool classifiable = join->type() == join_type::cross && join->children()[0]->has_output_types() &&
                                      join->children()[1]->has_output_types();
            const size_t left_width = classifiable ? join->children()[0]->output_types().size() : 0;
            const size_t right_width = classifiable ? join->children()[1]->output_types().size() : 0;

            // Recurse into children first — nested cross joins live in child[0] (left-deep).
            auto new_left = promote_join_subtree(resource, join->children()[0], conjuncts, claimed, any_claimed);
            auto new_right = promote_join_subtree(resource, join->children()[1], conjuncts, claimed, any_claimed);

            auto splice_children = [&]() {
                if (new_left != join->children()[0]) {
                    join->children()[0] = new_left;
                }
                if (new_right != join->children()[1]) {
                    join->children()[1] = new_right;
                }
            };

            if (!classifiable || left_width == 0 || right_width == 0) {
                splice_children(); // carry promoted children, but leave THIS join as-is
                return node;
            }

            size_t picked_index = conjuncts.size();
            for (size_t i = 0; i < conjuncts.size(); ++i) {
                if (!claimed[i] && is_boundary_equi(conjuncts[i], left_width, right_width)) {
                    picked_index = i;
                    break;
                }
            }
            if (picked_index == conjuncts.size()) {
                splice_children(); // no key for this join -> keep it cross
                return node;
            }

            claimed[picked_index] = 1;
            any_claimed = true;

            auto picked = conjuncts[picked_index];
            auto* eq = static_cast<compare_expression_t*>(picked.get());
            // Re-stamp both keys against THIS join's boundary. The right-range key
            // becomes side=right with a right-local path (mergedIdx - left_width) built on
            // the rule's resource (NOT set_path({...}), which pulls the default resource).
            // The left-range key's merged path is already left-child-local, so
            // only its side is (re)affirmed left.
            key_t& kl = as_key(eq->left());
            key_t& kr = as_key(eq->right());
            const bool kl_is_right = kl.path()[0] >= left_width;
            key_t& right_key = kl_is_right ? kl : kr;
            key_t& left_key = kl_is_right ? kr : kl;
            const size_t merged_idx = right_key.path()[0];
            right_key.set_side(side_t::right);
            std::pmr::vector<size_t> p{resource};
            p.push_back(merged_idx - left_width);
            right_key.set_path(std::move(p));
            left_key.set_side(side_t::left);

            // Immutable join_type -> a fresh inner join. oid flows from the moved scan
            // children. Hash selection stays in rewrite_hash_joins (runs
            // after); the re-stamp makes detect_equi_columns accept it. Use the PROMOTED
            // children so a nested inner join is carried up.
            auto inner = make_node_join(resource, core::dbname_t{}, core::relname_t{}, join_type::inner);
            inner->append_child(new_left);
            inner->append_child(new_right);
            inner->append_expression(picked);
            // Stamp the promoted join's own output schema = left ++ right output_types in
            // logical [left, right] order. The validator stamps a cross join, but a fresh
            // make_node_join is unstamped; every consumer then reads a reliable left_width
            // from children()[i]->output_types() — a PARENT promoted join (nested left-deep)
            // and pushdown_filter, which re-localizes a pushed right-side conjunct by that
            // left_width. The promoted children preserve their pre-promotion widths, so this
            // equals the width of the cross join it replaces. Built on `resource`.
            std::pmr::vector<components::types::complex_logical_type> merged{resource};
            merged.reserve(new_left->output_types().size() + new_right->output_types().size());
            for (const auto& t : new_left->output_types()) {
                merged.push_back(t);
            }
            for (const auto& t : new_right->output_types()) {
                merged.push_back(t);
            }
            inner->set_output_types(std::move(merged));
            return inner;
        }

        // === Star-schema pre-normalizer ===========================================
        //
        // A fact-LAST comma-join star (`FROM dim0, dim1, .., fact`) lowers to a
        // left-deep CROSS chain whose inner joins hold only DIMENSIONS -> no straddling
        // equi -> the canonical promotion leaves them CROSS -> a dimension cartesian.
        // normalize_star_shape is a pure PRE-NORMALIZER over the ONE canonical path: it
        // can only reshape the CROSS tree (fact-FIRST) and re-coordinate frozen consumers
        // by a block permutation P; it NEVER promotes anything. So a fact-FIRST chain has
        // a straddling equi at every boundary that the UNCHANGED promote_join_subtree then
        // claims. A non-star / already-claimable input flows through as a no-op.
        //
        // All read-only work (detect, verify) precedes ALL mutation, so any bail leaves
        // the tree pristine and the canonical path runs exactly as today.

        // Read-only mirror of promote_join_subtree's claiming, on a scratch `claimed`
        // array: returns true iff EVERY cross-join boundary in the subtree claims a
        // straddling equi. That is a chain / fact-threaded shape the canonical path
        // already promotes fully; reordering it would MIS-select the fact
        // (short-circuit -> caller returns the tree unchanged).
        bool simulate_all_boundaries_claim(const node_ptr& node,
                                           const std::pmr::vector<expression_ptr>& conjuncts,
                                           std::pmr::vector<char>& claimed) {
            if (!node || node->type() != node_type::join_t) {
                return true; // leaf: no boundary here
            }
            auto* join = static_cast<node_join_t*>(node.get());
            if (join->children().size() < 2) {
                return true;
            }
            const bool classifiable = join->type() == join_type::cross && join->children()[0]->has_output_types() &&
                                      join->children()[1]->has_output_types();
            const size_t left_width = classifiable ? join->children()[0]->output_types().size() : 0;
            const size_t right_width = classifiable ? join->children()[1]->output_types().size() : 0;

            const bool left_all = simulate_all_boundaries_claim(join->children()[0], conjuncts, claimed);
            const bool right_all = simulate_all_boundaries_claim(join->children()[1], conjuncts, claimed);

            if (!classifiable || left_width == 0 || right_width == 0) {
                // Not a promotable cross boundary (already inner / unstamped): pass the
                // children's verdict through unchanged.
                return left_all && right_all;
            }

            for (size_t i = 0; i < conjuncts.size(); ++i) {
                if (!claimed[i] && is_boundary_equi(conjuncts[i], left_width, right_width)) {
                    claimed[i] = 1;
                    return left_all && right_all; // this boundary claims
                }
            }
            return false; // this cross boundary does NOT claim
        }

        // Collect the left-deep spine leaves in FROM (merged) order. A leaf must be a base
        // table-scan: a childless node carrying output_types (a childless aggregate_t for
        // SSB, or a node_data raw scan in tests). An aggregate_t WITH a child (sub-select /
        // CTE) is NOT a base scan -> bail. Deliberately NOT gated on node_type == data_t:
        // SSB's leaves are aggregate_t, so that gate would reject the very case this fixes.
        bool collect_spine_leaves(const node_ptr& node, std::pmr::vector<node_ptr>& leaves) {
            if (!node) {
                return false;
            }
            if (node->type() == node_type::join_t) {
                auto* join = static_cast<node_join_t*>(node.get());
                if (join->children().size() != 2) {
                    return false;
                }
                if (!collect_spine_leaves(join->children()[0], leaves)) {
                    return false;
                }
                return collect_spine_leaves(join->children()[1], leaves);
            }
            if (!node->children().empty()) {
                return false; // sub-select / CTE / non-scan in a leaf position
            }
            if (!node->has_output_types()) {
                return false;
            }
            leaves.push_back(node);
            return true;
        }

        // The walker (per-form OWN-key dispatch). One pass over the
        // aggregate's non-source children, either VERIFY (apply=false: only classify every
        // merged locus, flag `ok=false` on any that is out of range / unresolvable) or
        // APPLY (apply=true: remap each merged locus IN PLACE via P). Every param_storage
        // is read through is_key/as_key/is_expr/as_expr (never raw std::get); every key is
        // remapped in place `k.path()[0] = perm[old]` (never set_path).
        struct key_remapper {
            const std::pmr::vector<size_t>* perm; // size n; perm[old] = new
            size_t n;
            bool has_group;
            bool apply;
            bool ok;

            // Remap the merged column index held in the ROOT of a key path (path()[0]);
            // a nested struct/subfield suffix rides along unchanged.
            void remap_key(key_t& k) {
                if (k.path().empty()) {
                    return; // no merged locus
                }
                const size_t idx = k.path()[0];
                if (idx == SIZE_MAX) {
                    return; // post-aggregate sentinel: not a merged locus
                }
                if (idx >= n) {
                    ok = false; // not classifiable against the merged schema -> verify fails
                    return;
                }
                if (apply) {
                    k.path()[0] = (*perm)[idx];
                }
            }

            // A single operand: a key is remapped; a nested compare / scalar operand is
            // recursed (own key of a nested scalar is an alias/sentinel -> not touched).
            void walk_operand(param_storage& p) {
                if (is_key(p)) {
                    remap_key(as_key(p));
                    return;
                }
                if (is_expr(p)) {
                    auto& sub = as_expr(p);
                    if (!sub) {
                        return;
                    }
                    if (sub->group() == expression_group::compare) {
                        walk_compare(sub);
                    } else if (sub->group() == expression_group::scalar) {
                        auto* s = static_cast<scalar_expression_t*>(sub.get());
                        walk_params(s->params());
                    }
                }
                // parameter_id_t: no locus
            }

            void walk_params(std::pmr::vector<param_storage>& params) {
                for (auto& p : params) {
                    walk_operand(p);
                }
            }

            // A compare node: union_and / union_or / union_not carry their operands in
            // children(); a binary compare carries them in left()/right() (either of which
            // may itself be a nested expression, e.g. arithmetic or a CASE condition).
            void walk_compare(const expression_ptr& expr) {
                if (!expr || expr->group() != expression_group::compare) {
                    return;
                }
                auto* cmp = static_cast<compare_expression_t*>(expr.get());
                const compare_type t = cmp->type();
                if (t == compare_type::union_and || t == compare_type::union_or || t == compare_type::union_not) {
                    for (auto& child : cmp->children()) {
                        walk_compare(child);
                    }
                    return;
                }
                walk_operand(cmp->left());
                walk_operand(cmp->right());
            }

            void walk_match(const node_ptr& match) {
                if (!match || match->expressions().empty()) {
                    return;
                }
                walk_compare(match->expressions()[0]);
            }

            // A group node's merged loci: group_field OWN key; get_field params().front()
            // (else OWN key); every aggregate's params() (the merged agg-argument keys,
            // incl. nested arithmetic like SUM(a - b)); pre-group computed operands. A
            // post-aggregate arithmetic (path()[0] == SIZE_MAX) is group-output -> skipped
            // whole. A group coalesce key is unresolved by group Pass 1 -> bail.
            void walk_group_scalar(scalar_expression_t* s) {
                switch (s->type()) {
                    case scalar_type::group_field:
                        if (!s->params().empty()) {
                            walk_params(s->params());
                        } else {
                            remap_key(s->key());
                        }
                        break;
                    case scalar_type::get_field:
                        if (!s->params().empty() && is_key(s->params().front())) {
                            remap_key(as_key(s->params().front()));
                        } else {
                            remap_key(s->key());
                        }
                        break;
                    case scalar_type::coalesce:
                        ok = false; // group Pass 1 has no coalesce handler -> unresolved -> bail
                        break;
                    case scalar_type::constant:
                        break;
                    case scalar_type::star_expand:
                        ok = false; // must not leak merged order upward
                        break;
                    default:
                        // arithmetic (add/subtract/multiply/divide/mod/case_expr/unary_minus)
                        if (!s->key().path().empty() && s->key().path()[0] == SIZE_MAX) {
                            break; // post-aggregate: group-output operands -> do not touch
                        }
                        walk_params(s->params()); // pre-group computed column operands (merged)
                        break;
                }
            }

            void walk_group(const node_ptr& group) {
                for (auto& expr : group->expressions()) {
                    if (!expr) {
                        continue;
                    }
                    if (expr->group() == expression_group::scalar) {
                        walk_group_scalar(static_cast<scalar_expression_t*>(expr.get()));
                    } else if (expr->group() == expression_group::aggregate) {
                        auto* a = static_cast<aggregate_expression_t*>(expr.get());
                        walk_params(a->params()); // OWN key is the output alias -> not touched
                    }
                }
            }

            // WITHOUT a group, sort keys index the merged join schema. A plain sort key
            // remaps its OWN key; a computed ORDER BY scalar keeps its OWN key (which
            // encodes ASC/DESC) and remaps only its merged operands.
            void walk_sort_nogroup(const node_ptr& sort) {
                for (auto& expr : sort->expressions()) {
                    if (!expr) {
                        continue;
                    }
                    if (expr->group() == expression_group::sort) {
                        walk_operand(static_cast<sort_expression_t*>(expr.get())->operand());
                    } else if (expr->group() == expression_group::scalar) {
                        walk_params(static_cast<scalar_expression_t*>(expr.get())->params());
                    }
                }
            }

            // WITHOUT a group, projection columns index the merged join schema.
            void walk_select_nogroup(const node_ptr& select) {
                for (auto& expr : select->expressions()) {
                    if (!expr || expr->group() != expression_group::scalar) {
                        continue;
                    }
                    auto* s = static_cast<scalar_expression_t*>(expr.get());
                    switch (s->type()) {
                        case scalar_type::group_field:
                            remap_key(s->key());
                            break;
                        case scalar_type::get_field:
                            if (!s->params().empty() && is_key(s->params().front())) {
                                remap_key(as_key(s->params().front()));
                            } else {
                                remap_key(s->key());
                            }
                            break;
                        case scalar_type::constant:
                        case scalar_type::star_expand:
                            break;
                        default:
                            // arithmetic / coalesce / case_expr operands (merged)
                            walk_params(s->params());
                            break;
                    }
                }
            }

            // Dispatch one aggregate child. match_t is walked in both group modes; group_t
            // only with a group; sort_t / select_t only without a group; limit_t and any
            // other sibling carry no merged loci.
            void walk_sibling(const node_ptr& sib) {
                if (!sib) {
                    return;
                }
                switch (sib->type()) {
                    case node_type::match_t:
                        walk_match(sib);
                        break;
                    case node_type::group_t:
                        if (has_group) {
                            walk_group(sib);
                        }
                        break;
                    case node_type::sort_t:
                        if (!has_group) {
                            walk_sort_nogroup(sib);
                        }
                        break;
                    case node_type::select_t:
                        if (!has_group) {
                            walk_select_nogroup(sib);
                        }
                        break;
                    default:
                        break;
                }
            }
        };

        // Reshape a fact-LAST star `source` into a fact-FIRST cross chain and P-remap
        // every frozen consumer (the match, plus the aggregate's other children in
        // `siblings`). Returns the reshaped source, or `source` unchanged on any bail
        // (short-circuit, non-star, leaking projection, or a Phase-A verify failure) so
        // the caller's canonical promotion path runs on a pristine tree.
        node_ptr normalize_star_shape(std::pmr::memory_resource* resource,
                                      node_ptr source,
                                      const node_ptr& match_child,
                                      std::pmr::vector<node_ptr>& siblings) {
            using components::types::complex_logical_type;

            if (!source || source->type() != node_type::join_t) {
                return source;
            }
            if (!match_child || match_child->expressions().empty()) {
                return source;
            }
            auto conjuncts = split_conjuncts(resource, match_child->expressions()[0]);
            if (conjuncts.empty()) {
                return source;
            }

            // (1a) short-circuit: if the canonical path already claims every cross
            // boundary, this is a chain / fact-threaded shape -> leave it untouched.
            {
                std::pmr::vector<char> scratch{resource};
                scratch.resize(conjuncts.size(), 0);
                if (simulate_all_boundaries_claim(source, conjuncts, scratch)) {
                    return source;
                }
            }

            // (1b) Spine leaves + widths/offsets from output_types() (FROM/merged order).
            std::pmr::vector<node_ptr> leaves{resource};
            if (!collect_spine_leaves(source, leaves)) {
                return source; // non-scan leaf / sub-select
            }
            if (leaves.size() < 3) {
                return source; // fewer than fact + 2 dims is not a star
            }
            std::pmr::vector<size_t> width{resource};
            std::pmr::vector<size_t> offset_old{resource};
            width.reserve(leaves.size());
            offset_old.reserve(leaves.size());
            size_t n = 0;
            for (const auto& lf : leaves) {
                offset_old.push_back(n);
                const size_t w = lf->output_types().size();
                if (w == 0) {
                    return source; // unstamped leaf
                }
                width.push_back(w);
                n += w;
            }

            // An explicit, non-star projection is required: a SELECT * leaks the merged order
            // upward (validate_logical_plan star_expand path). A GROUP node counts as one — a
            // grouped query emits one row per group over exactly its own expression list, which
            // IS the projection, and it therefore carries no node_select at all.
            {
                bool has_projection = false;
                for (size_t i = 1; i < siblings.size(); ++i) {
                    if (!siblings[i] ||
                        (siblings[i]->type() != node_type::select_t && siblings[i]->type() != node_type::group_t)) {
                        continue;
                    }
                    has_projection = true;
                    for (const auto& expr : siblings[i]->expressions()) {
                        if (expr && expr->group() == expression_group::scalar &&
                            static_cast<scalar_expression_t*>(expr.get())->type() == scalar_type::star_expand) {
                            return source;
                        }
                    }
                }
                if (!has_projection) {
                    return source; // SELECT * (no projection node) -> leaks merged order
                }
            }

            // leaf_of(merged_idx) -> owning leaf, or leaves.size() if out of range.
            auto leaf_of = [&](size_t idx) -> size_t {
                for (size_t l = 0; l < leaves.size(); ++l) {
                    if (idx >= offset_old[l] && idx < offset_old[l] + width[l]) {
                        return l;
                    }
                }
                return leaves.size();
            };

            // Classify each eq(key,key) with single-element paths into a leaf edge; drop
            // same-leaf pairs and non-classifiable / non-key comparisons (filters).
            std::pmr::vector<size_t> edge_a{resource};
            std::pmr::vector<size_t> edge_b{resource};
            for (const auto& c : conjuncts) {
                if (!c || c->group() != expression_group::compare) {
                    continue;
                }
                auto* cmp = static_cast<compare_expression_t*>(c.get());
                if (cmp->type() != compare_type::eq || !is_key(cmp->left()) || !is_key(cmp->right())) {
                    continue;
                }
                const auto& kl = as_key(cmp->left());
                const auto& kr = as_key(cmp->right());
                if (kl.path().size() != 1 || kr.path().size() != 1) {
                    continue;
                }
                const size_t li = leaf_of(kl.path()[0]);
                const size_t lj = leaf_of(kr.path()[0]);
                if (li == leaves.size() || lj == leaves.size() || li == lj) {
                    continue;
                }
                edge_a.push_back(li < lj ? li : lj);
                edge_b.push_back(li < lj ? lj : li);
            }
            if (edge_a.empty()) {
                return source; // no join edge
            }

            // Star = exactly one fact leaf incident to EVERY edge.
            size_t fact = leaves.size();
            for (size_t f = 0; f < leaves.size(); ++f) {
                bool in_all = true;
                for (size_t e = 0; e < edge_a.size(); ++e) {
                    if (edge_a[e] != f && edge_b[e] != f) {
                        in_all = false;
                        break;
                    }
                }
                if (in_all) {
                    fact = f;
                    break;
                }
            }
            if (fact == leaves.size()) {
                return source; // snowflake dim-dim edge / multi-fact
            }
            // Every non-fact leaf must have EXACTLY one edge to the fact: 0 = a dim with no
            // fact-equi; >1 = a composite fact key (track multiplicity) -> bail.
            std::pmr::vector<size_t> deg{resource};
            deg.resize(leaves.size(), 0);
            for (size_t e = 0; e < edge_a.size(); ++e) {
                const size_t dim = (edge_a[e] == fact) ? edge_b[e] : edge_a[e];
                deg[dim] += 1;
            }
            for (size_t l = 0; l < leaves.size(); ++l) {
                if (l != fact && deg[l] != 1) {
                    return source;
                }
            }

            // (2) choose_dim_order (v1 = FROM order, fact first) + offset_new + P.
            std::pmr::vector<size_t> new_order{resource};
            new_order.reserve(leaves.size());
            new_order.push_back(fact);
            for (size_t l = 0; l < leaves.size(); ++l) {
                if (l != fact) {
                    new_order.push_back(l);
                }
            }
            std::pmr::vector<size_t> offset_new{resource};
            offset_new.resize(leaves.size(), 0);
            {
                size_t acc = 0;
                for (size_t pos = 0; pos < new_order.size(); ++pos) {
                    const size_t l = new_order[pos];
                    offset_new[l] = acc;
                    acc += width[l];
                }
            }
            std::pmr::vector<size_t> perm{resource}; // perm[old_merged] = new_merged
            perm.resize(n, 0);
            for (size_t l = 0; l < leaves.size(); ++l) {
                for (size_t local = 0; local < width[l]; ++local) {
                    perm[offset_old[l] + local] = offset_new[l] + local;
                }
            }

            bool has_group = false;
            for (size_t i = 1; i < siblings.size(); ++i) {
                if (siblings[i] && siblings[i]->type() == node_type::group_t) {
                    has_group = true;
                    break;
                }
            }

            // (3) Phase-A verify (READ-ONLY): every merged locus classifiable (< n) and no
            // unsupported form. On any failure the tree stays pristine for the canonical path.
            {
                key_remapper verify{&perm, n, has_group, /*apply=*/false, /*ok=*/true};
                for (size_t i = 1; i < siblings.size(); ++i) {
                    verify.walk_sibling(siblings[i]);
                }
                if (!verify.ok) {
                    return source;
                }
            }

            // (4) Commit. Rebuild the CROSS chain fact-first over the ORIGINAL leaf scans
            // (oids flow); stamp each new cross join's output_types = left ++ right
            // BOTTOM-UP so a parent reads its freshly-stamped child's width.
            node_ptr new_source = leaves[new_order[0]];
            for (size_t pos = 1; pos < new_order.size(); ++pos) {
                const node_ptr& dim = leaves[new_order[pos]];
                auto cross = make_node_join(resource, core::dbname_t{}, core::relname_t{}, join_type::cross);
                cross->append_child(new_source);
                cross->append_child(dim);
                cross->append_expression(make_compare_expression(resource, compare_type::all_true));
                std::pmr::vector<complex_logical_type> merged{resource};
                merged.reserve(new_source->output_types().size() + dim->output_types().size());
                for (const auto& t : new_source->output_types()) {
                    merged.push_back(t);
                }
                for (const auto& t : dim->output_types()) {
                    merged.push_back(t);
                }
                cross->set_output_types(std::move(merged));
                new_source = cross;
            }

            // Phase-B apply: remap every merged locus in the match + siblings via P. The
            // match keys move merged->new; the canonical promote_join_subtree then re-stamps
            // each claimed equi new->child-local (two in-place mutations of a single-owned key).
            key_remapper commit{&perm, n, has_group, /*apply=*/true, /*ok=*/true};
            for (size_t i = 1; i < siblings.size(); ++i) {
                commit.walk_sibling(siblings[i]);
            }

            return new_source;
        }

        node_ptr promote_impl(std::pmr::memory_resource* resource, node_ptr node) {
            if (!node) {
                return node;
            }

            for (size_t i = 0; i < node->children().size(); ++i) {
                auto optimized = promote_impl(resource, node->children()[i]);
                if (optimized != node->children()[i]) {
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
            if (source->type() != node_type::join_t) {
                return node;
            }

            // Split the WHERE into top-level conjuncts (OUTER-merged coordinates) and
            // promote every cross join in the source subtree — each nested join claims
            // the single conjunct that straddles ITS boundary. The current 2-table case
            // is the depth-1 instance of this (one join, one claimed conjunct).
            auto conjuncts = split_conjuncts(resource, match_child->expressions()[0]);
            if (conjuncts.empty()) {
                return node;
            }

            // Star pre-normalizer: reshape a fact-LAST star into a fact-FIRST
            // cross chain and P-remap frozen consumers so the ONE canonical promotion path
            // below claims every boundary. This mutates `match_child`'s keys (and the other
            // siblings') in place merged->new; since `conjuncts` holds those same shared
            // expressions they are now in NEW coordinates for promote_join_subtree. A
            // non-star / already-claimable shape returns `source` unchanged (no-op).
            source = normalize_star_shape(resource, source, match_child, agg->children());

            std::pmr::vector<char> claimed{resource};
            claimed.resize(conjuncts.size(), 0);
            bool any_claimed = false;
            auto promoted = promote_join_subtree(resource, source, conjuncts, claimed, any_claimed);
            if (!any_claimed) {
                return node; // no qualifying equi anywhere -> leave the subtree untouched
            }

            // Residual = every unclaimed conjunct (incl. extra eqs of a composite key and
            // non-join filters). Never bail — the residual match keeps applying them.
            std::pmr::vector<expression_ptr> residual{resource};
            for (size_t i = 0; i < conjuncts.size(); ++i) {
                if (!claimed[i]) {
                    residual.push_back(conjuncts[i]);
                }
            }
            auto residual_expr = rebuild_conjunction(resource, residual);
            if (residual_expr) {
                match_child->expressions()[0] = residual_expr;
            } else {
                // Nothing left to filter -> drop the residual match. The join sits at
                // index 0, the match at index >= 1, so erasing it does not shift index 0.
                auto& children = agg->children();
                for (size_t i = 0; i < children.size(); ++i) {
                    if (children[i] == match_child) {
                        children.erase(children.begin() + static_cast<std::ptrdiff_t>(i));
                        break;
                    }
                }
            }

            agg->children()[0] = promoted;
            return node;
        }

    } // namespace

    logical_plan::node_ptr promote_cross_joins(std::pmr::memory_resource* resource, logical_plan::node_ptr node) {
        return promote_impl(resource, std::move(node));
    }

} // namespace components::planner::optimizer
