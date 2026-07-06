#include "promote_cross_join.hpp"

#include "conjunct_utils.hpp"

#include <memory_resource>

#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>

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
        // unchanged (kept as-is), never throwing — optimizer rules have no error channel
        // (rules 2, 9). Each claimed conjunct is flagged so a later join cannot re-claim
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
            const bool classifiable = join->type() == join_type::cross &&
                                      join->children()[0]->has_output_types() &&
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
            // the rule's resource (NOT set_path({...}), which pulls the default resource —
            // rule 14). The left-range key's merged path is already left-child-local, so
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
            // children (rule 16 OK). Hash selection stays in rewrite_hash_joins (runs
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
            // equals the width of the cross join it replaces. Built on `resource` (rule 8).
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
