#include "pushdown_limit.hpp"

#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_sort.hpp>

namespace components::planner::optimizer {

    namespace {
        namespace lp = components::logical_plan;

        bool is_effective(const lp::limit_t& limit) noexcept {
            return limit.limit() != lp::limit_t::unlimit().limit() || limit.offset() != 0;
        }

        // If `node` is a limit-carrying aggregate, stamp the one eligible
        // cardinality-preserving source with a pure count read-cap; otherwise leave
        // everything untouched. Total (no-op on non-match). Mirrors
        // create_plan_aggregate's child classification.
        void try_stamp_limit(const lp::node_ptr& node) {
            if (node->type() != lp::node_type::aggregate_t) {
                return;
            }
            auto* aggregate = static_cast<lp::node_aggregate_t*>(node.get());
            // This rule's own reading of the roles: a projection ($select) is irrelevant
            // to where the cap lands, and everything that is NOT a role — including a
            // HAVING — counts as a non-scan source that blocks capping.
            const auto roles = aggregate->pipeline();

            // The effective limit is carried by the limit_t child (the same source
            // create_plan_aggregate and the executor read). Unlimit when there is none.
            const lp::limit_t limit = roles.limit
                                          ? static_cast<const lp::node_limit_t*>(roles.limit.get())->limit()
                                          : lp::limit_t::unlimit();
            if (!is_effective(limit)) {
                return;
            }

            // DISTINCT is a FLAG on the aggregate and operator_distinct is layered
            // ABOVE the terminal scan — capping any source below would dedup too few
            // rows and drop distinct values operator_limit can never recover. Stamp
            // nothing: every source stays unlimit, operator_limit windows after dedup.
            if (aggregate->is_distinct()) {
                return;
            }

            const lp::node_ptr& match_child = roles.match;
            const lp::node_ptr& sort_child = roles.sort;
            const bool has_group = roles.group != nullptr;
            const bool has_nonscan = roles.source != nullptr || roles.having != nullptr;

            const lp::limit_t read_cap{limit.head_cap(), 0};

            // ORDER BY: the FULL blocking sort's OUTPUT is the final ordered set — cap
            // it to limit+offset; everything below (group/match/scan) stays unlimit.
            // The sort sits above any GROUP BY, so it is the single cardinality-
            // preserving cap point below operator_limit (only when NO DISTINCT, which
            // is excluded above).
            if (sort_child) {
                static_cast<lp::node_sort_t*>(sort_child.get())->set_read_cap(read_cap);
                return;
            }
            // GROUP BY / non-scan source (UNION / recursive-CTE / join): output is not
            // cardinality-preserving from the scan and there is no output-cap hook
            // (create_plan_group / create_plan_union take no limit) — operator_limit
            // windows the full output. Stamp nothing.
            if (has_group || has_nonscan) {
                return;
            }
            // Plain scan. A WHERE (match child) caps the POST-filter matched stream on
            // the match node (create_plan_match / the disk place it post-filter); no
            // WHERE caps the base scan via the aggregate's own read_cap (the terminal
            // transfer_scan create_plan_aggregate builds directly).
            if (match_child) {
                static_cast<lp::node_match_t*>(match_child.get())->set_read_cap(read_cap);
            } else {
                aggregate->set_read_cap(read_cap);
            }
        }

        void walk(const lp::node_ptr& node) {
            if (!node) {
                return;
            }
            for (auto& child : node->children()) {
                walk(child);
            }
            try_stamp_limit(node);
        }
    } // namespace

    logical_plan::node_ptr pushdown_limit(std::pmr::memory_resource* /*resource*/, logical_plan::node_ptr root) {
        walk(root);
        return root;
    }

} // namespace components::planner::optimizer
