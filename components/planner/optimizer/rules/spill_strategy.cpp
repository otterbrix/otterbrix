#include "spill_strategy.hpp"

#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_sort.hpp>

namespace components::planner::optimizer {

    namespace {
        namespace lp = components::logical_plan;

        // Stamp the exec_strategy annotation on `node` in place according to the
        // spill decision. Only the three grace-eligible node types carry the
        // annotation; every other node is returned unchanged. Each node declares
        // its OWN exec_strategy enum, so the value is resolved per-branch.
        lp::node_ptr try_stamp(const lp::node_ptr& node, bool spill) {
            switch (node->type()) {
                case lp::node_type::sort_t: {
                    using es = lp::node_sort_t::exec_strategy;
                    static_cast<lp::node_sort_t*>(node.get())->set_strategy(spill ? es::spill : es::in_memory);
                    break;
                }
                case lp::node_type::group_t: {
                    using es = lp::node_group_t::exec_strategy;
                    static_cast<lp::node_group_t*>(node.get())->set_strategy(spill ? es::spill : es::in_memory);
                    break;
                }
                case lp::node_type::join_t: {
                    using es = lp::node_join_t::exec_strategy;
                    static_cast<lp::node_join_t*>(node.get())->set_strategy(spill ? es::spill : es::in_memory);
                    break;
                }
                default:
                    // Not a grace-eligible node — leave it alone.
                    break;
            }
            return node;
        }

        // Post-order recursive walk mirroring rewrite_hash_joins (hash_join.cpp).
        lp::node_ptr walk(const lp::node_ptr& node, bool spill) {
            if (!node) {
                return node;
            }
            for (auto& child : node->children()) {
                child = walk(child, spill);
            }
            return try_stamp(node, spill);
        }
    } // namespace

    logical_plan::node_ptr spill_strategy(logical_plan::node_ptr root, bool spill_enabled) {
        return walk(root, spill_enabled);
    }

} // namespace components::planner::optimizer
