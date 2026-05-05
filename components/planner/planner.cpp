#include "planner.hpp"

#include <components/catalog/oid_batch.hpp>
#include <logical_plan/node_check_constraint.hpp>
#include <logical_plan/node_insert.hpp>
#include <logical_plan/node_update.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>

namespace components::planner {

    namespace {
        using node_ptr = logical_plan::node_ptr;

        node_ptr rewrite_insert(std::pmr::memory_resource* r, node_ptr node) {
            auto* ins = static_cast<logical_plan::node_insert_t*>(node.get());
            if (ins->not_null_cols().empty()) return node;

            auto cc = boost::intrusive_ptr(new logical_plan::node_check_constraint_t(
                r, ins->collection_full_name(),
                std::vector<std::string>(ins->not_null_cols())));
            cc->append_child(node);
            return cc;
        }

        node_ptr rewrite_update(std::pmr::memory_resource* r, node_ptr node) {
            auto* upd = static_cast<logical_plan::node_update_t*>(node.get());
            if (upd->not_null_cols().empty()) return node;

            auto cc = boost::intrusive_ptr(new logical_plan::node_check_constraint_t(
                r, upd->collection_full_name(),
                std::vector<std::string>(upd->not_null_cols()),
                {}));
            cc->append_child(node);
            return cc;
        }

        node_ptr walk(std::pmr::memory_resource* r, node_ptr node) {
            using namespace logical_plan;
            switch (node->type()) {
            case node_type::insert_t:
                return rewrite_insert(r, node);
            case node_type::update_t:
                return rewrite_update(r, node);
            default:
                for (auto& child : node->children()) {
                    child = walk(r, child);
                }
                return node;
            }
        }

    } // anonymous namespace

    auto planner_t::create_plan(std::pmr::memory_resource* resource,
                                 logical_plan::node_ptr node)
        -> logical_plan::node_ptr {
        return walk(resource, std::move(node));
    }

    auto planner_t::create_plan(std::pmr::memory_resource* resource,
                                 logical_plan::node_ptr node,
                                 catalog::oid_batch_t /*oid_batch*/)
        -> logical_plan::node_ptr {
        // oid_batch is consumed by DDL rewrites (rewrite_create_table etc.) once implemented.
        return walk(resource, std::move(node));
    }

} // namespace components::planner