#include "planner.hpp"

#include "ddl_metadata_builder.hpp"
#include <catalog/oid_batch.hpp>
#include <logical_plan/node_check_constraint.hpp>
#include <logical_plan/node_create_collection.hpp>
#include <logical_plan/node_insert.hpp>
#include <logical_plan/node_sequence.hpp>
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

        // DDL rewrite: expand node_create_collection_t into a node_sequence_t that
        // contains one node_primitive_write_t per pg_catalog row to insert.
        node_ptr rewrite_create_table(std::pmr::memory_resource* r,
                                       node_ptr node,
                                       catalog::oid_batch_t& oid_batch) {
            auto* cc = static_cast<logical_plan::node_create_collection_t*>(node.get());
            const catalog::oid_t ns_oid = cc->namespace_oid();

            auto writes = catalog::build_create_table_writes(r, *cc, ns_oid, oid_batch);

            auto seq = boost::intrusive_ptr(new logical_plan::node_sequence_t(r));
            for (auto& w : writes) {
                seq->append_child(w);
            }
            return seq;
        }

        // DDL-aware walk: handles DDL nodes in addition to DML rewrites.
        node_ptr walk_ddl(std::pmr::memory_resource* r,
                           node_ptr node,
                           catalog::oid_batch_t& oid_batch) {
            using namespace logical_plan;
            switch (node->type()) {
            case node_type::insert_t:
                return rewrite_insert(r, node);
            case node_type::update_t:
                return rewrite_update(r, node);
            case node_type::create_collection_t:
                return rewrite_create_table(r, node, oid_batch);
            default:
                for (auto& child : node->children()) {
                    child = walk_ddl(r, child, oid_batch);
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
                                 catalog::oid_batch_t oid_batch)
        -> logical_plan::node_ptr {
        return walk_ddl(resource, std::move(node), oid_batch);
    }

} // namespace components::planner