#include "planner.hpp"

#include "ddl_metadata_builder.hpp"
#include <catalog/oid_batch.hpp>
#include <logical_plan/node_check_constraint.hpp>
#include <logical_plan/node_create_collection.hpp>
#include <logical_plan/node_delete.hpp>
#include <logical_plan/node_fk_cascade.hpp>
#include <logical_plan/node_fk_check.hpp>
#include <logical_plan/node_insert.hpp>
#include <logical_plan/node_sequence.hpp>
#include <logical_plan/node_update.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>

namespace components::planner {

    namespace {
        using node_ptr = logical_plan::node_ptr;

        node_ptr rewrite_insert(std::pmr::memory_resource* r, node_ptr node) {
            auto* ins = static_cast<logical_plan::node_insert_t*>(node.get());
            node_ptr cur = node;

            // Wrap with FK check nodes (outermost = last FK, so checks run innermost-first).
            for (const auto& fk : ins->outgoing_fks()) {
                auto fk_node = boost::intrusive_ptr(new logical_plan::node_fk_check_t(
                    r, ins->collection_full_name(), fk));
                fk_node->append_child(cur);
                cur = fk_node;
            }

            // Wrap with NOT NULL check (outer relative to original insert, inner to FK check).
            if (!ins->not_null_cols().empty()) {
                auto cc = boost::intrusive_ptr(new logical_plan::node_check_constraint_t(
                    r, ins->collection_full_name(),
                    std::vector<std::string>(ins->not_null_cols())));
                cc->append_child(cur);
                cur = cc;
            }

            return cur;
        }

        node_ptr rewrite_update(std::pmr::memory_resource* r, node_ptr node) {
            auto* upd = static_cast<logical_plan::node_update_t*>(node.get());
            node_ptr cur = node;

            for (const auto& fk : upd->outgoing_fks()) {
                auto fk_node = boost::intrusive_ptr(new logical_plan::node_fk_check_t(
                    r, upd->collection_full_name(), fk));
                fk_node->append_child(cur);
                cur = fk_node;
            }

            if (!upd->not_null_cols().empty()) {
                auto cc = boost::intrusive_ptr(new logical_plan::node_check_constraint_t(
                    r, upd->collection_full_name(),
                    std::vector<std::string>(upd->not_null_cols())));
                cc->append_child(cur);
                cur = cc;
            }

            return cur;
        }

        node_ptr rewrite_delete(std::pmr::memory_resource* r, node_ptr node) {
            auto* del = static_cast<logical_plan::node_delete_t*>(node.get());
            if (del->referencing_fks().empty()) return node;

            // Each cascade node wraps the previous (outermost handles last FK first).
            node_ptr cur = node;
            for (const auto& fk : del->referencing_fks()) {
                auto cascade = boost::intrusive_ptr(new logical_plan::node_fk_cascade_t(
                    r, del->collection_full_name(), fk));
                cascade->append_child(cur);
                cur = cascade;
            }
            return cur;
        }

        node_ptr walk(std::pmr::memory_resource* r, node_ptr node) {
            using namespace logical_plan;
            switch (node->type()) {
            case node_type::insert_t:
                return rewrite_insert(r, node);
            case node_type::update_t:
                return rewrite_update(r, node);
            case node_type::delete_t:
                return rewrite_delete(r, node);
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
        // CREATE TABLE rewrite is intentionally absent: storage creation is still
        // coupled to ddl_create_table in execute_ddl_inline and must be decoupled
        // before the planner can emit the full sequence autonomously.
        node_ptr walk_ddl(std::pmr::memory_resource* r,
                           node_ptr node,
                           catalog::oid_batch_t& oid_batch) {
            using namespace logical_plan;
            switch (node->type()) {
            case node_type::insert_t:
                return rewrite_insert(r, node);
            case node_type::update_t:
                return rewrite_update(r, node);
            case node_type::delete_t:
                return rewrite_delete(r, node);
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