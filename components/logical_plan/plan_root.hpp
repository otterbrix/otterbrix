#pragma once

#include "node.hpp"

namespace components::logical_plan {

    // === Plan-root / base-relation unwrap helpers ===
    //
    // Single canonical home for the resolve-wrap descent shared by the SQL
    // transformer, the dispatcher and the executor — no per-layer mirrors.

    // When the SQL transformer wraps a plan in
    //   sequence_t(catalog_resolve_namespace_t, catalog_resolve_table_t, <real_root>)
    // callers need to route on <real_root> (insert_t, aggregate_t, ...), not the
    // wrapping sequence_t. Descends such a resolve-wrapping sequence_t and returns
    // the LAST non-catalog_resolve_* child (the "consumer" node, after the
    // resolution-only prefix). A planner-style sequence_t (front child is not a
    // resolve) and any non-sequence root are returned unchanged; a sequence holding
    // only resolve children returns itself. nullptr only for a null input.
    inline const node_t* effective_root_node(const node_t* n) {
        if (!n) {
            return nullptr;
        }
        if (n->type() != node_type::sequence_t) {
            return n;
        }
        const auto& kids = n->children();
        // Only descend when the first child is a catalog_resolve_* — this
        // distinguishes the transformer's resolve-wrapping sequence_t from the
        // planner's DDL/DML rewrite sequence_t (which has e.g. create_collection_t
        // + catalog-write node_insert_t children, no resolves).
        if (kids.empty() || !kids.front() || kids.front()->type() != node_type::catalog_resolve_t) {
            return n;
        }
        // Walk children back-to-front: the real consumer is the last non-resolve
        // child (resolve nodes sit at the front of the sequence).
        for (auto it = kids.rbegin(); it != kids.rend(); ++it) {
            if (*it && (*it)->type() != node_type::catalog_resolve_t) {
                return it->get();
            }
        }
        return n;
    }

    // Mutable-pointer overload, for call sites that mutate the consumer node.
    inline node_t* effective_root_node(node_t* n) {
        return const_cast<node_t*>(effective_root_node(static_cast<const node_t*>(n)));
    }

    // The EFFECTIVE base relation behind a plan sub-tree: the node's own resolved
    // table stamp, or — for the filter wrapper pushdown_filter synthesizes around
    // a join input (an oid-less aggregate_t holding [source, match...]) — the
    // wrapped source's stamp, found by descending through match-only wrappers.
    // INVALID_OID for anything else (a join sub-tree, a set operation, raw data):
    // those have no single backing relation whose live row count could bound their
    // size. Both the count-fetch side (collect_inner_hash_join_oids in
    // services/collection/executor.cpp) and the consumer (create_plan_join.cpp's
    // hash-join build-side selection) resolve join inputs through THIS one descent,
    // so every side with a backing relation gets a live count.
    inline catalog::oid_t effective_table_oid(const node_ptr& n) {
        if (!n) {
            return catalog::INVALID_OID;
        }
        if (n->table_oid() != catalog::INVALID_OID) {
            return n->table_oid();
        }
        if (n->type() != node_type::aggregate_t || n->children().empty()) {
            return catalog::INVALID_OID;
        }
        for (size_t i = 1; i < n->children().size(); ++i) {
            const auto& c = n->children()[i];
            if (!c || c->type() != node_type::match_t) {
                return catalog::INVALID_OID;
            }
        }
        return effective_table_oid(n->children()[0]);
    }

} // namespace components::logical_plan
