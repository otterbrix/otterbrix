#pragma once

#include "node.hpp"

#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>

namespace components::logical_plan {

    struct detached_column_t {
        catalog::oid_t attoid{catalog::INVALID_OID};
        std::pmr::string name;
        types::complex_logical_type type;
    };

    struct detached_table_t {
        catalog::oid_t oid{catalog::INVALID_OID};
        qualified_name_t name;
        std::pmr::vector<detached_column_t> columns;
    };

    // A block offered to a backend: a copy of the plan's sub-tree on the backend's resource that shares nothing with
    // the plan, so the backend may keep it until it runs. The nodes are the plan's own classes with what a backend
    // reads of them; a node reads its table through table_oid() in `tables` (its table_metadata() is null).
    struct detached_block_t {
        node_ptr root;
        std::pmr::vector<detached_table_t> tables;

        explicit detached_block_t(std::pmr::memory_resource* resource)
            : tables(resource) {}

        const detached_table_t* table(catalog::oid_t oid) const noexcept;
    };

    // The classes a block holds: a relation (a SELECT block, a join, an UPDATE / DELETE) and the clauses under it.
    // Anything else (a function, a CTE scan, a catalog read, a data node) ends a block.
    bool may_be_in_a_block(node_type type) noexcept;

    // Refused for a node class a block may not hold.
    core::result_wrapper_t<detached_block_t> detach_block(std::pmr::memory_resource* target, const node_t& root);

} // namespace components::logical_plan
