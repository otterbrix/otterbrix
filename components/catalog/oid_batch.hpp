#pragma once

#include "catalog_oids.hpp"

#include <cassert>
#include <vector>

namespace components::catalog {

    // Pre-allocated batch of OIDs handed to the planner before DDL logical rewrite.
    // The dispatcher co_awaits allocate_oids_batch() from disk, then passes the
    // result here so planner_t::create_plan can build pg_class/pg_attribute rows
    // without async disk access.
    struct oid_batch_t {
        std::vector<oid_t> oids;
        std::size_t next = 0;

        bool empty() const noexcept { return next >= oids.size(); }
        oid_t allocate() noexcept {
            assert(!empty());
            return oids[next++];
        }
        // Inspect the next OID without consuming it. Used by the planner to mirror
        // the about-to-be-allocated table_oid onto the cc node for the physical
        // plan generator.
        oid_t peek() const noexcept {
            assert(!empty());
            return oids[next];
        }
    };

} // namespace components::catalog