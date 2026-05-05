#pragma once

#include <components/catalog/catalog_oids.hpp>

#include <string_view>
#include <vector>

namespace components::catalog {

    // Seed rows for the bootstrap phase of pg_catalog initialization.
    // Free functions returning plain data structs — no I/O, no state, no actor deps.
    // The caller (manager_disk_t::bootstrap_system_tables_sync) iterates these
    // and calls direct_append_sync using disk-layer row builders.

    struct ns_seed_row_t {
        oid_t oid;
        std::string_view name;
    };

    struct type_seed_row_t {
        oid_t oid;
        std::string_view name;
    };

    struct proc_seed_row_t {
        oid_t oid;
        std::string_view name;
    };

    // pg_database: single "main" database row seeded at bootstrap.
    ns_seed_row_t builtin_database_row();

    // pg_namespace: pg_catalog, public, information_schema.
    std::vector<ns_seed_row_t> builtin_namespace_rows();

    // pg_type: all builtin scalar types and their SQL/PG alias names.
    std::vector<type_seed_row_t> builtin_type_rows();

    // pg_proc: count, sum, avg, min, max.
    std::vector<proc_seed_row_t> builtin_proc_rows();

} // namespace components::catalog
