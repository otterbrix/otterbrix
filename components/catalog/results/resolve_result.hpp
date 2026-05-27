#pragma once

#include <components/catalog/results/ddl_result.hpp>

#include <components/catalog/catalog_oids.hpp>

#include <cstdint>
#include <memory_resource>
#include <string>
#include <vector>

namespace services::disk {

    struct resolve_namespace_result_t {
        bool found{false};
        components::catalog::oid_t oid{components::catalog::INVALID_OID};
        std::string name;

        resolve_namespace_result_t() = default;
        explicit resolve_namespace_result_t(std::pmr::memory_resource* /*resource*/) {}
    };

    // Per-column metadata returned by resolve_table. Mirrors pg_attribute row layout so the
    // dispatcher can reconstruct a column_definition_t with full type info
    // (decoding atttypspec when present, falling back to atttypid → built-in type).
    // Field order: strings → oid_t/int32_t → bool, to minimise padding.
    struct column_info_t {
        std::string attname;
        std::string atttypspec; // serialized complex_logical_type (empty for built-ins)
        std::string attdefspec; // serialized default value (empty if no default)
        components::catalog::oid_t atttypid{components::catalog::INVALID_OID};
        components::catalog::oid_t attoid{components::catalog::INVALID_OID};
        std::int32_t attnum{0};
        bool attnotnull{false};
        bool atthasdefault{false};
        bool attisdropped{false};
    };
    // Layout guard: libstdc++ sizeof(std::string)==32 → 112; libc++ sizeof==24 → 88.
    static_assert(sizeof(column_info_t) <= 112, "column_info_t layout regression");

    struct resolve_table_result_t {
        bool found{false};
        components::catalog::oid_t oid{components::catalog::INVALID_OID};
        components::catalog::oid_t namespace_oid{components::catalog::INVALID_OID};
        char relkind{'r'};
        std::string name;
        std::vector<column_info_t> columns;

        resolve_table_result_t() = default;
        explicit resolve_table_result_t(std::pmr::memory_resource* /*resource*/) {}
    };

    struct resolve_type_result_t {
        bool found{false};
        components::catalog::oid_t oid{components::catalog::INVALID_OID};
        components::catalog::oid_t namespace_oid{components::catalog::INVALID_OID};
        std::string name;
        std::string typdefspec;

        resolve_type_result_t() = default;
        explicit resolve_type_result_t(std::pmr::memory_resource* /*resource*/) {}
    };

    // Field order: strings → uint64 → oid_t/int32 → bool.
    struct resolve_function_result_t {
        std::string name;
        std::string proargmatchers;
        std::string prorettype;
        std::uint64_t prouid{0};
        components::catalog::oid_t oid{components::catalog::INVALID_OID};
        components::catalog::oid_t namespace_oid{components::catalog::INVALID_OID};
        std::int32_t pronargs{0};
        bool found{false};

        resolve_function_result_t() = default;
        explicit resolve_function_result_t(std::pmr::memory_resource* /*resource*/) {}
    };
    // Layout guard: libstdc++ (string==32) → 120; libc++ (string==24) → 96.
    static_assert(sizeof(resolve_function_result_t) <= 120, "resolve_function_result_t layout regression");

} // namespace services::disk
