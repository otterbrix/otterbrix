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
        std::vector<invalidation_event_t> events;
        std::uint64_t catalog_version{0};

        resolve_namespace_result_t() = default;
        explicit resolve_namespace_result_t(std::pmr::memory_resource* /*resource*/) {}
    };

    // Per-column metadata returned by resolve_table. Mirrors pg_attribute row layout so the
    // dispatcher (Phase E.2 catalog_view_t) can reconstruct a column_definition_t with full
    // type info (decoding atttypspec when present, falling back to atttypid → built-in type).
    struct column_info_t {
        std::string attname;
        components::catalog::oid_t atttypid{components::catalog::INVALID_OID};
        std::int32_t attnum{0};
        bool attnotnull{false};
        bool atthasdefault{false};
        bool attisdropped{false};
        std::string atttypspec; // serialized complex_logical_type (empty for built-ins)
        std::string attdefspec; // serialized default value (empty if no default)
        components::catalog::oid_t attoid{components::catalog::INVALID_OID};
    };

    struct resolve_table_result_t {
        bool found{false};
        components::catalog::oid_t oid{components::catalog::INVALID_OID};
        components::catalog::oid_t namespace_oid{components::catalog::INVALID_OID};
        char relkind{'r'};
        std::string name;
        // V4: full per-column info (replaces the old `column_oids` field). Sorted by attnum.
        // Dropped columns (attisdropped=true) are NOT included by default.
        std::vector<column_info_t> columns;
        std::vector<invalidation_event_t> events;
        std::uint64_t catalog_version{0};

        resolve_table_result_t() = default;
        explicit resolve_table_result_t(std::pmr::memory_resource* /*resource*/) {}
    };

    struct resolve_type_result_t {
        bool found{false};
        components::catalog::oid_t oid{components::catalog::INVALID_OID};
        components::catalog::oid_t namespace_oid{components::catalog::INVALID_OID};
        std::string name;
        // V4: pg_type.typdefspec — encoded complex_logical_type tree. Empty for built-in
        // scalar types (caller maps via oid_to_builtin_type). Non-empty for STRUCT, ENUM,
        // DECIMAL, ARRAY, MAP, etc. Decode via components::catalog::decode_type_spec.
        std::string typdefspec;
        std::vector<invalidation_event_t> events;
        std::uint64_t catalog_version{0};

        resolve_type_result_t() = default;
        explicit resolve_type_result_t(std::pmr::memory_resource* /*resource*/) {}
    };

    struct resolve_function_result_t {
        bool found{false};
        components::catalog::oid_t oid{components::catalog::INVALID_OID};
        components::catalog::oid_t namespace_oid{components::catalog::INVALID_OID};
        std::string name;
        // V4: pg_proc fields needed to reconstruct kernel_signature_t in catalog_view_t.
        // prouid is the bridge to the in-memory function_registry_t (compute::function_uid).
        // proargmatchers / prorettype are encoded strings — caller decodes via
        // components::catalog::decode_proargmatchers / decode_prorettype.
        std::int32_t pronargs{0};
        std::uint64_t prouid{0};
        std::string proargmatchers;
        std::string prorettype;
        std::vector<invalidation_event_t> events;
        std::uint64_t catalog_version{0};

        resolve_function_result_t() = default;
        explicit resolve_function_result_t(std::pmr::memory_resource* /*resource*/) {}
    };

} // namespace services::disk
