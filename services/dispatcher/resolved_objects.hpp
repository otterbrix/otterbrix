#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/fk_info.hpp>
#include <components/types/types.hpp>

#include <cstdint>
#include <string>
#include <vector>

namespace services::dispatcher {

    // Payload types stored in catalog_view_t's per-instance cache.
    // Each maps to one resolve_*_result_t shape from services/disk/resolve_result.hpp.

    struct resolved_namespace_t {
        components::catalog::oid_t oid{components::catalog::INVALID_OID};
        std::string name;
    };

    // Per-column metadata reconstructed from pg_attribute. Mirrors the column_definition_t
    // surface that validate_logical_plan reads (.name(), .type(), .is_not_null(),
    // .has_default_value(), .attoid()) — Phase E.2 catalog_view_t fills `type` by decoding
    // atttypspec or mapping atttypid → built-in.
    struct resolved_column_t {
        std::string attname;
        components::types::complex_logical_type type;
        std::int32_t attnum{0};
        bool attnotnull{false};
        bool atthasdefault{false};
        components::catalog::oid_t attoid{components::catalog::INVALID_OID};
        std::string attdefspec; // serialized default value; empty if no default
    };

    struct resolved_table_t {
        components::catalog::oid_t oid{components::catalog::INVALID_OID};
        components::catalog::oid_t namespace_oid{components::catalog::INVALID_OID};
        char relkind{'r'}; // 'r' table, 'g' computing, 'i' index, 'v' view, 'S' sequence, 'm' macro
        std::string name;
        // Full column metadata in attnum order. Validate consumes attname, type, attnotnull,
        // atthasdefault. Phase E.0a's resolve_table fills the wire side; Phase E.2 decodes.
        std::vector<resolved_column_t> columns;
    };

    struct resolved_type_t {
        components::catalog::oid_t oid{components::catalog::INVALID_OID};
        components::catalog::oid_t namespace_oid{components::catalog::INVALID_OID};
        std::string name;
        // Decoded type tree (Phase E.2 fills from typdefspec via decode_type_spec). For
        // built-in scalar types where atttypid alone is sufficient, this stays at default
        // (UNKNOWN) — caller maps via oid_to_builtin_type instead.
        components::types::complex_logical_type type;
        // Original typdefspec — kept for round-trip / debugging. May be empty.
        std::string typdefspec;
    };

    // Alias — dispatcher layer uses fk_info_t from catalog component. Kept here
    // for backward-compatibility of call sites that refer to resolved_fk_t.
    using resolved_fk_t = components::catalog::fk_info_t;

} // namespace services::dispatcher