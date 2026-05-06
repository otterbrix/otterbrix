#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/fk_info.hpp>
#include <components/compute/function.hpp>
#include <components/compute/kernel_signature.hpp>
#include <components/types/types.hpp>

#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <optional>
#include <string>
#include <vector>

namespace services::dispatcher {

    // Payload types stored in versioned_plan_cache_t for V4 lazy-resolve.
    // Each maps to one resolve_*_result_t shape from services/disk/resolve_result.hpp,
    // augmented with a memory_bytes() method for the cache budget.
    //
    // Spec ref: catalog-migration-to-postgresql-style.md §181-198 (V4 access path),
    // §1083-1106 (cache flow). docs/v4-catalog-refactoring.md §5 Phase E.1.

    struct resolved_namespace_t {
        components::catalog::oid_t oid{components::catalog::INVALID_OID};
        std::string name;

        [[nodiscard]] std::size_t memory_bytes() const noexcept {
            return sizeof(resolved_namespace_t) + name.capacity();
        }
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

        [[nodiscard]] std::size_t memory_bytes() const noexcept {
            // complex_logical_type's deep size isn't exposed; assume amortized 64 bytes per
            // type tree. Adequate for budget tracking — exact accounting isn't required.
            return sizeof(resolved_column_t) + attname.capacity() + 64;
        }
    };

    struct resolved_table_t {
        components::catalog::oid_t oid{components::catalog::INVALID_OID};
        components::catalog::oid_t namespace_oid{components::catalog::INVALID_OID};
        char relkind{'r'}; // 'r' table, 'g' computing, 'i' index, 'v' view, 'S' sequence, 'm' macro
        std::string name;
        // Full column metadata in attnum order. Validate consumes attname, type, attnotnull,
        // atthasdefault. Phase E.0a's resolve_table fills the wire side; Phase E.2 decodes.
        std::vector<resolved_column_t> columns;

        [[nodiscard]] std::size_t memory_bytes() const noexcept {
            std::size_t bytes = sizeof(resolved_table_t) + name.capacity()
                              + columns.capacity() * sizeof(resolved_column_t);
            for (const auto& col : columns) {
                bytes += col.memory_bytes() - sizeof(resolved_column_t);
            }
            return bytes;
        }
    };

    struct resolved_function_t {
        components::catalog::oid_t oid{components::catalog::INVALID_OID};
        components::catalog::oid_t namespace_oid{components::catalog::INVALID_OID};
        std::string name;
        // Argument type OIDs — matches the cache key the validator probes with for
        // overload resolution.
        std::vector<components::catalog::oid_t> arg_type_oids;
        // V4: full kernel signature (matchers + output type templates) decoded from
        // pg_proc.proargmatchers / prorettype. validate_schema reads this to resolve
        // function output type from input types.
        components::compute::function_uid uid{0};
        // Use std::optional-like sentinel: signature with empty input_types means "not yet
        // populated". Phase E.2 populates from resolve_function_result_t.
        std::optional<components::compute::kernel_signature_t> signature;

        [[nodiscard]] std::size_t memory_bytes() const noexcept {
            return sizeof(resolved_function_t) + name.capacity()
                 + arg_type_oids.capacity() * sizeof(components::catalog::oid_t)
                 + (signature ? 256u : 0u); // amortized
        }
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

        [[nodiscard]] std::size_t memory_bytes() const noexcept {
            return sizeof(resolved_type_t) + name.capacity() + typdefspec.capacity() + 64;
        }
    };

    // Alias — dispatcher layer uses fk_info_t from catalog component. Kept here
    // for backward-compatibility of call sites that refer to resolved_fk_t.
    using resolved_fk_t = components::catalog::fk_info_t;

} // namespace services::dispatcher