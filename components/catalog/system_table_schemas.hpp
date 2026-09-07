#pragma once

// Stage 2 catalog modules intentionally left unwritten — each folded inline at current scale
// rather than adding indirection with no reuse benefit: fk_rules into operator_fk_check/cascade,
// constraint_evaluator into operator_check_constraint, pg_catalog_decoders into the disk resolvers.

#include "catalog_oids.hpp"
#include <components/compute/kernel_signature.hpp>
#include <components/table/column_definition.hpp>

#include <components/types/logical_value.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace components::catalog {

    // PostgreSQL-style system catalog schemas: 14 hardcoded relation definitions plus their
    // bootstrap rows (deviations from PG-canonical columns are listed in system_table_schemas.cpp).
    // After bootstrap, every catalog operation is a regular insert/scan against one of these tables.

    struct system_table_def_t {
        std::string_view name; // e.g. "pg_class"
        oid_t relation_oid;    // pg_class.oid for this relation itself
        oid_t namespace_oid;   // always well_known_oid::pg_catalog_namespace
        char relkind;          // 'r' relation, 'i' index, etc.
        std::vector<table::column_definition_t> columns;
    };

    // Backed by a function-local static array populated once (C++11 magic statics); subsequent
    // calls return a zero-cost span view.
    std::span<const system_table_def_t> all_system_tables();

    // System tables are addressed by oid, never by name; nullptr means the OID isn't a system table.
    const system_table_def_t* find_system_table(oid_t relation_oid);


    // Round-trip helpers for pg_attribute.atttypspec / pg_type.typdefspec. `decode_type_spec` is
    // fail-loud: anything outside the encoder's exact grammar is a data_corruption error, never a
    // guessed type; only the empty spec and explicit "UNKNOWN(name)" are legitimate UNKNOWN answers.
    std::string encode_type_spec(const types::complex_logical_type& t);
    [[nodiscard]] core::result_wrapper_t<types::complex_logical_type>
    decode_type_spec(std::pmr::memory_resource* resource, std::string_view spec);

    // Flat text for pg_proc.proargmatchers: "e:N" a concrete type, "v:I[:N1,N2,...]" a variable
    // (I=id, optional admissible-type list); args are pipe-separated.
    std::string encode_proargmatchers(const std::vector<components::compute::parameter_type>& parameters);

    // Flat text for pg_proc.prorettype: "f:N" fixed type, "s:N" same_type_at_index N, "c" a
    // custom resolver (see the K::custom case below for why "c" can't fold into "s:N").
    std::string encode_prorettype(const std::vector<components::compute::output_type>& outputs);

    // Canonical pg_type.typname for a built-in logical_type; "" for DECIMAL, UNKNOWN, and complex types.
    std::string_view logical_type_to_pg_name(types::logical_type t) noexcept;

    // UNKNOWN for non-builtin OIDs — caller resolves complex types from pg_type by name + typdefspec.
    types::logical_type oid_to_builtin_type(oid_t oid) noexcept;
    oid_t builtin_type_to_oid(types::logical_type lt) noexcept;

    // Resolves legacy type-name aliases too (e.g. "string", "boolean"); UNKNOWN for user-defined types.
    types::logical_type pg_name_to_logical_type(std::string_view name) noexcept;

    // Binary-codec encoding (logical_value_binary_codec.hpp), hex-armoured to stay printable text;
    // type-directed, so no width/layout is stored beyond atttypspec. Three distinguishable states:
    //   ""        no default at all
    //   "N"       an explicit DEFAULT NULL
    //   "V"<hex>  the encoded value
    // A type the codec can't carry is refused at CREATE TABLE/ALTER; a spec that doesn't decode
    // against `column_type` is reported as catalog corruption.
    [[nodiscard]] core::error_t
    encode_default_spec(std::pmr::memory_resource* resource, const types::logical_value_t& v, std::string& out);
    [[nodiscard]] core::error_t decode_default_spec(std::pmr::memory_resource* resource,
                                                    const types::complex_logical_type& column_type,
                                                    std::string_view spec,
                                                    std::optional<types::logical_value_t>& out);

} // namespace components::catalog
