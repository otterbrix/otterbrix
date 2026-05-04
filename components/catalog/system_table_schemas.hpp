#pragma once

#include "catalog_oids.hpp"
#include <components/compute/kernel_signature.hpp>
#include <components/table/column_definition.hpp>

#include <components/types/logical_value.hpp>

#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace components::catalog {

    // PostgreSQL-style system catalog schemas. The 10 hardcoded relation definitions plus
    // their bootstrap rows. After bootstrap, every catalog operation is a regular insert/scan
    // against one of these tables (no special-cased in-memory structures).
    //
    // For each system table we expose:
    //   - the catalog identity (oid in pg_class) — `relation_oid`
    //   - the namespace where it lives (always pg_catalog) — `namespace_oid`
    //   - its column list — `columns()`
    //   - bootstrap rows that must exist on first start — populated by bootstrap_system_tables_sync.
    //
    // Intentional deviations from PG-canonical schema (otterbrix has no analogue of these
    // PG features, so storing the columns would be dead schema):
    //
    //   pg_namespace      — no `nspowner` (otterbrix has no role/user concept).
    //   pg_class          — no `reltuples`/`relpages`/`reltype` (no cost-based optimizer using
    //                       cardinality stats; no row composite types). Carries an otterbrix-
    //                       specific `relstoragemode` ('d'=disk, 'm'=in-memory) instead.
    //   pg_attribute      — no `attstattarget` (no stats target). `attdefval` (raw default
    //                       expression text) is replaced by `attdefspec` (flat-text-encoded
    //                       logical_value_t) — strictly richer round-trip. `atttypspec`
    //                       carries the full complex_logical_type tree for non-scalar types.
    //                       `attisdropped` (PG tombstone) prevents attnum reuse.
    //   pg_proc           — no `proowner` (no roles). `proargtypes` (CSV of input type OIDs)
    //                       is subsumed by `proargmatchers` (per-arg tagged matchers — exact,
    //                       numeric, integer, floating, any_of, always_true), strictly richer.
    //                       `prorettype` is encoded as a list of output_type tags, not a single
    //                       OID, to support same_type_at_index resolution. `prouid` carries the
    //                       opaque function_uid the executor produced via register_udf.
    //   pg_depend         — no `objsubid`/`refobjsubid` (column-level dependencies are not
    //                       tracked by dependency_walker yet — whole-object only).
    //   pg_constraint     — no `conindid` (constraint→supporting-index linkage isn't consumed)
    //                       and no `conexpr` (CHECK-expression text — CHECK constraints are
    //                       not yet validated through pg_constraint). Carries FK semantics
    //                       directly: `confrelid`/`conkey`/`confkey`/`confmatchtype`/
    //                       `confdeltype`/`confupdtype`.
    //   pg_index          — no `indisprimary`/`indisunique`/`indtype` (UNIQUE is recorded via
    //                       pg_constraint contype='u', and index type isn't read by the
    //                       planner). Carries `indisvalid` so the planner can hide a
    //                       not-yet-backfilled index.
    //
    // Additional system tables beyond the initial 10 (see catalog_oids.hpp):
    //   pg_sequence (oid=34): sequence start/increment/min/max/cycle/last_value — seqrelid FK
    //                         to pg_class.oid; no own OID column.
    //   pg_rewrite  (oid=35): view/macro body persistence — own OID column (oid); ev_class FK
    //                         to pg_class.oid; ev_action stores the SQL or macro body text.
    //
    // pg_database is bootstrapped with a single row for the default "main" database
    // (well_known_oid::main_database). otterbrix has no cluster-vs-database split, but a
    // pg_database table makes CREATE DATABASE / DROP DATABASE first-class DDL — additional
    // databases get OIDs from oid_generator and are stored as additional rows in pg_database.

    struct system_table_def_t {
        std::string_view name;             // e.g. "pg_class"
        oid_t relation_oid;                // pg_class.oid for this relation itself
        oid_t namespace_oid;               // always well_known_oid::pg_catalog_namespace
        char relkind;                      // 'r' relation, 'i' index, etc.
        std::vector<table::column_definition_t> columns;
    };

    // Returns the 9 system tables, in bootstrap order (pg_namespace first, since pg_class
    // and pg_attribute reference namespaces).
    std::vector<system_table_def_t> all_system_tables();

    // Convenience: lookup a system table by name (returns nullptr if not a system table).
    // Useful for routing during DDL — manager_disk_t needs to know which physical
    // collection ("pg_catalog.<name>") backs a logical pg_<x> reference.
    const system_table_def_t* find_system_table(std::string_view name);

    // Type-spec round-trip helpers used by both pg_attribute (atttypspec) and pg_type
    // (typdefspec). For built-in scalar types `encode_type_spec` returns "" — atttypid /
    // typdefspec=NULL is sufficient for round-trip. For complex types (DECIMAL, ARRAY,
    // LIST, ENUM, STRUCT, MAP, UNKNOWN) the full complex_logical_type tree is serialized
    // via msgpack so catalog_view_t can reconstruct precision/scale, element types, child
    // fields, enum entries, etc. across restart. `decode_type_spec` returns
    // logical_type::UNKNOWN on empty/malformed input — non-throwing best-effort.
    std::string encode_type_spec(const types::complex_logical_type& t);
    types::complex_logical_type decode_type_spec(std::pmr::memory_resource* resource,
                                                  const std::string& spec);

    // Encode/decode the per-arg `input_type` tagged matcher to a flat text format suitable
    // for pg_proc.proargmatchers. Format per arg: "e:N" exact, "n" numeric, "i" integer,
    // "f" floating, "a:N1,N2,..." any_of, "t" always_true, where N is numeric value of
    // types::logical_type. Multiple args are pipe-separated. Empty input vector → "".
    // decode_proargmatchers returns an empty vector on malformed/empty input — populate
    // falls back to placeholder always_true matchers in that case.
    std::string encode_proargmatchers(const std::vector<components::compute::input_type>& matchers);
    std::vector<components::compute::input_type> decode_proargmatchers(const std::string& spec);

    // Encode/decode output_type list to a flat text format. Per output: "f:N" fixed type
    // (N = logical_type id), "s:N" same_type_at_index N. Multiple outputs are comma-
    // separated. computed_fn outputs are encoded as "s:0" — lossy but the common case is
    // identity, and the resolver isn't reproducible across persistence anyway.
    std::string encode_prorettype(const std::vector<components::compute::output_type>& outputs);
    std::vector<components::compute::output_type> decode_prorettype(const std::string& spec);

    // Return the canonical pg_type.typname for a built-in logical_type (e.g. INTEGER →
    // "int4", BIGINT → "int8"). Returns "" for DECIMAL, UNKNOWN, and complex types —
    // caller should use "numeric" for DECIMAL and type_name() for UNKNOWN.
    std::string_view logical_type_to_pg_name(types::logical_type t) noexcept;

    // Map a well_known pg_type.oid back to its components::types::logical_type. Returns
    // logical_type::UNKNOWN for non-builtin OIDs — caller resolves complex types from
    // pg_type by name + typdefspec.
    types::logical_type oid_to_builtin_type(oid_t oid) noexcept;

    // Encode/decode a column default value (logical_value_t) to flat text for storage in
    // pg_attribute.attdefspec. Format: "type_name:value" for scalars, "NULL" for null.
    // Returns "" for complex types (ARRAY/STRUCT/LIST) — treated as no default on decode.
    std::string encode_default_spec(const types::logical_value_t& v);
    std::optional<types::logical_value_t>
        decode_default_spec(std::pmr::memory_resource* resource, const std::string& spec);

} // namespace components::catalog
