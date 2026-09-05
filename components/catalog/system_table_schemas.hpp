#pragma once

// Intentional architecture note — Stage 2 catalog modules not implemented:
//
//   fk_rules.{hpp,cpp} (should_skip_validation, classify_action)
//     → FK null semantics and action dispatch are inline in operator_fk_check /
//       operator_fk_cascade via fk_info_t.matchtype / del_action.  A separate
//       module would add indirection with no reuse benefit at current scale.
//
//   constraint_evaluator.{hpp,cpp} (enforce_not_null, evaluate_check)
//     → NOT NULL and CHECK enforcement are inline in operator_check_constraint.
//       Expressions are compiled once to predicate_ptr in the constructor;
//       a separate evaluator would duplicate the predicate infrastructure.
//
//   pg_catalog_decoders.{hpp,cpp} (typed views of pg_* rows)
//     → Typed decoding is inline in disk resolver methods (manager_disk_ddl,
//       manager_disk_resolve).  A shared decoder layer is deferred until there
//       are three or more call sites with identical row layouts.

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
    //                       expression text) is replaced by `attdefspec` (a hex-armoured
    //                       binary logical_value_t) — strictly richer round-trip. `atttypspec`
    //                       carries the full complex_logical_type tree for non-scalar types.
    //                       `attisdropped` (PG tombstone) prevents attnum reuse.
    //   pg_proc           — no `proowner` (no roles). `proargtypes` (CSV of input type OIDs)
    //                       is subsumed by `proargmatchers` (per-arg tagged matchers — exact,
    //                       numeric, integer, floating, any_of, always_true), strictly richer.
    //                       `prorettype` is encoded as a list of output_type tags, not a single
    //                       OID, to support same_type_at_index resolution. `prouid` carries the
    //                       opaque function_uid the executor produced via register_udf.
    //   pg_constraint     — no `conindid` (constraint→supporting-index linkage isn't consumed)
    //                       and no `conexpr` (CHECK-expression text — CHECK constraints are
    //                       not yet validated through pg_constraint). Carries FK semantics
    //                       directly: `confrelid`/`conkey`/`confkey`/`confmatchtype`/
    //                       `confdeltype`/`confupdtype`.
    //   pg_index          — no `indisprimary`/`indisunique` (UNIQUE is recorded via
    //                       pg_constraint contype='u'). Carries `indisvalid` so the planner
    //                       can hide a not-yet-backfilled index, and `indtype` — the
    //                       single-char physical-backend code (catalog_codes.hpp) the
    //                       restart bootstrap reads to pick the on-disk reader
    //                       (bitcask vs B+tree). indtype is NOT nullable and has no
    //                       default; a row without it is a loud bootstrap error.
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
        std::string_view name; // e.g. "pg_class"
        oid_t relation_oid;    // pg_class.oid for this relation itself
        oid_t namespace_oid;   // always well_known_oid::pg_catalog_namespace
        char relkind;          // 'r' relation, 'i' index, etc.
        std::vector<table::column_definition_t> columns;
    };

    // Returns the system tables in bootstrap order (pg_database first — every other
    // catalog object is conceptually scoped to a database). Backed by a function-local
    // `static const std::array<...,14>` populated on first call (C++11 magic-statics
    // — thread-safe init). Subsequent calls return a zero-cost `std::span` view.
    std::span<const system_table_def_t> all_system_tables();

    // Lookup a system table by its well-known relation OID (rule 16: system tables are
    // addressed by oid, never by name). Every system table has a fixed OID below
    // FIRST_USER_OID (catalog_oids.hpp), so a caller holding `well_known_oid::pg_*_table`
    // always gets a definition back; nullptr means the OID is not a system table at all.
    // This is the form production code must use — the schema array is the single place
    // that still knows the names.
    const system_table_def_t* find_system_table(oid_t relation_oid);


    // Type-spec round-trip helpers used by both pg_attribute (atttypspec) and pg_type
    // (typdefspec). For built-in scalar types `encode_type_spec` returns "" — atttypid /
    // typdefspec=NULL is sufficient for round-trip. For complex types (DECIMAL, ARRAY,
    // LIST, ENUM, STRUCT, MAP, UNKNOWN) the full complex_logical_type tree is serialized
    // as flat-text (e.g. "numeric(18,6)") so readers can reconstruct precision/scale,
    // element types, child fields, enum entries, etc. across restart.
    //
    // `decode_type_spec` is fail-loud (rule 6): every spec outside the encoder's exact
    // language — an unrecognised name or keyword, malformed or out-of-window DECIMAL
    // width/scale, a missing separator, trailing bytes after a complete type, nesting
    // beyond the depth window shared with the binary codec — is a data_corruption error,
    // never a guessed type. Two UNKNOWN answers remain LEGITIMATE values, not errors:
    // the empty spec (builtin scalars store no spec; atttypid alone reconstructs them)
    // and the explicit "UNKNOWN(name)" form (a named user-type reference the resolver
    // chases by name). The refusal reaches the reader's statement, where it costs one
    // resolve; no caller has to read UNKNOWN as a refusal channel.
    std::string encode_type_spec(const types::complex_logical_type& t);
    [[nodiscard]] core::result_wrapper_t<types::complex_logical_type>
    decode_type_spec(std::pmr::memory_resource* resource, std::string_view spec);

    // Encode the per-arg `parameter_type` to a flat text format suitable for
    // pg_proc.proargmatchers. Format per arg: "e:N" a concrete type, "v:I" a variable with
    // id I accepting anything, "v:I:N1,N2,..." a variable restricted to those types, where
    // N is the numeric value of types::logical_type. Multiple args are pipe-separated.
    // Empty input vector → "".
    // A concrete parameter's extension (decimal width/scale, element types) is NOT encoded,
    // matching what the matcher form stored; nothing decodes this column yet.
    std::string encode_proargmatchers(const std::vector<components::compute::parameter_type>& parameters);

    // Encode output_type list to a flat text format. Per output: "f:N" fixed type
    // (N = logical_type id), "s:N" same_type_at_index N, "c" a custom resolver
    // (output_type::computed). Multiple outputs are comma-separated. "c" is an EXPLICIT
    // non-introspectable marker: persisting a custom resolver as "s:0" silently claims a
    // same-type-as-argument-0 contract the function never declared. A custom resolver
    // cannot be refused outright either — registering one is pinned legal behaviour
    // (test_udfs registers computed(same_type_resolver(0))), and its runtime form is
    // reconstructed through pg_proc.prouid → compute::function_registry, never by
    // parsing this column — so the honest answer is a truthful tag, not a guessed
    // contract and not a refusal.
    std::string encode_prorettype(const std::vector<components::compute::output_type>& outputs);

    // Return the canonical pg_type.typname for a built-in logical_type (e.g. INTEGER →
    // "int4", BIGINT → "int8"). Returns "" for DECIMAL, UNKNOWN, and complex types —
    // caller should use "numeric" for DECIMAL and type_name() for UNKNOWN.
    std::string_view logical_type_to_pg_name(types::logical_type t) noexcept;

    // Map a well_known pg_type.oid back to its components::types::logical_type. Returns
    // logical_type::UNKNOWN for non-builtin OIDs — caller resolves complex types from
    // pg_type by name + typdefspec.
    types::logical_type oid_to_builtin_type(oid_t oid) noexcept;
    oid_t builtin_type_to_oid(types::logical_type lt) noexcept;

    // Resolve a type name (including legacy aliases like "string", "boolean", "integer")
    // to its canonical logical_type. Returns logical_type::UNKNOWN for user-defined types.
    types::logical_type pg_name_to_logical_type(std::string_view name) noexcept;

    // Encode/decode a column DEFAULT value for storage in pg_attribute.attdefspec.
    //
    // The value is encoded BINARY, by the one binary value codec in the tree
    // (components/index/logical_value_binary_codec.hpp — the same codec that writes index keys),
    // then hex-armoured so the column stays printable text like its neighbour atttypspec. The
    // encoding is type-DIRECTED: the payload SHAPE comes from the column's own type, which sits
    // one column away in atttypspec, so no width or field layout is stored. That is what makes it
    // lossless for every type the codec carries, nested types included, rather than only for the
    // ones somebody remembered to list in a switch. The payload does carry one logical tag byte
    // per present value, purely as a check: it catches a SAME-WIDTH type divergence, which the
    // shape alone cannot see.
    //
    // Three states, all distinguishable — a flat "type_name:value" form collapses the last two
    // into "no default" and drops every type outside its switch:
    //   ""        no default at all
    //   "N"       an explicit DEFAULT NULL
    //   "V"<hex>  the encoded value
    //
    // Rule 6: a value whose type the codec cannot carry is an ERROR, surfaced at CREATE TABLE /
    // ALTER SET DEFAULT, never a silent "no default". Symmetrically, a non-empty spec that does
    // not decode against `column_type` is catalog corruption and is reported as such — `out` is
    // set to nullopt ONLY for a genuinely absent default.
    [[nodiscard]] core::error_t
    encode_default_spec(std::pmr::memory_resource* resource, const types::logical_value_t& v, std::string& out);
    [[nodiscard]] core::error_t decode_default_spec(std::pmr::memory_resource* resource,
                                                    const types::complex_logical_type& column_type,
                                                    std::string_view spec,
                                                    std::optional<types::logical_value_t>& out);

} // namespace components::catalog
