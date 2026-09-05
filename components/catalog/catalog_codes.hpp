#pragma once

// Named constants for pg_catalog single-character code columns.
// Mirrors the PostgreSQL convention of storing kind/type discriminators
// as single chars in catalog tables (relkind, contype, etc.).

namespace components::catalog {

    // pg_class.relkind
    namespace relkind {
        inline constexpr char regular = 'r';           // ordinary table
        inline constexpr char index = 'i';             // index
        inline constexpr char sequence = 'S';          // sequence
        inline constexpr char view = 'v';              // view
        inline constexpr char materialized_view = 'm'; // materialized view (PostgreSQL-canonical)
        inline constexpr char composite_type = 'c';    // composite type
        inline constexpr char computed = 'g';          // computed/virtual table (otterbrix extension)
        inline constexpr char macro = 'F';             // pg_rewrite-backed macro (function-like)
    }                                                  // namespace relkind

    // pg_index.indtype (otterbrix extension: which physical index backend owns the
    // index's on-disk directory). NOT nullable and has no default: a pg_index row
    // whose indtype is missing or outside this alphabet is catalog corruption and
    // the reader must fail LOUDLY. Mapping to/from logical_plan::index_type lives
    // in node_create_index.hpp (catalog must not depend on logical_plan).
    namespace indtype {
        inline constexpr char single = 's';    // ordered B+tree
        inline constexpr char composite = 'c'; // composite key (B+tree)
        inline constexpr char multikey = 'm';  // multikey (B+tree)
        inline constexpr char hashed = 'h';    // bitcask LSM + disk hash
        inline constexpr char wildcard = 'w';  // wildcard (B+tree)
    }                                          // namespace indtype

    // pg_constraint.contype
    namespace contype {
        inline constexpr char check = 'c';
        inline constexpr char foreign_key = 'f';
    } // namespace contype

    // pg_class.relstoragemode (otterbrix-specific: physical storage backing).
    // B1a: every table is disk-backed; the column stays (write-only, no readers)
    // and is always written 'd'. The old 'm' (in-memory) value is gone.
    namespace relstoragemode {
        inline constexpr char disk = 'd'; // table.otbx on disk
    }                                     // namespace relstoragemode

    // pg_constraint.confmatchtype (FK match strategy)
    namespace fk_match {
        inline constexpr char simple = 's';
    } // namespace fk_match

    // pg_constraint.confdeltype / confupdtype (FK referential action)
    namespace fk_action {
        inline constexpr char no_action = 'a';
    } // namespace fk_action

} // namespace components::catalog
