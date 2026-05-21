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

    // pg_constraint.contype
    namespace contype {
        inline constexpr char check = 'c';
        inline constexpr char foreign_key = 'f';
    } // namespace contype

    // pg_class.relstoragemode (otterbrix-specific: physical storage backing)
    namespace relstoragemode {
        inline constexpr char disk = 'd';      // table.otbx on disk
        inline constexpr char in_memory = 'm'; // no persistence
    }                                          // namespace relstoragemode

    // pg_constraint.confmatchtype (FK match strategy)
    namespace fk_match {
        inline constexpr char simple = 's';
    } // namespace fk_match

    // pg_constraint.confdeltype / confupdtype (FK referential action)
    namespace fk_action {
        inline constexpr char no_action = 'a';
    } // namespace fk_action

} // namespace components::catalog
