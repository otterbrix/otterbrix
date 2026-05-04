#pragma once

// Named constants for pg_catalog single-character code columns.
// Mirrors the PostgreSQL convention of storing kind/type discriminators
// as single chars in catalog tables (relkind, contype, etc.).

namespace components::catalog {

    // pg_class.relkind
    namespace relkind {
        inline constexpr char regular          = 'r'; // ordinary table
        inline constexpr char index            = 'i'; // index
        inline constexpr char sequence         = 'S'; // sequence
        inline constexpr char view             = 'v'; // view
        inline constexpr char composite_type   = 'c'; // composite type
        inline constexpr char computed         = 'g'; // computed/virtual table (otterbrix extension)
    } // namespace relkind

    // pg_constraint.contype
    namespace contype {
        inline constexpr char check      = 'c';
        inline constexpr char foreign_key = 'f';
        inline constexpr char primary_key = 'p';
        inline constexpr char unique      = 'u';
        inline constexpr char not_null    = 'n';
    } // namespace contype

    // pg_constraint.confmatchtype (FK match strategy)
    namespace fk_match {
        inline constexpr char simple  = 's';
        inline constexpr char full    = 'f';
        inline constexpr char partial = 'p';
    } // namespace fk_match

    // pg_constraint.confdeltype / confupdtype (FK referential action)
    namespace fk_action {
        inline constexpr char no_action   = 'a';
        inline constexpr char restrict_   = 'r'; // trailing _ avoids clash with restrict keyword
        inline constexpr char cascade     = 'c';
        inline constexpr char set_null    = 'n';
        inline constexpr char set_default = 'd';
    } // namespace fk_action

} // namespace components::catalog
