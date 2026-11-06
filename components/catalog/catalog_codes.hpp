#pragma once

namespace components::catalog {

    namespace relkind {
        inline constexpr char regular = 'r'; // ordinary table
        inline constexpr char index = 'i';
        inline constexpr char sequence = 'S';
        inline constexpr char view = 'v';
        inline constexpr char materialized_view = 'm'; // PostgreSQL-canonical
        inline constexpr char composite_type = 'c';
        inline constexpr char computed = 'g'; // otterbrix extension
        inline constexpr char macro = 'F';    // pg_rewrite-backed (function-like)
    }                                         // namespace relkind

    // pg_index.indtype (otterbrix extension): a row whose value is outside this alphabet is catalog corruption and must
    // fail loudly. Mapping to/from logical_plan::index_type lives in node_create_index.hpp, because catalog must
    // not depend on logical_plan.
    namespace indtype {
        inline constexpr char single = 's';    // ordered B+tree
        inline constexpr char composite = 'c'; // composite key (B+tree)
        inline constexpr char multikey = 'm';  // B+tree
        inline constexpr char hashed = 'h';    // bitcask LSM + disk hash
        inline constexpr char wildcard = 'w';  // B+tree
    }                                          // namespace indtype

    namespace contype {
        inline constexpr char check = 'c';
        inline constexpr char foreign_key = 'f';
    } // namespace contype

    // pg_class.relstoragemode (otterbrix-specific): every table is disk-backed, so this is always 'd'.
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
