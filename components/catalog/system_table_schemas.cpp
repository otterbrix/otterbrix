#include "system_table_schemas.hpp"
#include "catalog_codes.hpp"

#include <array>
#include <charconv>

#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>

// Intentional schema deviations from PostgreSQL — otterbrix is a single-process actor
// framework that does not aim for PG wire-protocol compatibility. Each deviation:
//
//   pg_namespace  — no `nspowner`           : no role/user system today.
//   pg_class      — no `reltuples/relpages` : optimizer reads counts live from data_table_t.
//                 — no `reltype`             : composite-row types not implemented.
//                 — adds `relstoragemode`    : 'd'/'m' for DISK/IN_MEMORY (otterbrix-specific).
//                 — relkind 'g' = computing : doc proposed 'c', but 'c' collides with
//                                              PG's "composite type" relkind. 'g' aligns
//                                              with PG GENERATED terminology.
//   pg_attribute  — adds `attoid`            : stable column OID (FK target for indexes,
//                                              constraints, deps).
//                 — adds `atttypspec`        : flat-text encoded complex_logical_type for
//                                              types that don't fit a single pg_type.oid.
//                 — adds `attdefspec`        : flat-text encoded default value (replaces
//                                              text `attdefval` — survives roundtrip).
//                 — adds `atthasdefault`/`attisdropped` : tombstone; attnum is never reused.
//                 — no `attstattarget`       : no statistics layer yet.
//   pg_type       — no `typlen/typbyval/typtype` : not used by current resolution path.
//                 — adds `typdefspec`        : flat-text encoded type tree (mirrors
//                                              `pg_attribute.atttypspec`).
//   pg_proc       — no `proowner`            : same reason as nspowner.
//                 — `proargmatchers`/`prorettype` as text  : matcher form lets a function
//                                              declare polymorphic arity without N rows.
//                 — adds `prouid`            : index into compute::function_registry where
//                                              kernel_signature_t (function pointers) lives.
//   pg_constraint — no `conindid`              : constraint→index backlink resolved via
//                                              pg_index.indrelid instead.
//                 — adds `conexpr`            : CHECK expr SQL text (stored verbatim;
//                                              executor-side evaluation not yet wired).
//                 — adds confrelid/confkey/conf{matchtype,deltype,updtype} : full FK metadata.
//   pg_index      — no `indisprimary/indisunique/indtype` : PK/uniqueness is enforced via
//                                              pg_constraint, not pg_index. Index implementation
//                                              picker not exposed via SQL DDL yet.
//   pg_database   — added                     : full hierarchy database → namespace → relation.
//                                              10th system table beyond PG's 9.
//
// These deltas are intentional; do not revert them to plain PostgreSQL shapes.

namespace components::catalog {

    using components::table::column_definition_t;
    using components::types::complex_logical_type;
    using components::types::logical_type;

    namespace {
        // OID columns: uint32_t → UINTEGER. Booleans → BOOLEAN. Single-char flags
        // (relkind, deptype) → STRING_LITERAL.

        complex_logical_type oid_col() { return complex_logical_type{logical_type::UINTEGER}; }
        complex_logical_type i32_col() { return complex_logical_type{logical_type::INTEGER}; }
        complex_logical_type i64_col() { return complex_logical_type{logical_type::BIGINT}; }
        complex_logical_type str_col() { return complex_logical_type{logical_type::STRING_LITERAL}; }
        complex_logical_type bool_col() { return complex_logical_type{logical_type::BOOLEAN}; }

        std::vector<column_definition_t> pg_database_columns() {
            std::vector<column_definition_t> c;
            c.emplace_back("oid", oid_col(), /*not_null*/ true);     // pg_database.oid
            c.emplace_back("datname", str_col(), /*not_null*/ true); // database name (unique)
            return c;
        }

        std::vector<column_definition_t> pg_namespace_columns() {
            std::vector<column_definition_t> c;
            c.emplace_back("oid", oid_col(), /*not_null*/ true); // pg_namespace.oid
            c.emplace_back("nspname", str_col(), /*not_null*/ true);
            return c;
        }

        std::vector<column_definition_t> pg_class_columns() {
            std::vector<column_definition_t> c;
            c.emplace_back("oid", oid_col(), true);
            c.emplace_back("relname", str_col(), true);
            c.emplace_back("relnamespace", oid_col(), true);
            c.emplace_back(
                "relkind",
                str_col(),
                true); // 'r' relation, 'i' index, 'S' sequence, 'v' view, 'm' macro, 'c' composite type, 'g' generated/computing
            c.emplace_back("relstoragemode", str_col(), true); // 'd' DISK, 'm' IN_MEMORY (otterbrix-specific)
            return c;
        }

        std::vector<column_definition_t> pg_attribute_columns() {
            std::vector<column_definition_t> c;
            c.emplace_back("attoid", oid_col(), true);   // pg_attribute identity (== column attoid)
            c.emplace_back("attrelid", oid_col(), true); // pg_class.oid (parent relation)
            c.emplace_back("attname", str_col(), true);
            c.emplace_back("atttypid",
                           oid_col(),
                           true); // pg_type.oid (builtin scalar only; complex types use atttypspec)
            c.emplace_back("attnum", i32_col(), true); // 1-based ordinal
            c.emplace_back("attnotnull", bool_col(), true);
            c.emplace_back("atthasdefault", bool_col(), true);
            c.emplace_back("attisdropped", bool_col(), true); // tombstone; attnum is never reused
            // atttypspec is empty and atttypid alone reconstructs the type. For ARRAY /
            // DECIMAL / STRUCT / ENUM / UNKNOWN, atttypspec carries the flat-text encoded
            // complex_logical_type (preserves precision/scale, element types, child types).
            c.emplace_back("atttypspec", str_col(), false);
            // attdefspec: flat-text encoded logical_value_t default via encode_default_spec
            // (pg_attrdef-equivalent inlined into pg_attribute). Empty when
            // atthasdefault=false.
            c.emplace_back("attdefspec", str_col(), false);
            // MVCC column versioning. added_at_commit_id = ADD COLUMN's commit_id;
            // dropped_at_commit_id = DROP COLUMN's commit_id (0 = still alive).
            // Snapshot sees column iff added_at_commit_id <= snapshot.horizon
            // AND (dropped_at_commit_id == 0 OR dropped_at_commit_id > snapshot).
            // attisdropped tombstone is set in lockstep with dropped_at_commit_id > 0.
            c.emplace_back("added_at_commit_id", i64_col(), true);   // 10
            c.emplace_back("dropped_at_commit_id", i64_col(), true); // 11
            return c;
        }

        std::vector<column_definition_t> pg_type_columns() {
            std::vector<column_definition_t> c;
            c.emplace_back("oid", oid_col(), true);
            c.emplace_back("typname", str_col(), true);
            c.emplace_back("typnamespace", oid_col(), true);
            // typdefspec: flat-text encoded complex_logical_type via encode_type_spec
            // (mirrors pg_attribute's atttypspec). Empty for built-in scalar pg_type entries;
            // STRUCT/ENUM/UDT rows carry the full child-type tree so readers can
            // reconstruct the rich definition after restart. Optional column; rows missing
            // this field round-trip as UNKNOWN per decode_type_spec's empty-string fallback.
            c.emplace_back("typdefspec", str_col(), false);
            return c;
        }

        std::vector<column_definition_t> pg_proc_columns() {
            std::vector<column_definition_t> c;
            c.emplace_back("oid", oid_col(), true);
            c.emplace_back("proname", str_col(), true);
            c.emplace_back("pronamespace", oid_col(), true);
            // pronargs: arity (input count) of the function's first signature.
            c.emplace_back("pronargs", i32_col(), false);
            // prouid: opaque function_uid produced by executor's register_udf. Used by the
            // dispatcher to route execution; restored by populate so the cat's
            // registered_func_id matches what the executor knows.
            c.emplace_back("prouid", i64_col(), false);
            // proargmatchers: encoded per-arg type matcher kinds + parameters. Format is
            // pipe-separated per arg: "e:N" exact (N=numeric logical_type id), "n" numeric,
            // "i" integer, "f" floating, "a:N1,N2,..." any_of, "t" always_true. Empty when
            // no matcher info was persisted (legacy rows / placeholder UDFs). Serializable
            // tagged-kind form so matchers survive a catalog roundtrip.
            c.emplace_back("proargmatchers", str_col(), false);
            // prorettype: encoded output_type list. Format is comma-separated: "f:N" fixed
            // (N=logical_type id), "s:N" same_type_at_index N. Empty falls back to
            // same_type_at_index(0) — covers the legacy default.
            c.emplace_back("prorettype", str_col(), false);
            return c;
        }

        std::vector<column_definition_t> pg_depend_columns() {
            std::vector<column_definition_t> c;
            c.emplace_back("classid", oid_col(), true); // catalog of dependent (e.g. pg_class.oid)
            c.emplace_back("objid", oid_col(), true);
            c.emplace_back("refclassid", oid_col(), true); // catalog of referenced
            c.emplace_back("refobjid", oid_col(), true);
            c.emplace_back("deptype", str_col(), true); // 'n','a','i','p' — see PG docs
            return c;
        }

        std::vector<column_definition_t> pg_constraint_columns() {
            std::vector<column_definition_t> c;
            c.emplace_back("oid", oid_col(), true);
            c.emplace_back("conname", str_col(), true);
            c.emplace_back("conrelid", oid_col(), true);
            c.emplace_back("contype", str_col(), true);    // 'p','f','u','c','n'
            c.emplace_back("confrelid", oid_col(), false); // FK reference — 0 if not FK
            c.emplace_back("conkey", str_col(), false);    // CSV of attoids in this constraint
            c.emplace_back("confkey", str_col(), false);   // CSV of attoids in referenced relation (FK only)
            // FK match/delete/update behavior — null/empty defaults to ('s','a','a') = MATCH SIMPLE / NO ACTION.
            //   confmatchtype: 's' SIMPLE (default), 'f' FULL, 'p' PARTIAL
            //   confdeltype:   'a' NO ACTION (default), 'r' RESTRICT, 'c' CASCADE, 'n' SET NULL, 'd' SET DEFAULT
            //   confupdtype:   same alphabet as confdeltype
            c.emplace_back("confmatchtype", str_col(), false);
            c.emplace_back("confdeltype", str_col(), false);
            c.emplace_back("confupdtype", str_col(), false);
            c.emplace_back("conexpr", str_col(), false); // CHECK expr SQL text; NULL for non-CHECK
            return c;
        }

        std::vector<column_definition_t> pg_index_columns() {
            std::vector<column_definition_t> c;
            c.emplace_back("indexrelid", oid_col(), true); // pg_class.oid of the index
            c.emplace_back("indrelid", oid_col(), true);   // pg_class.oid of the indexed table
            c.emplace_back("indkey", str_col(), true);     // CSV of attoid (compact serialization)
            c.emplace_back("indisvalid",
                           bool_col(),
                           true); // false until backfill completes; planner ignores invalid indexes
            return c;
        }

        std::vector<column_definition_t> pg_sequence_columns() {
            std::vector<column_definition_t> c;
            c.emplace_back("seqrelid", oid_col(), /*not_null*/ true); // FK pg_class.oid
            c.emplace_back("seqstart", i64_col(), true);
            c.emplace_back("seqincrement", i64_col(), true);
            c.emplace_back("seqmin", i64_col(), true);
            c.emplace_back("seqmax", i64_col(), true);
            c.emplace_back("seqcycle", bool_col(), true);
            c.emplace_back("seqlast", i64_col(), true);
            return c;
        }

        std::vector<column_definition_t> pg_rewrite_columns() {
            std::vector<column_definition_t> c;
            c.emplace_back("oid", oid_col(), /*not_null*/ true); // rule OID
            c.emplace_back("rulename", str_col(), true);         // mirrors pg_class.relname
            c.emplace_back("ev_class", oid_col(), true);         // FK pg_class.oid
            c.emplace_back("ev_type", str_col(), true);          // 'v' or 'm'
            c.emplace_back("ev_action", str_col(), true);        // SQL or macro body
            return c;
        }

        std::vector<column_definition_t> pg_settings_columns() {
            std::vector<column_definition_t> c;
            c.emplace_back("name", str_col(), /*not_null*/ true);    // setting name (e.g. "TimeZone")
            c.emplace_back("setting", str_col(), /*not_null*/ true); // setting value
            return c;
        }

        std::vector<column_definition_t> pg_computed_column_columns() {
            std::vector<column_definition_t> c;
            c.emplace_back("relid",
                           oid_col(),
                           true); // 0: pg_class.oid (parent relation, always relkind='g' generated/computing)
            c.emplace_back("attoid", oid_col(), true);     // 1
            c.emplace_back("attname", str_col(), true);    // 2
            c.emplace_back("atttypid", oid_col(), true);   // 3: builtin scalar oid (complex types use atttypspec)
            c.emplace_back("atttypspec", str_col(), true); // 4: flat-text encoded complex_logical_type
                //    for ARRAY / STRUCT / UNION / DECIMAL / fixed-width sub-types.
                //    Empty for builtin scalars (atttypid alone reconstructs the type).
                //    Mirrors pg_attribute.atttypspec.
            c.emplace_back("attversion", i64_col(), true);  // 5
            c.emplace_back("attrefcount", i64_col(), true); // 6
            return c;
        }
    } // namespace

    std::span<const system_table_def_t> all_system_tables() {
        // Built once on first call into a fixed-size std::array (no heap), thereafter
        // returned as a zero-cost std::span. Schemas are immutable for the life of the
        // process. C++11 magic-statics guarantee single-threaded initialisation.
        //
        // pg_database is bootstrapped first because every other catalog object (namespace,
        // relation, type, function) is conceptually scoped to a database. The default "main"
        // database row is seeded with well_known_oid::main_database in
        // manager_disk_t::bootstrap_system_tables_sync.
        static const std::array<system_table_def_t, 13> tables = []() {
            const oid_t pg_catalog = well_known_oid::pg_catalog_namespace;
            return std::array<system_table_def_t, 13>{{
                {"pg_database", well_known_oid::pg_database_table, pg_catalog, relkind::regular, pg_database_columns()},
                {"pg_namespace",
                 well_known_oid::pg_namespace_table,
                 pg_catalog,
                 relkind::regular,
                 pg_namespace_columns()},
                {"pg_class", well_known_oid::pg_class_table, pg_catalog, relkind::regular, pg_class_columns()},
                {"pg_attribute",
                 well_known_oid::pg_attribute_table,
                 pg_catalog,
                 relkind::regular,
                 pg_attribute_columns()},
                {"pg_type", well_known_oid::pg_type_table, pg_catalog, relkind::regular, pg_type_columns()},
                {"pg_proc", well_known_oid::pg_proc_table, pg_catalog, relkind::regular, pg_proc_columns()},
                {"pg_depend", well_known_oid::pg_depend_table, pg_catalog, relkind::regular, pg_depend_columns()},
                {"pg_constraint",
                 well_known_oid::pg_constraint_table,
                 pg_catalog,
                 relkind::regular,
                 pg_constraint_columns()},
                {"pg_index", well_known_oid::pg_index_table, pg_catalog, relkind::regular, pg_index_columns()},
                {"pg_computed_column",
                 well_known_oid::pg_computed_column_table,
                 pg_catalog,
                 relkind::regular,
                 pg_computed_column_columns()},
                {"pg_sequence", well_known_oid::pg_sequence_table, pg_catalog, relkind::regular, pg_sequence_columns()},
                {"pg_rewrite", well_known_oid::pg_rewrite_table, pg_catalog, relkind::regular, pg_rewrite_columns()},
                {"pg_settings", well_known_oid::pg_settings_table, pg_catalog, relkind::regular, pg_settings_columns()},
            }};
        }();
        return tables;
    }

    const system_table_def_t* find_system_table(std::string_view name) {
        for (const auto& t : all_system_tables()) {
            if (t.name == name) {
                return &t;
            }
        }
        return nullptr;
    }

    // ── flat-text type spec helpers ──────────────────────────────────────────────
    // Format (recursive, scalar names match pg_type.typname):
    //   scalar            →  bool int1 int2 int4 int8 float4 float8 text
    //                        timestamp bytea uuid
    //   numeric(w,s)      →  DECIMAL (matches pg_type.typname)
    //   UNKNOWN(name)
    //   LIST(inner)
    //   ARRAY(inner,size)
    //   MAP(key,val)
    //   STRUCT(name,f1:t1,f2:t2,...)
    //   UNION(f1:t1,f2:t2,...)
    //   VARIANT
    //   ENUM:name:label=val,...  (legacy flat format; kept unchanged)
    // ─────────────────────────────────────────────────────────────────────────────

    // Canonical names match pg_type.typname so they're consistent with the rest of the catalog.
    static std::string_view scalar_type_to_name(types::logical_type lt) {
        using LT = types::logical_type;
        switch (lt) {
            case LT::BOOLEAN:
                return "bool";
            case LT::TINYINT:
                return "int1"; // no PG equivalent; 1-byte signed
            case LT::UTINYINT:
                return "uint1";
            case LT::SMALLINT:
                return "int2"; // pg: int2
            case LT::USMALLINT:
                return "uint2";
            case LT::INTEGER:
                return "int4"; // pg: int4
            case LT::UINTEGER:
                return "uint4";
            case LT::BIGINT:
                return "int8"; // pg: int8
            case LT::UBIGINT:
                return "uint8";
            case LT::FLOAT:
                return "float4"; // pg: float4
            case LT::DOUBLE:
                return "float8"; // pg: float8
            case LT::STRING_LITERAL:
                return "text"; // pg: text
            case LT::TIMESTAMP:
                return "timestamp";
            case LT::TIMESTAMP_TZ:
                return "timestamp with time zone";
            case LT::DATE:
                return "date";
            case LT::TIME:
                return "time";
            case LT::TIME_TZ:
                return "time with time zone";
            case LT::INTERVAL:
                return "interval";
            case LT::BLOB:
                return "bytea"; // pg: bytea
            case LT::UUID:
                return "uuid";
            default:
                return "";
        }
    }

    static types::logical_type scalar_name_to_type(std::string_view n) {
        using LT = types::logical_type;
        // Canonical pg_type.typname names
        if (n == "bool")
            return LT::BOOLEAN;
        if (n == "int1")
            return LT::TINYINT;
        if (n == "uint1")
            return LT::UTINYINT;
        if (n == "int2")
            return LT::SMALLINT;
        if (n == "uint2")
            return LT::USMALLINT;
        if (n == "int4")
            return LT::INTEGER;
        if (n == "uint4")
            return LT::UINTEGER;
        if (n == "int8")
            return LT::BIGINT;
        if (n == "uint8")
            return LT::UBIGINT;
        if (n == "float4")
            return LT::FLOAT;
        if (n == "float8")
            return LT::DOUBLE;
        if (n == "text")
            return LT::STRING_LITERAL;
        if (n == "timestamp")
            return LT::TIMESTAMP;
        if (n == "timestamp with time zone")
            return LT::TIMESTAMP_TZ;
        if (n == "date")
            return LT::DATE;
        if (n == "time")
            return LT::TIME;
        if (n == "time with time zone")
            return LT::TIME_TZ;
        if (n == "interval")
            return LT::INTERVAL;
        if (n == "bytea")
            return LT::BLOB;
        if (n == "uuid")
            return LT::UUID;
        // Legacy aliases written by older builds (before PostgreSQL-style names)
        if (n == "int16")
            return LT::SMALLINT;
        if (n == "int32")
            return LT::INTEGER;
        if (n == "int64")
            return LT::BIGINT;
        if (n == "float32")
            return LT::FLOAT;
        if (n == "float64")
            return LT::DOUBLE;
        if (n == "string")
            return LT::STRING_LITERAL;
        if (n == "blob")
            return LT::BLOB;
        if (n == "boolean")
            return LT::BOOLEAN;
        if (n == "integer")
            return LT::INTEGER;
        if (n == "bigint")
            return LT::BIGINT;
        // SQL standard aliases the parser emits when no pg_catalog prefix is used
        if (n == "double")
            return LT::DOUBLE;
        if (n == "float")
            return LT::FLOAT;
        if (n == "smallint")
            return LT::SMALLINT;
        if (n == "tinyint")
            return LT::TINYINT;
        if (n == "varchar")
            return LT::STRING_LITERAL;
        // Grammar-internal names (SystemTypeName → pg_catalog.<name>)
        if (n == "int8_t")
            return LT::BIGINT; // BIGINT keyword in parser/gram.y
        return LT::UNKNOWN;
    }

    // Forward declaration for mutual recursion.
    static std::string encode_type_nested(const types::complex_logical_type& t);

    static std::string encode_type_nested(const types::complex_logical_type& t) {
        using LT = types::logical_type;
        auto sn = scalar_type_to_name(t.type());
        if (!sn.empty())
            return std::string(sn);

        if (t.type() == LT::DECIMAL) {
            const auto* ext = static_cast<const types::decimal_logical_type_extension*>(t.extension());
            return "numeric(" + std::to_string(static_cast<unsigned>(ext->width())) + "," +
                   std::to_string(static_cast<unsigned>(ext->scale())) + ")";
        }
        if (t.type() == LT::UNKNOWN) {
            return "UNKNOWN(" + t.type_name() + ")";
        }
        if (t.type() == LT::LIST) {
            return "LIST(" + encode_type_nested(t.child_type()) + ")";
        }
        if (t.type() == LT::ARRAY) {
            const auto* ext = static_cast<const types::array_logical_type_extension*>(t.extension());
            return "ARRAY(" + encode_type_nested(ext->internal_type()) + "," + std::to_string(ext->size()) + ")";
        }
        if (t.type() == LT::MAP) {
            const auto* ext = static_cast<const types::map_logical_type_extension*>(t.extension());
            return "MAP(" + encode_type_nested(ext->key()) + "," + encode_type_nested(ext->value()) + ")";
        }
        if (t.type() == LT::STRUCT) {
            const auto* ext = static_cast<const types::struct_logical_type_extension*>(t.extension());
            std::string out = "STRUCT(" + ext->type_name();
            for (const auto& f : ext->child_types()) {
                out += ',';
                out += f.alias();
                out += ':';
                out += encode_type_nested(f);
            }
            out += ')';
            return out;
        }
        if (t.type() == LT::UNION) {
            // child_types()[0] is the hidden tag (UTINYINT); real members start at [1]
            const auto& children = t.child_types();
            std::string out = "UNION(";
            bool first = true;
            for (size_t i = 1; i < children.size(); ++i) {
                if (!first)
                    out += ',';
                first = false;
                out += children[i].alias();
                out += ':';
                out += encode_type_nested(children[i]);
            }
            out += ')';
            return out;
        }
        if (t.type() == LT::VARIANT) {
            return "VARIANT";
        }
        // Enum handled by the outer encode_type_spec; shouldn't reach here.
        return "UNKNOWN(" + std::to_string(static_cast<int>(t.type())) + ")";
    }

    // Recursive descent parser for the flat-text format.
    static types::complex_logical_type
    parse_flat_type(std::pmr::memory_resource* resource, std::string_view s, size_t& pos);

    // Read characters until one of the stop chars (at depth 0).
    static std::string read_token(std::string_view s, size_t& pos) {
        size_t start = pos;
        while (pos < s.size() && s[pos] != '(' && s[pos] != ')' && s[pos] != ',' && s[pos] != ':') {
            ++pos;
        }
        return std::string{s.substr(start, pos - start)};
    }

    static types::complex_logical_type
    parse_flat_type(std::pmr::memory_resource* resource, std::string_view s, size_t& pos) {
        using LT = types::logical_type;
        std::string name = read_token(s, pos);

        if (pos >= s.size() || s[pos] != '(') {
            if (name == "VARIANT")
                return types::complex_logical_type::create_variant(resource);
            auto lt = scalar_name_to_type(name);
            if (lt != LT::UNKNOWN)
                return types::complex_logical_type{lt};
            return types::complex_logical_type::create_unknown(name);
        }
        ++pos; // consume '('

        if (name == "numeric" || name == "DECIMAL") {
            std::string w = read_token(s, pos);
            ++pos; // ','
            std::string sc = read_token(s, pos);
            ++pos; // ')'
            int wv{};
            int scv{};
            auto [wp, wec] = std::from_chars(w.data(), w.data() + w.size(), wv);
            auto [scp, scec] = std::from_chars(sc.data(), sc.data() + sc.size(), scv);
            if (wec != std::errc{} || scec != std::errc{}) {
                return types::complex_logical_type{LT::UNKNOWN};
            }
            return types::complex_logical_type::create_decimal(static_cast<uint8_t>(wv), static_cast<uint8_t>(scv));
        }
        if (name == "UNKNOWN") {
            std::string tname = read_token(s, pos);
            ++pos; // ')'
            return types::complex_logical_type::create_unknown(tname);
        }
        if (name == "LIST") {
            auto inner = parse_flat_type(resource, s, pos);
            ++pos; // ')'
            return types::complex_logical_type::create_list(inner);
        }
        if (name == "ARRAY") {
            auto inner = parse_flat_type(resource, s, pos);
            ++pos; // ','
            std::string sz = read_token(s, pos);
            ++pos; // ')'
            unsigned long long sv{};
            auto [sp, sec] = std::from_chars(sz.data(), sz.data() + sz.size(), sv);
            if (sec != std::errc{}) {
                return types::complex_logical_type{LT::UNKNOWN};
            }
            return types::complex_logical_type::create_array(inner, sv);
        }
        if (name == "MAP") {
            auto key = parse_flat_type(resource, s, pos);
            ++pos; // ','
            auto val = parse_flat_type(resource, s, pos);
            ++pos; // ')'
            return types::complex_logical_type::create_map(key, val);
        }
        if (name == "STRUCT") {
            std::string struct_name = read_token(s, pos);
            std::pmr::vector<types::complex_logical_type> fields(resource);
            while (pos < s.size() && s[pos] == ',') {
                ++pos; // ','
                std::string fname = read_token(s, pos);
                ++pos; // ':'
                auto ftype = parse_flat_type(resource, s, pos);
                ftype.set_alias(fname);
                fields.push_back(std::move(ftype));
            }
            if (pos < s.size() && s[pos] == ')')
                ++pos;
            return types::complex_logical_type::create_struct(struct_name, fields);
        }
        if (name == "UNION") {
            std::pmr::vector<types::complex_logical_type> fields(resource);
            // First member
            if (pos < s.size() && s[pos] != ')') {
                std::string fname = read_token(s, pos);
                ++pos; // ':'
                auto ftype = parse_flat_type(resource, s, pos);
                ftype.set_alias(fname);
                fields.push_back(std::move(ftype));
            }
            while (pos < s.size() && s[pos] == ',') {
                ++pos; // ','
                std::string fname = read_token(s, pos);
                ++pos; // ':'
                auto ftype = parse_flat_type(resource, s, pos);
                ftype.set_alias(fname);
                fields.push_back(std::move(ftype));
            }
            if (pos < s.size() && s[pos] == ')')
                ++pos;
            return types::complex_logical_type::create_union(std::move(fields));
        }
        // Unknown keyword with args — skip to matching ')'
        int depth = 1;
        while (pos < s.size() && depth > 0) {
            if (s[pos] == '(')
                ++depth;
            else if (s[pos] == ')')
                --depth;
            ++pos;
        }
        return types::complex_logical_type::create_unknown(name);
    }

    std::string encode_type_spec(const types::complex_logical_type& t) {
        using LT = types::logical_type;
        switch (t.type()) {
            case LT::BOOLEAN:
            case LT::TINYINT:
            case LT::UTINYINT:
            case LT::SMALLINT:
            case LT::USMALLINT:
            case LT::INTEGER:
            case LT::UINTEGER:
            case LT::BIGINT:
            case LT::UBIGINT:
            case LT::FLOAT:
            case LT::DOUBLE:
            case LT::STRING_LITERAL:
            case LT::TIMESTAMP:
            case LT::TIMESTAMP_TZ:
            case LT::DATE:
            case LT::TIME:
            case LT::TIME_TZ:
            case LT::INTERVAL:
            case LT::BLOB:
            case LT::UUID:
                return "";
            default:
                break;
        }
        // ENUM: flat text "ENUM:type_name:label0=val0,label1=val1,..."
        if (t.type() == LT::ENUM) {
            std::string out = "ENUM:";
            out += t.type_name();
            out += ':';
            const auto* ext = t.extension();
            if (ext != nullptr) {
                const auto* enum_ext = static_cast<const components::types::enum_logical_type_extension*>(ext);
                bool first = true;
                for (const auto& entry : enum_ext->entries()) {
                    if (!first)
                        out += ',';
                    first = false;
                    const auto& etype = entry.type();
                    out += etype.has_alias() ? etype.alias() : std::string{};
                    out += '=';
                    out += std::to_string(entry.value<std::int32_t>());
                }
            }
            return out;
        }
        return encode_type_nested(t);
    }

    types::complex_logical_type decode_type_spec(std::pmr::memory_resource* resource, std::string_view spec) {
        using LT = types::logical_type;
        if (spec.empty()) {
            return types::complex_logical_type{LT::UNKNOWN};
        }
        // ENUM flat format (pre-existing; kept for backward compat)
        if (spec.size() >= 5 && spec.compare(0, 5, "ENUM:") == 0) {
            auto rest = spec.substr(5);
            auto colon = rest.find(':');
            std::string name =
                (colon == std::string_view::npos) ? std::string{rest} : std::string{rest.substr(0, colon)};
            std::vector<components::types::logical_value_t> entries;
            if (colon != std::string::npos) {
                auto entries_str = rest.substr(colon + 1);
                std::size_t i = 0;
                while (i < entries_str.size()) {
                    std::size_t comma = entries_str.find(',', i);
                    std::string token{
                        entries_str.substr(i, comma == std::string_view::npos ? std::string_view::npos : comma - i)};
                    std::size_t eq = token.find('=');
                    if (eq != std::string::npos) {
                        std::string label = token.substr(0, eq);
                        const auto val_str = token.substr(eq + 1);
                        int v{};
                        auto [vp, vec_] = std::from_chars(val_str.data(), val_str.data() + val_str.size(), v);
                        if (vec_ != std::errc{}) {
                            return types::complex_logical_type{LT::UNKNOWN};
                        }
                        components::types::logical_value_t lv(resource, v);
                        lv.set_alias(label);
                        entries.push_back(std::move(lv));
                    }
                    if (comma == std::string::npos)
                        break;
                    i = comma + 1;
                }
            }
            return components::types::complex_logical_type::create_enum(name, std::move(entries));
        }
        // Flat-text format for all other types.
        try {
            size_t pos = 0;
            return parse_flat_type(resource, spec, pos);
        } catch (...) {
            return types::complex_logical_type{LT::UNKNOWN};
        }
    }

    std::string encode_proargmatchers(const std::vector<components::compute::input_type>& matchers) {
        using K = components::compute::input_type::kind_t;
        std::string out;
        for (size_t i = 0; i < matchers.size(); ++i) {
            if (i > 0)
                out += '|';
            const auto& m = matchers[i];
            switch (m.kind()) {
                case K::exact:
                    out += "e:";
                    out += std::to_string(static_cast<int>(m.exact_type()));
                    break;
                case K::numeric:
                    out += "n";
                    break;
                case K::integer:
                    out += "i";
                    break;
                case K::floating:
                    out += "f";
                    break;
                case K::string:
                    out += "s";
                    break;
                case K::any_of: {
                    out += "a:";
                    const auto& list = m.any_of_list();
                    for (size_t j = 0; j < list.size(); ++j) {
                        if (j > 0)
                            out += ',';
                        out += std::to_string(static_cast<int>(list[j]));
                    }
                    break;
                }
                case K::always_true:
                    out += "t";
                    break;
                case K::custom:
                    // Closure-only matcher (non-introspectable). Persist as a
                    // placeholder so the row still parses; the on-restart
                    // restore will need to rebuild the matcher elsewhere or
                    // skip restoring this overload.
                    out += "t";
                    break;
            }
        }
        return out;
    }

    std::string encode_prorettype(const std::vector<components::compute::output_type>& outputs) {
        using K = components::compute::output_type::kind_t;
        std::string out;
        for (size_t i = 0; i < outputs.size(); ++i) {
            if (i > 0)
                out += ',';
            const auto& o = outputs[i];
            switch (o.kind()) {
                case K::fixed_value:
                    out += "f:";
                    out += std::to_string(static_cast<int>(o.fixed_value().type()));
                    break;
                case K::same_type_at_index:
                    out += "s:";
                    out += std::to_string(o.input_index());
                    break;
                case K::custom:
                    // Lossy fallback for raw resolver closures: persist as
                    // same_type_at_index(0).
                    out += "s:0";
                    break;
            }
        }
        return out;
    }

    std::string_view logical_type_to_pg_name(types::logical_type t) noexcept { return scalar_type_to_name(t); }

    types::logical_type oid_to_builtin_type(oid_t oid) noexcept {
        using LT = types::logical_type;
        namespace ns = well_known_oid;
        switch (oid) {
            case ns::boolean_type:
                return LT::BOOLEAN;
            case ns::int8_type:
                return LT::TINYINT;
            case ns::int16_type:
                return LT::SMALLINT;
            case ns::int32_type:
                return LT::INTEGER;
            case ns::int64_type:
                return LT::BIGINT;
            case ns::float32_type:
                return LT::FLOAT;
            case ns::float64_type:
                return LT::DOUBLE;
            case ns::string_type:
                return LT::STRING_LITERAL;
            case ns::timestamp_type:
                return LT::TIMESTAMP;
            case ns::timestamp_tz_type:
                return LT::TIMESTAMP_TZ;
            case ns::date_type:
                return LT::DATE;
            case ns::time_type:
                return LT::TIME;
            case ns::time_tz_type:
                return LT::TIME_TZ;
            case ns::interval_type:
                return LT::INTERVAL;
            default:
                return LT::UNKNOWN;
        }
    }

    oid_t builtin_type_to_oid(types::logical_type lt) noexcept {
        using LT = types::logical_type;
        namespace ns = well_known_oid;
        switch (lt) {
            case LT::BOOLEAN:
                return ns::boolean_type;
            case LT::TINYINT:
                return ns::int8_type;
            case LT::SMALLINT:
                return ns::int16_type;
            case LT::INTEGER:
                return ns::int32_type;
            case LT::BIGINT:
                return ns::int64_type;
            case LT::FLOAT:
                return ns::float32_type;
            case LT::DOUBLE:
                return ns::float64_type;
            case LT::STRING_LITERAL:
                return ns::string_type;
            case LT::TIMESTAMP:
                return ns::timestamp_type;
            case LT::TIMESTAMP_TZ:
                return ns::timestamp_tz_type;
            case LT::DATE:
                return ns::date_type;
            case LT::TIME:
                return ns::time_type;
            case LT::TIME_TZ:
                return ns::time_tz_type;
            case LT::INTERVAL:
                return ns::interval_type;
            default:
                return INVALID_OID;
        }
    }

    types::logical_type pg_name_to_logical_type(std::string_view name) noexcept { return scalar_name_to_type(name); }

    std::string encode_default_spec(const types::logical_value_t& v) {
        if (v.is_null())
            return "NULL";
        using LT = types::logical_type;
        const auto lt = v.type().type();
        const auto name = scalar_type_to_name(lt);
        if (name.empty())
            return ""; // complex type — not persisted
        std::string out(name);
        out += ':';
        switch (lt) {
            case LT::BOOLEAN:
                out += v.value<bool>() ? '1' : '0';
                break;
            case LT::TINYINT:
                out += std::to_string(v.value<int8_t>());
                break;
            case LT::UTINYINT:
                out += std::to_string(static_cast<unsigned>(v.value<uint8_t>()));
                break;
            case LT::SMALLINT:
                out += std::to_string(v.value<int16_t>());
                break;
            case LT::USMALLINT:
                out += std::to_string(static_cast<unsigned>(v.value<uint16_t>()));
                break;
            case LT::INTEGER:
                out += std::to_string(v.value<int32_t>());
                break;
            case LT::UINTEGER:
                out += std::to_string(v.value<uint32_t>());
                break;
            case LT::BIGINT:
                out += std::to_string(v.value<int64_t>());
                break;
            case LT::UBIGINT:
                out += std::to_string(v.value<uint64_t>());
                break;
            case LT::FLOAT:
                out += std::to_string(v.value<float>());
                break;
            case LT::DOUBLE:
                out += std::to_string(v.value<double>());
                break;
            case LT::STRING_LITERAL:
                out += v.value<std::string_view>();
                break;
            default:
                return "";
        }
        return out;
    }

    std::optional<types::logical_value_t> decode_default_spec(std::pmr::memory_resource* resource,
                                                              const std::string& spec) {
        if (spec.empty() || spec == "NULL")
            return std::nullopt;
        const auto colon = spec.find(':');
        if (colon == std::string::npos)
            return std::nullopt;
        const auto type_name = std::string_view(spec).substr(0, colon);
        const auto val_str = spec.substr(colon + 1);
        const auto lt = scalar_name_to_type(type_name);
        using LT = types::logical_type;
        const char* b = val_str.data();
        const char* e = val_str.data() + val_str.size();
        switch (lt) {
            case LT::BOOLEAN:
                return types::logical_value_t(resource, val_str == "1");
            case LT::TINYINT: {
                int v{};
                auto [p, ec] = std::from_chars(b, e, v);
                if (ec != std::errc{})
                    return std::nullopt;
                return types::logical_value_t(resource, static_cast<int8_t>(v));
            }
            case LT::UTINYINT: {
                unsigned long v{};
                auto [p, ec] = std::from_chars(b, e, v);
                if (ec != std::errc{})
                    return std::nullopt;
                return types::logical_value_t(resource, static_cast<uint8_t>(v));
            }
            case LT::SMALLINT: {
                int v{};
                auto [p, ec] = std::from_chars(b, e, v);
                if (ec != std::errc{})
                    return std::nullopt;
                return types::logical_value_t(resource, static_cast<int16_t>(v));
            }
            case LT::USMALLINT: {
                unsigned long v{};
                auto [p, ec] = std::from_chars(b, e, v);
                if (ec != std::errc{})
                    return std::nullopt;
                return types::logical_value_t(resource, static_cast<uint16_t>(v));
            }
            case LT::INTEGER: {
                int v{};
                auto [p, ec] = std::from_chars(b, e, v);
                if (ec != std::errc{})
                    return std::nullopt;
                return types::logical_value_t(resource, v);
            }
            case LT::UINTEGER: {
                unsigned long v{};
                auto [p, ec] = std::from_chars(b, e, v);
                if (ec != std::errc{})
                    return std::nullopt;
                return types::logical_value_t(resource, static_cast<uint32_t>(v));
            }
            case LT::BIGINT: {
                long long v{};
                auto [p, ec] = std::from_chars(b, e, v);
                if (ec != std::errc{})
                    return std::nullopt;
                return types::logical_value_t(resource, static_cast<int64_t>(v));
            }
            case LT::UBIGINT: {
                unsigned long long v{};
                auto [p, ec] = std::from_chars(b, e, v);
                if (ec != std::errc{})
                    return std::nullopt;
                return types::logical_value_t(resource, static_cast<uint64_t>(v));
            }
            case LT::FLOAT:
                try {
                    return types::logical_value_t(resource, std::stof(val_str));
                } catch (...) {
                    return std::nullopt;
                }
            case LT::DOUBLE:
                try {
                    return types::logical_value_t(resource, std::stod(val_str));
                } catch (...) {
                    return std::nullopt;
                }
            case LT::STRING_LITERAL:
                return types::logical_value_t(resource, val_str);
            default:
                return std::nullopt;
        }
    }

} // namespace components::catalog
