#include "system_table_schemas.hpp"
#include "catalog_codes.hpp"

#include <array>
#include <charconv>
#include <components/index/logical_value_binary_codec.hpp>

#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>

// Intentional schema deviations from PostgreSQL — otterbrix is a single-process actor
// framework that does not aim for PG wire-protocol compatibility. Each deviation:
//
//   pg_namespace  — no `nspowner`           : no role/user system today.
//   pg_class      — no `reltuples/relpages` : optimizer reads counts live from data_table_t.
//                 — no `reltype`             : composite-row types not implemented.
//                 — adds `relstoragemode`    : always 'd' (disk-only; write-only column).
//                 — relkind 'g' = computing : doc proposed 'c', but 'c' collides with
//                                              PG's "composite type" relkind. 'g' aligns
//                                              with PG GENERATED terminology.
//   pg_attribute  — adds `attoid`            : stable column OID (FK target for indexes,
//                                              constraints, deps).
//                 — adds `atttypspec`        : flat-text encoded complex_logical_type for
//                                              types that don't fit a single pg_type.oid.
//                 — adds `attdefspec`        : binary-encoded default value (replaces
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
//   pg_index      — no `indisprimary/indisunique` : PK/uniqueness is enforced via
//                                              pg_constraint, not pg_index.
//                 — adds `indtype`            : single-char physical-backend code (see
//                                              catalog_codes.hpp); restart picks the on-disk
//                                              reader (bitcask vs B+tree) from it.
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
                true); // 'r' relation, 'i' index, 'S' sequence, 'v' view, 'm' macro, 'c' composite, 'g' computing
            c.emplace_back("relstoragemode", str_col(), true); // always 'd' (disk-only, write-only)
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
            // attdefspec: binary logical_value_t default, hex-armoured (encode_default_spec)
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
            // this field round-trip as UNKNOWN — decode_type_spec's legitimate empty-spec
            // answer (not an error: the oid alone reconstructs builtin scalars).
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
            c.emplace_back("indtype",
                           str_col(),
                           true); // single-char backend code (see catalog_codes.hpp indtype namespace);
                                  // NOT nullable, no default — a row without it is catalog corruption
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

        std::vector<column_definition_t> pg_cast_columns() {
            std::vector<column_definition_t> c;
            c.emplace_back("oid", oid_col(), /*not_null*/ true); // cast identity — pg_depend anchor
            c.emplace_back("castsource", oid_col(), true);       // pg_type.oid of the source type
            c.emplace_back("casttarget", oid_col(), true);       // pg_type.oid of the target type
            // (castsource, casttarget) is the registry lookup key; oid is the catalog-row identity
            // used for by-oid delete and pg_depend edges. No castcontext column yet — implicitness
            // lives on the in-memory cast_entry (set by register_default_casts) and is only needed
            // here once user-defined CREATE CAST persists its own level.
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
        static const std::array<system_table_def_t, 14> tables = []() {
            const oid_t pg_catalog = well_known_oid::pg_catalog_namespace;
            return std::array<system_table_def_t, 14>{{
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
                {"pg_cast", well_known_oid::pg_cast_table, pg_catalog, relkind::regular, pg_cast_columns()},
            }};
        }();
        return tables;
    }

    const system_table_def_t* find_system_table(oid_t relation_oid) {
        for (const auto& t : all_system_tables()) {
            if (t.relation_oid == relation_oid) {
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
            case LT::HUGEINT:
                return "int16"; // no PG equivalent
            case LT::UHUGEINT:
                return "uint16";
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
        if (n == "int16")
            return LT::HUGEINT;
        if (n == "uint16")
            return LT::UHUGEINT;
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
        // Canonical seed names and SQL aliases that are not PG spellings. These exact strings
        // are seeded into pg_type by this build's bootstrap (manager_disk_bootstrap.cpp,
        // builtin_type_rows) and are resolved through pg_name_to_logical_type whenever the
        // user writes one of them as a type name. Byte-width spellings live in the PG block
        // above: "int16" is the 128-bit type there, NOT SMALLINT.
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

    // THE DEPTH WINDOW IS SHARED with the binary codec: components/types/
    // type_spec_codec.cpp refuses nesting beyond MAX_SPEC_DEPTH = 64 on BOTH encode and
    // decode, and the dispatcher's write gate (gate_persistable_type in
    // services/dispatcher/validate_logical_plan.cpp) runs that encoder over every
    // plan-level column/type before this codec's text is written — so a flat spec deeper
    // than the window is not something this engine wrote. Without the limit the parser
    // recurses unbounded: a 2^20-deep LIST(...) walks it off the stack. encode_type_nested
    // carries the same counter, because a writer that accepts more than the reader does
    // manufactures a catalog row that can never be read back.
    static constexpr uint32_t MAX_FLAT_SPEC_DEPTH = 64;

    // Forward declaration for mutual recursion.
    static std::string encode_type_nested(const types::complex_logical_type& t, uint32_t depth);

    // TWO ENCODERS WRITE THE SAME COLUMN TYPE, AND THE WRITE GATE ASKS ONLY THE OTHER ONE.
    //
    // gate_persistable_type (services/dispatcher/validate_logical_plan.cpp) runs the BINARY
    // codec — components::types::encode_type_spec — over every plan-level type and refuses
    // the statement when it says no. The row that lands in pg_attribute.atttypspec /
    // pg_type.typdefspec is written by THIS codec, which returns a plain std::string and so
    // cannot say no at all. The gate is sound only while the two domains coincide, and this
    // file is the half that has to make them coincide, since it is the half with no channel.
    //
    // So every place the binary codec REFUSES, this one emits a spec the strict decoder
    // refuses (kFlatUnpersistable) instead of a plausible type — the same shape as
    // encode_type_spec_or_poison in components/vector/data_chunk_binary.cpp. And every
    // place the binary codec ACCEPTS, this one has to have a spelling, or a column that
    // passed the gate rehydrates as a DIFFERENT type in silence.
    //
    // The marker is deliberately outside the format's language: read_token stops at '(',
    // the keyword chain in parse_flat_type matches nothing, and the parse fails with
    // data_corruption. "" cannot serve as the marker — it already means "a builtin scalar
    // stored with atttypid alone", a legitimate answer.
    static constexpr std::string_view kFlatUnpersistable = "!unpersistable";

    static std::string flat_unpersistable(types::logical_type lt) {
        return std::string{kFlatUnpersistable} + "(" + std::to_string(static_cast<int>(lt)) + ")";
    }

    // The extension fetch the binary codec makes (checked_extension in
    // components/types/type_spec_codec.cpp) and this one used not to: a type tagged DECIMAL
    // whose extension is absent, or GENERIC because set_alias() ran on a bare tag, is
    // in-memory corruption. Reading width/scale out of an object that has none is a
    // SIGSEGV or two garbage bytes written to the catalog; both were live here.
    static const types::logical_type_extension* checked_flat_extension(
        const types::complex_logical_type& t,
        types::logical_type_extension::extension_type expected) {
        const auto* ext = t.extension();
        return (ext != nullptr && ext->type() == expected) ? ext : nullptr;
    }

    // Plain scalars the binary codec persists (is_plain_scalar in type_spec_codec.cpp) that
    // have NO pg_type name, so scalar_type_to_name cannot spell them and the empty-spec /
    // atttypid leg cannot carry them either. Without a spelling they fell through to
    // "UNKNOWN(<enum number>)" — which decodes back as a NAMED user-type reference, i.e. a
    // gate-approved column rehydrating as a different type without a word.
    //
    // They get the one form that cannot be confused with a name: BUILTIN(<logical_type>),
    // the flat mirror of the binary codec's leading type byte. The list is a WHITELIST on
    // both sides — decoding BUILTIN(105) back into FUNCTION would reopen the very hole this
    // closes. catalog::encoder_domains::every_plain_scalar_the_gate_blesses_survives_the_
    // flat_writer walks is_plain_scalar's full contents and pins the correspondence.
    static bool is_nameless_flat_builtin(types::logical_type lt) {
        using LT = types::logical_type;
        switch (lt) {
            case LT::NA:
            case LT::ANY:
            case LT::BIT:
            case LT::INTEGER_LITERAL:
            case LT::POINTER:
            case LT::VALIDITY:
                return true;
            default:
                return false;
        }
    }

    // Names written AS IS produce a spec the strict decoder refuses whenever a struct
    // field, union member, enum name/label or user-type name carries one of the format's
    // own delimiters ( ) , : = — the DDL goes through and every later resolve fails
    // per-statement, with no writer gate anywhere. So the encoder escapes those characters
    // (backslash-prefixed) and the decoder reads the escapes back. A backslash before
    // anything OUTSIDE this set — including a raw backslash an old build may have written —
    // is a loud data_corruption refusal, never a silent decode to a DIFFERENT name.
    static bool flat_name_needs_escape(char c) {
        return c == '\\' || c == '(' || c == ')' || c == ',' || c == ':' || c == '=';
    }

    static std::string escape_flat_name(std::string_view name) {
        std::string out;
        out.reserve(name.size());
        for (char c : name) {
            if (flat_name_needs_escape(c)) {
                out += '\\';
            }
            out += c;
        }
        return out;
    }

    static std::string encode_type_nested(const types::complex_logical_type& t, uint32_t depth) {
        using LT = types::logical_type;
        // The depth window the decoder enforces, enforced on the way OUT too. The binary
        // codec refuses past MAX_SPEC_DEPTH on BOTH sides for the same reason; this encoder
        // used to have no limit at all, so a type past the window was written happily and
        // then refused forever by the reader.
        if (depth > MAX_FLAT_SPEC_DEPTH) {
            return flat_unpersistable(t.type());
        }
        auto sn = scalar_type_to_name(t.type());
        if (!sn.empty())
            return std::string(sn);
        if (is_nameless_flat_builtin(t.type())) {
            return "BUILTIN(" + std::to_string(static_cast<int>(t.type())) + ")";
        }

        if (t.type() == LT::DECIMAL) {
            const auto* ext = checked_flat_extension(t, types::logical_type_extension::extension_type::DECIMAL);
            if (ext == nullptr) {
                return flat_unpersistable(t.type());
            }
            const auto* dec = static_cast<const types::decimal_logical_type_extension*>(ext);
            return "numeric(" + std::to_string(static_cast<unsigned>(dec->width())) + "," +
                   std::to_string(static_cast<unsigned>(dec->scale())) + ")";
        }
        if (t.type() == LT::UNKNOWN) {
            // Mirrors the binary codec's has_type_name byte exactly: a bare UNKNOWN (the
            // value decode_type_spec("") and oid_to_builtin_type() hand a reader back) has
            // NO name and must not acquire one here. A GENERIC extension is a bare tag that
            // was merely aliased — its alias() is a COLUMN name, not a type name, and
            // type_name() would hand it over as one.
            const auto* ext = checked_flat_extension(t, types::logical_type_extension::extension_type::UNKNOWN);
            if (ext == nullptr) {
                return "UNKNOWN()";
            }
            return "UNKNOWN(" +
                   escape_flat_name(static_cast<const types::unknown_logical_type_extension*>(ext)->type_name()) + ")";
        }
        if (t.type() == LT::LIST) {
            const auto* ext = checked_flat_extension(t, types::logical_type_extension::extension_type::LIST);
            if (ext == nullptr) {
                return flat_unpersistable(t.type());
            }
            return "LIST(" +
                   encode_type_nested(static_cast<const types::list_logical_type_extension*>(ext)->node(), depth + 1) +
                   ")";
        }
        if (t.type() == LT::ARRAY) {
            const auto* raw = checked_flat_extension(t, types::logical_type_extension::extension_type::ARRAY);
            if (raw == nullptr) {
                return flat_unpersistable(t.type());
            }
            const auto* ext = static_cast<const types::array_logical_type_extension*>(raw);
            return "ARRAY(" + encode_type_nested(ext->internal_type(), depth + 1) + "," +
                   std::to_string(ext->size()) + ")";
        }
        if (t.type() == LT::MAP) {
            const auto* raw = checked_flat_extension(t, types::logical_type_extension::extension_type::MAP);
            if (raw == nullptr) {
                return flat_unpersistable(t.type());
            }
            const auto* ext = static_cast<const types::map_logical_type_extension*>(raw);
            return "MAP(" + encode_type_nested(ext->key(), depth + 1) + "," +
                   encode_type_nested(ext->value(), depth + 1) + ")";
        }
        if (t.type() == LT::STRUCT) {
            const auto* raw = checked_flat_extension(t, types::logical_type_extension::extension_type::STRUCT);
            if (raw == nullptr) {
                return flat_unpersistable(t.type());
            }
            const auto* ext = static_cast<const types::struct_logical_type_extension*>(raw);
            std::string out = "STRUCT(" + escape_flat_name(ext->type_name());
            for (const auto& f : ext->child_types()) {
                out += ',';
                out += escape_flat_name(f.alias());
                out += ':';
                out += encode_type_nested(f, depth + 1);
            }
            out += ')';
            return out;
        }
        if (t.type() == LT::UNION) {
            // UNION reuses struct_logical_type_extension; child_types()[0] is the hidden
            // UTINYINT tag create_union() prepends, and real members start at [1]. The
            // binary codec refuses a UNION whose tag child is missing rather than writing a
            // member list that means something else — same refusal here.
            const auto* raw = checked_flat_extension(t, types::logical_type_extension::extension_type::STRUCT);
            if (raw == nullptr) {
                return flat_unpersistable(t.type());
            }
            const auto& children = static_cast<const types::struct_logical_type_extension*>(raw)->child_types();
            if (children.empty() || children.front().type() != LT::UTINYINT) {
                return flat_unpersistable(t.type());
            }
            std::string out = "UNION(";
            bool first = true;
            for (size_t i = 1; i < children.size(); ++i) {
                if (!first)
                    out += ',';
                first = false;
                out += escape_flat_name(children[i].alias());
                out += ':';
                out += encode_type_nested(children[i], depth + 1);
            }
            out += ')';
            return out;
        }
        if (t.type() == LT::VARIANT) {
            // The internal struct layout is fixed and create_variant() rebuilds it on
            // decode, so no payload is written — but a VARIANT that never got that struct
            // is not a VARIANT, and the binary codec refuses it.
            if (checked_flat_extension(t, types::logical_type_extension::extension_type::STRUCT) == nullptr) {
                return flat_unpersistable(t.type());
            }
            return "VARIANT";
        }
        // ENUM is handled by the outer encode_type_spec; reaching here means either a
        // malformed ENUM or one of USER / TABLE / FUNCTION / LAMBDA / INVALID, which the
        // binary codec refuses outright ("these never describe stored data"). Writing
        // "UNKNOWN(<number>)" for them — the previous behaviour — produced the exact shape
        // of a legitimate named user-type reference, so the one type the gate exists to
        // stop travelled on as something a resolver would chase by name.
        return flat_unpersistable(t.type());
    }

    // Recursive-descent parser for the flat-text format. The parse context owns the
    // error channel (rule 2 — no exceptions): the FIRST failure wins, every later step
    // short-circuits, and decode_type_spec turns the failure into a core::error_t.
    // Without a channel everything unreadable collapses into logical_type::UNKNOWN, the
    // very value that also means "named user-type reference" — corruption indistinguishable
    // from a legitimate answer.

    struct flat_parse_ctx_t {
        std::string_view s;
        size_t pos = 0;
        bool failed = false;
        std::string what;

        void fail(std::string reason) {
            if (!failed) {
                failed = true;
                what = std::move(reason);
                what += " (at offset ";
                what += std::to_string(pos);
                what += ")";
            }
        }
        // Consume exactly `c` or fail; short-circuits after a previous failure.
        bool expect(char c, const char* inside) {
            if (failed) {
                return false;
            }
            if (pos >= s.size() || s[pos] != c) {
                fail(std::string{"type spec: expected '"} + c + "' in " + inside);
                return false;
            }
            ++pos;
            return true;
        }
    };

    static types::complex_logical_type
    parse_flat_type(std::pmr::memory_resource* resource, flat_parse_ctx_t& ctx, uint32_t depth);

    // Read characters until one of the stop chars (at depth 0). Keyword/number positions
    // only — a backslash is consumed literally here and the keyword match refuses it.
    static std::string read_token(std::string_view s, size_t& pos) {
        size_t start = pos;
        while (pos < s.size() && s[pos] != '(' && s[pos] != ')' && s[pos] != ',' && s[pos] != ':') {
            ++pos;
        }
        return std::string{s.substr(start, pos - start)};
    }

    // Read a NAME (struct/union field, struct/enum type name, UNKNOWN reference): the
    // escape-aware twin of read_token. `\c` for c in the delimiter set yields c; a
    // backslash before anything else — a trailing one included — fails the context, so a
    // raw backslash written by a pre-escaping build refuses instead of decoding to a
    // different name.
    static std::string read_name_token(flat_parse_ctx_t& ctx) {
        std::string out;
        while (ctx.pos < ctx.s.size() && ctx.s[ctx.pos] != '(' && ctx.s[ctx.pos] != ')' && ctx.s[ctx.pos] != ',' &&
               ctx.s[ctx.pos] != ':') {
            char c = ctx.s[ctx.pos];
            if (c == '\\') {
                if (ctx.pos + 1 >= ctx.s.size() || !flat_name_needs_escape(ctx.s[ctx.pos + 1])) {
                    ctx.fail("type spec: malformed escape in a name");
                    return out;
                }
                c = ctx.s[ctx.pos + 1];
                ++ctx.pos;
            }
            out += c;
            ++ctx.pos;
        }
        return out;
    }

    // Escape-aware scan for the ENUM leg's flat splitting: the position of `target`
    // outside any `\x` pair, npos when absent.
    static size_t find_unescaped(std::string_view s, char target, size_t from) {
        for (size_t i = from; i < s.size(); ++i) {
            if (s[i] == '\\') {
                ++i;
                continue;
            }
            if (s[i] == target) {
                return i;
            }
        }
        return std::string_view::npos;
    }

    // Unescape one already-delimited name; false on a malformed escape.
    static bool unescape_flat_name(std::string_view in, std::string& out) {
        out.clear();
        out.reserve(in.size());
        for (size_t i = 0; i < in.size(); ++i) {
            char c = in[i];
            if (c == '\\') {
                if (i + 1 >= in.size() || !flat_name_needs_escape(in[i + 1])) {
                    return false;
                }
                c = in[++i];
            }
            out += c;
        }
        return true;
    }

    // Whole-token integer read (mirrors parse_oid_csv): from_chars stops at the first
    // character it cannot use and still reports success, so "12x" would read as 12.
    template<typename Int>
    static bool read_whole_int(const std::string& tok, Int& out) {
        const auto [ptr, ec] = std::from_chars(tok.data(), tok.data() + tok.size(), out);
        return ec == std::errc{} && ptr == tok.data() + tok.size();
    }

    static types::complex_logical_type
    parse_flat_type(std::pmr::memory_resource* resource, flat_parse_ctx_t& ctx, uint32_t depth) {
        using LT = types::logical_type;
        const auto unknown = [] { return types::complex_logical_type{LT::UNKNOWN}; };
        if (ctx.failed) {
            return unknown();
        }
        if (depth > MAX_FLAT_SPEC_DEPTH) {
            ctx.fail("type spec: nesting exceeds the depth window shared with the binary codec");
            return unknown();
        }
        std::string name = read_token(ctx.s, ctx.pos);

        if (ctx.pos >= ctx.s.size() || ctx.s[ctx.pos] != '(') {
            if (name == "VARIANT") {
                return types::complex_logical_type::create_variant(resource);
            }
            auto lt = scalar_name_to_type(name);
            if (lt != LT::UNKNOWN) {
                return types::complex_logical_type{lt};
            }
            // A bare name outside the scalar table is NOT in the encoder's language:
            // named user-type references are written as "UNKNOWN(name)", never bare.
            ctx.fail("type spec: unrecognised type name '" + name + "'");
            return unknown();
        }
        ++ctx.pos; // consume '('

        if (name == "numeric" || name == "DECIMAL") {
            // Both spellings of the decimal head are READ ("numeric" is what the encoder
            // writes; "DECIMAL" is pinned by catalog::type_spec::decimal_with_old_name_compat).
            const std::string w = read_token(ctx.s, ctx.pos);
            if (!ctx.expect(',', "numeric(width,scale)")) {
                return unknown();
            }
            const std::string sc = read_token(ctx.s, ctx.pos);
            if (!ctx.expect(')', "numeric(width,scale)")) {
                return unknown();
            }
            int wv{};
            int scv{};
            if (!read_whole_int(w, wv) || !read_whole_int(sc, scv)) {
                ctx.fail("type spec: numeric width/scale is not a number");
                return unknown();
            }
            // Range-check BEFORE narrowing: "numeric(256,0)" would otherwise wrap to
            // DECIMAL(0,0). An out-of-window pair is refused HERE, through the decode
            // error channel, rather than leaning on callers to read UNKNOWN as a rejection.
            if (wv < 0 || scv < 0 || wv > types::DECIMAL_MAX_WIDTH || scv > types::DECIMAL_MAX_WIDTH) {
                ctx.fail("type spec: numeric(" + w + "," + sc + ") is outside the DECIMAL window");
                return unknown();
            }
            auto decimal = types::complex_logical_type::create_decimal(resource,
                                                                       static_cast<uint8_t>(wv),
                                                                       static_cast<uint8_t>(scv));
            if (decimal.has_error()) {
                ctx.fail(std::string{"type spec: "} + decimal.error().what.c_str());
                return unknown();
            }
            return std::move(decimal.value());
        }
        if (name == "UNKNOWN") {
            std::string tname = read_name_token(ctx);
            if (!ctx.expect(')', "UNKNOWN(name)")) {
                return unknown();
            }
            // "UNKNOWN()" is the bare form — the mirror of the binary codec's
            // has_type_name = 0. create_unknown("") would attach an UNKNOWN extension
            // holding an empty name, which is a DIFFERENT value (operator== tells them
            // apart) and would not round-trip.
            if (tname.empty()) {
                return unknown();
            }
            return types::complex_logical_type::create_unknown(tname);
        }
        // The flat mirror of the binary codec's leading type byte, for the plain scalars
        // that have no pg_type name (see is_nameless_flat_builtin). WHITELISTED on the way
        // in as well as on the way out: reading an arbitrary number back would hand a
        // reader FUNCTION or INVALID out of a catalog column.
        if (name == "BUILTIN") {
            const std::string tok = read_token(ctx.s, ctx.pos);
            if (!ctx.expect(')', "BUILTIN(type)")) {
                return unknown();
            }
            unsigned int raw{};
            if (!read_whole_int(tok, raw) || raw > 255) {
                ctx.fail("type spec: BUILTIN(" + tok + ") is not a logical_type number");
                return unknown();
            }
            const auto lt = static_cast<LT>(static_cast<uint8_t>(raw));
            if (!is_nameless_flat_builtin(lt)) {
                ctx.fail("type spec: BUILTIN(" + tok + ") is not one this codec writes");
                return unknown();
            }
            return types::complex_logical_type{lt};
        }
        if (name == "LIST") {
            auto inner = parse_flat_type(resource, ctx, depth + 1);
            if (!ctx.expect(')', "LIST(inner)")) {
                return unknown();
            }
            return types::complex_logical_type::create_list(inner);
        }
        if (name == "ARRAY") {
            auto inner = parse_flat_type(resource, ctx, depth + 1);
            if (!ctx.expect(',', "ARRAY(inner,size)")) {
                return unknown();
            }
            const std::string sz = read_token(ctx.s, ctx.pos);
            if (!ctx.expect(')', "ARRAY(inner,size)")) {
                return unknown();
            }
            unsigned long long sv{};
            if (!read_whole_int(sz, sv)) {
                ctx.fail("type spec: ARRAY size is not a number");
                return unknown();
            }
            return types::complex_logical_type::create_array(inner, sv);
        }
        if (name == "MAP") {
            auto key = parse_flat_type(resource, ctx, depth + 1);
            if (!ctx.expect(',', "MAP(key,value)")) {
                return unknown();
            }
            auto val = parse_flat_type(resource, ctx, depth + 1);
            if (!ctx.expect(')', "MAP(key,value)")) {
                return unknown();
            }
            return types::complex_logical_type::create_map(resource, key, val);
        }
        if (name == "STRUCT") {
            std::string struct_name = read_name_token(ctx);
            std::pmr::vector<types::complex_logical_type> fields(resource);
            while (!ctx.failed && ctx.pos < ctx.s.size() && ctx.s[ctx.pos] == ',') {
                ++ctx.pos; // ','
                std::string fname = read_name_token(ctx);
                if (!ctx.expect(':', "STRUCT field")) {
                    return unknown();
                }
                auto ftype = parse_flat_type(resource, ctx, depth + 1);
                ftype.set_alias(fname);
                fields.push_back(std::move(ftype));
            }
            if (!ctx.expect(')', "STRUCT(name,fields...)")) {
                return unknown();
            }
            return types::complex_logical_type::create_struct(struct_name, fields);
        }
        if (name == "UNION") {
            std::pmr::vector<types::complex_logical_type> fields(resource);
            // First member
            if (ctx.pos < ctx.s.size() && ctx.s[ctx.pos] != ')') {
                std::string fname = read_name_token(ctx);
                if (!ctx.expect(':', "UNION member")) {
                    return unknown();
                }
                auto ftype = parse_flat_type(resource, ctx, depth + 1);
                ftype.set_alias(fname);
                fields.push_back(std::move(ftype));
            }
            while (!ctx.failed && ctx.pos < ctx.s.size() && ctx.s[ctx.pos] == ',') {
                ++ctx.pos; // ','
                std::string fname = read_name_token(ctx);
                if (!ctx.expect(':', "UNION member")) {
                    return unknown();
                }
                auto ftype = parse_flat_type(resource, ctx, depth + 1);
                ftype.set_alias(fname);
                fields.push_back(std::move(ftype));
            }
            if (!ctx.expect(')', "UNION(members...)")) {
                return unknown();
            }
            return types::complex_logical_type::create_union(std::move(fields));
        }
        // An unrecognised keyword WITH arguments is a REFUSAL: consuming it to the matching
        // ')' and answering a named UNKNOWN round-trips a spec this build cannot parse as a
        // plausible user-type reference.
        ctx.fail("type spec: unrecognised type keyword '" + name + "'");
        return unknown();
    }

    std::string encode_type_spec(const types::complex_logical_type& t) {
        using LT = types::logical_type;
        // Only a type atttypid can carry on its own goes specless — every built-in scalar,
        // signed and unsigned alike. Anything else — DECIMAL, ENUM, a nested type — is
        // written out below, or it would come back as neither an oid nor a spec.
        if (builtin_type_to_oid(t.type()) != INVALID_OID) {
            return "";
        }
        // ENUM: flat text "ENUM:type_name:label0=val0,label1=val1,..."
        // Name and labels are escape_flat_name-escaped — see the note above it.
        if (t.type() == LT::ENUM) {
            // An ENUM whose extension is absent, or GENERIC because set_alias() ran on a
            // bare tag, is what the binary codec calls in-memory corruption and refuses.
            // The old `if (ext != nullptr)` was two mistakes: a non-null GENERIC extension
            // was static_cast to an enum one and walked for entries, and type_name() on a
            // bare tag asserted (or, under NDEBUG, dereferenced null).
            const auto* ext = checked_flat_extension(t, types::logical_type_extension::extension_type::ENUM);
            if (ext == nullptr) {
                return flat_unpersistable(t.type());
            }
            const auto* enum_ext = static_cast<const types::enum_logical_type_extension*>(ext);
            std::string out = "ENUM:";
            out += escape_flat_name(enum_ext->type_name());
            out += ':';
            bool first = true;
            for (const auto& entry : enum_ext->entries()) {
                if (!first)
                    out += ',';
                first = false;
                const auto& etype = entry.type();
                out += escape_flat_name(etype.has_alias() ? etype.alias() : std::string{});
                out += '=';
                out += std::to_string(entry.value<std::int32_t>());
            }
            return out;
        }
        return encode_type_nested(t, 0);
    }

    core::result_wrapper_t<types::complex_logical_type> decode_type_spec(std::pmr::memory_resource* resource,
                                                                         std::string_view spec) {
        using LT = types::logical_type;
        if (spec.empty()) {
            // No spec at all — a builtin scalar stored without one; the caller
            // reconstructs the type from atttypid. A legitimate answer, not an error.
            return types::complex_logical_type{LT::UNKNOWN};
        }
        const auto corrupt = [resource](const std::string& what) {
            return core::error_t{core::error_code_t::data_corruption, std::pmr::string{what.c_str(), resource}};
        };
        // ENUM flat format: "ENUM:type_name:label0=val0,...". Actively WRITTEN by
        // encode_type_spec above — a live format, not a compatibility shim.
        if (spec.size() >= 5 && spec.compare(0, 5, "ENUM:") == 0) {
            auto rest = spec.substr(5);
            // Escape-aware from here on: the encoder escapes the format's delimiters
            // inside the name and the labels, so every split looks only at UNESCAPED
            // separators and every extracted name is unescaped (a malformed escape —
            // including a raw backslash a pre-escaping build wrote — refuses loudly).
            auto colon = find_unescaped(rest, ':', 0);
            if (colon == std::string_view::npos) {
                // The encoder always writes the second ':', even for zero entries.
                return corrupt("type spec: ENUM without an entry-list separator");
            }
            std::string name;
            if (!unescape_flat_name(rest.substr(0, colon), name)) {
                return corrupt("type spec: malformed escape in an ENUM name");
            }
            std::vector<components::types::logical_value_t> entries;
            auto entries_str = rest.substr(colon + 1);
            if (!entries_str.empty()) {
                std::size_t i = 0;
                // for(;;) so the empty token behind a trailing ',' is visited too — the
                // same truncation trace parse_oid_csv learned to keep (helpers.cpp).
                for (;;) {
                    const std::size_t comma = find_unescaped(entries_str, ',', i);
                    const std::string_view token =
                        entries_str.substr(i, (comma == std::string_view::npos ? entries_str.size() : comma) - i);
                    const std::size_t eq = find_unescaped(token, '=', 0);
                    if (eq == std::string_view::npos) {
                        // Covers the empty token as well: the encoder writes label=value
                        // pairs and nothing else.
                        return corrupt("type spec: ENUM entry without '='");
                    }
                    std::string label;
                    if (!unescape_flat_name(token.substr(0, eq), label)) {
                        return corrupt("type spec: malformed escape in an ENUM label");
                    }
                    const auto val_str = token.substr(eq + 1);
                    int v{};
                    const auto [vp, vec_] = std::from_chars(val_str.data(), val_str.data() + val_str.size(), v);
                    if (vec_ != std::errc{} || vp != val_str.data() + val_str.size()) {
                        return corrupt("type spec: ENUM entry value is not a number");
                    }
                    components::types::logical_value_t lv(resource, v);
                    lv.set_alias(label);
                    entries.push_back(std::move(lv));
                    if (comma == std::string_view::npos) {
                        break;
                    }
                    i = comma + 1;
                }
            }
            return components::types::complex_logical_type::create_enum(name, std::move(entries));
        }
        // Flat-text format for all other types. No catch(...): it would swallow EVERYTHING
        // (bad_alloc included) into UNKNOWN. The parser reports through its context instead
        // of throwing.
        flat_parse_ctx_t ctx{spec, 0, false, std::string{}};
        auto parsed = parse_flat_type(resource, ctx, 0);
        if (ctx.failed) {
            return corrupt(ctx.what);
        }
        if (ctx.pos != spec.size()) {
            return corrupt("type spec: trailing bytes after a complete type (at offset " +
                           std::to_string(ctx.pos) + ")");
        }
        return parsed;
    }

    std::string encode_proargmatchers(const std::vector<components::compute::parameter_type>& parameters) {
        std::string out;
        for (size_t i = 0; i < parameters.size(); ++i) {
            if (i > 0)
                out += '|';
            const auto& parameter = parameters[i];
            if (!parameter.is_variable()) {
                out += "e:";
                out += std::to_string(static_cast<int>(parameter.type().type()));
                continue;
            }
            out += "v:";
            out += std::to_string(static_cast<int>(parameter.id()));
            const auto& admissible = parameter.admissible();
            for (size_t j = 0; j < admissible.size(); ++j) {
                out += j > 0 ? ',' : ':';
                out += std::to_string(static_cast<int>(admissible[j].type()));
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
                    // A raw resolver closure has no introspectable form; its runtime
                    // shape comes back through prouid → compute::function_registry,
                    // never by parsing this column. Writing "s:0" here would claim a
                    // same-type-as-argument-0 contract the function never declared; "c"
                    // states the truth instead of guessing one. Refusing is not an option
                    // either: registering a computed(...) output is pinned legal behaviour
                    // (integration test_udfs does exactly that).
                    out += 'c';
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
            case ns::uint8_type:
                return LT::UTINYINT;
            case ns::int16_type:
                return LT::SMALLINT;
            case ns::uint16_type:
                return LT::USMALLINT;
            case ns::int32_type:
                return LT::INTEGER;
            case ns::uint32_type:
                return LT::UINTEGER;
            case ns::int64_type:
                return LT::BIGINT;
            case ns::uint64_type:
                return LT::UBIGINT;
            case ns::int128_type:
                return LT::HUGEINT;
            case ns::uint128_type:
                return LT::UHUGEINT;
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
            case ns::blob_type:
                return LT::BLOB;
            case ns::uuid_type:
                return LT::UUID;
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
            case LT::UTINYINT:
                return ns::uint8_type;
            case LT::SMALLINT:
                return ns::int16_type;
            case LT::USMALLINT:
                return ns::uint16_type;
            case LT::INTEGER:
                return ns::int32_type;
            case LT::UINTEGER:
                return ns::uint32_type;
            case LT::BIGINT:
                return ns::int64_type;
            case LT::UBIGINT:
                return ns::uint64_type;
            case LT::HUGEINT:
                return ns::int128_type;
            case LT::UHUGEINT:
                return ns::uint128_type;
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
            case LT::BLOB:
                return ns::blob_type;
            case LT::UUID:
                return ns::uuid_type;
            default:
                return INVALID_OID;
        }
    }

    types::logical_type pg_name_to_logical_type(std::string_view name) noexcept { return scalar_name_to_type(name); }

    namespace {

        constexpr char kDefaultSpecNull = 'N';
        constexpr char kDefaultSpecValue = 'V';

        core::error_t default_spec_error(std::pmr::memory_resource* resource,
                                         core::error_code_t code,
                                         const std::string& text) {
            return core::error_t{code, std::pmr::string{text.c_str(), resource}};
        }

        // Readable type for the rule-6 rejection message. encode_type_spec renders the
        // full tree for complex types and "" for scalars, where the pg name is the answer.
        std::string describe_default_type(const types::complex_logical_type& t) {
            auto spec = encode_type_spec(t);
            if (!spec.empty()) {
                return spec;
            }
            const auto name = scalar_type_to_name(t.type());
            if (!name.empty()) {
                return std::string{name};
            }
            return "type#" + std::to_string(static_cast<int>(t.type()));
        }

        void append_hex(std::string& out, const std::pmr::string& raw) {
            static constexpr char kDigits[] = "0123456789ABCDEF";
            out.reserve(out.size() + raw.size() * 2);
            for (char raw_byte : raw) {
                const auto byte = static_cast<unsigned char>(raw_byte);
                out.push_back(kDigits[byte >> 4U]);
                out.push_back(kDigits[byte & 0x0FU]);
            }
        }

        bool read_hex(std::pmr::memory_resource* resource, std::string_view hex, std::pmr::string& out) {
            if (hex.size() % 2 != 0) {
                return false;
            }
            const auto nibble = [](char c) -> int {
                if (c >= '0' && c <= '9') {
                    return c - '0';
                }
                if (c >= 'A' && c <= 'F') {
                    return c - 'A' + 10;
                }
                if (c >= 'a' && c <= 'f') {
                    return c - 'a' + 10;
                }
                return -1;
            };
            out = std::pmr::string{resource};
            out.reserve(hex.size() / 2);
            for (std::size_t i = 0; i < hex.size(); i += 2) {
                const int hi = nibble(hex[i]);
                const int lo = nibble(hex[i + 1]);
                if (hi < 0 || lo < 0) {
                    return false;
                }
                out.push_back(static_cast<char>((static_cast<unsigned>(hi) << 4U) | static_cast<unsigned>(lo)));
            }
            return true;
        }

    } // namespace

    core::error_t
    encode_default_spec(std::pmr::memory_resource* resource, const types::logical_value_t& v, std::string& out) {
        out.clear();
        if (v.is_null()) {
            // An explicit DEFAULT NULL is a default. Recording it as "" would make it
            // indistinguishable from having none.
            out.push_back(kDefaultSpecNull);
            return core::error_t::no_error();
        }
        if (!index::codec::is_encodable_value_type(v.type())) {
            return default_spec_error(resource,
                                      core::error_code_t::schema_error,
                                      std::string{"DEFAULT of type "} + describe_default_type(v.type()) +
                                          " cannot be persisted");
        }
        std::pmr::string payload{resource};
        if (!index::codec::append_typed_value(payload, v)) {
            return default_spec_error(resource,
                                      core::error_code_t::schema_error,
                                      std::string{"DEFAULT of type "} + describe_default_type(v.type()) +
                                          " cannot be persisted");
        }
        out.push_back(kDefaultSpecValue);
        append_hex(out, payload);
        return core::error_t::no_error();
    }

    core::error_t decode_default_spec(std::pmr::memory_resource* resource,
                                      const types::complex_logical_type& column_type,
                                      std::string_view spec,
                                      std::optional<types::logical_value_t>& out) {
        out.reset();
        if (spec.empty()) {
            return core::error_t::no_error(); // no default at all
        }
        if (spec.size() == 1 && spec.front() == kDefaultSpecNull) {
            // An explicit DEFAULT NULL: present, and NULL. NULL is NA-typed here
            // (is_null() IS type()==NA), so the value cannot carry column_type — the
            // caller holds the type separately.
            out.emplace(resource, types::complex_logical_type{types::logical_type::NA});
            return core::error_t::no_error();
        }
        if (spec.front() != kDefaultSpecValue) {
            return default_spec_error(resource,
                                      core::error_code_t::data_corruption,
                                      "pg_attribute.attdefspec is not a recognised default encoding");
        }
        std::pmr::string payload{resource};
        if (!read_hex(resource, spec.substr(1), payload)) {
            return default_spec_error(resource,
                                      core::error_code_t::data_corruption,
                                      "pg_attribute.attdefspec payload is not valid hex");
        }
        std::size_t pos = 0;
        bool ok = true;
        auto value = index::codec::read_typed_value(resource, column_type, payload, pos, ok);
        if (!ok || pos != payload.size()) {
            return default_spec_error(resource,
                                      core::error_code_t::data_corruption,
                                      "pg_attribute.attdefspec does not decode against the column type");
        }
        out.emplace(std::move(value));
        return core::error_t::no_error();
    }

} // namespace components::catalog
