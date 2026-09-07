#include "system_table_schemas.hpp"
#include "catalog_codes.hpp"

#include <array>
#include <charconv>
#include <components/index/logical_value_binary_codec.hpp>

#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>

// Deviations below from stock PostgreSQL catalog shapes (missing owner/statistics columns;
// relkind 'g' instead of 'c' to avoid colliding with PG's composite-type code; extra
// attoid/atttypspec/prouid/indtype-family columns; pg_database as a 10th table) are
// intentional for this single-process engine — do not revert them to plain PostgreSQL shapes.

namespace components::catalog {

    using components::table::column_definition_t;
    using components::types::complex_logical_type;
    using components::types::logical_type;

    namespace {
        complex_logical_type oid_col() { return complex_logical_type{logical_type::UINTEGER}; }
        complex_logical_type i32_col() { return complex_logical_type{logical_type::INTEGER}; }
        complex_logical_type i64_col() { return complex_logical_type{logical_type::BIGINT}; }
        complex_logical_type str_col() { return complex_logical_type{logical_type::STRING_LITERAL}; }
        complex_logical_type bool_col() { return complex_logical_type{logical_type::BOOLEAN}; }

        std::vector<column_definition_t> pg_database_columns() {
            std::vector<column_definition_t> c;
            c.emplace_back("oid", oid_col(), /*not_null*/ true);
            c.emplace_back("datname", str_col(), /*not_null*/ true);
            return c;
        }

        std::vector<column_definition_t> pg_namespace_columns() {
            std::vector<column_definition_t> c;
            c.emplace_back("oid", oid_col(), /*not_null*/ true);
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
                true); // see catalog_codes.hpp::relkind
            c.emplace_back("relstoragemode", str_col(), true);
            return c;
        }

        std::vector<column_definition_t> pg_attribute_columns() {
            std::vector<column_definition_t> c;
            c.emplace_back("attoid", oid_col(), true); // stable column OID; FK target for constraints/indexes
            c.emplace_back("attrelid", oid_col(), true);
            c.emplace_back("attname", str_col(), true);
            c.emplace_back("atttypid",
                           oid_col(),
                           true); // builtin scalar only; complex types use atttypspec
            c.emplace_back("attnum", i32_col(), true);
            c.emplace_back("attnotnull", bool_col(), true);
            c.emplace_back("atthasdefault", bool_col(), true);
            c.emplace_back("attisdropped", bool_col(), true); // tombstone; attnum is never reused
            c.emplace_back("atttypspec", str_col(), false); // non-empty only for ARRAY/DECIMAL/STRUCT/ENUM/UNKNOWN
            c.emplace_back("attdefspec", str_col(), false); // hex-armoured default (encode_default_spec)
            // MVCC: added_at_commit_id/dropped_at_commit_id (0=alive) gate visibility; attisdropped mirrors dropped>0.
            c.emplace_back("added_at_commit_id", i64_col(), true);
            c.emplace_back("dropped_at_commit_id", i64_col(), true);
            return c;
        }

        std::vector<column_definition_t> pg_type_columns() {
            std::vector<column_definition_t> c;
            c.emplace_back("oid", oid_col(), true);
            c.emplace_back("typname", str_col(), true);
            c.emplace_back("typnamespace", oid_col(), true);
            c.emplace_back("typdefspec", str_col(), false); // encode_type_spec; empty/missing decodes as UNKNOWN
            return c;
        }

        std::vector<column_definition_t> pg_proc_columns() {
            std::vector<column_definition_t> c;
            c.emplace_back("oid", oid_col(), true);
            c.emplace_back("proname", str_col(), true);
            c.emplace_back("pronamespace", oid_col(), true);
            c.emplace_back("pronargs", i32_col(), false); // arity of the function's first signature
            c.emplace_back("prouid", i64_col(), false); // opaque function_uid from register_udf
            c.emplace_back("proargmatchers", str_col(), false); // see encode_proargmatchers below
            c.emplace_back("prorettype", str_col(), false); // see encode_prorettype below
            return c;
        }

        std::vector<column_definition_t> pg_depend_columns() {
            std::vector<column_definition_t> c;
            c.emplace_back("classid", oid_col(), true);
            c.emplace_back("objid", oid_col(), true);
            c.emplace_back("refclassid", oid_col(), true);
            c.emplace_back("refobjid", oid_col(), true);
            c.emplace_back("deptype", str_col(), true); // 'n','a','i','p' — see PG docs
            return c;
        }

        std::vector<column_definition_t> pg_constraint_columns() {
            std::vector<column_definition_t> c;
            c.emplace_back("oid", oid_col(), true);
            c.emplace_back("conname", str_col(), true);
            c.emplace_back("conrelid", oid_col(), true);
            c.emplace_back("contype", str_col(), true); // 'p','f','u','c','n'
            c.emplace_back("confrelid", oid_col(), false); // 0 if not FK
            c.emplace_back("conkey", str_col(), false);
            c.emplace_back("confkey", str_col(), false);
            // null/empty = ('s','a','a'); matchtype is s/f/p, del/updtype share a/r/c/n/d.
            c.emplace_back("confmatchtype", str_col(), false);
            c.emplace_back("confdeltype", str_col(), false);
            c.emplace_back("confupdtype", str_col(), false);
            c.emplace_back("conexpr", str_col(), false); // CHECK expr SQL text; NULL for non-CHECK
            return c;
        }

        std::vector<column_definition_t> pg_index_columns() {
            std::vector<column_definition_t> c;
            c.emplace_back("indexrelid", oid_col(), true);
            c.emplace_back("indrelid", oid_col(), true);
            c.emplace_back("indkey", str_col(), true);
            c.emplace_back("indisvalid",
                           bool_col(),
                           true); // false until backfill completes; planner ignores invalid indexes
            c.emplace_back("indtype",
                           str_col(),
                           true); // see catalog_codes.hpp::indtype
            return c;
        }

        std::vector<column_definition_t> pg_sequence_columns() {
            std::vector<column_definition_t> c;
            c.emplace_back("seqrelid", oid_col(), /*not_null*/ true);
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
            c.emplace_back("oid", oid_col(), /*not_null*/ true);
            c.emplace_back("rulename", str_col(), true);
            c.emplace_back("ev_class", oid_col(), true);
            c.emplace_back("ev_type", str_col(), true); // 'v' or 'm'
            c.emplace_back("ev_action", str_col(), true);
            return c;
        }

        std::vector<column_definition_t> pg_settings_columns() {
            std::vector<column_definition_t> c;
            c.emplace_back("name", str_col(), /*not_null*/ true);
            c.emplace_back("setting", str_col(), /*not_null*/ true);
            return c;
        }

        std::vector<column_definition_t> pg_cast_columns() {
            std::vector<column_definition_t> c;
            c.emplace_back("oid", oid_col(), /*not_null*/ true);
            c.emplace_back("castsource", oid_col(), true);
            c.emplace_back("casttarget", oid_col(), true);
            // (castsource,casttarget) is the lookup key; no castcontext yet (lives on cast_entry
            // until CREATE CAST needs to persist it).
            return c;
        }

        std::vector<column_definition_t> pg_computed_column_columns() {
            std::vector<column_definition_t> c;
            c.emplace_back("relid",
                           oid_col(),
                           true); // always relkind='g' (generated/computing)
            c.emplace_back("attoid", oid_col(), true);
            c.emplace_back("attname", str_col(), true);
            c.emplace_back("atttypid", oid_col(), true);
            c.emplace_back("atttypspec", str_col(), true); // mirrors pg_attribute.atttypspec
            c.emplace_back("attversion", i64_col(), true);
            c.emplace_back("attrefcount", i64_col(), true);
            return c;
        }
    } // namespace

    std::span<const system_table_def_t> all_system_tables() {
        // pg_database must come first — every catalog object is scoped to a database (seeded via
        // well_known_oid::main_database, manager_disk_t::bootstrap_system_tables_sync).
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


    // Flat-text type-spec grammar (recursive; scalar names match pg_type.typname):
    //   scalar → bool int1 int2 int4 int8 float4 float8 text timestamp bytea uuid
    //   numeric(w,s) | UNKNOWN(name) | LIST(inner) | ARRAY(inner,size) | MAP(key,val)
    //   STRUCT(name,f1:t1,...) | UNION(f1:t1,...) | VARIANT | ENUM:name:label=val,...

    static std::string_view scalar_type_to_name(types::logical_type lt) {
        using LT = types::logical_type;
        switch (lt) {
            case LT::BOOLEAN:
                return "bool";
            case LT::TINYINT:
                return "int1"; // no PG equivalent
            case LT::UTINYINT:
                return "uint1";
            case LT::SMALLINT:
                return "int2";
            case LT::USMALLINT:
                return "uint2";
            case LT::INTEGER:
                return "int4";
            case LT::UINTEGER:
                return "uint4";
            case LT::BIGINT:
                return "int8";
            case LT::UBIGINT:
                return "uint8";
            case LT::HUGEINT:
                return "int16"; // no PG equivalent
            case LT::UHUGEINT:
                return "uint16";
            case LT::FLOAT:
                return "float4";
            case LT::DOUBLE:
                return "float8";
            case LT::STRING_LITERAL:
                return "text";
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
                return "bytea";
            case LT::UUID:
                return "uuid";
            default:
                return "";
        }
    }

    static types::logical_type scalar_name_to_type(std::string_view n) {
        using LT = types::logical_type;
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
        // Seed/alias names (manager_disk_bootstrap.cpp); this "int16" is unrelated to the PG-block one above.
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
        if (n == "int8_t")
            return LT::BIGINT; // BIGINT keyword in parser/gram.y
        return LT::UNKNOWN;
    }

    // Bounded, not unlimited: a deeply nested LIST would blow the stack, and an unbounded writer
    // produces rows its own reader refuses forever.
    static constexpr uint32_t MAX_FLAT_SPEC_DEPTH = 64; // must match type_spec_codec.cpp's MAX_SPEC_DEPTH

    static std::string encode_type_nested(const types::complex_logical_type& t, uint32_t depth);

    // Two encoders write the same column type: the binary codec's gate (gate_persistable_type,
    // validate_logical_plan.cpp) decides refusal, but atttypspec/typdefspec on disk comes from
    // this flat codec — wherever the gate refuses, this must emit kFlatUnpersistable too, or an
    // approved column silently rehydrates as a different type.
    static constexpr std::string_view kFlatUnpersistable = "!unpersistable";

    static std::string flat_unpersistable(types::logical_type lt) {
        return std::string{kFlatUnpersistable} + "(" + std::to_string(static_cast<int>(lt)) + ")";
    }

    // Mirrors type_spec_codec.cpp's checked_extension: an absent or GENERIC extension must not be
    // dereferenced here — that crashes or writes garbage.
    static const types::logical_type_extension* checked_flat_extension(
        const types::complex_logical_type& t,
        types::logical_type_extension::extension_type expected) {
        const auto* ext = t.extension();
        return (ext != nullptr && ext->type() == expected) ? ext : nullptr;
    }

    // Plain scalars with no pg_type name get whitelisted as BUILTIN(<logical_type>) instead of falling
    // through to UNKNOWN(<number>) and rehydrating as a named type
    // (encoder_domains::every_plain_scalar_the_gate_blesses_survives_the_flat_writer pins it).
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

    // Names with one of the format's own delimiters ( ) , : = are backslash-escaped so they
    // can't corrupt the spec; any other backslash is a loud refusal, never a silent rename.
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
        if (t.type() == LT::UNKNOWN) { // bare UNKNOWN gets no name; GENERIC's alias() is a column name, not this
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
            const auto* raw = checked_flat_extension(t, types::logical_type_extension::extension_type::STRUCT);
            if (raw == nullptr) {
                return flat_unpersistable(t.type());
            }
            // child_types()[0] is the hidden UTINYINT tag create_union() prepends; members start at [1].
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
            // create_variant() rebuilds the fixed layout on decode, so no payload is written.
            if (checked_flat_extension(t, types::logical_type_extension::extension_type::STRUCT) == nullptr) {
                return flat_unpersistable(t.type());
            }
            return "VARIANT";
        }
        return flat_unpersistable(t.type()); // malformed ENUM or USER/TABLE/FUNCTION/LAMBDA/INVALID
    }

    // No-exceptions error channel: first failure wins; without it, unreadable input collapses
    // into UNKNOWN, the same value that means "named user-type reference".
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

    static std::string read_token(std::string_view s, size_t& pos) { // backslash is literal here
        size_t start = pos;
        while (pos < s.size() && s[pos] != '(' && s[pos] != ')' && s[pos] != ',' && s[pos] != ':') {
            ++pos;
        }
        return std::string{s.substr(start, pos - start)};
    }

    static std::string read_name_token(flat_parse_ctx_t& ctx) { // `\c` yields c; other backslashes fail the context
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

    // from_chars stops at the first unusable char and still succeeds, so "12x" would read as 12.
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
            // named user-type references are always written as "UNKNOWN(name)", never bare
            ctx.fail("type spec: unrecognised type name '" + name + "'");
            return unknown();
        }
        ++ctx.pos; // consume '('

        if (name == "numeric" || name == "DECIMAL") { // "DECIMAL" pinned by decimal_with_old_name_compat
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
            // range-check before narrowing — "numeric(256,0)" would otherwise wrap to DECIMAL(0,0)
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
            // bare form mirrors has_type_name=0; create_unknown("") is a DIFFERENT value (operator==)
            if (tname.empty()) {
                return unknown();
            }
            return types::complex_logical_type::create_unknown(tname);
        }
        if (name == "BUILTIN") { // whitelisted on read too — an arbitrary number could hand back FUNCTION
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
                ++ctx.pos;
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
                ++ctx.pos;
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
        ctx.fail("type spec: unrecognised type keyword '" + name + "'");
        return unknown();
    }

    std::string encode_type_spec(const types::complex_logical_type& t) {
        using LT = types::logical_type;
        if (builtin_type_to_oid(t.type()) != INVALID_OID) { // every builtin scalar goes specless
            return "";
        }
        // ENUM flat format: "ENUM:type_name:label0=val0,...", escape_flat_name-escaped.
        if (t.type() == LT::ENUM) {
            // An absent or GENERIC extension here is corruption, not a value to static_cast as ENUM.
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
        if (spec.empty()) { // builtin scalar stored without one — caller reconstructs from atttypid
            return types::complex_logical_type{LT::UNKNOWN};
        }
        const auto corrupt = [resource](const std::string& what) {
            return core::error_t{core::error_code_t::data_corruption, std::pmr::string{what.c_str(), resource}};
        };
        if (spec.size() >= 5 && spec.compare(0, 5, "ENUM:") == 0) { // live format, not a legacy shim
            auto rest = spec.substr(5);
            auto colon = find_unescaped(rest, ':', 0);
            if (colon == std::string_view::npos) { // the encoder always writes this ':', even for zero entries
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
                for (;;) { // visits the empty token behind a trailing ',' too (parse_oid_csv's truncation trap)
                    const std::size_t comma = find_unescaped(entries_str, ',', i);
                    const std::string_view token =
                        entries_str.substr(i, (comma == std::string_view::npos ? entries_str.size() : comma) - i);
                    const std::size_t eq = find_unescaped(token, '=', 0);
                    if (eq == std::string_view::npos) {
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
        flat_parse_ctx_t ctx{spec, 0, false, std::string{}}; // no catch(...): would swallow everything into UNKNOWN
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
                    // Not introspectable (shape comes via prouid → function_registry); "s:0"
                    // would falsely claim a same-type-as-arg-0 contract, and computed(...)
                    // outputs are pinned legal behaviour (integration test_udfs), so refusing isn't an option.
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

        std::string describe_default_type(const types::complex_logical_type& t) { // for the rule-6 rejection message
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
        if (v.is_null()) { // must be recorded — "" would be indistinguishable from having no default
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
            return core::error_t::no_error();
        }
        if (spec.size() == 1 && spec.front() == kDefaultSpecNull) { // NA-typed; caller holds column_type separately
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
