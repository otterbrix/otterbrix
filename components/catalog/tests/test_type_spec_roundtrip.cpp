#include <catch2/catch_test_macros.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/types/types.hpp>

#include <set>

using namespace components::catalog;
using namespace components::types;

namespace {
    // create_decimal reports an out-of-window (width, scale) through core::error_t now,
    // instead of an assert that vanished under NDEBUG. Every literal these tests use is
    // inside the window, so the helper checks the result and hands back the type.
    components::types::complex_logical_type
    make_decimal(uint8_t width, uint8_t scale, std::string alias = "") {
        auto created = components::types::complex_logical_type::create_decimal(width, scale, std::move(alias));
        REQUIRE_FALSE(created.has_error());
        return std::move(created.value());
    }
} // namespace

namespace {
    auto* g_resource = std::pmr::new_delete_resource();

    // decode_type_spec answers through core::result_wrapper_t now; every spec in this
    // file is well-formed, so the helper unwraps and lets a refusal fail the test.
    components::types::complex_logical_type decode_ok(std::string_view spec) {
        auto decoded = decode_type_spec(g_resource, spec);
        REQUIRE_FALSE(decoded.has_error());
        return std::move(decoded.value());
    }
} // namespace

TEST_CASE("catalog::type_spec::scalars_encode_empty") {
    REQUIRE(encode_type_spec(complex_logical_type{logical_type::BOOLEAN}) == "");
    REQUIRE(encode_type_spec(complex_logical_type{logical_type::INTEGER}) == "");
    REQUIRE(encode_type_spec(complex_logical_type{logical_type::BIGINT}) == "");
    REQUIRE(encode_type_spec(complex_logical_type{logical_type::FLOAT}) == "");
    REQUIRE(encode_type_spec(complex_logical_type{logical_type::DOUBLE}) == "");
    REQUIRE(encode_type_spec(complex_logical_type{logical_type::STRING_LITERAL}) == "");
    REQUIRE(encode_type_spec(complex_logical_type{logical_type::TIMESTAMP}) == "");
}

// The real persisted read-back path (operator_resolve_table / bootstrap):
// non-empty atttypspec wins, else decode from atttypid. Mirror it here.
static logical_type persisted_readback(const complex_logical_type& t) {
    auto spec = encode_type_spec(t);
    if (!spec.empty()) {
        return decode_ok(spec).type();
    }
    return oid_to_builtin_type(builtin_type_to_oid(t.type()));
}

TEST_CASE("catalog::type_spec::empty_spec_scalars_must_have_oid_mapping") {
    // INVARIANT: every type that encodes an EMPTY spec relies on atttypid for
    // read-back — so builtin_type_to_oid MUST map it, else a persisted column
    // of that type silently rehydrates as UNKNOWN (the BLOB/UUID bug).
    const logical_type empty_spec_scalars[] = {
        logical_type::BOOLEAN,   logical_type::TINYINT,        logical_type::UTINYINT,  logical_type::SMALLINT,
        logical_type::USMALLINT, logical_type::INTEGER,        logical_type::UINTEGER,  logical_type::BIGINT,
        logical_type::UBIGINT,   logical_type::HUGEINT,        logical_type::UHUGEINT,  logical_type::FLOAT,
        logical_type::DOUBLE,    logical_type::STRING_LITERAL, logical_type::TIMESTAMP, logical_type::TIMESTAMP_TZ,
        logical_type::DATE,      logical_type::TIME,           logical_type::TIME_TZ,   logical_type::INTERVAL,
        logical_type::BLOB,      logical_type::UUID,
    };
    std::set<oid_t> seen;
    for (auto lt : empty_spec_scalars) {
        INFO("logical_type = " << static_cast<int>(lt));
        REQUIRE(encode_type_spec(complex_logical_type{lt}) == "");
        REQUIRE(builtin_type_to_oid(lt) != INVALID_OID);
        REQUIRE(oid_to_builtin_type(builtin_type_to_oid(lt)) == lt);
        REQUIRE(persisted_readback(complex_logical_type{lt}) == lt);
        // Two types sharing one oid makes a column rehydrate as the other type.
        REQUIRE(seen.insert(builtin_type_to_oid(lt)).second);
    }
}

TEST_CASE("catalog::type_spec::unsigned_ints_have_their_own_oids") {
    REQUIRE(builtin_type_to_oid(logical_type::UTINYINT) != builtin_type_to_oid(logical_type::TINYINT));
    REQUIRE(builtin_type_to_oid(logical_type::USMALLINT) != builtin_type_to_oid(logical_type::SMALLINT));
    REQUIRE(builtin_type_to_oid(logical_type::UINTEGER) != builtin_type_to_oid(logical_type::INTEGER));
    REQUIRE(builtin_type_to_oid(logical_type::UBIGINT) != builtin_type_to_oid(logical_type::BIGINT));
    REQUIRE(builtin_type_to_oid(logical_type::UHUGEINT) != builtin_type_to_oid(logical_type::HUGEINT));
}

TEST_CASE("catalog::type_spec::blob_uuid_oid_roundtrip") {
    REQUIRE(builtin_type_to_oid(logical_type::BLOB) == well_known_oid::blob_type);
    REQUIRE(builtin_type_to_oid(logical_type::UUID) == well_known_oid::uuid_type);
    REQUIRE(oid_to_builtin_type(well_known_oid::blob_type) == logical_type::BLOB);
    REQUIRE(oid_to_builtin_type(well_known_oid::uuid_type) == logical_type::UUID);
}

TEST_CASE("catalog::type_spec::decimal_roundtrip") {
    auto t = make_decimal(10, 2);
    auto spec = encode_type_spec(t);
    REQUIRE(spec == "numeric(10,2)");

    auto t2 = decode_ok(spec);
    REQUIRE(t2.type() == logical_type::DECIMAL);
    const auto* ext = static_cast<const decimal_logical_type_extension*>(t2.extension());
    REQUIRE(ext->width() == 10);
    REQUIRE(ext->scale() == 2);
}

TEST_CASE("catalog::type_spec::unknown_roundtrip") {
    auto t = complex_logical_type::create_unknown("myudt");
    auto spec = encode_type_spec(t);
    REQUIRE(spec == "UNKNOWN(myudt)");

    auto t2 = decode_ok(spec);
    REQUIRE(t2.type() == logical_type::UNKNOWN);
    REQUIRE(t2.type_name() == "myudt");
}

TEST_CASE("catalog::type_spec::list_roundtrip") {
    auto inner = complex_logical_type{logical_type::INTEGER};
    auto t = complex_logical_type::create_list(inner);
    auto spec = encode_type_spec(t);
    REQUIRE(spec == "LIST(int4)");

    auto t2 = decode_ok(spec);
    REQUIRE(t2.type() == logical_type::LIST);
    REQUIRE(t2.child_type().type() == logical_type::INTEGER);
}

TEST_CASE("catalog::type_spec::array_roundtrip") {
    auto inner = complex_logical_type{logical_type::DOUBLE};
    auto t = complex_logical_type::create_array(inner, 100);
    auto spec = encode_type_spec(t);
    REQUIRE(spec == "ARRAY(float8,100)");

    auto t2 = decode_ok(spec);
    REQUIRE(t2.type() == logical_type::ARRAY);
    REQUIRE(t2.child_type().type() == logical_type::DOUBLE);
    const auto* ext = static_cast<const array_logical_type_extension*>(t2.extension());
    REQUIRE(ext->size() == 100);
}

TEST_CASE("catalog::type_spec::map_roundtrip") {
    auto key = complex_logical_type{logical_type::STRING_LITERAL};
    auto val = complex_logical_type{logical_type::BIGINT};
    auto t = complex_logical_type::create_map(g_resource, key, val);
    auto spec = encode_type_spec(t);
    REQUIRE(spec == "MAP(text,int8)");

    auto t2 = decode_ok(spec);
    REQUIRE(t2.type() == logical_type::MAP);
    const auto* ext = static_cast<const map_logical_type_extension*>(t2.extension());
    REQUIRE(ext->key().type() == logical_type::STRING_LITERAL);
    REQUIRE(ext->value().type() == logical_type::BIGINT);
}

TEST_CASE("catalog::type_spec::struct_roundtrip") {
    auto f1 = complex_logical_type{logical_type::INTEGER};
    f1.set_alias("x");
    auto f2 = complex_logical_type{logical_type::STRING_LITERAL};
    f2.set_alias("y");
    std::pmr::vector<complex_logical_type> point_fields(g_resource);
    point_fields.push_back(f1);
    point_fields.push_back(f2);
    auto t = complex_logical_type::create_struct("point", point_fields);
    auto spec = encode_type_spec(t);
    REQUIRE(spec == "STRUCT(point,x:int4,y:text)");

    auto t2 = decode_ok(spec);
    REQUIRE(t2.type() == logical_type::STRUCT);
    const auto& fields = t2.child_types();
    REQUIRE(fields.size() == 2);
    REQUIRE(fields[0].alias() == "x");
    REQUIRE(fields[0].type() == logical_type::INTEGER);
    REQUIRE(fields[1].alias() == "y");
    REQUIRE(fields[1].type() == logical_type::STRING_LITERAL);
}

TEST_CASE("catalog::type_spec::union_roundtrip") {
    auto m1 = complex_logical_type{logical_type::INTEGER};
    m1.set_alias("i");
    auto m2 = complex_logical_type{logical_type::STRING_LITERAL};
    m2.set_alias("s");
    std::pmr::vector<complex_logical_type> union_members(g_resource);
    union_members.push_back(m1);
    union_members.push_back(m2);
    auto t = complex_logical_type::create_union(union_members);
    auto spec = encode_type_spec(t);
    REQUIRE(spec == "UNION(i:int4,s:text)");

    auto t2 = decode_ok(spec);
    REQUIRE(t2.type() == logical_type::UNION);
    // child_types()[0] is the hidden tag; real members start at [1]
    const auto& ch = t2.child_types();
    REQUIRE(ch.size() >= 3);
    REQUIRE(ch[1].alias() == "i");
    REQUIRE(ch[2].alias() == "s");
}

TEST_CASE("catalog::type_spec::variant_roundtrip") {
    auto t = complex_logical_type::create_variant(g_resource);
    auto spec = encode_type_spec(t);
    REQUIRE(spec == "VARIANT");

    auto t2 = decode_ok(spec);
    REQUIRE(t2.type() == logical_type::VARIANT);
}

TEST_CASE("catalog::type_spec::nested_list_of_struct") {
    auto f1 = complex_logical_type{logical_type::FLOAT};
    f1.set_alias("lat");
    auto f2 = complex_logical_type{logical_type::FLOAT};
    f2.set_alias("lon");
    std::pmr::vector<complex_logical_type> coord_fields(g_resource);
    coord_fields.push_back(f1);
    coord_fields.push_back(f2);
    auto inner = complex_logical_type::create_struct("coord", coord_fields);
    auto t = complex_logical_type::create_list(inner);
    auto spec = encode_type_spec(t);
    REQUIRE(spec == "LIST(STRUCT(coord,lat:float4,lon:float4))");

    auto t2 = decode_ok(spec);
    REQUIRE(t2.type() == logical_type::LIST);
    REQUIRE(t2.child_type().type() == logical_type::STRUCT);
    const auto& fields = t2.child_type().child_types();
    REQUIRE(fields.size() == 2);
    REQUIRE(fields[0].alias() == "lat");
}

TEST_CASE("catalog::type_spec::decimal_with_old_name_compat") {
    // The decoder reads both spellings of the decimal head: "numeric" (what the
    // encoder writes) and "DECIMAL". This pins the second so no writer rename can
    // silently orphan a spec that spells it this way.
    auto t = decode_ok("DECIMAL(18,6)");
    REQUIRE(t.type() == logical_type::DECIMAL);
    const auto* ext = static_cast<const decimal_logical_type_extension*>(t.extension());
    REQUIRE(ext->width() == 18);
    REQUIRE(ext->scale() == 6);
}

TEST_CASE("catalog::type_spec::empty_returns_unknown") {
    auto t = decode_ok("");
    REQUIRE(t.type() == logical_type::UNKNOWN);
}

TEST_CASE("catalog::type_spec::unknown_prefix_no_crash") {
    // Garbage input must not crash — and it must not become a type either. A bare
    // name outside the encoder's language is a data_corruption refusal.
    auto decoded = decode_type_spec(g_resource, "garbage_that_is_not_valid_type_spec");
    REQUIRE(decoded.has_error());
    REQUIRE(decoded.error().type == core::error_code_t::data_corruption);
}

// Names written AS IS produce a spec the strict decoder refuses whenever a field, alias,
// label or type name carries one of the format's own delimiters ( ) , : = — the DDL goes
// through and every later resolve fails per-statement. The encoder escapes those
// characters and the decoder reads the escapes back.
TEST_CASE("catalog::type_spec::struct_field_names_with_delimiters_roundtrip") {
    auto f1 = complex_logical_type{logical_type::INTEGER};
    f1.set_alias("we:ird,na(me)");
    auto f2 = complex_logical_type{logical_type::STRING_LITERAL};
    f2.set_alias("back\\slash=eq");
    std::pmr::vector<complex_logical_type> fields(g_resource);
    fields.push_back(f1);
    fields.push_back(f2);
    auto t = complex_logical_type::create_struct("na:me(d)", fields);
    auto spec = encode_type_spec(t);

    // Unescaped, the raw delimiters make this spec unreadable (or misread).
    auto t2 = decode_ok(spec);
    REQUIRE(t2.type() == logical_type::STRUCT);
    const auto& out = t2.child_types();
    REQUIRE(out.size() == 2);
    REQUIRE(out[0].alias() == "we:ird,na(me)");
    REQUIRE(out[0].type() == logical_type::INTEGER);
    REQUIRE(out[1].alias() == "back\\slash=eq");
    REQUIRE(out[1].type() == logical_type::STRING_LITERAL);
}

TEST_CASE("catalog::type_spec::union_member_names_with_delimiters_roundtrip") {
    auto m1 = complex_logical_type{logical_type::INTEGER};
    m1.set_alias("i:n,t");
    auto m2 = complex_logical_type{logical_type::STRING_LITERAL};
    m2.set_alias("s)t(r");
    std::pmr::vector<complex_logical_type> members(g_resource);
    members.push_back(m1);
    members.push_back(m2);
    auto t = complex_logical_type::create_union(members);
    auto spec = encode_type_spec(t);

    auto t2 = decode_ok(spec);
    REQUIRE(t2.type() == logical_type::UNION);
    const auto& ch = t2.child_types();
    REQUIRE(ch.size() >= 3);
    REQUIRE(ch[1].alias() == "i:n,t");
    REQUIRE(ch[2].alias() == "s)t(r");
}

TEST_CASE("catalog::type_spec::enum_names_with_delimiters_roundtrip") {
    std::vector<components::types::logical_value_t> entries;
    components::types::logical_value_t e0(g_resource, 0);
    e0.set_alias("a=b,c:d");
    entries.push_back(std::move(e0));
    components::types::logical_value_t e1(g_resource, 1);
    e1.set_alias("plain");
    entries.push_back(std::move(e1));
    auto t = complex_logical_type::create_enum("mo:od,s", std::move(entries));
    auto spec = encode_type_spec(t);

    auto t2 = decode_ok(spec);
    REQUIRE(t2.type() == logical_type::ENUM);
    REQUIRE(t2.type_name() == "mo:od,s");
    const auto* ext = static_cast<const enum_logical_type_extension*>(t2.extension());
    REQUIRE(ext->entries().size() == 2);
    REQUIRE(ext->entries()[0].type().alias() == "a=b,c:d");
    REQUIRE(ext->entries()[1].type().alias() == "plain");
}

TEST_CASE("catalog::type_spec::unknown_name_with_delimiters_roundtrip") {
    auto t = complex_logical_type::create_unknown("user)type,");
    auto spec = encode_type_spec(t);

    auto t2 = decode_ok(spec);
    REQUIRE(t2.type() == logical_type::UNKNOWN);
    REQUIRE(t2.type_name() == "user)type,");
}

TEST_CASE("catalog::type_spec::malformed_escape_refuses") {
    // A backslash followed by anything outside the escapable set — or a trailing one —
    // is not silently absorbed into the name: an old raw-written backslash name must
    // refuse loudly rather than decode to a DIFFERENT name.
    auto bad = decode_type_spec(g_resource, "UNKNOWN(a\\zb)");
    REQUIRE(bad.has_error());
    REQUIRE(bad.error().type == core::error_code_t::data_corruption);
    auto trailing = decode_type_spec(g_resource, "UNKNOWN(ab\\");
    REQUIRE(trailing.has_error());
}
