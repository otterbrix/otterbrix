#include <catch2/catch_test_macros.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/types/types.hpp>

using namespace components::catalog;
using namespace components::types;

namespace {
    auto* g_resource = std::pmr::new_delete_resource();
} // namespace

TEST_CASE("catalog::type_spec::scalars_encode_empty") {
    REQUIRE(encode_type_spec(complex_logical_type{logical_type::BOOLEAN}) == "");
    REQUIRE(encode_type_spec(complex_logical_type{logical_type::INTEGER}) == "");
    REQUIRE(encode_type_spec(complex_logical_type{logical_type::BIGINT}) == "");
    REQUIRE(encode_type_spec(complex_logical_type{logical_type::FLOAT}) == "");
    REQUIRE(encode_type_spec(complex_logical_type{logical_type::DOUBLE}) == "");
    REQUIRE(encode_type_spec(complex_logical_type{logical_type::STRING_LITERAL}) == "");
    REQUIRE(encode_type_spec(complex_logical_type{logical_type::TIMESTAMP}) == "");
    REQUIRE(encode_type_spec(complex_logical_type{logical_type::BLOB}) == "");
}

// The real persisted read-back path (operator_resolve_table / bootstrap):
// non-empty atttypspec wins, else decode from atttypid. Mirror it here.
static logical_type persisted_readback(const complex_logical_type& t) {
    auto spec = encode_type_spec(t);
    if (!spec.empty()) {
        return decode_type_spec(g_resource, spec).type();
    }
    return oid_to_builtin_type(builtin_type_to_oid(t.type()));
}

TEST_CASE("catalog::type_spec::empty_spec_scalars_must_have_oid_mapping") {
    // INVARIANT: every type that encodes an EMPTY spec relies on atttypid for
    // read-back — so builtin_type_to_oid MUST map it, else a persisted column
    // of that type silently rehydrates as UNKNOWN (the BLOB/UUID bug).
    const logical_type empty_spec_scalars[] = {
        logical_type::BOOLEAN,
        logical_type::TINYINT,
        logical_type::SMALLINT,
        logical_type::INTEGER,
        logical_type::BIGINT,
        logical_type::FLOAT,
        logical_type::DOUBLE,
        logical_type::STRING_LITERAL,
        logical_type::TIMESTAMP,
        logical_type::TIMESTAMP_TZ,
        logical_type::DATE,
        logical_type::TIME,
        logical_type::TIME_TZ,
        logical_type::INTERVAL,
        logical_type::BLOB,
        logical_type::UUID,
    };
    for (auto lt : empty_spec_scalars) {
        INFO("logical_type = " << static_cast<int>(lt));
        REQUIRE(encode_type_spec(complex_logical_type{lt}) == "");
        REQUIRE(builtin_type_to_oid(lt) != INVALID_OID);
        REQUIRE(oid_to_builtin_type(builtin_type_to_oid(lt)) == lt);
        REQUIRE(persisted_readback(complex_logical_type{lt}) == lt);
    }
}

TEST_CASE("catalog::type_spec::unsigned_ints_roundtrip_via_spec") {
    // Unsigned ints have no well-known pg_type oid — they must encode a
    // NON-empty flat-text spec ("uint1".."uint8") and decode back exactly.
    REQUIRE(encode_type_spec(complex_logical_type{logical_type::UTINYINT}) == "uint1");
    REQUIRE(encode_type_spec(complex_logical_type{logical_type::USMALLINT}) == "uint2");
    REQUIRE(encode_type_spec(complex_logical_type{logical_type::UINTEGER}) == "uint4");
    REQUIRE(encode_type_spec(complex_logical_type{logical_type::UBIGINT}) == "uint8");
    for (auto lt :
         {logical_type::UTINYINT, logical_type::USMALLINT, logical_type::UINTEGER, logical_type::UBIGINT}) {
        INFO("logical_type = " << static_cast<int>(lt));
        REQUIRE(persisted_readback(complex_logical_type{lt}) == lt);
    }
}

TEST_CASE("catalog::type_spec::blob_uuid_oid_roundtrip") {
    REQUIRE(builtin_type_to_oid(logical_type::BLOB) == well_known_oid::blob_type);
    REQUIRE(builtin_type_to_oid(logical_type::UUID) == well_known_oid::uuid_type);
    REQUIRE(oid_to_builtin_type(well_known_oid::blob_type) == logical_type::BLOB);
    REQUIRE(oid_to_builtin_type(well_known_oid::uuid_type) == logical_type::UUID);
}

TEST_CASE("catalog::type_spec::decimal_roundtrip") {
    auto t = complex_logical_type::create_decimal(10, 2);
    auto spec = encode_type_spec(t);
    REQUIRE(spec == "numeric(10,2)");

    auto t2 = decode_type_spec(g_resource, spec);
    REQUIRE(t2.type() == logical_type::DECIMAL);
    const auto* ext = static_cast<const decimal_logical_type_extension*>(t2.extension());
    REQUIRE(ext->width() == 10);
    REQUIRE(ext->scale() == 2);
}

TEST_CASE("catalog::type_spec::unknown_roundtrip") {
    auto t = complex_logical_type::create_unknown("myudt");
    auto spec = encode_type_spec(t);
    REQUIRE(spec == "UNKNOWN(myudt)");

    auto t2 = decode_type_spec(g_resource, spec);
    REQUIRE(t2.type() == logical_type::UNKNOWN);
    REQUIRE(t2.type_name() == "myudt");
}

TEST_CASE("catalog::type_spec::list_roundtrip") {
    auto inner = complex_logical_type{logical_type::INTEGER};
    auto t = complex_logical_type::create_list(inner);
    auto spec = encode_type_spec(t);
    REQUIRE(spec == "LIST(int4)");

    auto t2 = decode_type_spec(g_resource, spec);
    REQUIRE(t2.type() == logical_type::LIST);
    REQUIRE(t2.child_type().type() == logical_type::INTEGER);
}

TEST_CASE("catalog::type_spec::array_roundtrip") {
    auto inner = complex_logical_type{logical_type::DOUBLE};
    auto t = complex_logical_type::create_array(inner, 100);
    auto spec = encode_type_spec(t);
    REQUIRE(spec == "ARRAY(float8,100)");

    auto t2 = decode_type_spec(g_resource, spec);
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

    auto t2 = decode_type_spec(g_resource, spec);
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

    auto t2 = decode_type_spec(g_resource, spec);
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

    auto t2 = decode_type_spec(g_resource, spec);
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

    auto t2 = decode_type_spec(g_resource, spec);
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

    auto t2 = decode_type_spec(g_resource, spec);
    REQUIRE(t2.type() == logical_type::LIST);
    REQUIRE(t2.child_type().type() == logical_type::STRUCT);
    const auto& fields = t2.child_type().child_types();
    REQUIRE(fields.size() == 2);
    REQUIRE(fields[0].alias() == "lat");
}

TEST_CASE("catalog::type_spec::decimal_with_old_name_compat") {
    // Files written before the pg-style rename used "DECIMAL(w,s)".
    // decode must still accept that form.
    auto t = decode_type_spec(g_resource, "DECIMAL(18,6)");
    REQUIRE(t.type() == logical_type::DECIMAL);
    const auto* ext = static_cast<const decimal_logical_type_extension*>(t.extension());
    REQUIRE(ext->width() == 18);
    REQUIRE(ext->scale() == 6);
}

TEST_CASE("catalog::type_spec::empty_returns_unknown") {
    auto t = decode_type_spec(g_resource, "");
    REQUIRE(t.type() == logical_type::UNKNOWN);
}

TEST_CASE("catalog::type_spec::unknown_prefix_no_crash") {
    // Garbage input must not crash. The flat-text decoder may return any type for
    // accidentally-valid input — we only verify no exception is thrown.
    auto t = decode_type_spec(g_resource, "garbage_that_is_not_valid_type_spec");
    (void) t; // result type is implementation-defined for garbage input
}
