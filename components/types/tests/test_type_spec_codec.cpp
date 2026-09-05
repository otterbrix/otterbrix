#include <catch2/catch_test_macros.hpp>

#include <components/types/logical_value.hpp>
#include <components/types/type_spec_codec.hpp>
#include <components/types/types.hpp>
#include <core/pmr.hpp>

#include <cstddef>
#include <memory_resource>
#include <vector>

using namespace components::types;

namespace {

    // Encode -> decode -> compare. Checks both operator== and (for the interesting types)
    // targeted accessors, because operator== treats a GENERIC-extension type and an
    // extension-less type as equal — the accessors prove the extension really came back.
    complex_logical_type roundtrip(std::pmr::memory_resource* resource, const complex_logical_type& type) {
        std::pmr::vector<std::byte> spec(resource);
        auto encoded = encode_type_spec(type, spec);
        REQUIRE_FALSE(encoded.has_error());
        REQUIRE_FALSE(spec.empty());
        auto decoded = decode_type_spec(resource, spec.data(), spec.size());
        REQUIRE_FALSE(decoded.has_error());
        REQUIRE(decoded.value() == type);
        return std::move(decoded.value());
    }

    void expect_decode_corruption(std::pmr::memory_resource* resource, const std::pmr::vector<std::byte>& spec) {
        auto decoded = decode_type_spec(resource, spec.data(), spec.size());
        REQUIRE(decoded.has_error());
        REQUIRE(decoded.error().type == core::error_code_t::data_corruption);
    }

} // namespace

TEST_CASE("types::type_spec_codec::every_plain_scalar_roundtrips") {
    auto resource = core::pmr::otterbrix_resource();

    const logical_type scalars[] = {
        logical_type::NA,        logical_type::ANY,          logical_type::BOOLEAN,
        logical_type::TINYINT,   logical_type::SMALLINT,     logical_type::INTEGER,
        logical_type::BIGINT,    logical_type::HUGEINT,      logical_type::DATE,
        logical_type::TIME,      logical_type::TIME_TZ,      logical_type::TIMESTAMP,
        logical_type::TIMESTAMP_TZ, logical_type::INTERVAL,  logical_type::FLOAT,
        logical_type::DOUBLE,    logical_type::BLOB,         logical_type::UTINYINT,
        logical_type::USMALLINT, logical_type::UINTEGER,     logical_type::UBIGINT,
        logical_type::UHUGEINT,  logical_type::BIT,          logical_type::STRING_LITERAL,
        logical_type::INTEGER_LITERAL, logical_type::POINTER, logical_type::VALIDITY,
        logical_type::UUID,
    };
    for (auto t : scalars) {
        INFO("scalar logical_type " << static_cast<int>(t));
        auto back = roundtrip(&resource, complex_logical_type{t});
        REQUIRE(back.type() == t);
    }
}

TEST_CASE("types::type_spec_codec::decimal_roundtrips_width_scale_and_storage") {
    auto resource = core::pmr::otterbrix_resource();

    struct case_t {
        uint8_t width;
        uint8_t scale;
        physical_type stored_as;
    };
    // One case per DECIMAL storage class — width drives the physical type, which is
    // exactly the information the one-byte format destroyed.
    const case_t cases[] = {
        {4, 2, physical_type::INT16},
        {9, 3, physical_type::INT32},
        {10, 2, physical_type::INT64},
        {18, 18, physical_type::INT64},
        {38, 10, physical_type::INT128},
    };
    for (const auto& c : cases) {
        INFO("DECIMAL(" << int(c.width) << "," << int(c.scale) << ")");
        auto back = roundtrip(&resource, complex_logical_type::create_decimal(c.width, c.scale));
        REQUIRE(back.type() == logical_type::DECIMAL);
        const auto* ext = back.extension_as<decimal_logical_type_extension>();
        REQUIRE(ext != nullptr);
        REQUIRE(ext->width() == c.width);
        REQUIRE(ext->scale() == c.scale);
        REQUIRE(ext->stored_as() == c.stored_as);
        REQUIRE(back.to_physical_type() == c.stored_as);
        // The decimal payload byte-size must be safe to query again — this is the exact
        // call that SIGABRTed on reload when the extension was lost.
        REQUIRE(back.size() > 0);
    }
}

TEST_CASE("types::type_spec_codec::list_roundtrips_child_type") {
    auto resource = core::pmr::otterbrix_resource();

    {
        auto back = roundtrip(&resource, complex_logical_type::create_list(logical_type::BIGINT));
        REQUIRE(back.type() == logical_type::LIST);
        REQUIRE(back.child_type().type() == logical_type::BIGINT);
    }
    {
        // Nested list + a decimal element: both layers of extension must survive.
        auto inner = complex_logical_type::create_list(complex_logical_type::create_decimal(12, 3));
        auto back = roundtrip(&resource, complex_logical_type::create_list(inner));
        REQUIRE(back.child_type().type() == logical_type::LIST);
        const auto* dec = back.child_type().child_type().extension_as<decimal_logical_type_extension>();
        REQUIRE(dec != nullptr);
        REQUIRE(dec->width() == 12);
        REQUIRE(dec->scale() == 3);
    }
    {
        // Non-default list metadata (field_id / required) round-trips too.
        auto typed = complex_logical_type(
            logical_type::LIST,
            std::make_unique<list_logical_type_extension>(uint64_t{7}, complex_logical_type{logical_type::DOUBLE}, false));
        auto back = roundtrip(&resource, typed);
        const auto* ext = back.extension_as<list_logical_type_extension>();
        REQUIRE(ext != nullptr);
        REQUIRE(ext->field_id() == 7);
        REQUIRE_FALSE(ext->required());
    }
}

TEST_CASE("types::type_spec_codec::array_roundtrips_size_and_child") {
    auto resource = core::pmr::otterbrix_resource();

    auto back = roundtrip(&resource, complex_logical_type::create_array(logical_type::DOUBLE, 4));
    REQUIRE(back.type() == logical_type::ARRAY);
    const auto* ext = back.extension_as<array_logical_type_extension>();
    REQUIRE(ext != nullptr);
    REQUIRE(ext->size() == 4);
    REQUIRE(ext->internal_type().type() == logical_type::DOUBLE);

    // Array of lists of strings — composite child.
    auto nested = complex_logical_type::create_array(complex_logical_type::create_list(logical_type::STRING_LITERAL), 7);
    auto nested_back = roundtrip(&resource, nested);
    const auto* nested_ext = nested_back.extension_as<array_logical_type_extension>();
    REQUIRE(nested_ext->size() == 7);
    REQUIRE(nested_ext->internal_type().type() == logical_type::LIST);
    REQUIRE(nested_ext->internal_type().child_type().type() == logical_type::STRING_LITERAL);
}

TEST_CASE("types::type_spec_codec::struct_roundtrips_fields_and_names") {
    auto resource = core::pmr::otterbrix_resource();

    std::pmr::vector<complex_logical_type> fields(&resource);
    fields.emplace_back(logical_type::INTEGER, "a");
    fields.emplace_back(logical_type::STRING_LITERAL, "b");
    fields.push_back(complex_logical_type::create_decimal(9, 3, "c"));
    auto strct = complex_logical_type::create_struct("point", fields);

    auto back = roundtrip(&resource, strct);
    REQUIRE(back.type() == logical_type::STRUCT);
    const auto* ext = back.extension_as<struct_logical_type_extension>();
    REQUIRE(ext != nullptr);
    REQUIRE(ext->type_name() == "point");
    REQUIRE(ext->child_types().size() == 3);
    REQUIRE(back.child_name(0) == "a");
    REQUIRE(back.child_name(1) == "b");
    REQUIRE(back.child_name(2) == "c");
    const auto* dec = ext->child_types()[2].extension_as<decimal_logical_type_extension>();
    REQUIRE(dec != nullptr);
    REQUIRE(dec->width() == 9);

    // Struct nested inside a struct, holding a list.
    std::pmr::vector<complex_logical_type> outer_fields(&resource);
    outer_fields.push_back(strct);
    outer_fields.back().set_alias("inner");
    outer_fields.push_back(complex_logical_type::create_list(logical_type::BIGINT, "ids"));
    auto outer = complex_logical_type::create_struct("outer", outer_fields);
    auto outer_back = roundtrip(&resource, outer);
    REQUIRE(outer_back.child_types()[0].type() == logical_type::STRUCT);
    REQUIRE(outer_back.child_types()[1].child_type().type() == logical_type::BIGINT);
}

TEST_CASE("types::type_spec_codec::map_roundtrips_key_and_value") {
    auto resource = core::pmr::otterbrix_resource();

    auto map = complex_logical_type::create_map(&resource,
                                                complex_logical_type{logical_type::STRING_LITERAL},
                                                complex_logical_type{logical_type::BIGINT});
    auto back = roundtrip(&resource, map);
    const auto* ext = back.extension_as<map_logical_type_extension>();
    REQUIRE(ext != nullptr);
    REQUIRE(ext->key().type() == logical_type::STRING_LITERAL);
    REQUIRE(ext->value().type() == logical_type::BIGINT);

    // Composite value + non-default ids via the explicit extension constructor.
    auto typed = complex_logical_type(
        logical_type::MAP,
        std::make_unique<map_logical_type_extension>(&resource,
                                                     uint64_t{3},
                                                     complex_logical_type{logical_type::INTEGER},
                                                     uint64_t{4},
                                                     complex_logical_type::create_list(logical_type::DOUBLE),
                                                     false));
    auto typed_back = roundtrip(&resource, typed);
    const auto* typed_ext = typed_back.extension_as<map_logical_type_extension>();
    REQUIRE(typed_ext->key_id() == 3);
    REQUIRE(typed_ext->value_id() == 4);
    REQUIRE_FALSE(typed_ext->value_required());
    REQUIRE(typed_ext->value().child_type().type() == logical_type::DOUBLE);
}

TEST_CASE("types::type_spec_codec::union_roundtrips_members_and_hidden_tag") {
    auto resource = core::pmr::otterbrix_resource();

    std::pmr::vector<complex_logical_type> members(&resource);
    members.emplace_back(logical_type::INTEGER, "num");
    members.emplace_back(logical_type::STRING_LITERAL, "text");
    auto uni = complex_logical_type::create_union(std::move(members));

    auto back = roundtrip(&resource, uni);
    REQUIRE(back.type() == logical_type::UNION);
    // create_union() must have re-prepended the hidden UTINYINT tag on decode.
    REQUIRE(back.child_types().size() == 3);
    REQUIRE(back.child_types()[0].type() == logical_type::UTINYINT);
    REQUIRE(back.child_types()[1].alias() == "num");
    REQUIRE(back.child_types()[2].alias() == "text");
}

TEST_CASE("types::type_spec_codec::enum_roundtrips_labels_and_values") {
    auto resource = core::pmr::otterbrix_resource();

    std::vector<logical_value_t> entries;
    {
        logical_value_t happy(&resource, int32_t{0});
        happy.set_alias("happy");
        entries.push_back(std::move(happy));
        logical_value_t sad(&resource, int32_t{7});
        sad.set_alias("sad");
        entries.push_back(std::move(sad));
    }
    auto enm = complex_logical_type::create_enum("mood", std::move(entries));

    auto back = roundtrip(&resource, enm);
    REQUIRE(back.type() == logical_type::ENUM);
    const auto* ext = back.extension_as<enum_logical_type_extension>();
    REQUIRE(ext != nullptr);
    REQUIRE(ext->type_name() == "mood");
    REQUIRE(ext->entries().size() == 2);
    REQUIRE(ext->entries()[0].type().alias() == "happy");
    REQUIRE(ext->entries()[0].value<int32_t>() == 0);
    REQUIRE(ext->entries()[1].type().alias() == "sad");
    REQUIRE(ext->entries()[1].value<int32_t>() == 7);
}

TEST_CASE("types::type_spec_codec::variant_and_unknown_roundtrip") {
    auto resource = core::pmr::otterbrix_resource();

    {
        auto back = roundtrip(&resource, complex_logical_type::create_variant(&resource));
        REQUIRE(back.type() == logical_type::VARIANT);
        REQUIRE(back.to_physical_type() == physical_type::STRUCT);
    }
    {
        auto back = roundtrip(&resource, complex_logical_type::create_unknown("mystery"));
        REQUIRE(back.type() == logical_type::UNKNOWN);
        REQUIRE(back.type_name() == "mystery");
    }
    {
        // Bare UNKNOWN (no extension) must round-trip as bare, not gain a name.
        auto back = roundtrip(&resource, complex_logical_type{logical_type::UNKNOWN});
        REQUIRE(back.type() == logical_type::UNKNOWN);
        REQUIRE(back.extension() == nullptr);
    }
}

TEST_CASE("types::type_spec_codec::aliases_roundtrip_at_every_level") {
    auto resource = core::pmr::otterbrix_resource();

    {
        auto back = roundtrip(&resource, complex_logical_type(logical_type::BIGINT, "my_col"));
        REQUIRE(back.has_alias());
        REQUIRE(back.alias() == "my_col");
    }
    {
        auto back = roundtrip(&resource, complex_logical_type::create_decimal(10, 2, "price"));
        REQUIRE(back.alias() == "price");
        REQUIRE(back.extension_as<decimal_logical_type_extension>()->width() == 10);
    }
    {
        auto aliased_child = complex_logical_type(logical_type::INTEGER, "elem");
        auto back = roundtrip(&resource, complex_logical_type::create_list(aliased_child, "lst"));
        REQUIRE(back.alias() == "lst");
        REQUIRE(back.child_type().alias() == "elem");
    }
}

TEST_CASE("types::type_spec_codec::decode_rejects_garbage_loudly") {
    auto resource = core::pmr::otterbrix_resource();

    INFO("empty buffer");
    {
        std::pmr::vector<std::byte> spec(&resource);
        expect_decode_corruption(&resource, spec);
    }
    INFO("unknown logical_type byte");
    {
        std::pmr::vector<std::byte> spec(&resource);
        spec.push_back(std::byte{0xEE}); // no logical_type has value 0xEE
        spec.push_back(std::byte{0x00});
        expect_decode_corruption(&resource, spec);
    }
    INFO("unknown flag bits");
    {
        std::pmr::vector<std::byte> spec(&resource);
        spec.push_back(std::byte{static_cast<uint8_t>(logical_type::BIGINT)});
        spec.push_back(std::byte{0x80});
        expect_decode_corruption(&resource, spec);
    }
    INFO("truncated DECIMAL payload");
    {
        std::pmr::vector<std::byte> spec(&resource);
        spec.push_back(std::byte{static_cast<uint8_t>(logical_type::DECIMAL)});
        spec.push_back(std::byte{0x00});
        spec.push_back(std::byte{10}); // width present, scale missing
        expect_decode_corruption(&resource, spec);
    }
    INFO("DECIMAL width out of range must NOT reach create_decimal");
    {
        std::pmr::vector<std::byte> spec(&resource);
        spec.push_back(std::byte{static_cast<uint8_t>(logical_type::DECIMAL)});
        spec.push_back(std::byte{0x00});
        spec.push_back(std::byte{200}); // width 200 > 38
        spec.push_back(std::byte{2});
        expect_decode_corruption(&resource, spec);
    }
    INFO("DECIMAL scale > width");
    {
        std::pmr::vector<std::byte> spec(&resource);
        spec.push_back(std::byte{static_cast<uint8_t>(logical_type::DECIMAL)});
        spec.push_back(std::byte{0x00});
        spec.push_back(std::byte{5});
        spec.push_back(std::byte{9});
        expect_decode_corruption(&resource, spec);
    }
    INFO("trailing bytes after a valid spec");
    {
        std::pmr::vector<std::byte> spec(&resource);
        auto encoded = encode_type_spec(complex_logical_type{logical_type::BIGINT}, spec);
        REQUIRE_FALSE(encoded.has_error());
        spec.push_back(std::byte{0x00});
        expect_decode_corruption(&resource, spec);
    }
    INFO("truncated LIST child");
    {
        std::pmr::vector<std::byte> spec(&resource);
        auto encoded = encode_type_spec(complex_logical_type::create_list(logical_type::BIGINT), spec);
        REQUIRE_FALSE(encoded.has_error());
        spec.pop_back(); // cut into the child record
        expect_decode_corruption(&resource, spec);
    }
    INFO("STRUCT field count larger than the buffer can hold");
    {
        std::pmr::vector<std::byte> spec(&resource);
        spec.push_back(std::byte{static_cast<uint8_t>(logical_type::STRUCT)});
        spec.push_back(std::byte{0x00});
        // type_name = "" (u32 zero), field_count = 0xFFFFFFFF
        for (int i = 0; i < 4; ++i) {
            spec.push_back(std::byte{0x00});
        }
        for (int i = 0; i < 4; ++i) {
            spec.push_back(std::byte{0xFF});
        }
        expect_decode_corruption(&resource, spec);
    }
}

TEST_CASE("types::type_spec_codec::encode_refuses_unpersistable_types") {
    auto resource = core::pmr::otterbrix_resource();

    const logical_type unpersistable[] = {
        logical_type::USER,
        logical_type::TABLE,
        logical_type::FUNCTION,
        logical_type::LAMBDA,
        logical_type::INVALID,
    };
    for (auto t : unpersistable) {
        INFO("logical_type " << static_cast<int>(t));
        std::pmr::vector<std::byte> spec(&resource);
        auto encoded = encode_type_spec(complex_logical_type{t}, spec);
        REQUIRE(encoded.has_error());
        REQUIRE(encoded.error().type == core::error_code_t::schema_error);
    }

    INFO("DECIMAL whose extension was clobbered by bare-type set_alias (the UB shape)");
    {
        // Reproduce the exact in-memory shape the old loader produced: a DECIMAL type
        // byte with a GENERIC extension. Persisting it would launder the corruption.
        complex_logical_type broken(logical_type::DECIMAL);
        broken.set_alias("d");
        std::pmr::vector<std::byte> spec(&resource);
        auto encoded = encode_type_spec(broken, spec);
        REQUIRE(encoded.has_error());
        REQUIRE(encoded.error().type == core::error_code_t::schema_error);
    }
}
