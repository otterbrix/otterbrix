#include "operations_helper.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/physical_value.hpp>
#include <core/operations_helper.hpp>
#include <memory_resource>
#include <random>

using namespace components::types;

TEST_CASE("components::types::physical_value") {
    std::vector<physical_value> values;
    std::string_view str1 = "test string";
    std::string_view str2 = "bigger test string but shouldn't be; b < t";

    INFO("initialization");
    {
        values.emplace_back();
        values.emplace_back(false);
        values.emplace_back(true);
        values.emplace_back(uint8_t(53));
        values.emplace_back(uint16_t(643));
        values.emplace_back(uint32_t(3167));
        values.emplace_back(uint64_t(47853));
        values.emplace_back(int8_t(-57));
        values.emplace_back(int16_t(-731));
        values.emplace_back(int32_t(-9691));
        values.emplace_back(int64_t(-478346));
        values.emplace_back(float(-63.239f));
        values.emplace_back(double(577.3910246));
        values.emplace_back(str1);
        values.emplace_back(str2);
    }

    INFO("value getters");
    {
        REQUIRE(values[0].value<physical_type::NA>() == nullptr);
        REQUIRE(values[1].value<physical_type::BOOL>() == false);
        REQUIRE(values[2].value<physical_type::BOOL>() == true);
        REQUIRE(values[3].value<physical_type::UINT8>() == uint8_t(53));
        REQUIRE(values[4].value<physical_type::UINT16>() == uint16_t(643));
        REQUIRE(values[5].value<physical_type::UINT32>() == uint32_t(3167));
        REQUIRE(values[6].value<physical_type::UINT64>() == uint64_t(47853));
        REQUIRE(values[7].value<physical_type::INT8>() == int8_t(-57));
        REQUIRE(values[8].value<physical_type::INT16>() == int16_t(-731));
        REQUIRE(values[9].value<physical_type::INT32>() == int32_t(-9691));
        REQUIRE(values[10].value<physical_type::INT64>() == int64_t(-478346));
        REQUIRE(core::is_equals(values[11].value<physical_type::FLOAT>(), -63.239f));
        REQUIRE(core::is_equals(values[12].value<physical_type::DOUBLE>(), 577.3910246));
        REQUIRE(values[13].value<physical_type::STRING>() == str1);
        REQUIRE(values[14].value<physical_type::STRING>() == str2);
    }

    INFO("sort");
    {
        std::shuffle(values.begin(), values.end(), std::default_random_engine{0});
        std::sort(values.begin(), values.end());

        REQUIRE(values[0].type() == physical_type::BOOL);
        REQUIRE(values[1].type() == physical_type::BOOL);
        REQUIRE(values[2].type() == physical_type::INT64);
        REQUIRE(values[3].type() == physical_type::INT32);
        REQUIRE(values[4].type() == physical_type::INT16);
        REQUIRE(values[5].type() == physical_type::FLOAT);
        REQUIRE(values[6].type() == physical_type::INT8);
        REQUIRE(values[7].type() == physical_type::UINT8);
        REQUIRE(values[8].type() == physical_type::DOUBLE);
        REQUIRE(values[9].type() == physical_type::UINT16);
        REQUIRE(values[10].type() == physical_type::UINT32);
        REQUIRE(values[11].type() == physical_type::UINT64);
        REQUIRE(values[12].type() == physical_type::STRING);
        REQUIRE(values[12].value<physical_type::STRING>() == str2);
        REQUIRE(values[13].type() == physical_type::STRING);
        REQUIRE(values[13].value<physical_type::STRING>() == str1);
        REQUIRE(values[14].type() == physical_type::NA);
    }
}

TEST_CASE("components::types::operations_helper::powers_of_ten") {
    for (size_t i = 1; i < sizeof(POWERS_OF_TEN) / sizeof(int128_t); i++) {
        REQUIRE(POWERS_OF_TEN[i - 1] * 10 == POWERS_OF_TEN[i]);
    }
}

TEST_CASE("components::types::decimal") {
    auto check_conversion =
        []<typename Storage, typename Source>(Source val, uint8_t width, uint8_t scale, const std::string& result) {
            Storage decimal_value = components::types::to_decimal<Storage, Source>(val, width, scale);
            REQUIRE(decimal_to_string(decimal_value, width, scale) == result);
        };
    auto check_arithmetics =
        []<typename Storage, typename Source>(Source val, uint8_t width, uint8_t scale, const std::string& result) {
            Storage decimal_value = components::types::to_decimal<Storage, Source>(val, width, scale);
            decimal_value /= 10;
            REQUIRE(decimal_to_string(decimal_value, width, scale) == result);
        };

    SECTION("int16_t") {
        static constexpr uint8_t width = 3;
        static constexpr uint8_t scale = 1;
        // verify storage size
        REQUIRE(complex_logical_type::create_decimal(width, scale).to_physical_type() == physical_type::INT16);

        SECTION("convert") {
            // round up
            check_conversion.operator()<int16_t, double>(1.27, width, scale, "1.3");
            // round down
            check_conversion.operator()<int16_t, double>(1.21, width, scale, "1.2");
            // from int
            check_conversion.operator()<int16_t, int64_t>(1, width, scale, "1.0");
            // round up
            check_conversion.operator()<int16_t, double>(-1.27, width, scale, "-1.3");
            // round down
            check_conversion.operator()<int16_t, double>(-1.21, width, scale, "-1.2");
            // from int
            check_conversion.operator()<int16_t, int64_t>(-1, width, scale, "-1.0");
            // special_values
            check_conversion.operator()<int16_t, double>(10000000, width, scale, "Infinity");
            check_conversion.operator()<int16_t, double>(-10000000, width, scale, "-Infinity");
            check_conversion.operator()<int16_t, double>(std::numeric_limits<double>::quiet_NaN(), width, scale, "NaN");
            check_conversion.operator()<int16_t, int64_t>(10000000, width, scale, "Infinity");
            check_conversion.operator()<int16_t, int64_t>(-10000000, width, scale, "-Infinity");
        }
        SECTION("convert an divide by 10") {
            // round up
            check_arithmetics.operator()<int16_t, double>(1.27, width, scale, "0.1");
            check_arithmetics.operator()<int16_t, double>(1.21, width, scale, "0.1");
            check_arithmetics.operator()<int16_t, int64_t>(1, width, scale, "0.1");
            check_arithmetics.operator()<int16_t, double>(-1.27, width, scale, "-0.1");
            check_arithmetics.operator()<int16_t, double>(-1.21, width, scale, "-0.1");
            check_arithmetics.operator()<int16_t, int64_t>(-1, width, scale, "-0.1");
        }
    }

    SECTION("int32_t") {
        static constexpr uint8_t width = 8;
        static constexpr uint8_t scale = 2;
        // verify storage size
        REQUIRE(complex_logical_type::create_decimal(width, scale).to_physical_type() == physical_type::INT32);

        SECTION("convert") {
            // round up
            check_conversion.operator()<int32_t, double>(502.215, width, scale, "502.22");
            // round down
            check_conversion.operator()<int32_t, double>(502.214, width, scale, "502.21");
            // from int
            check_conversion.operator()<int32_t, int64_t>(502, width, scale, "502.00");
            // round up
            check_conversion.operator()<int32_t, double>(-502.215, width, scale, "-502.22");
            // round down
            check_conversion.operator()<int32_t, double>(-502.214, width, scale, "-502.21");
            // from int
            check_conversion.operator()<int32_t, int64_t>(-502, width, scale, "-502.00");
            // special_values
            check_conversion.operator()<int32_t, double>(10000000000, width, scale, "Infinity");
            check_conversion.operator()<int32_t, double>(-10000000000, width, scale, "-Infinity");
            check_conversion.operator()<int32_t, double>(std::numeric_limits<double>::quiet_NaN(), width, scale, "NaN");
            check_conversion.operator()<int32_t, int64_t>(10000000000, width, scale, "Infinity");
            check_conversion.operator()<int32_t, int64_t>(-10000000000, width, scale, "-Infinity");
        }
        SECTION("convert an divide by 10") {
            check_arithmetics.operator()<int32_t, double>(502.215, width, scale, "50.22");
            check_arithmetics.operator()<int32_t, double>(502.214, width, scale, "50.22");
            check_arithmetics.operator()<int32_t, int64_t>(502, width, scale, "50.20");
            check_arithmetics.operator()<int32_t, double>(-502.215, width, scale, "-50.22");
            check_arithmetics.operator()<int32_t, double>(-502.214, width, scale, "-50.22");
            check_arithmetics.operator()<int32_t, int64_t>(-502, width, scale, "-50.20");
        }
    }

    SECTION("int64_t") {
        static constexpr uint8_t width = 12;
        static constexpr uint8_t scale = 3;
        // verify storage size
        REQUIRE(complex_logical_type::create_decimal(width, scale).to_physical_type() == physical_type::INT64);

        SECTION("convert") {
            // round up
            check_conversion.operator()<int64_t, double>(502.2157, width, scale, "502.216");
            // round down
            check_conversion.operator()<int64_t, double>(502.2151, width, scale, "502.215");
            // from int
            check_conversion.operator()<int64_t, int64_t>(502, width, scale, "502.000");
            // round up
            check_conversion.operator()<int64_t, double>(-502.2157, width, scale, "-502.216");
            // round down
            check_conversion.operator()<int64_t, double>(-502.2151, width, scale, "-502.215");
            // from int
            check_conversion.operator()<int64_t, int64_t>(-502, width, scale, "-502.000");
            // special_values
            check_conversion.operator()<int64_t, double>(10000000000000, width, scale, "Infinity");
            check_conversion.operator()<int64_t, double>(-10000000000000, width, scale, "-Infinity");
            check_conversion.operator()<int64_t, double>(std::numeric_limits<double>::quiet_NaN(), width, scale, "NaN");
            check_conversion.operator()<int64_t, int64_t>(10000000000000, width, scale, "Infinity");
            check_conversion.operator()<int64_t, int64_t>(-10000000000000, width, scale, "-Infinity");
        }
        SECTION("convert an divide by 10") {
            check_arithmetics.operator()<int64_t, double>(502.2157, width, scale, "50.221");
            check_arithmetics.operator()<int64_t, double>(502.2151, width, scale, "50.221");
            check_arithmetics.operator()<int64_t, int64_t>(502, width, scale, "50.200");
            check_arithmetics.operator()<int64_t, double>(-502.2157, width, scale, "-50.221");
            check_arithmetics.operator()<int64_t, double>(-502.2151, width, scale, "-50.221");
            check_arithmetics.operator()<int64_t, int64_t>(-502, width, scale, "-50.200");
        }
    }

    INFO("int128_t");
    {
        static constexpr uint8_t width = 20;
        static constexpr uint8_t scale = 4;
        // verify storage size
        REQUIRE(complex_logical_type::create_decimal(width, scale).to_physical_type() == physical_type::INT128);

        SECTION("convert") {
            // round up
            check_conversion.operator()<int128_t, double>(502.21575, width, scale, "502.2158");
            // round down
            check_conversion.operator()<int128_t, double>(502.21572, width, scale, "502.2157");
            // from int
            check_conversion.operator()<int128_t, int64_t>(502, width, scale, "502.0000");
            // round up
            check_conversion.operator()<int128_t, double>(-502.21575, width, scale, "-502.2158");
            // round down
            check_conversion.operator()<int128_t, double>(-502.21572, width, scale, "-502.2157");
            // from int
            check_conversion.operator()<int128_t, int64_t>(-502, width, scale, "-502.0000");
            // special_values
            check_conversion.operator()<int128_t, double>(1e30, width, scale, "Infinity");
            check_conversion.operator()<int128_t, double>(-1e30, width, scale, "-Infinity");
            check_conversion.operator()<int128_t, double>(std::numeric_limits<double>::quiet_NaN(),
                                                          width,
                                                          scale,
                                                          "NaN");
            check_conversion.operator()<int128_t, int128_t>(absl::MakeInt128(10000000, 0), width, scale, "Infinity");
            check_conversion.operator()<int128_t, int128_t>(absl::MakeInt128(-10000000, 0), width, scale, "-Infinity");
        }
        SECTION("convert an divide by 10") {
            check_arithmetics.operator()<int128_t, double>(502.21575, width, scale, "50.2215");
            check_arithmetics.operator()<int128_t, double>(502.21572, width, scale, "50.2215");
            check_arithmetics.operator()<int128_t, int64_t>(502, width, scale, "50.2000");
            check_arithmetics.operator()<int128_t, double>(-502.21575, width, scale, "-50.2215");
            check_arithmetics.operator()<int128_t, double>(-502.21572, width, scale, "-50.2215");
            check_arithmetics.operator()<int128_t, int64_t>(-502, width, scale, "-50.2000");
        }
    }
}
TEST_CASE("components::types::logical_value::null_children_safe") {
    // Regression: children() on a NULL (NA-typed) value dereferenced the null
    // payload pointer. NULL nested values are ordinary result-set data, so
    // reading them through the children() idiom must be safe.
    auto* resource = std::pmr::get_default_resource();
    logical_value_t null_value(resource, complex_logical_type{logical_type::NA});
    REQUIRE(null_value.is_null());
    CHECK(null_value.children().empty());
    // A non-null nested value keeps returning its real elements.
    auto list =
        logical_value_t::create_list(resource,
                                     complex_logical_type{logical_type::BIGINT},
                                     {logical_value_t(resource, int64_t{1}), logical_value_t(resource, int64_t{2})});
    REQUIRE_FALSE(list.is_null());
    CHECK(list.children().size() == 2);
}

TEST_CASE("components::types::logical_value::interval_children_present") {
    // INTERVAL owns a heap vector payload (see destroy_heap), yet is_nested() reports false
    // for it. Narrowing children() to is_nested() would silently drop these elements.
    auto* resource = std::pmr::get_default_resource();
    logical_value_t interval(
        resource,
        core::date::interval_t{core::date::microseconds{5}, core::date::days{2}, core::date::months{1}});
    REQUIRE_FALSE(interval.is_null());
    CHECK(interval.children().size() == 3);
}

TEST_CASE("components::types::logical_value::map_children_present") {
    // MAP is payload-owning per destroy_heap but is likewise excluded by is_nested().
    auto* resource = std::pmr::get_default_resource();
    complex_logical_type key_type{logical_type::BIGINT};
    complex_logical_type value_type{logical_type::BIGINT};
    auto map = logical_value_t::create_map(resource,
                                           key_type,
                                           value_type,
                                           {logical_value_t(resource, int64_t{1})},
                                           {logical_value_t(resource, int64_t{2})});
    REQUIRE_FALSE(map.is_null());
    CHECK_FALSE(map.children().empty());
}

TEST_CASE("components::types::logical_value::string_children_safe") {
    // Regression: children() guards only NULL, so a STRING_LITERAL value reinterprets its
    // std::string payload as std::vector<logical_value_t> and reads the string's data/size
    // words as the vector's begin/end pointers. A string value has no children.
    // The literal is deliberately longer than the SSO buffer so the two reinterpreted words
    // are a heap pointer and a length -- never accidentally equal, which an empty or short
    // string could be, giving a false pass.
    auto* resource = std::pmr::get_default_resource();
    logical_value_t str_value(resource, std::string{"not a vector of children"});
    REQUIRE_FALSE(str_value.is_null());
    CHECK(str_value.children().empty());
}

TEST_CASE("components::types::logical_value::scalar_children_safe") {
    // Regression: for a scalar, data_ holds the payload itself, so children() reinterprets
    // the integer as a std::vector pointer and dereferences it.
    auto* resource = std::pmr::get_default_resource();
    logical_value_t big(resource, int64_t{0x7fff'ffff'ffff'ffff});
    REQUIRE_FALSE(big.is_null());
    CHECK(big.children().empty());
}

TEST_CASE("components::types::logical_value::as_rejects_type_mismatch") {
    // value<T>() reads the payload union blind -- value<int64_t>() on a STRING_LITERAL returns the
    // std::string pointer's bits as a number, silently. as<T>() is the checked accessor, for the
    // sites where T is NOT derived from the value's own to_physical_type().
    std::pmr::monotonic_buffer_resource resource;

    logical_value_t str_value(&resource, std::string{"hello"});
    auto mismatched = str_value.as<int64_t>();
    REQUIRE(mismatched.has_error());
    CHECK(mismatched.error().type == core::error_code_t::conversion_failure);

    auto matched = str_value.as<std::string_view>();
    REQUIRE_FALSE(matched.has_error());
    CHECK(matched.value() == "hello");

    // A NULL value carries no payload of any type, so every as<T>() on it is an error.
    logical_value_t null_value(&resource, complex_logical_type{logical_type::NA});
    CHECK(null_value.as<int64_t>().has_error());
}

TEST_CASE("components::types::logical_value::cast_as_null_returns_error") {
    // Regression: cast_as() on a NULL/NA-typed value used to dispatch into the scalar physical-type
    // switch whose `default:` arm threw std::logic_error. Under the executor's -fno-exceptions
    // coroutine that becomes unhandled_exception() -> assert(false) -> SIGABRT. It must instead
    // surface a conversion_failure through result_wrapper_t.
    std::pmr::monotonic_buffer_resource resource;

    logical_value_t null_value(&resource, complex_logical_type{logical_type::NA});
    REQUIRE(null_value.is_null());

    auto casted = null_value.cast_as(complex_logical_type{logical_type::BIGINT}, {});
    REQUIRE(casted.has_error());
    CHECK(casted.error().type == core::error_code_t::conversion_failure);

    // A well-typed numeric cast still succeeds and yields the converted value.
    logical_value_t int_value(&resource, int32_t{7});
    auto ok = int_value.cast_as(complex_logical_type{logical_type::BIGINT}, {});
    REQUIRE_FALSE(ok.has_error());
    CHECK(ok.value().value<int64_t>() == 7);
}

// ---------------------------------------------------------------------------
// Characterization: what complex_logical_type's single name slot currently means.
//
// logical_type_extension holds ONE name string, and four different roles are read
// out of it: a column's name, a STRUCT field's name, an ENUM entry's label, and a
// value's label. These cases pin the observable answer for each role so a change
// that re-routes any of them to separate storage is provably behaviour-preserving.
// ---------------------------------------------------------------------------

TEST_CASE("components::types::name_roles::struct_field_names") {
    std::pmr::monotonic_buffer_resource resource;

    std::pmr::vector<complex_logical_type> fields(&resource);
    fields.emplace_back(logical_type::BOOLEAN);
    fields.back().set_field_name("flag");
    fields.emplace_back(logical_type::BIGINT);
    fields.back().set_field_name("number");

    auto point = complex_logical_type::create_struct("point", fields);

    // The struct's own type name is separate storage already.
    CHECK(point.type_name() == "point");
    // Field names come out of each field type's name slot.
    CHECK(point.child_name(0) == "flag");
    CHECK(point.child_name(1) == "number");
    CHECK(point.child_types()[0].field_name() == "flag");
    CHECK(point.child_types()[1].field_name() == "number");

    // Nesting keeps field names at every level.
    std::pmr::vector<complex_logical_type> outer_fields(&resource);
    outer_fields.emplace_back(point);
    outer_fields.back().set_field_name("origin");
    auto shape = complex_logical_type::create_struct("shape", outer_fields);
    CHECK(shape.child_name(0) == "origin");
    CHECK(shape.child_types()[0].child_name(1) == "number");

    // A copy carries every field name across the extension's deep copy.
    complex_logical_type copied = shape;
    CHECK(copied.child_name(0) == "origin");
    CHECK(copied.child_types()[0].child_name(0) == "flag");
    complex_logical_type assigned{logical_type::NA};
    assigned = shape;
    CHECK(assigned.child_types()[0].child_name(1) == "number");
}

TEST_CASE("components::types::name_roles::is_unnamed_answers_about_fields_and_never_dereferences_null") {
    // M3-B5, RED before the change (this is the RED the plan lists for the stage).
    //
    // is_unnamed() read `extension_->field_name().empty()` with no guard at all, so every type
    // without an extension — which is every scalar type nobody named — dereferenced null.
    // In a Debug build that is a segfault; in Release it reads a std::string out of address
    // zero. Its one caller (struct_column_data.cpp:18) only ever passed struct types, which
    // is why it survived: latent, not unreachable.
    //
    // The question it was ASKING was wrong too, and B5 is what makes that visible. It asked
    // whether the STRUCT type itself had a name in the alias slot — but that slot held the
    // COLUMN's name (column_definition_t stamps it), so once the column name moves off the
    // type, the old reading answers "unnamed" for every struct column in the engine and the
    // caller rejects them all. The question the caller needs is the one DuckDB's
    // StructType::IsUnnamed asks: are this struct's FIELDS named? A positional ROW(1,2) has
    // no field names and cannot become a table column; STRUCT(a INT, b INT) can.
    std::pmr::monotonic_buffer_resource resource;

    // Total on every type, named or not, extension or not.
    CHECK(complex_logical_type{logical_type::BIGINT}.is_unnamed());
    CHECK(complex_logical_type{logical_type::NA}.is_unnamed());
    CHECK(complex_logical_type{logical_type::STRUCT}.is_unnamed());

    std::pmr::vector<complex_logical_type> named_fields(&resource);
    named_fields.emplace_back(logical_type::BOOLEAN);
    named_fields.back().set_field_name("flag");
    named_fields.emplace_back(logical_type::BIGINT);
    named_fields.back().set_field_name("number");
    auto named = complex_logical_type::create_struct("point", named_fields);
    CHECK_FALSE(named.is_unnamed());

    std::pmr::vector<complex_logical_type> positional_fields(&resource);
    positional_fields.emplace_back(logical_type::BOOLEAN);
    positional_fields.emplace_back(logical_type::BIGINT);
    auto positional = complex_logical_type::create_struct("row", positional_fields);
    CHECK(positional.is_unnamed());

    // And the answer does not depend on what the COLUMN is called: a named struct column and
    // an unnamed one answer alike, because the field names are what is being asked about.
    auto named_column = complex_logical_type::create_struct("point", named_fields, "the_column");
    CHECK_FALSE(named_column.is_unnamed());
    auto positional_column = complex_logical_type::create_struct("row", positional_fields, "the_column");
    CHECK(positional_column.is_unnamed());
}

TEST_CASE("components::types::name_roles::map_entry_field_names") {
    std::pmr::monotonic_buffer_resource resource;

    auto map_type = complex_logical_type::create_map(&resource,
                                                     complex_logical_type{logical_type::STRING_LITERAL},
                                                     complex_logical_type{logical_type::BIGINT});
    // A MAP is a LIST of struct<"key","value">; those two field names are stamped by
    // create_map itself, so they must survive independently of any column naming.
    const auto& entries = map_type.child_type();
    REQUIRE(entries.type() == logical_type::STRUCT);
    CHECK(entries.type_name() == "entries");
    CHECK(entries.child_name(0) == "key");
    CHECK(entries.child_name(1) == "value");
}

TEST_CASE("components::types::name_roles::enum_entry_labels") {
    std::pmr::monotonic_buffer_resource resource;

    std::vector<logical_value_t> entries;
    entries.emplace_back(&resource, 0);
    entries.back().set_label("even");
    entries.emplace_back(&resource, 1);
    entries.back().set_label("odd");
    auto oddness = complex_logical_type::create_enum("oddness_t", std::move(entries));

    REQUIRE(oddness.type() == logical_type::ENUM);
    // The ENUM's own type name is separate storage; the labels are not.
    CHECK(oddness.type_name() == "oddness_t");

    // Asserted through the label-consuming API only, never through the slot the label
    // happens to live in -- which slot that is, is what this refactor is free to move.
    const auto* ext = static_cast<const enum_logical_type_extension*>(oddness.extension());
    REQUIRE(ext != nullptr);
    REQUIRE(ext->entries().size() == 2);
    CHECK(ext->entries()[0].value<int32_t>() == 0);
    CHECK(ext->entries()[1].value<int32_t>() == 1);
    CHECK(enum_value_matches_string(logical_value_t::create_enum(&resource, oddness, int32_t{0}), "even"));
    CHECK(enum_value_matches_string(logical_value_t::create_enum(&resource, oddness, int32_t{1}), "odd"));
    CHECK_FALSE(enum_value_matches_string(logical_value_t::create_enum(&resource, oddness, int32_t{0}), "odd"));

    // Label -> value lookup.
    auto odd = logical_value_t::create_enum(&resource, oddness, std::string_view{"odd"});
    REQUIRE(odd.type().type() == logical_type::ENUM);
    CHECK(odd.value<int32_t>() == 1);
    // An unknown label yields NA rather than an arbitrary entry.
    auto missing = logical_value_t::create_enum(&resource, oddness, std::string_view{"neither"});
    CHECK(missing.type().type() == logical_type::NA);

    // Value -> label comparison.
    CHECK(enum_value_matches_string(odd, "odd"));
    CHECK_FALSE(enum_value_matches_string(odd, "even"));

    // STRING_LITERAL -> ENUM cast resolves through the labels.
    logical_value_t as_text(&resource, std::string{"even"});
    auto casted = as_text.cast_as(oddness, {});
    REQUIRE_FALSE(casted.has_error());
    CHECK(casted.value().value<int32_t>() == 0);
    CHECK(enum_value_matches_string(casted.value(), "even"));

    // Labels survive the extension's deep copy.
    complex_logical_type copied = oddness;
    const auto* copied_ext = static_cast<const enum_logical_type_extension*>(copied.extension());
    REQUIRE(copied_ext != nullptr);
    REQUIRE(copied_ext->entries().size() == 2);
    CHECK(logical_value_t::create_enum(&resource, copied, std::string_view{"even"}).value<int32_t>() == 0);
    CHECK(logical_value_t::create_enum(&resource, copied, std::string_view{"odd"}).value<int32_t>() == 1);
    CHECK(enum_value_matches_string(logical_value_t::create_enum(&resource, copied, int32_t{1}), "odd"));
}

TEST_CASE("components::types::name_roles::field_name_on_a_scalar_type") {
    std::pmr::monotonic_buffer_resource resource;

    // The slot this used to pin held a COLUMN's name. M3-B5 moved that onto the column
    // (vector_t::name()) and left the slot with the one role that is a property of a type:
    // the name it answers to as a FIELD of the struct that contains it.
    complex_logical_type field{logical_type::BIGINT};
    CHECK(field.field_name().empty());
    field.set_field_name("id");
    CHECK(field.field_name() == "id");
    // type_name() no longer falls through to it. A scalar does not name ITSELF, and the
    // fall-through made every named column look like a named type — the confusion that let a
    // gate ask whether a TYPE existed under a COLUMN's name.
    CHECK(field.type_name().empty());

    complex_logical_type copied = field;
    CHECK(copied.field_name() == "id");
    // The constructor's trailing argument writes the same slot.
    complex_logical_type direct{logical_type::BIGINT, "id"};
    CHECK(direct.field_name() == "id");

    // And a type with no field name has no extension at all — which is what makes copying
    // the overwhelmingly common shape (a plain scalar column type) free.
    complex_logical_type plain{logical_type::BIGINT};
    CHECK(plain.extension() == nullptr);
    CHECK(plain.field_name().empty());
    CHECK(plain.type_name().empty());
}

TEST_CASE("components::types::name_roles::a_field_value_s_name_becomes_the_struct_s_field_name") {
    // logical_value_t::create_struct(name, fields) turns each field VALUE's name into the
    // resulting struct type's FIELD name with no assignment in between. This chain was the
    // reason the two roles could not be separated by renaming (M3-B0 refuted itself on it);
    // it survives the split intact because both ends of it are the FIELD role.
    std::pmr::monotonic_buffer_resource resource;

    logical_value_t first(&resource, int64_t{1});
    first.set_field_name("id");
    logical_value_t second(&resource, std::string{"ok"});
    second.set_field_name("status");

    auto row = logical_value_t::create_struct(&resource, "row", {first, second});
    REQUIRE(row.type().type() == logical_type::STRUCT);
    CHECK(row.type().type_name() == "row");
    // The column names arrived as struct field names.
    CHECK(row.type().child_name(0) == "id");
    CHECK(row.type().child_name(1) == "status");
}

TEST_CASE("components::types::name_roles::equality_is_the_shape_question") {
    // M3-B5, and the end of a three-stage argument. B3 made operator== notice the name slot,
    // because the slot then held a COLUMN's name and a BIGINT column called `a` is not a
    // BIGINT column called `b`; the 32 production callers that were asking "can these carry
    // the same values" moved to a separate same_shape(). B5 took the column's name off the
    // type, so the two relations answer the same question and there is one of them again.
    complex_logical_type left{logical_type::BIGINT, "a"};
    complex_logical_type right{logical_type::BIGINT, "b"};
    CHECK(left == right);

    complex_logical_type unnamed{logical_type::BIGINT};
    CHECK(left == unnamed);
    CHECK(unnamed == right);

    // A difference that is NOT a name is still a difference.
    complex_logical_type other_type{logical_type::INTEGER, "a"};
    CHECK_FALSE(left == other_type);
    CHECK(left != other_type);

    // Equality is still an equivalence relation on the shapes.
    complex_logical_type unnamed_again{logical_type::BIGINT};
    CHECK(unnamed == unnamed_again);
}

TEST_CASE("components::types::name_roles::equality_ignores_names_at_every_depth") {
    // The boundary, pinned so it is a decision and not an accident. Neither the outermost
    // field name nor the names INSIDE a struct take part: what a struct's fields are called
    // does not change what values it can hold, which is the only question equality answers
    // now. (The struct's OWN registered name is a different slot and IS compared — see
    // enum/struct type_name below.)
    std::pmr::monotonic_buffer_resource resource;

    std::pmr::vector<complex_logical_type> left_fields{&resource};
    left_fields.emplace_back(logical_type::BIGINT, "id");
    std::pmr::vector<complex_logical_type> right_fields{&resource};
    right_fields.emplace_back(logical_type::BIGINT, "key");

    auto left = complex_logical_type::create_struct("row", left_fields, "outer_a");
    auto differing_field_names = complex_logical_type::create_struct("row", right_fields, "outer_a");
    CHECK(left == differing_field_names);

    auto same_fields_other_field_name = complex_logical_type::create_struct("row", left_fields, "outer_b");
    CHECK(left == same_fields_other_field_name);
}
