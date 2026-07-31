#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <components/vector/vector.hpp>
#include <components/vector/vector_operations.hpp>

using namespace components;
using types::logical_type;

// components/vector has no exceptions: every path that used to `throw` now answers a
// core::error_t. These cases pin the two halves of that contract for set_value —
// the errors it must raise, and the (unchanged) success paths it must keep silent on.

TEST_CASE("vector_t::set_value reports a type mismatch instead of dropping the cell") {
    auto resource = core::pmr::otterbrix_resource();

    // A BIGINT column handed an INTEGER value. Previously: assert(false) in a debug build,
    // and in release a bare `return` that dropped the cell — the row stayed at whatever the
    // buffer already held while validity claimed it was written.
    vector::vector_t v(&resource, types::complex_logical_type(logical_type::BIGINT), 4);
    auto wrong = types::logical_value_t(&resource, int32_t(42));

    auto err = v.set_value(0, wrong);
    REQUIRE(err.contains_error());
    REQUIRE(err.type == core::error_code_t::conversion_failure);

    // The matching type still stores, and reports no error.
    auto ok = v.set_value(0, types::logical_value_t(&resource, int64_t(42)));
    REQUIRE_FALSE(ok.contains_error());
    REQUIRE(v.get_value<int64_t>(0) == 42);
}

TEST_CASE("vector_t::set_value accepts a NULL of any type") {
    auto resource = core::pmr::otterbrix_resource();

    // A NULL carries no payload to mistype, so the type check must not fire on it:
    // set_null-style writes come through here with an NA-typed value.
    vector::vector_t v(&resource, types::complex_logical_type(logical_type::BIGINT), 4);
    auto null_value = types::logical_value_t(&resource, types::complex_logical_type(logical_type::NA));

    auto err = v.set_value(1, null_value);
    REQUIRE_FALSE(err.contains_error());
    REQUIRE(v.is_null(1));
}

TEST_CASE("vector_t::set_value bounds-checks the ARRAY arm against the value's children") {
    auto resource = core::pmr::otterbrix_resource();

    auto element = types::complex_logical_type(logical_type::INTEGER);
    auto array_type = types::complex_logical_type::create_array(element, 3);
    vector::vector_t v(&resource, array_type, 2);

    SECTION("a 2-element array value for an INT[3] column") {
        // The array width is part of the ARRAY logical_type, and create_array derives the
        // width from the element count — so this value is typed INT[2] and the type check
        // at the top of set_value answers first. Before the error channel existed that
        // check was assert(false) + a bare `return`: in a release build the cell was
        // silently dropped here.
        std::vector<types::logical_value_t> two;
        two.emplace_back(&resource, int32_t(10));
        two.emplace_back(&resource, int32_t(20));
        auto short_value = types::logical_value_t::create_array(&resource, element, two);
        REQUIRE(short_value.children().size() == 2);

        auto err = v.set_value(0, short_value);
        REQUIRE(err.contains_error());
        REQUIRE(err.type == core::error_code_t::conversion_failure);
    }

    SECTION("an INT[3]-typed value that carries only 2 children") {
        // The ARRAY arm's own bound. It looped to array_size and read val_children[i]
        // without consulting val_children.size(), so a value whose declared width and
        // child count disagree produced an out-of-bounds read on the value's child vector.
        // create_struct stores the type verbatim, which is how such a value is built.
        std::vector<types::logical_value_t> two;
        two.emplace_back(&resource, int32_t(10));
        two.emplace_back(&resource, int32_t(20));
        auto short_value = types::logical_value_t::create_struct(&resource, array_type, two);
        REQUIRE(short_value.type() == array_type);
        REQUIRE(short_value.children().size() == 2);

        auto err = v.set_value(0, short_value);
        REQUIRE(err.contains_error());
        REQUIRE(err.type == core::error_code_t::invalid_parameter);
    }

    SECTION("a full-width value is unaffected") {
        std::vector<types::logical_value_t> three;
        three.emplace_back(&resource, int32_t(10));
        three.emplace_back(&resource, int32_t(20));
        three.emplace_back(&resource, int32_t(30));
        auto full_value = types::logical_value_t::create_array(&resource, element, three);
        auto ok = v.set_value(1, full_value);
        REQUIRE_FALSE(ok.contains_error());
        REQUIRE(v.entry().get_value<int32_t>(3) == 10);
        REQUIRE(v.entry().get_value<int32_t>(4) == 20);
        REQUIRE(v.entry().get_value<int32_t>(5) == 30);
    }
}

TEST_CASE("vector_t::set_value bounds-checks the STRUCT arm against the value's children") {
    auto resource = core::pmr::otterbrix_resource();

    std::pmr::vector<types::complex_logical_type> fields(&resource);
    fields.emplace_back(logical_type::INTEGER, "a");
    fields.emplace_back(logical_type::BIGINT, "b");
    auto struct_type = types::complex_logical_type::create_struct("s", fields, "pair");
    vector::vector_t v(&resource, struct_type, 2);

    // One field for a two-field struct: the arm used to assert on the size and then index
    // val_children[1] out of bounds in a release build.
    std::vector<types::logical_value_t> one;
    one.emplace_back(&resource, int32_t(1));
    auto short_value = types::logical_value_t::create_struct(&resource, struct_type, one);

    auto err = v.set_value(0, short_value);
    REQUIRE(err.contains_error());
    REQUIRE(err.type == core::error_code_t::invalid_parameter);
}

TEST_CASE("vector_t::set_value round-trips a MAP built by logical_value_t::create_map") {
    auto resource = core::pmr::otterbrix_resource();

    // A MAP is physically a LIST whose element is struct<key,value> — that is what the map
    // vector's child vector is typed as, what vector_t::value() hands back, and what the Arrow
    // appender reads. A MAP logical_value must carry the same shape: one child per entry.
    types::complex_logical_type key_type{logical_type::BIGINT};
    types::complex_logical_type value_type{logical_type::BIGINT};
    auto map_type = types::complex_logical_type::create_map(&resource, key_type, value_type);
    vector::vector_t v(&resource, map_type, 2);

    std::vector<types::logical_value_t> keys;
    keys.emplace_back(&resource, int64_t(10));
    keys.emplace_back(&resource, int64_t(20));
    std::vector<types::logical_value_t> values;
    values.emplace_back(&resource, int64_t(100));
    values.emplace_back(&resource, int64_t(200));

    auto map_value = types::logical_value_t::create_map(&resource, key_type, value_type, keys, values);
    REQUIRE(map_value.type() == map_type);
    // Two entries, not two columns: create_map used to build [keys array, values array], a shape
    // the LIST arm of set_value cannot store into a struct<key,value> child.
    CHECK(map_value.children().size() == 2);
    CHECK(map_value.children()[0].type() == map_type.child_type());

    REQUIRE_FALSE(v.set_value(0, map_value).contains_error());

    // The cell actually landed: read the entries struct out of the physical layout.
    auto& entries = v.entry();
    REQUIRE(entries.entries()[0]->get_value<int64_t>(0) == 10);
    REQUIRE(entries.entries()[1]->get_value<int64_t>(0) == 100);
    REQUIRE(entries.entries()[0]->get_value<int64_t>(1) == 20);
    REQUIRE(entries.entries()[1]->get_value<int64_t>(1) == 200);

    // ... and reading the row back reproduces the value that was written.
    REQUIRE(v.value(0) == map_value);
}

TEST_CASE("vector_t::set_value reports a MAP entry that is not a struct<key,value>") {
    auto resource = core::pmr::otterbrix_resource();

    // The silent-loss path that made the create_map mismatch invisible: the LIST arm pushed each
    // child into the element vector through list_vector_buffer_t::push_back, which discarded the
    // core::error_t that set_value returned, then recorded a list_entry_t claiming those elements
    // had been written. A mistyped entry must now reach the caller.
    types::complex_logical_type key_type{logical_type::BIGINT};
    types::complex_logical_type value_type{logical_type::BIGINT};
    auto map_type = types::complex_logical_type::create_map(&resource, key_type, value_type);
    vector::vector_t v(&resource, map_type, 2);

    std::vector<types::logical_value_t> bogus;
    bogus.emplace_back(&resource, int64_t(1));
    auto wrong_shape = types::logical_value_t::create_struct(&resource, map_type, bogus);

    auto err = v.set_value(0, wrong_shape);
    REQUIRE(err.contains_error());
    REQUIRE(err.type == core::error_code_t::conversion_failure);
}

TEST_CASE("vector_t::set_value: unchanged success paths stay error-free") {
    auto resource = core::pmr::otterbrix_resource();

    // Characterization of the paths the error channel must NOT disturb. Each of these
    // stored a value before the de-exception change and still has to, reporting no error.
    SECTION("flat numeric") {
        vector::vector_t v(&resource, types::complex_logical_type(logical_type::DOUBLE), 4);
        REQUIRE_FALSE(v.set_value(2, types::logical_value_t(&resource, 2.5)).contains_error());
        REQUIRE(v.get_value<double>(2) == Catch::Approx(2.5));
    }

    SECTION("string") {
        vector::vector_t v(&resource, types::complex_logical_type(logical_type::STRING_LITERAL), 4);
        REQUIRE_FALSE(v.set_value(0, types::logical_value_t(&resource, std::string{"otterbrix"})).contains_error());
        REQUIRE(std::string{v.get_value<std::string_view>(0)} == "otterbrix");
    }

    SECTION("struct") {
        std::pmr::vector<types::complex_logical_type> fields(&resource);
        fields.emplace_back(logical_type::INTEGER, "a");
        fields.emplace_back(logical_type::BIGINT, "b");
        auto struct_type = types::complex_logical_type::create_struct("s", fields, "pair");
        vector::vector_t v(&resource, struct_type, 2);

        std::vector<types::logical_value_t> both;
        both.emplace_back(&resource, int32_t(7));
        both.emplace_back(&resource, int64_t(9));
        auto value = types::logical_value_t::create_struct(&resource, struct_type, both);
        REQUIRE_FALSE(v.set_value(0, value).contains_error());
        REQUIRE(v.entries()[0]->get_value<int32_t>(0) == 7);
        REQUIRE(v.entries()[1]->get_value<int64_t>(0) == 9);
    }

    SECTION("list") {
        auto element = types::complex_logical_type(logical_type::INTEGER);
        auto list_type = types::complex_logical_type::create_list(element);
        vector::vector_t v(&resource, list_type, 2);

        std::vector<types::logical_value_t> elements;
        elements.emplace_back(&resource, int32_t(3));
        elements.emplace_back(&resource, int32_t(4));
        auto value = types::logical_value_t::create_list(&resource, element, elements);
        REQUIRE_FALSE(v.set_value(0, value).contains_error());
        REQUIRE(v.entry().get_value<int32_t>(0) == 3);
        REQUIRE(v.entry().get_value<int32_t>(1) == 4);
    }

    SECTION("null into a nested type") {
        auto element = types::complex_logical_type(logical_type::INTEGER);
        auto array_type = types::complex_logical_type::create_array(element, 2);
        vector::vector_t v(&resource, array_type, 2);
        auto null_value = types::logical_value_t(&resource, types::complex_logical_type(logical_type::NA));
        REQUIRE_FALSE(v.set_value(0, null_value).contains_error());
        REQUIRE(v.is_null(0));
    }
}
