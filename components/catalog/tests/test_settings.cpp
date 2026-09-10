#include <catch2/catch_test_macros.hpp>
#include <components/catalog/settings.hpp>

#include <string>

using namespace components::catalog;

namespace {
    auto* resource = std::pmr::new_delete_resource();

    std::string canonical(setting_id id, std::string_view raw) {
        auto value = canonical_setting_value(id, raw, resource);
        REQUIRE_FALSE(value.has_error());
        return std::string(value.value().data(), value.value().size());
    }
} // namespace

TEST_CASE("catalog::settings::try_invalid_values") {
    const session_catalog_t before;
    session_catalog_t cache;

    const std::pair<setting_id, std::string_view> refusals[] = {
        {setting_id::timezone, "Not/A_Real_Zone_XYZ"},
        {setting_id::timezone, ""},
        // width must be at least 1
        {setting_id::decimal_width, "0"},
        // past DECIMAL_MAX_WIDTH
        {setting_id::decimal_width, "39"},
        // trailing garbage, not a prefix parse
        {setting_id::decimal_width, "12x"},
        {setting_id::decimal_width, "abc"},
        {setting_id::decimal_width, ""},
        {setting_id::decimal_width, "-1"},
        {setting_id::decimal_scale, "39"},
        {setting_id::decimal_scale, "two"},
        {setting_id::autocommit, "yes"},
        {setting_id::autocommit, "2"},
        {setting_id::autocommit, ""}};

    for (const auto& [id, raw] : refusals) {
        CAPTURE(find_setting_by_id(id).sql_name, raw);
        REQUIRE(set_setting(cache, id, raw, resource).contains_error());
        REQUIRE(cache.timezone_name == before.timezone_name);
        REQUIRE(cache.timezone_offset == before.timezone_offset);
        REQUIRE(cache.decimal_width == before.decimal_width);
        REQUIRE(cache.decimal_scale == before.decimal_scale);
        REQUIRE(cache.autocommit == before.autocommit);
    }
}

TEST_CASE("catalog::settings::case_insensitive_storage") {
    REQUIRE(canonical(setting_id::timezone, "Europe/London") == "europe/london");
    REQUIRE(canonical(setting_id::timezone, "EUROPE/LONDON") == "europe/london");

    for (std::string_view truthy : {"on", "ON", "true", "True", "1"}) {
        REQUIRE(canonical(setting_id::autocommit, truthy) == "on");
    }
    for (std::string_view falsy : {"off", "OFF", "false", "False", "0"}) {
        REQUIRE(canonical(setting_id::autocommit, falsy) == "off");
    }

    REQUIRE(canonical(setting_id::decimal_width, "18") == "18");
    REQUIRE(canonical(setting_id::decimal_width, "08") == "8");
}

TEST_CASE("catalog::settings::decimal_settings_joined") {
    REQUIRE_FALSE(validate_decimal_defaults(18, 3, resource).contains_error());
    REQUIRE_FALSE(validate_decimal_defaults(1, 1, resource).contains_error());
    REQUIRE(validate_decimal_defaults(3, 18, resource).contains_error());
    REQUIRE(validate_decimal_defaults(0, 0, resource).contains_error());
}
