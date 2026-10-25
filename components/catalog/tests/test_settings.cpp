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

    const std::pair<setting_id, std::string_view> refusals[] = {{setting_id::timezone, "Not/A_Real_Zone_XYZ"},
                                                                {setting_id::timezone, ""},
                                                                {setting_id::autocommit, "yes"},
                                                                {setting_id::autocommit, "2"},
                                                                {setting_id::autocommit, ""}};

    for (const auto& [id, raw] : refusals) {
        CAPTURE(find_setting_by_id(id).sql_name, raw);
        REQUIRE(set_setting(cache, id, raw, resource).contains_error());
        REQUIRE(cache.timezone_name == before.timezone_name);
        REQUIRE(cache.timezone_offset == before.timezone_offset);
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
}

TEST_CASE("catalog::settings::the_known_settings") {
    REQUIRE(all_settings().size() == 2);
    REQUIRE(find_setting_by_sql_name("TimeZone") != nullptr);
    REQUIRE(find_setting_by_sql_name("autocommit") != nullptr);
    REQUIRE(find_setting_by_sql_name("decimal_width") == nullptr);
    REQUIRE(find_setting_by_sql_name("decimal_scale") == nullptr);
}
