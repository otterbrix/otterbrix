#include <catch2/catch_test_macros.hpp>
#include <components/logical_plan/node_set_setting.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/test/parse_ok.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

#include <string>

using namespace components::sql;
using namespace components::logical_plan;
using namespace components::sql::transform;

namespace {
    struct transformed_t {
        bool refused;
        std::string text;
    };

    transformed_t transform_one(const char* query) {
        auto resource = core::pmr::otterbrix_resource();
        std::pmr::monotonic_buffer_resource arena_resource(&resource);
        auto statement = linitial(parse_ok(&arena_resource, query));
        transform::transformer local(&resource, query);
        auto result = local.transform(pg_cell_to_node_cast(statement)).finalize();
        if (result.has_error()) {
            return {true, std::string(result.error().what)};
        }
        auto node = result.value().sub_queries.back();
        REQUIRE(node->type() == node_type::set_setting_t);
        return {false, node->to_string()};
    }

    std::string set_text(const char* query) {
        const auto transformed = transform_one(query);
        INFO(query << " -> " << transformed.text);
        REQUIRE_FALSE(transformed.refused);
        return transformed.text;
    }

    std::string refusal(const char* query) {
        const auto transformed = transform_one(query);
        INFO(query << " -> " << transformed.text);
        REQUIRE(transformed.refused);
        return transformed.text;
    }
} // namespace

TEST_CASE("components::sql::set_setting::scopes") {
    REQUIRE(set_text("SET TIME ZONE '+03:00';") == "$set_setting: timezone = +03:00");
    REQUIRE(set_text("SET SESSION timezone = 'UTC';") == "$set_setting: timezone = UTC");
    REQUIRE(set_text("SET LOCAL TIME ZONE '+03:00';") == "$set_setting local: timezone = +03:00");
    REQUIRE(set_text("SET LOCAL autocommit = 'off';") == "$set_setting local: autocommit = off");
    REQUIRE(set_text("ALTER SYSTEM SET timezone = '+03:00';") == "$set_setting system: timezone = +03:00");
    REQUIRE(set_text("ALTER SYSTEM SET autocommit TO 'off';") == "$set_setting system: autocommit = off");
}

TEST_CASE("components::sql::set_setting::reset_and_set_to_default") {
    REQUIRE(set_text("RESET timezone;") == "$set_setting: timezone = DEFAULT");
    REQUIRE(set_text("RESET TIME ZONE;") == "$set_setting: timezone = DEFAULT");
    REQUIRE(set_text("SET timezone TO DEFAULT;") == "$set_setting: timezone = DEFAULT");
    REQUIRE(set_text("SET LOCAL timezone TO DEFAULT;") == "$set_setting local: timezone = DEFAULT");
    REQUIRE(set_text("RESET ALL;") == "$set_setting: timezone = DEFAULT, autocommit = DEFAULT, search_path = DEFAULT");
    REQUIRE(set_text("ALTER SYSTEM RESET timezone;") == "$set_setting system: timezone = DEFAULT");
    REQUIRE(set_text("ALTER SYSTEM SET timezone TO DEFAULT;") == "$set_setting system: timezone = DEFAULT");
    REQUIRE(set_text("ALTER SYSTEM RESET ALL;") ==
            "$set_setting system: timezone = DEFAULT, autocommit = DEFAULT, search_path = DEFAULT");
}

TEST_CASE("components::sql::set_setting::refusals") {
    REQUIRE(refusal("SET nosuch = 'x';") == "SET nosuch is not supported");
    REQUIRE(refusal("RESET nosuch;") == "RESET nosuch is not supported");
    REQUIRE(refusal("ALTER SYSTEM SET nosuch = 'x';") == "ALTER SYSTEM SET nosuch is not supported");
    REQUIRE(refusal("ALTER SYSTEM RESET nosuch;") == "ALTER SYSTEM RESET nosuch is not supported");
    REQUIRE(refusal("SET decimal_width = '10';") == "SET decimal_width is not supported");
    REQUIRE(refusal("SET decimal_scale = '2';") == "SET decimal_scale is not supported");
    REQUIRE(refusal("ALTER SYSTEM SET decimal_scale = '2';") == "ALTER SYSTEM SET decimal_scale is not supported");
}
