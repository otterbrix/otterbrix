#include <catch2/catch_test_macros.hpp>
#include <components/base/collection_full_name.hpp>
#include <components/catalog/table_id.hpp>
#include <core/pmr.hpp>

#include <string_view>

using namespace components::catalog;

// qualified_name_t ordering must be a strict weak ordering: asymmetric,
// transitive, with a consistent equivalence. The comparison is lexicographic
// over (unique_identifier, database, schema, collection) — the 4-part
// uid.db.schema.rel syntax order, uid outermost.
TEST_CASE("catalog::identity::qualified_name_ordering_is_asymmetric") {
    const qualified_name_t a("1", "z", "s", "t");
    const qualified_name_t b("2", "a", "s", "t");
    REQUIRE_FALSE(((a < b) && (b < a)));
}

TEST_CASE("catalog::identity::qualified_name_ordering_trichotomy") {
    const qualified_name_t names[] = {
        qualified_name_t("1", "b", "", "t"),
        qualified_name_t("2", "a", "", "t"),
        qualified_name_t("2", "c", "", "t"),
    };
    for (const auto& p : names) {
        for (const auto& q : names) {
            const bool lt = p < q;
            const bool gt = q < p;
            const bool equiv = !lt && !gt;
            const int holds = static_cast<int>(lt) + static_cast<int>(gt) + static_cast<int>(equiv);
            REQUIRE(holds == 1);
        }
    }
}

TEST_CASE("catalog::identity::qualified_name_equality_matches_ordering") {
    const qualified_name_t a("u", "db", "s", "t");
    const qualified_name_t b("u", "db", "s", "t");
    const qualified_name_t c("u", "db", "s", "other");
    REQUIRE(a == b);
    REQUIRE_FALSE(a < b);
    REQUIRE_FALSE(b < a);
    REQUIRE_FALSE(a == c);
}

// table_id namespace layout: the database is the FIRST namespace part
// whenever one exists — consumers (check_namespace_exists,
// check_collection_exists, the executor's type-search-path builder) read
// the database through database().
TEST_CASE("catalog::identity::table_id_two_part_database_first") {
    core::pmr::otterbrix_resource resource;
    const table_id tid(&resource, qualified_name_t("db", "tbl"));
    REQUIRE(tid.get_namespace().size() == 1);
    REQUIRE(tid.database() == "db");
    REQUIRE(std::string_view(tid.table_name()) == "tbl");
}

TEST_CASE("catalog::identity::table_id_three_part_database_first") {
    core::pmr::otterbrix_resource resource;
    const table_id tid(&resource, qualified_name_t("db", "sch", "tbl"));
    REQUIRE(tid.database() == "db");
    REQUIRE(std::string_view(tid.table_name()) == "tbl");
}

TEST_CASE("catalog::identity::table_id_four_part_database_first") {
    core::pmr::otterbrix_resource resource;
    const table_id tid(&resource, qualified_name_t("9f8e-uid", "db", "sch", "tbl"));
    REQUIRE(tid.database() == "db");
    REQUIRE(std::string_view(tid.table_name()) == "tbl");
}

TEST_CASE("catalog::identity::table_id_no_empty_namespace_parts") {
    core::pmr::otterbrix_resource resource;
    // uid present, schema absent: no empty placeholder part may appear.
    const table_id tid(&resource, qualified_name_t("9f8e-uid", "db", "", "tbl"));
    for (const auto& part : tid.get_namespace()) {
        REQUIRE_FALSE(part.empty());
    }
    REQUIRE(tid.database() == "db");
}

TEST_CASE("catalog::identity::table_id_unqualified_has_no_database") {
    core::pmr::otterbrix_resource resource;
    const table_id tid(&resource, qualified_name_t("", "tbl"));
    REQUIRE(tid.get_namespace().empty());
    REQUIRE(tid.database().empty());
}
