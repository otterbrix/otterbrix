#include <catch2/catch_test_macros.hpp>
#include <components/base/collection_full_name.hpp>
#include <components/catalog/table_id.hpp>
#include <core/pmr.hpp>

#include <string_view>

using namespace components::catalog;

TEST_CASE("catalog::identity::qualified_name_equality") {
    const qualified_name_t a(core::uid_t{"u"}, core::dbname_t{"db"}, core::schema_t{"s"}, core::relname_t{"t"});
    const qualified_name_t b(core::uid_t{"u"}, core::dbname_t{"db"}, core::schema_t{"s"}, core::relname_t{"t"});
    const qualified_name_t c(core::uid_t{"u"}, core::dbname_t{"db"}, core::schema_t{"s"}, core::relname_t{"other"});
    REQUIRE(a == b);
    REQUIRE_FALSE(a == c);
}

// table_id namespace layout: the database is the FIRST namespace part
// whenever one exists — consumers (check_namespace_exists,
// check_collection_exists, the executor's type-search-path builder) read
// the database through database().
TEST_CASE("catalog::identity::table_id_two_part_database_first") {
    core::pmr::otterbrix_resource resource;
    const table_id tid(&resource, qualified_name_t(core::dbname_t{"db"}, core::relname_t{"tbl"}));
    REQUIRE(tid.get_namespace().size() == 1);
    REQUIRE(tid.database() == "db");
    REQUIRE(std::string_view(tid.table_name()) == "tbl");
}

TEST_CASE("catalog::identity::table_id_three_part_database_first") {
    core::pmr::otterbrix_resource resource;
    const table_id tid(&resource,
                       qualified_name_t(core::dbname_t{"db"}, core::schema_t{"sch"}, core::relname_t{"tbl"}));
    REQUIRE(tid.database() == "db");
    REQUIRE(std::string_view(tid.table_name()) == "tbl");
}

TEST_CASE("catalog::identity::table_id_four_part_database_first") {
    core::pmr::otterbrix_resource resource;
    const table_id tid(
        &resource,
        qualified_name_t(core::uid_t{"9f8e-uid"}, core::dbname_t{"db"}, core::schema_t{"sch"}, core::relname_t{"tbl"}));
    REQUIRE(tid.database() == "db");
    REQUIRE(std::string_view(tid.table_name()) == "tbl");
}

TEST_CASE("catalog::identity::table_id_no_empty_namespace_parts") {
    core::pmr::otterbrix_resource resource;
    // uid present, schema absent: no empty placeholder part may appear.
    const table_id tid(
        &resource,
        qualified_name_t(core::uid_t{"9f8e-uid"}, core::dbname_t{"db"}, core::schema_t{}, core::relname_t{"tbl"}));
    for (const auto& part : tid.get_namespace()) {
        REQUIRE_FALSE(part.empty());
    }
    REQUIRE(tid.database() == "db");
}

TEST_CASE("catalog::identity::table_id_unqualified_has_no_database") {
    core::pmr::otterbrix_resource resource;
    const table_id tid(&resource, qualified_name_t(core::relname_t{"tbl"}));
    REQUIRE(tid.get_namespace().empty());
    REQUIRE(tid.database().empty());
}
