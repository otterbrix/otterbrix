#include <catch2/catch.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/system_table_schemas.hpp>

#include <unordered_set>

using namespace components::catalog;

// 1. The catalog has exactly 13 system tables (10 original + pg_sequence + pg_rewrite + pg_settings).
TEST_CASE("catalog::system_schemas::tables_count_10") {
    auto tables = all_system_tables();
    REQUIRE(tables.size() == 13);
}

// 2. Every system table has a unique relation_oid drawn from the well-known range.
TEST_CASE("catalog::system_schemas::distinct_well_known_oids") {
    std::unordered_set<oid_t> seen;
    for (const auto& def : all_system_tables()) {
        REQUIRE(def.relation_oid >= well_known_oid::pg_namespace_table);
        REQUIRE(def.relation_oid <= well_known_oid::pg_settings_table);
        REQUIRE(seen.insert(def.relation_oid).second);
        REQUIRE(def.namespace_oid == well_known_oid::pg_catalog_namespace);
        REQUIRE(def.relkind == 'r');
    }
}

// 3. find_system_table — basic name lookup including non-existent.
TEST_CASE("catalog::system_schemas::find_system_table") {
    REQUIRE(find_system_table("pg_class") != nullptr);
    REQUIRE(find_system_table("pg_class")->relation_oid == well_known_oid::pg_class_table);
    REQUIRE(find_system_table("pg_attribute") != nullptr);
    REQUIRE(find_system_table("nonexistent") == nullptr);
    REQUIRE(find_system_table("") == nullptr);
}

// 4. pg_class describes itself — has relname / relnamespace / relkind columns.
TEST_CASE("catalog::system_schemas::pg_class_self_describable") {
    const auto* def = find_system_table("pg_class");
    REQUIRE(def != nullptr);
    std::unordered_set<std::string> col_names;
    for (const auto& c : def->columns) {
        col_names.insert(c.name());
    }
    REQUIRE(col_names.count("oid") == 1);
    REQUIRE(col_names.count("relname") == 1);
    REQUIRE(col_names.count("relnamespace") == 1);
    REQUIRE(col_names.count("relkind") == 1);
}

// 5. pg_attribute has the columns required for column lifecycle.
TEST_CASE("catalog::system_schemas::pg_attribute_supports_attnum_lifecycle") {
    const auto* def = find_system_table("pg_attribute");
    REQUIRE(def != nullptr);
    std::unordered_set<std::string> col_names;
    for (const auto& c : def->columns) {
        col_names.insert(c.name());
    }
    // Required for DDL (attnum never reused; tombstone on drop)
    REQUIRE(col_names.count("attoid") == 1);
    REQUIRE(col_names.count("attrelid") == 1);
    REQUIRE(col_names.count("attname") == 1);
    REQUIRE(col_names.count("attnum") == 1);
    REQUIRE(col_names.count("attisdropped") == 1);
}

// 6. pg_index has indisvalid (Decision 3 — index visibility to planner).
TEST_CASE("catalog::system_schemas::pg_index_has_indisvalid") {
    const auto* def = find_system_table("pg_index");
    REQUIRE(def != nullptr);
    bool has_indisvalid = false;
    for (const auto& c : def->columns) {
        if (c.name() == "indisvalid") {
            has_indisvalid = true;
            break;
        }
    }
    REQUIRE(has_indisvalid);
}

// 7. pg_database carries (oid, datname). Required for CREATE/DROP DATABASE DDL plumbing.
TEST_CASE("catalog::system_schemas::pg_database_minimal_columns") {
    const auto* def = find_system_table("pg_database");
    REQUIRE(def != nullptr);
    REQUIRE(def->relation_oid == well_known_oid::pg_database_table);
    std::unordered_set<std::string> col_names;
    for (const auto& c : def->columns) {
        col_names.insert(c.name());
    }
    REQUIRE(col_names.count("oid") == 1);
    REQUIRE(col_names.count("datname") == 1);
    REQUIRE(def->columns.size() == 2);
}
