#include <catch2/catch_test_macros.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/helpers.hpp>
#include <components/catalog/system_table_schemas.hpp>

#include <unordered_set>

using namespace components::catalog;

// 1. The catalog has exactly 14 system tables (10 original + pg_sequence + pg_rewrite + pg_settings + pg_cast).
TEST_CASE("catalog::system_schemas::tables_count_10") {
    auto tables = all_system_tables();
    REQUIRE(tables.size() == 14);
}

// 2. Every system table has a unique relation_oid drawn from the well-known range.
TEST_CASE("catalog::system_schemas::distinct_well_known_oids") {
    std::unordered_set<oid_t> seen;
    for (const auto& def : all_system_tables()) {
        REQUIRE(def.relation_oid >= well_known_oid::pg_namespace_table);
        REQUIRE(def.relation_oid <= well_known_oid::pg_cast_table);
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

// 8. Column ORDER of the system tables is part of their contract.
//
// Readers of pg_* address columns POSITIONALLY: literal indices in manager_disk_bootstrap.cpp
// and ddl_metadata_builder.cpp for pg_index, in operator_vacuum and the computed-field
// operators for pg_computed_column, and the keyed catalog reads pass column ordinals across
// the mailbox. Every other assertion here is order-independent (name sets and counts), so
// without this one, inserting a column in the middle of a schema leaves the whole suite green
// while every positional reader silently shifts onto its neighbour.
namespace {
    void require_layout(const char* table, std::initializer_list<const char*> expected) {
        const auto* def = find_system_table(table);
        INFO("system table: " << table);
        REQUIRE(def != nullptr);
        REQUIRE(def->columns.size() == expected.size());
        std::size_t i = 0;
        for (const auto* name : expected) {
            INFO("column position " << i << " of " << table);
            REQUIRE(std::string(def->columns[i].name()) == std::string(name));
            ++i;
        }
    }
} // namespace

TEST_CASE("catalog::system_schemas::column_order_is_pinned") {
    require_layout("pg_namespace", {"oid", "nspname"});
    require_layout("pg_class", {"oid", "relname", "relnamespace", "relkind", "relstoragemode"});
    require_layout("pg_attribute",
                   {"attoid",
                    "attrelid",
                    "attname",
                    "atttypid",
                    "attnum",
                    "attnotnull",
                    "atthasdefault",
                    "attisdropped",
                    "atttypspec",
                    "attdefspec",
                    "added_at_commit_id",
                    "dropped_at_commit_id"});
    require_layout("pg_index", {"indexrelid", "indrelid", "indkey", "indisvalid", "indtype"});
    require_layout("pg_computed_column",
                   {"relid", "attoid", "attname", "atttypid", "atttypspec", "attversion", "attrefcount"});
}

// 9. Every column-index constant names the column it points at.
//
// The keyed catalog reads address columns by ORDINAL — no name crosses the mailbox — so
// components/catalog/helpers.hpp IS the identity of a catalog column. Test 8 pins the layout
// of the schemas it lists; this one ties each CONSTANT to the schema it claims to mirror, for
// every table that has constants. Reorder a schema, rename a column or mistype an index, and the
// constant that silently started pointing at its neighbour fails here instead of in a resolve.
namespace {
    void require_col(const char* table, std::uint64_t position, const char* name) {
        const auto* def = find_system_table(table);
        INFO("system table " << table << ", column position " << position);
        REQUIRE(def != nullptr);
        REQUIRE(position < def->columns.size());
        REQUIRE(std::string(def->columns[position].name()) == std::string(name));
    }
} // namespace

TEST_CASE("catalog::system_schemas::column_constants_match_the_schema") {
    // pg_constraint
    require_col("pg_constraint", pg_constraint_col::oid, "oid");
    require_col("pg_constraint", pg_constraint_col::conname, "conname");
    require_col("pg_constraint", pg_constraint_col::conrelid, "conrelid");
    require_col("pg_constraint", pg_constraint_col::contype, "contype");
    require_col("pg_constraint", pg_constraint_col::confrelid, "confrelid");
    require_col("pg_constraint", pg_constraint_col::conkey, "conkey");
    require_col("pg_constraint", pg_constraint_col::confkey, "confkey");
    require_col("pg_constraint", pg_constraint_col::confmatchtype, "confmatchtype");
    require_col("pg_constraint", pg_constraint_col::confdeltype, "confdeltype");
    require_col("pg_constraint", pg_constraint_col::confupdtype, "confupdtype");
    require_col("pg_constraint", pg_constraint_col::conexpr, "conexpr");
    // pg_attribute
    require_col("pg_attribute", pg_attribute_col::attoid, "attoid");
    require_col("pg_attribute", pg_attribute_col::attrelid, "attrelid");
    require_col("pg_attribute", pg_attribute_col::attname, "attname");
    require_col("pg_attribute", pg_attribute_col::atttypid, "atttypid");
    require_col("pg_attribute", pg_attribute_col::attnum, "attnum");
    require_col("pg_attribute", pg_attribute_col::attnotnull, "attnotnull");
    require_col("pg_attribute", pg_attribute_col::atthasdefault, "atthasdefault");
    require_col("pg_attribute", pg_attribute_col::attisdropped, "attisdropped");
    require_col("pg_attribute", pg_attribute_col::atttypspec, "atttypspec");
    require_col("pg_attribute", pg_attribute_col::attdefspec, "attdefspec");
    require_col("pg_attribute", pg_attribute_col::added_at_commit_id, "added_at_commit_id");
    require_col("pg_attribute", pg_attribute_col::dropped_at_commit_id, "dropped_at_commit_id");
    // pg_class
    require_col("pg_class", pg_class_col::oid, "oid");
    require_col("pg_class", pg_class_col::relname, "relname");
    require_col("pg_class", pg_class_col::relnamespace, "relnamespace");
    require_col("pg_class", pg_class_col::relkind, "relkind");
    require_col("pg_class", pg_class_col::relstoragemode, "relstoragemode");
    // pg_namespace
    require_col("pg_namespace", pg_namespace_col::oid, "oid");
    require_col("pg_namespace", pg_namespace_col::nspname, "nspname");
    // pg_index
    require_col("pg_index", pg_index_col::indexrelid, "indexrelid");
    require_col("pg_index", pg_index_col::indrelid, "indrelid");
    require_col("pg_index", pg_index_col::indkey, "indkey");
    require_col("pg_index", pg_index_col::indisvalid, "indisvalid");
    require_col("pg_index", pg_index_col::indtype, "indtype");
    // pg_computed_column
    require_col("pg_computed_column", pg_computed_column_col::relid, "relid");
    require_col("pg_computed_column", pg_computed_column_col::attoid, "attoid");
    require_col("pg_computed_column", pg_computed_column_col::attname, "attname");
    require_col("pg_computed_column", pg_computed_column_col::atttypid, "atttypid");
    require_col("pg_computed_column", pg_computed_column_col::atttypspec, "atttypspec");
    require_col("pg_computed_column", pg_computed_column_col::attversion, "attversion");
    require_col("pg_computed_column", pg_computed_column_col::attrefcount, "attrefcount");
    // pg_type
    require_col("pg_type", pg_type_col::oid, "oid");
    require_col("pg_type", pg_type_col::typname, "typname");
    require_col("pg_type", pg_type_col::typnamespace, "typnamespace");
    require_col("pg_type", pg_type_col::typdefspec, "typdefspec");
    // pg_database
    require_col("pg_database", pg_database_col::oid, "oid");
    require_col("pg_database", pg_database_col::datname, "datname");
    // pg_rewrite
    require_col("pg_rewrite", pg_rewrite_col::oid, "oid");
    require_col("pg_rewrite", pg_rewrite_col::rulename, "rulename");
    require_col("pg_rewrite", pg_rewrite_col::ev_class, "ev_class");
    require_col("pg_rewrite", pg_rewrite_col::ev_type, "ev_type");
    require_col("pg_rewrite", pg_rewrite_col::ev_action, "ev_action");
    // pg_depend
    require_col("pg_depend", pg_depend_col::classid, "classid");
    require_col("pg_depend", pg_depend_col::objid, "objid");
    require_col("pg_depend", pg_depend_col::refclassid, "refclassid");
    require_col("pg_depend", pg_depend_col::refobjid, "refobjid");
    require_col("pg_depend", pg_depend_col::deptype, "deptype");
    // pg_proc
    require_col("pg_proc", pg_proc_col::oid, "oid");
    require_col("pg_proc", pg_proc_col::proname, "proname");
    require_col("pg_proc", pg_proc_col::pronamespace, "pronamespace");
    require_col("pg_proc", pg_proc_col::pronargs, "pronargs");
    require_col("pg_proc", pg_proc_col::prouid, "prouid");
    // pg_sequence
    require_col("pg_sequence", pg_sequence_col::seqrelid, "seqrelid");
    require_col("pg_sequence", pg_sequence_col::seqstart, "seqstart");
    require_col("pg_sequence", pg_sequence_col::seqincrement, "seqincrement");
    require_col("pg_sequence", pg_sequence_col::seqmin, "seqmin");
    require_col("pg_sequence", pg_sequence_col::seqmax, "seqmax");
    require_col("pg_sequence", pg_sequence_col::seqcycle, "seqcycle");
    require_col("pg_sequence", pg_sequence_col::seqlast, "seqlast");
}
