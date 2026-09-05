#include <catch2/catch_test_macros.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/helpers.hpp>
#include <components/catalog/oid_batch.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>

#include <cstring>
#include <memory_resource>
#include <vector>

// CREATE TABLE must write pg_attribute's added_at_commit_id (col 10) / dropped_at_commit_id
// (col 11) itself: operator_resolve_table uses them for snapshot visibility, and leaving them
// to vector_t's memset-on-allocate would make column visibility depend on an initialisation
// that exists for unrelated reasons.
//
// Characterisation, not reproduction: the poison resource fills allocations with 0xA5, but
// vector_t's ctor memsets right after and the validity mask starts all-valid, so this can't
// fail against the old writer — it only catches a future regression that stops zeroing.

namespace {
    // Hands out memory that is deliberately NOT zero, so a cell nobody wrote cannot be mistaken for
    // a cell someone wrote a zero into.
    class poison_resource_t final : public std::pmr::memory_resource {
    public:
        explicit poison_resource_t(std::pmr::memory_resource* upstream)
            : upstream_(upstream) {}

    private:
        void* do_allocate(std::size_t bytes, std::size_t alignment) override {
            void* p = upstream_->allocate(bytes, alignment);
            std::memset(p, 0xA5, bytes);
            return p;
        }
        void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override {
            upstream_->deallocate(p, bytes, alignment);
        }
        bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }
        std::pmr::memory_resource* upstream_;
    };
} // namespace

TEST_CASE("catalog::ddl::create_table_writes_the_mvcc_columns_of_pg_attribute") {
    using namespace components::catalog;

    poison_resource_t poison(std::pmr::new_delete_resource());

    std::vector<components::table::column_definition_t> columns;
    columns.emplace_back("id", components::types::complex_logical_type(components::types::logical_type::BIGINT));
    columns.emplace_back("name",
                         components::types::complex_logical_type(components::types::logical_type::STRING_LITERAL));

    oid_batch_t oids;
    for (oid_t i = 0; i < 64; ++i) {
        oids.oids.push_back(static_cast<oid_t>(1000 + i));
    }

    auto writes = build_create_table_writes(&poison, "db", "t", columns, 100, oids);

    const auto* schema = find_system_table(well_known_oid::pg_attribute_table);
    REQUIRE(schema != nullptr);
    REQUIRE(schema->columns.size() == 12);

    bool saw_pg_attribute = false;
    for (const auto& w : writes) {
        if (w.row.column_count() != schema->columns.size()) {
            continue; // a different catalog table
        }
        saw_pg_attribute = true;
        for (std::size_t row = 0; row < w.row.size(); ++row) {
            INFO("pg_attribute row " << row);
            // Both columns are NOT NULL and the reader dereferences them.
            CHECK_FALSE(w.row.is_null(pg_attribute_col::added_at_commit_id, row));
            CHECK_FALSE(w.row.is_null(pg_attribute_col::dropped_at_commit_id, row));
            // A freshly created table's columns are visible to every snapshot and not dropped.
            CHECK(w.row.get_value<std::int64_t>(pg_attribute_col::added_at_commit_id, row) == 0);
            CHECK(w.row.get_value<std::int64_t>(pg_attribute_col::dropped_at_commit_id, row) == 0);
        }
    }
    // Positive control: if no pg_attribute chunk was produced the loop above checked nothing.
    REQUIRE(saw_pg_attribute);
}

// pg_class.relstoragemode is always written 'd' (every table is disk-backed), for regular
// and schemaless computed (relkind='g') tables alike. Integration counterpart:
// test_persistence::b1a_disk_is_default.
TEST_CASE("catalog::ddl::create_table_writes_relstoragemode_disk_always") {
    using namespace components::catalog;

    poison_resource_t poison(std::pmr::new_delete_resource());

    const auto* schema = find_system_table(well_known_oid::pg_class_table);
    REQUIRE(schema != nullptr);

    auto check_pg_class_mode = [&](const std::vector<catalog_write_t>& writes) {
        bool saw_pg_class = false;
        for (const auto& w : writes) {
            if (w.table_oid != well_known_oid::pg_class_table) {
                continue;
            }
            saw_pg_class = true;
            REQUIRE(w.row.size() == 1);
            REQUIRE_FALSE(w.row.is_null(pg_class_col::relstoragemode, 0));
            CHECK(w.row.get_value<std::string_view>(pg_class_col::relstoragemode, 0) == "d");
        }
        REQUIRE(saw_pg_class);
    };

    // Regular table with columns.
    {
        std::vector<components::table::column_definition_t> columns;
        columns.emplace_back("id", components::types::complex_logical_type(components::types::logical_type::BIGINT));
        oid_batch_t oids;
        for (oid_t i = 0; i < 8; ++i) {
            oids.oids.push_back(static_cast<oid_t>(2000 + i));
        }
        check_pg_class_mode(build_create_table_writes(&poison, "db", "t_regular", columns, 100, oids));
    }

    // Schemaless computed table (relkind='g') — no columns, still 'd'.
    {
        oid_batch_t oids;
        oids.oids.push_back(static_cast<oid_t>(3000));
        // The builder stamps allocated attoids back onto the column list, so it takes
        // a non-const lvalue. A schemaless table has none, but the list still has to be one.
        std::vector<components::table::column_definition_t> no_columns;
        check_pg_class_mode(
            build_create_table_writes(&poison, "db", "t_computed", no_columns, 100, oids, relkind::computed));
    }
}
