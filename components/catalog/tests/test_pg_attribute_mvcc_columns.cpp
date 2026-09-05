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

// CREATE TABLE must write pg_attribute's two MVCC columns itself.
//
// pg_attribute has twelve columns; added_at_commit_id is 10 and dropped_at_commit_id is 11, both
// declared NOT NULL and both used by the reader to decide whether a column is visible to a snapshot
// (operator_resolve_table skips a row whose added_at is past the snapshot, or whose dropped_at is
// non-zero and at or before it). build_create_table_writes used to fill columns 0 through 9 and stop:
// the rows still read correctly, but only because vector_t memsets every buffer it allocates, so the
// visibility of every column of every table rested on an initialisation that exists for unrelated
// reasons, and narrowing that memset would have made columns silently vanish from a table.
//
// This is a characterisation test, not a reproducing one, and it cannot be made to fail against the
// old writer: the poison resource below fills allocations with 0xA5, but vector_t's constructor
// memsets the buffer straight afterwards and the validity mask starts all-valid, so unwritten cells
// read as non-NULL zeros whatever the allocator hands over. What it does buy is that it fails the
// instant anyone stops zeroing pg_attribute's buffer.

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

// B1a: every table is disk-backed. The pg_class row's relstoragemode column stays
// (write-only, no readers) and is ALWAYS written 'd' — for regular tables and for
// the schemaless computed (relkind='g') creation path alike. This is the write-site
// half of the B1a gate; the integration half (a plain CREATE TABLE producing a
// .otbx) lives in test_persistence::b1a_disk_is_default.
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
        // RN-oid: the builder stamps allocated attoids back onto the column list, so it takes
        // a non-const lvalue. A schemaless table has none, but the list still has to be one.
        std::vector<components::table::column_definition_t> no_columns;
        check_pg_class_mode(
            build_create_table_writes(&poison, "db", "t_computed", no_columns, 100, oids, relkind::computed));
    }
}
