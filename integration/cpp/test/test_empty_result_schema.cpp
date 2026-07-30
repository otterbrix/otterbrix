// ============================================================================
// A ZERO-ROW result still describes its columns.
//
// Every streaming scan ends the same way: the source drains, and if it never
// emitted anything it emits ONE 0-row "guard" chunk so that a scalar aggregate
// can answer COUNT=0 and an OUTER join can NULL-pad. That guard chunk is also
// what the USER sees when a query matches nothing — its columns are the result's
// columns, and their names are the names the cursor reports.
//
// Each scan builds that chunk in its own make_drain_chunk (full_scan,
// transfer_scan, index_scan) from the schema it fetched from storage. These
// tests pin that a zero-row result names its columns on purpose, per drain
// site, rather than by whatever a bare type list happened to carry.
//
// The identity half is pinned too: the guard describes the RELATION's columns, so
// each one answers with the attoid pg_attribute gave it — exactly as the
// displaced-relation guard (scan_identity_projection_t::make_guard_chunk) already
// does for the same query shape on a table with a dropped column.
// ============================================================================

#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/tests/generaty.hpp>
#include <components/tests/temp_dir.hpp>

using namespace test_helpers;

namespace {

    // The cursor's column names, in order. `SELECT *` is deliberate: the star fans
    // the INPUT chunk's columns through untouched, so what comes back is what the
    // scan named them, not what a projection list re-labelled them.
    std::vector<std::string> column_names(const components::cursor::cursor_t& cursor) {
        std::vector<std::string> names;
        names.reserve(cursor.column_count());
        for (const auto& column : cursor.columns()) {
            names.emplace_back(column.name);
        }
        return names;
    }

} // namespace

// full_scan: a predicate the disk layer can lower, matching no row. The scan opens a
// fetch-next cursor, gets a cardinality-0 batch back on the first call and emits its
// guard.
TEST_CASE("integration::cpp::empty_result_schema::filtered_scan_names_its_columns") {
    auto config = make_test_config(test_temp_path("empty_result_schema/filtered"), /*disk_on=*/true, /*wal_on=*/true);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE EmptyDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE EmptyDb.t (a BIGINT, b BIGINT, c BIGINT);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO EmptyDb.t (a, b, c) VALUES (1, 2, 3), (4, 5, 6);")->is_success());

    auto cur = exec(dispatcher, "SELECT * FROM EmptyDb.t WHERE a = 999;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 0);
    REQUIRE(cur->column_count() == 3);
    REQUIRE(column_names(*cur) == std::vector<std::string>{"a", "b", "c"});

    // Same shape, non-empty, so the pin above is compared against a result whose
    // names nobody doubts.
    auto rows = exec(dispatcher, "SELECT * FROM EmptyDb.t WHERE a = 1;");
    REQUIRE(rows->is_success());
    REQUIRE(rows->size() == 1);
    REQUIRE(column_names(*rows) == std::vector<std::string>{"a", "b", "c"});
}

// transfer_scan: no predicate at all, over a table with no rows. The guard schema is
// fetched lazily, on the drained-with-zero-rows path only.
TEST_CASE("integration::cpp::empty_result_schema::unfiltered_scan_of_empty_table_names_its_columns") {
    auto config = make_test_config(test_temp_path("empty_result_schema/unfiltered"), /*disk_on=*/true, /*wal_on=*/true);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE EmptyDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE EmptyDb.t (id BIGINT, label TEXT, amount BIGINT);")->is_success());

    auto cur = exec(dispatcher, "SELECT * FROM EmptyDb.t;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 0);
    REQUIRE(cur->column_count() == 3);
    REQUIRE(column_names(*cur) == std::vector<std::string>{"id", "label", "amount"});
}

// index_scan: an indexed equality that matches nothing. The index search returns an
// empty row-id window, so the fetch yields no chunk and the cached schema becomes the
// guard.
TEST_CASE("integration::cpp::empty_result_schema::index_scan_miss_names_its_columns") {
    auto config = make_test_config(test_temp_path("empty_result_schema/index"), /*disk_on=*/true, /*wal_on=*/true);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE EmptyDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE EmptyDb.t (id BIGINT, grp BIGINT, val BIGINT);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO EmptyDb.t (id, grp, val) VALUES (1, 7, 70), (2, 8, 80);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE INDEX idx_grp ON EmptyDb.t (grp);")->is_success());

    auto cur = exec(dispatcher, "SELECT * FROM EmptyDb.t WHERE grp = 4242;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 0);
    REQUIRE(cur->column_count() == 3);
    REQUIRE(column_names(*cur) == std::vector<std::string>{"id", "grp", "val"});
}

// A zero-row scalar aggregate: the guard chunk is what lets COUNT answer 0 at all, so
// this pins that the guard keeps its WIDTH (an aggregate over a guard that dropped its
// columns would have nothing to aggregate).
TEST_CASE("integration::cpp::empty_result_schema::scalar_aggregate_over_no_rows") {
    auto config = make_test_config(test_temp_path("empty_result_schema/aggregate"), /*disk_on=*/true, /*wal_on=*/true);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE EmptyDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE EmptyDb.t (a BIGINT, b BIGINT);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO EmptyDb.t (a, b) VALUES (1, 2);")->is_success());

    {
        auto cur = exec(dispatcher, "SELECT COUNT(*) FROM EmptyDb.t WHERE a = 999;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 0);
    }
    {
        // Aggregate pushdown shape (pushed_reduce_scan): the reduce runs on the owning
        // agent and replies no group rows; the empty scalar row is owned by the
        // group-merge above it, not by a scan guard.
        auto cur = exec(dispatcher, "SELECT SUM(b) FROM EmptyDb.t WHERE a = 999;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).is_null());
    }
}

// The guard describes the RELATION, so its columns carry the relation's catalog
// identity — the same thing the non-empty result carries, and the same thing the
// displaced-relation guard already stamps.
TEST_CASE("integration::cpp::empty_result_schema::zero_row_result_carries_column_identity") {
    auto config = make_test_config(test_temp_path("empty_result_schema/identity"), /*disk_on=*/true, /*wal_on=*/true);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE EmptyDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE EmptyDb.t (a BIGINT, b BIGINT, c BIGINT);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO EmptyDb.t (a, b, c) VALUES (1, 2, 3);")->is_success());

    auto rows = exec(dispatcher, "SELECT * FROM EmptyDb.t;");
    REQUIRE(rows->is_success());
    REQUIRE(rows->size() == 1);

    auto empty = exec(dispatcher, "SELECT * FROM EmptyDb.t WHERE a = 999;");
    REQUIRE(empty->is_success());
    REQUIRE(empty->size() == 0);
    REQUIRE(empty->column_count() == rows->column_count());

    for (std::size_t i = 0; i < rows->column_count(); ++i) {
        INFO("column " << i);
        REQUIRE(rows->columns()[i].attoid != components::catalog::INVALID_OID);
        REQUIRE(empty->columns()[i].attoid == rows->columns()[i].attoid);
    }
}

// A WHERE narrows which ROWS a relation answers with. It does not change what its columns
// ARE — so a filtered result describes its columns exactly as an unfiltered one does.
//
// operator_match gathers the surviving rows into a chunk it builds itself, and a gather moves
// values, not identities; built from a bare type list, that chunk answered INVALID_OID for
// every column while the same query without a WHERE answered with the relation's attoids. The
// second arm is the one that makes this more than cosmetic: `INSERT INTO t SELECT * FROM t`
// with no target list is routed by the append matcher's identity pass, and it has to reach the
// same columns whether or not a WHERE sits in the middle.
TEST_CASE("integration::cpp::empty_result_schema::a_filter_preserves_column_identity") {
    auto config = make_test_config(test_temp_path("empty_result_schema/filter_identity"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE FilterDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE FilterDb.t (a BIGINT, b BIGINT);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO FilterDb.t (a, b) VALUES (1, 10), (2, 20);")->is_success());

    INFO("a filtered SELECT * describes its columns as the unfiltered one does");
    {
        auto all = exec(dispatcher, "SELECT * FROM FilterDb.t;");
        REQUIRE(all->is_success());
        REQUIRE(all->column_count() == 2);
        // A WHERE the disk layer cannot lower keeps operator_match above the scan, which is the
        // operator under test; `a + 0 = 1` is a computed predicate, not a column-vs-constant one.
        auto filtered = exec(dispatcher, "SELECT * FROM FilterDb.t WHERE a + 0 = 1;");
        REQUIRE(filtered->is_success());
        REQUIRE(filtered->size() == 1);
        REQUIRE(filtered->column_count() == 2);
        for (std::size_t i = 0; i < all->column_count(); ++i) {
            INFO("column " << i);
            REQUIRE(std::string{filtered->columns()[i].name} == std::string{all->columns()[i].name});
            REQUIRE(all->columns()[i].attoid != components::catalog::INVALID_OID);
            REQUIRE(filtered->columns()[i].attoid == all->columns()[i].attoid);
        }
    }

    INFO("self-insert through a filter routes each value to the column it came from");
    {
        REQUIRE(exec(dispatcher, "INSERT INTO FilterDb.t SELECT * FROM FilterDb.t WHERE a + 0 = 1;")->is_success());
        auto cur = exec(dispatcher, "SELECT a, b FROM FilterDb.t ORDER BY a, b;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        // The copied row is (1, 10) again — never (10, 1).
        REQUIRE(cur->value(0, 0).value<int64_t>() == 1);
        REQUIRE(cur->value(1, 0).value<int64_t>() == 10);
        REQUIRE(cur->value(0, 1).value<int64_t>() == 1);
        REQUIRE(cur->value(1, 1).value<int64_t>() == 10);
    }

    INFO("a reversed target list still wins over identity: the rename drops it");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE FilterDb.u (a BIGINT, b BIGINT);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO FilterDb.u (a, b) VALUES (7, 70);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO FilterDb.u (b, a) SELECT * FROM FilterDb.u WHERE a + 0 = 7;")
                    ->is_success());
        auto cur = exec(dispatcher, "SELECT a, b FROM FilterDb.u ORDER BY a;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        // Row (7, 70) written into (b, a) must land as (a=70, b=7).
        REQUIRE(cur->value(0, 0).value<int64_t>() == 7);
        REQUIRE(cur->value(1, 0).value<int64_t>() == 70);
        REQUIRE(cur->value(0, 1).value<int64_t>() == 70);
        REQUIRE(cur->value(1, 1).value<int64_t>() == 7);
    }
}
