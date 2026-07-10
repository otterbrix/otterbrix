#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/types/logical_value.hpp>

#include <set>
#include <utility>

using namespace components;
using namespace components::cursor;

// Regression coverage for the storage_types cache in pushed_reduce_scan
// (PR #534, review comment #3). A correlated LATERAL scalar-aggregate subquery
// re-drives the SAME pushed_reduce_scan instance once per outer row: the first
// open pays the storage_types round-trip and caches the (schema-invariant) column
// types; every subsequent re-open reuses the cache and rebuilds ONLY the filter
// (its correlated parameter changes per outer row). The cache must not perturb the
// per-outer-row aggregate results — this pins them across the re-drives.

namespace {

    int find_column(const cursor_t& cur, std::string_view name) {
        for (uint64_t i = 0; i < cur.column_count(); ++i) {
            if (cur.chunks().front().data[i].type().alias() == name) {
                return static_cast<int>(i);
            }
        }
        return -1;
    }

} // namespace

TEST_CASE("integration::cpp::pushed_reduce_scan::lateral_correlated_aggregate_redrive") {
    auto config = test_create_config("/tmp/test_pushed_reduce_scan_lateral");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto session = otterbrix::session_id_t();
    dispatcher->execute_sql(session, "CREATE DATABASE s;");
    dispatcher->execute_sql(session, "CREATE TABLE s.outer_t (id BIGINT, n BIGINT);");
    // >= 2 outer rows => >= 1 re-drive of the pushed_reduce_scan instance.
    dispatcher->execute_sql(session, "INSERT INTO s.outer_t (id, n) VALUES (1, 10), (2, 20);");
    // Do NOT index k: the inner scalar-aggregate subquery lowers to a full_scan =>
    // pushed_reduce_scan (the pushdown path under test), not an index_scan.
    dispatcher->execute_sql(session, "CREATE TABLE s.inner_t (k BIGINT, v BIGINT);");
    dispatcher->execute_sql(session, "INSERT INTO s.inner_t (k, v) VALUES (1, 100), (1, 101), (2, 200), (3, 300);");

    // Correlated LATERAL scalar aggregate: sum(inner_t.v) filtered by the outer id.
    // The WHERE's correlated parameter changes per outer row, so the filter is rebuilt
    // each drive while the cached column types are reused.
    auto cur = dispatcher->execute_sql(
        session,
        "SELECT o.id, sub.s "
        "FROM s.outer_t o, "
        "     LATERAL (SELECT sum(inner_t.v) AS s FROM s.inner_t WHERE inner_t.k = o.id) sub;");
    INFO("error: " << (cur->is_error() ? cur->get_error().what.c_str() : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);

    int id_i = find_column(*cur, "id");
    int s_i = find_column(*cur, "s");
    REQUIRE(id_i >= 0);
    REQUIRE(s_i >= 0);

    // id=1 -> inner v {100,101} -> sum 201; id=2 -> inner v {200} -> sum 200.
    // The cache must NOT change these results.
    std::multiset<std::pair<int64_t, int64_t>> got;
    for (uint64_t r = 0; r < cur->size(); ++r) {
        got.emplace(cur->value(static_cast<uint64_t>(id_i), r).value<int64_t>(),
                    cur->value(static_cast<uint64_t>(s_i), r).value<int64_t>());
    }
    std::multiset<std::pair<int64_t, int64_t>> expected{{1, 201}, {2, 200}};
    REQUIRE(got == expected);
}
