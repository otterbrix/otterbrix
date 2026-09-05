#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/physical_plan/operators/operator_insert.hpp>
#include <string>

// A table with no indexes must not pay for index maintenance.
//
// The gate was `ctx->index_address != empty_address()` — "does an index manager exist" — which
// is true for every table in every configuration, because register_collection creates an engine
// per table whether or not any index was ever declared. So every INSERT deep-copied its whole
// chunk a second time, shipped it across a mailbox, and the index manager walked the rows
// against an empty index list.
//
// The guard on the other side matters just as much: an indexed table MUST still mirror, or the
// table stays right while the index quietly goes stale.
TEST_CASE("integration::cpp::test_index_mirror_gate::table_without_indexes_skips_the_index") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index_mirror_gate/plain");
    test_clear_directory(config);
    config.wal.on = false;
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE m;")->is_success());
    REQUIRE(exec("CREATE TABLE m.plain (id bigint, v bigint);")->is_success());

    components::operators::reset_insert_index_mirror_sends();
    for (int i = 0; i < 5; ++i) {
        REQUIRE(exec("INSERT INTO m.plain (id, v) VALUES (" + std::to_string(i) + ", 1);")->is_success());
    }
    const auto sends = components::operators::insert_index_mirror_sends();

    INFO("index-mirror sends for 5 inserts into an unindexed table: " << sends);
    CHECK(sends == 0);

    auto cur = exec("SELECT id FROM m.plain WHERE id = 3;");
    REQUIRE(cur->is_success());
    CHECK(cur->size() == 1);
}

TEST_CASE("integration::cpp::test_index_mirror_gate::indexed_table_still_mirrors") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index_mirror_gate/indexed");
    test_clear_directory(config);
    config.wal.on = false;
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE m;")->is_success());
    REQUIRE(exec("CREATE TABLE m.idx (id bigint, k bigint);")->is_success());
    REQUIRE(exec("CREATE INDEX idx_k ON m.idx (k);")->is_success());

    components::operators::reset_insert_index_mirror_sends();
    for (int i = 0; i < 5; ++i) {
        REQUIRE(exec("INSERT INTO m.idx (id, k) VALUES (" + std::to_string(i) + ", " + std::to_string(100 + i) + ");")
                    ->is_success());
    }
    const auto sends = components::operators::insert_index_mirror_sends();

    INFO("index-mirror sends for 5 inserts into an INDEXED table: " << sends);
    CHECK(sends == 5);

    // Read back through the index: equality on the indexed column picks an index scan, so a
    // row that never reached the index would be missing here while present in the table.
    auto cur = exec("SELECT id FROM m.idx WHERE k = 103;");
    REQUIRE(cur->is_success());
    CHECK(cur->size() == 1);
}
