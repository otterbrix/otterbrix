#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/physical_plan/operators/operator_insert.hpp>
#include <string>

static std::string mirror_gate_plan_text(const components::cursor::cursor_t_ptr& cur) {
    std::string out;
    for (std::size_t r = 0; r < cur->size(); ++r) {
        auto cell = cur->value(0, r);
        out += std::string(cell.value<std::string_view>());
        out += '\n';
    }
    return out;
}

// Regression: the gate used to be "does an index manager exist," true for every table since
// register_collection creates one per table, so every table paid the mirror's chunk-copy tax.
TEST_CASE("integration::cpp::test_index_mirror_gate::table_without_indexes_pays_no_chunk_copy") {
    auto config = test_create_config(integration_fixture_path("test_index_mirror_gate/plain"));
    test_clear_directory(config);
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

    {
        auto plan = exec("EXPLAIN SELECT id FROM m.plain WHERE id = 3;");
        REQUIRE(plan->is_success());
        const auto text = mirror_gate_plan_text(plan);
        INFO("plan:\n" << text);
        INFO("an Index Scan on a table that declared no index would route the read at an engine "
             "that does not exist");
        CHECK(text.find("Index Scan") == std::string::npos);
        CHECK(text.find("Seq Scan") != std::string::npos);
    }

    auto cur = exec("SELECT id FROM m.plain WHERE id = 3;");
    REQUIRE(cur->is_success());
    CHECK(cur->size() == 1);
}

TEST_CASE("integration::cpp::test_index_mirror_gate::indexed_table_still_mirrors") {
    auto config = test_create_config(integration_fixture_path("test_index_mirror_gate/indexed"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE m;")->is_success());
    REQUIRE(exec("CREATE TABLE m.idx (id bigint, k bigint);")->is_success());
    REQUIRE(exec("CREATE TABLE m.twin (id bigint, k bigint);")->is_success());
    REQUIRE(exec("CREATE INDEX idx_k ON m.idx (k);")->is_success());

    components::operators::reset_insert_index_mirror_sends();
    for (int i = 0; i < 5; ++i) {
        const std::string values = "(" + std::to_string(i) + ", " + std::to_string(100 + i) + ")";
        REQUIRE(exec("INSERT INTO m.idx (id, k) VALUES " + values + ";")->is_success());
        REQUIRE(exec("INSERT INTO m.twin (id, k) VALUES " + values + ";")->is_success());
    }
    const auto sends = components::operators::insert_index_mirror_sends();

    INFO("index-mirror sends for 5 inserts into an INDEXED table (the twin is unindexed and "
         "must not add any): "
         << sends);
    CHECK(sends == 5);

    const auto probe = [&](const std::string& predicate) {
        {
            auto plan = exec("EXPLAIN SELECT id FROM m.idx WHERE " + predicate + ";");
            REQUIRE(plan->is_success());
            const auto text = mirror_gate_plan_text(plan);
            INFO("predicate: " << predicate << "\nplan:\n" << text);
            INFO("a Seq Scan here would answer out of the heap and pass even with an empty index");
            REQUIRE(text.find("Index Scan") != std::string::npos);
        }
        auto indexed = exec("SELECT id FROM m.idx WHERE " + predicate + ";");
        REQUIRE(indexed->is_success());
        auto heap = exec("SELECT id FROM m.twin WHERE " + predicate + ";");
        REQUIRE(heap->is_success());
        INFO("predicate: " << predicate << " -- index answered " << indexed->size() << " row(s), the "
                           << "unindexed twin holds " << heap->size());
        INFO("a registered index engine answering short is the silent wrong answer rule 6 forbids");
        CHECK(indexed->size() == heap->size());
        return indexed->size();
    };

    CHECK(probe("k = 103") == 1);
    CHECK(probe("k = 999") == 0);
    CHECK(probe("k >= 102") == 3);
    CHECK(probe("k < 102") == 2);

    REQUIRE(exec("DELETE FROM m.idx WHERE k = 103;")->is_success());
    REQUIRE(exec("DELETE FROM m.twin WHERE k = 103;")->is_success());
    CHECK(probe("k = 103") == 0);
    CHECK(probe("k >= 102") == 2);
}

// Regression: the registry answered "which key sets are indexed" from a map keyed by key set
// alone, so dropping either twin erased that shared slot and silently un-registered the survivor too.
TEST_CASE("integration::cpp::test_index_mirror_gate::dropping_a_twin_index_leaves_the_survivor_live") {
    auto config = test_create_config(integration_fixture_path("test_index_mirror_gate/twin"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE m;")->is_success());
    REQUIRE(exec("CREATE TABLE m.twin (id bigint, k bigint);")->is_success());
    REQUIRE(exec("CREATE INDEX twin_k ON m.twin (k);")->is_success());
    REQUIRE(exec("CREATE INDEX twin_k_h ON m.twin USING hash (k);")->is_success());

    REQUIRE(exec("INSERT INTO m.twin (id, k) VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);")->is_success());

    REQUIRE(exec("DROP INDEX m.twin.twin_k_h;")->is_success());

    REQUIRE(exec("INSERT INTO m.twin (id, k) VALUES (6, 100), (7, 110), (8, 120);")->is_success());

    auto probe = [&](const std::string& predicate, std::size_t expected_rows) {
        {
            auto plan = exec("EXPLAIN SELECT id FROM m.twin WHERE " + predicate + ";");
            REQUIRE(plan->is_success());
            const auto text = mirror_gate_plan_text(plan);
            INFO("predicate: " << predicate << "\nplan:\n" << text);
            INFO("a Seq Scan here means the planner cannot see an index the table still holds");
            REQUIRE(text.find("Index Scan") != std::string::npos);
        }
        auto cur = exec("SELECT id FROM m.twin WHERE " + predicate + ";");
        REQUIRE(cur->is_success());
        INFO("predicate: " << predicate);
        INFO("the plan above is an Index Scan and an ordered index answers out of its disk "
             "agent, so a row the index never received is simply missing here");
        CHECK(cur->size() == expected_rows);
    };

    probe("k = 30", 1);
    probe("k = 110", 1);
    probe("k >= 100", 3);
    probe("k < 100", 5);

    REQUIRE(exec("DELETE FROM m.twin WHERE k = 110;")->is_success());
    probe("k >= 100", 2);
}
