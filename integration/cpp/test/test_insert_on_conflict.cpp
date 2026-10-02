#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <map>
#include <string>
#include <tuple>

using namespace components::cursor;

namespace {

    using items_t = std::map<std::int64_t, std::int64_t>;

    const items_t SEEDED_ITEMS{{1, 10}, {2, 20}, {3, 30}};

    cursor_t_ptr
    run(otterbrix::wrapper_dispatcher_t* dispatcher, const otterbrix::session_id_t& session, const std::string& sql) {
        return dispatcher->execute_sql(session, sql);
    }

    std::string why(const cursor_t& cursor) {
        return cursor.is_error() ? std::string{cursor.get_error().what.begin(), cursor.get_error().what.end()}
                                 : std::string{"<no error: statement reported success>"};
    }

    void require_ok(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        auto cursor = run(dispatcher, session, sql);
        INFO(sql);
        INFO(why(*cursor));
        REQUIRE(cursor->is_success());
    }

    // id -> v of a cursor whose first two columns are id and v; a repeated id fails here.
    items_t items_of(const cursor_t& cursor) {
        items_t items;
        for (std::size_t row = 0; row < cursor.size(); ++row) {
            items[cursor.value(0, row).value<std::int64_t>()] = cursor.value(1, row).value<std::int64_t>();
        }
        INFO("a key returned more than once");
        REQUIRE(items.size() == cursor.size());
        return items;
    }

    items_t read_items(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& table = "ocdb.items") {
        auto session = otterbrix::session_id_t();
        auto cursor = run(dispatcher, session, "SELECT id, v FROM " + table + ";");
        INFO(why(*cursor));
        REQUIRE(cursor->is_success());
        return items_of(*cursor);
    }

    // Runs a statement with RETURNING id, v and gives back the returned rows.
    items_t returned(otterbrix::wrapper_dispatcher_t* dispatcher,
                     const otterbrix::session_id_t& session,
                     const std::string& sql) {
        auto cursor = run(dispatcher, session, sql);
        INFO(sql);
        INFO(why(*cursor));
        REQUIRE(cursor->is_success());
        return items_of(*cursor);
    }

    void seed_items(otterbrix::wrapper_dispatcher_t* dispatcher) {
        require_ok(dispatcher, "CREATE DATABASE ocdb;");
        require_ok(dispatcher, "CREATE TABLE ocdb.items (id bigint, v bigint);");
        require_ok(dispatcher, "INSERT INTO ocdb.items (id, v) VALUES (1, 10), (2, 20), (3, 30);");
        require_ok(dispatcher, "ALTER TABLE ocdb.items ADD CONSTRAINT pk_items_id PRIMARY KEY (id);");
    }

    // (row % distinct_keys, row + value_offset) for every row.
    std::string values_list(std::int64_t rows, std::int64_t distinct_keys, std::int64_t value_offset = 0) {
        std::string values;
        for (std::int64_t row = 0; row < rows; ++row) {
            if (row > 0) {
                values += ", ";
            }
            values += "(" + std::to_string(row % distinct_keys) + ", " + std::to_string(row + value_offset) + ")";
        }
        return values;
    }

} // namespace

TEST_CASE("integration::cpp::insert_on_conflict") {
    SECTION("DO NOTHING") {
        auto config = test_create_config(integration_fixture_path("insert_on_conflict/do_nothing"));
        test_clear_directory(config);
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        seed_items(dispatcher);

        INFO("without ON CONFLICT the key is refused, so a test that passes below did skip the row");
        REQUIRE(
            run(dispatcher, otterbrix::session_id_t(), "INSERT INTO ocdb.items (id, v) VALUES (2, 99);")->is_error());

        SECTION("a column target skips the conflicting row and writes the rest") {
            auto cursor = run(dispatcher,
                              otterbrix::session_id_t(),
                              "INSERT INTO ocdb.items (id, v) VALUES (2, 99), (4, 40) ON CONFLICT (id) DO NOTHING;");
            INFO(why(*cursor));
            REQUIRE(cursor->is_success());
            CHECK(cursor->size() == 1);
            CHECK(read_items(dispatcher) == items_t{{1, 10}, {2, 20}, {3, 30}, {4, 40}});
        }
        SECTION("a column target with RETURNING") {
            CHECK(returned(dispatcher,
                           otterbrix::session_id_t(),
                           "INSERT INTO ocdb.items (id, v) VALUES (2, 99), (4, 40) ON CONFLICT (id) DO NOTHING "
                           "RETURNING id, v;") == items_t{{4, 40}});
            CHECK(read_items(dispatcher) == items_t{{1, 10}, {2, 20}, {3, 30}, {4, 40}});
        }
        SECTION("a constraint named by ON CONSTRAINT") {
            auto cursor = run(dispatcher,
                              otterbrix::session_id_t(),
                              "INSERT INTO ocdb.items (id, v) VALUES (3, 99), (5, 50) ON CONFLICT ON CONSTRAINT "
                              "pk_items_id DO NOTHING;");
            INFO(why(*cursor));
            REQUIRE(cursor->is_success());
            CHECK(cursor->size() == 1);
            CHECK(read_items(dispatcher) == items_t{{1, 10}, {2, 20}, {3, 30}, {5, 50}});
        }
        SECTION("ON CONSTRAINT with RETURNING") {
            CHECK(returned(dispatcher,
                           otterbrix::session_id_t(),
                           "INSERT INTO ocdb.items (id, v) VALUES (3, 99), (5, 50) ON CONFLICT ON CONSTRAINT "
                           "pk_items_id DO NOTHING RETURNING id, v;") == items_t{{5, 50}});
            CHECK(read_items(dispatcher) == items_t{{1, 10}, {2, 20}, {3, 30}, {5, 50}});
        }
        SECTION("no target arbitrates on every UNIQUE and PRIMARY KEY constraint") {
            auto cursor = run(dispatcher,
                              otterbrix::session_id_t(),
                              "INSERT INTO ocdb.items (id, v) VALUES (1, 99), (6, 60) ON CONFLICT DO NOTHING;");
            INFO(why(*cursor));
            REQUIRE(cursor->is_success());
            CHECK(cursor->size() == 1);
            CHECK(read_items(dispatcher) == items_t{{1, 10}, {2, 20}, {3, 30}, {6, 60}});
        }
        SECTION("no target with RETURNING") {
            CHECK(returned(dispatcher,
                           otterbrix::session_id_t(),
                           "INSERT INTO ocdb.items (id, v) VALUES (1, 99), (6, 60) ON CONFLICT DO NOTHING "
                           "RETURNING id, v;") == items_t{{6, 60}});
            CHECK(read_items(dispatcher) == items_t{{1, 10}, {2, 20}, {3, 30}, {6, 60}});
        }
        SECTION("a target WHERE infers the same constraint") {
            CHECK(returned(dispatcher,
                           otterbrix::session_id_t(),
                           "INSERT INTO ocdb.items (id, v) VALUES (1, 99), (7, 70) ON CONFLICT (id) WHERE v > 0 "
                           "DO NOTHING RETURNING id, v;") == items_t{{7, 70}});
            CHECK(read_items(dispatcher) == items_t{{1, 10}, {2, 20}, {3, 30}, {7, 70}});
        }
        SECTION("the same new key twice in one statement keeps the first row") {
            CHECK(returned(dispatcher,
                           otterbrix::session_id_t(),
                           "INSERT INTO ocdb.items (id, v) VALUES (8, 80), (8, 81), (9, 90), (8, 82) "
                           "ON CONFLICT (id) DO NOTHING RETURNING id, v;") == items_t{{8, 80}, {9, 90}});
            CHECK(read_items(dispatcher) == items_t{{1, 10}, {2, 20}, {3, 30}, {8, 80}, {9, 90}});
        }
        SECTION("a key written earlier in the same transaction") {
            auto session = otterbrix::session_id_t();
            REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());
            REQUIRE(run(dispatcher, session, "INSERT INTO ocdb.items (id, v) VALUES (10, 100);")->is_success());
            CHECK(returned(dispatcher,
                           session,
                           "INSERT INTO ocdb.items (id, v) VALUES (10, 101), (11, 110) ON CONFLICT (id) DO NOTHING "
                           "RETURNING id, v;") == items_t{{11, 110}});
            REQUIRE(run(dispatcher, session, "COMMIT;")->is_success());
            CHECK(read_items(dispatcher) == items_t{{1, 10}, {2, 20}, {3, 30}, {10, 100}, {11, 110}});
        }
        SECTION("every proposed row conflicts: RETURNING is empty") {
            auto cursor = run(dispatcher,
                              otterbrix::session_id_t(),
                              "INSERT INTO ocdb.items (id, v) VALUES (1, 99), (2, 99) ON CONFLICT (id) DO NOTHING "
                              "RETURNING id, v;");
            INFO(why(*cursor));
            REQUIRE(cursor->is_success());
            CHECK(cursor->size() == 0);
            CHECK(read_items(dispatcher) == SEEDED_ITEMS);
        }
        SECTION("INSERT ... SELECT") {
            require_ok(dispatcher, "CREATE TABLE ocdb.source (id bigint, v bigint);");
            require_ok(dispatcher, "INSERT INTO ocdb.source (id, v) VALUES (1, 99), (12, 120), (12, 121);");
            const auto rows = returned(dispatcher,
                                       otterbrix::session_id_t(),
                                       "INSERT INTO ocdb.items (id, v) SELECT id, v FROM ocdb.source "
                                       "ON CONFLICT (id) DO NOTHING RETURNING id, v;");
            REQUIRE(rows.size() == 1);
            CHECK(rows.count(12) == 1);
            const auto items = read_items(dispatcher);
            CHECK(items.size() == 4);
            CHECK(items.at(1) == 10);
            CHECK(items.at(12) == rows.at(12));
        }
    }
    SECTION("DO UPDATE") {
        auto config = test_create_config(integration_fixture_path("insert_on_conflict/do_update"));
        test_clear_directory(config);
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        seed_items(dispatcher);

        INFO("without ON CONFLICT the key is refused, so a test that passes below did resolve the conflict");
        REQUIRE(
            run(dispatcher, otterbrix::session_id_t(), "INSERT INTO ocdb.items (id, v) VALUES (2, 99);")->is_error());

        SECTION("SET reads both the target row and EXCLUDED") {
            // 20 + 5: either side bound to the wrong row gives another number.
            CHECK(returned(dispatcher,
                           otterbrix::session_id_t(),
                           "INSERT INTO ocdb.items (id, v) VALUES (2, 5), (4, 40) ON CONFLICT (id) "
                           "DO UPDATE SET v = items.v + excluded.v RETURNING id, v;") == items_t{{2, 25}, {4, 40}});
            CHECK(read_items(dispatcher) == items_t{{1, 10}, {2, 25}, {3, 30}, {4, 40}});
        }
        SECTION("without RETURNING the count is the inserted and the updated rows") {
            auto cursor = run(dispatcher,
                              otterbrix::session_id_t(),
                              "INSERT INTO ocdb.items (id, v) VALUES (3, 33), (1, 11), (7, 70) ON CONFLICT (id) "
                              "DO UPDATE SET v = excluded.v;");
            INFO(why(*cursor));
            REQUIRE(cursor->is_success());
            CHECK(cursor->size() == 3);
            CHECK(read_items(dispatcher) == items_t{{1, 11}, {2, 20}, {3, 33}, {7, 70}});
        }
        SECTION("a constraint named by ON CONSTRAINT") {
            CHECK(returned(dispatcher,
                           otterbrix::session_id_t(),
                           "INSERT INTO ocdb.items (id, v) VALUES (1, 7) ON CONFLICT ON CONSTRAINT pk_items_id "
                           "DO UPDATE SET v = excluded.v * 2 RETURNING id, v;") == items_t{{1, 14}});
            CHECK(read_items(dispatcher) == items_t{{1, 14}, {2, 20}, {3, 30}});
        }
        SECTION("a target WHERE infers the same constraint") {
            CHECK(returned(dispatcher,
                           otterbrix::session_id_t(),
                           "INSERT INTO ocdb.items (id, v) VALUES (3, 1) ON CONFLICT (id) WHERE v > 0 "
                           "DO UPDATE SET v = items.v - excluded.v RETURNING id, v;") == items_t{{3, 29}});
            CHECK(read_items(dispatcher) == items_t{{1, 10}, {2, 20}, {3, 29}});
        }
        SECTION("a row the DO UPDATE WHERE rejects is neither updated nor inserted") {
            // id 1 holds 10, so the WHERE rejects it; id 2 holds 20 and is updated; id 5 is new.
            CHECK(returned(dispatcher,
                           otterbrix::session_id_t(),
                           "INSERT INTO ocdb.items (id, v) VALUES (1, 11), (2, 22), (5, 50) ON CONFLICT (id) "
                           "DO UPDATE SET v = excluded.v WHERE items.v > 15 RETURNING id, v;") ==
                  items_t{{2, 22}, {5, 50}});
            CHECK(read_items(dispatcher) == items_t{{1, 10}, {2, 22}, {3, 30}, {5, 50}});
        }
        SECTION("the DO UPDATE WHERE reads EXCLUDED") {
            CHECK(returned(dispatcher,
                           otterbrix::session_id_t(),
                           "INSERT INTO ocdb.items (id, v) VALUES (1, 5), (2, 50) ON CONFLICT (id) "
                           "DO UPDATE SET v = excluded.v WHERE excluded.v > items.v RETURNING id, v;") ==
                  items_t{{2, 50}});
            CHECK(read_items(dispatcher) == items_t{{1, 10}, {2, 50}, {3, 30}});
        }
        SECTION("one existing row is affected at most once") {
            CHECK(run(dispatcher,
                      otterbrix::session_id_t(),
                      "INSERT INTO ocdb.items (id, v) VALUES (1, 11), (1, 12) ON CONFLICT (id) DO UPDATE SET v = "
                      "excluded.v;")
                      ->is_error());
            CHECK(read_items(dispatcher) == SEEDED_ITEMS);
        }
        SECTION("a new key twice in one statement fails: the second would update the first") {
            CHECK(run(dispatcher,
                      otterbrix::session_id_t(),
                      "INSERT INTO ocdb.items (id, v) VALUES (6, 60), (6, 61) ON CONFLICT (id) DO UPDATE SET v = "
                      "excluded.v;")
                      ->is_error());
            CHECK(read_items(dispatcher) == SEEDED_ITEMS);
        }
        SECTION("a SET that moves the key onto another row fails") {
            CHECK(run(dispatcher,
                      otterbrix::session_id_t(),
                      "INSERT INTO ocdb.items (id, v) VALUES (2, 0) ON CONFLICT (id) DO UPDATE SET id = 1;")
                      ->is_error());
            CHECK(read_items(dispatcher) == SEEDED_ITEMS);
        }
        SECTION("inside a transaction") {
            auto session = otterbrix::session_id_t();
            REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());
            REQUIRE(run(dispatcher, session, "INSERT INTO ocdb.items (id, v) VALUES (8, 80);")->is_success());
            CHECK(returned(dispatcher,
                           session,
                           "INSERT INTO ocdb.items (id, v) VALUES (8, 1), (1, 1) ON CONFLICT (id) "
                           "DO UPDATE SET v = items.v + excluded.v RETURNING id, v;") == items_t{{1, 11}, {8, 81}});
            REQUIRE(run(dispatcher, session, "COMMIT;")->is_success());
            CHECK(read_items(dispatcher) == items_t{{1, 11}, {2, 20}, {3, 30}, {8, 81}});
        }
        SECTION("a failed DO UPDATE takes the transaction's earlier rows with it") {
            auto session = otterbrix::session_id_t();
            REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());
            REQUIRE(run(dispatcher, session, "INSERT INTO ocdb.items (id, v) VALUES (9, 90);")->is_success());
            CHECK(run(dispatcher,
                      session,
                      "INSERT INTO ocdb.items (id, v) VALUES (1, 11), (1, 12) ON CONFLICT (id) DO UPDATE SET v = "
                      "excluded.v;")
                      ->is_error());
            std::ignore = run(dispatcher, session, "ROLLBACK;");
            CHECK(read_items(dispatcher) == SEEDED_ITEMS);
        }
    }
    SECTION("target resolution") {
        auto config = test_create_config(integration_fixture_path("insert_on_conflict/target_resolution"));
        test_clear_directory(config);
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        seed_items(dispatcher);

        SECTION("refused targets leave the table as it was") {
            auto session = otterbrix::session_id_t();
            INFO("no UNIQUE or PRIMARY KEY on the target column");
            CHECK(run(dispatcher, session, "INSERT INTO ocdb.items (id, v) VALUES (20, 1) ON CONFLICT (v) DO NOTHING;")
                      ->is_error());
            INFO("a constraint that does not exist");
            CHECK(run(dispatcher,
                      session,
                      "INSERT INTO ocdb.items (id, v) VALUES (20, 1) ON CONFLICT ON CONSTRAINT missing DO NOTHING;")
                      ->is_error());
            INFO("an expression target");
            CHECK(run(dispatcher,
                      session,
                      "INSERT INTO ocdb.items (id, v) VALUES (20, 1) ON CONFLICT ((id + 1)) DO NOTHING;")
                      ->is_error());
            INFO("a target WHERE over a column the table does not have");
            CHECK(run(dispatcher,
                      session,
                      "INSERT INTO ocdb.items (id, v) VALUES (20, 1) ON CONFLICT (id) WHERE missing > 0 DO NOTHING;")
                      ->is_error());
            INFO("DO UPDATE without a target");
            CHECK(run(dispatcher,
                      session,
                      "INSERT INTO ocdb.items (id, v) VALUES (1, 1) ON CONFLICT DO UPDATE SET v = 2;")
                      ->is_error());
            INFO("an unqualified column both the target and EXCLUDED have");
            CHECK(run(dispatcher,
                      session,
                      "INSERT INTO ocdb.items (id, v) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET v = v + 1;")
                      ->is_error());
            INFO("EXCLUDED in RETURNING");
            CHECK(run(dispatcher,
                      session,
                      "INSERT INTO ocdb.items (id, v) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET v = 2 "
                      "RETURNING excluded.v;")
                      ->is_error());
            CHECK(read_items(dispatcher) == SEEDED_ITEMS);
        }
        SECTION("a multi-column target matches its constraint in any column order") {
            require_ok(dispatcher, "CREATE TABLE ocdb.pairs (id bigint, v bigint, w bigint);");
            require_ok(dispatcher, "ALTER TABLE ocdb.pairs ADD CONSTRAINT uq_pairs UNIQUE (v, w);");
            require_ok(dispatcher, "INSERT INTO ocdb.pairs (id, v, w) VALUES (1, 1, 1);");
            CHECK(returned(dispatcher,
                           otterbrix::session_id_t(),
                           "INSERT INTO ocdb.pairs (id, v, w) VALUES (2, 1, 1), (3, 1, 2) ON CONFLICT (w, v) "
                           "DO NOTHING RETURNING id, v;") == items_t{{3, 1}});
            CHECK(returned(dispatcher,
                           otterbrix::session_id_t(),
                           "INSERT INTO ocdb.pairs (id, v, w) VALUES (4, 1, 1) ON CONFLICT (w, v) "
                           "DO UPDATE SET id = excluded.id RETURNING id, v;") == items_t{{4, 1}});
            CHECK(read_items(dispatcher, "ocdb.pairs") == items_t{{3, 1}, {4, 1}});
        }
    }
    SECTION("other unique keys") {
        auto config = test_create_config(integration_fixture_path("insert_on_conflict/other_unique_keys"));
        test_clear_directory(config);
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        require_ok(dispatcher, "CREATE DATABASE ocdb;");
        require_ok(dispatcher, "CREATE TABLE ocdb.items (id bigint, v bigint, w bigint);");
        require_ok(dispatcher, "INSERT INTO ocdb.items (id, v, w) VALUES (1, 10, 100), (2, 20, NULL);");
        require_ok(dispatcher, "ALTER TABLE ocdb.items ADD CONSTRAINT pk_items_id PRIMARY KEY (id);");
        require_ok(dispatcher, "ALTER TABLE ocdb.items ADD CONSTRAINT uq_items_w UNIQUE (w);");
        const items_t seeded{{1, 10}, {2, 20}};

        SECTION("a conflict on a constraint that is not the target still fails the statement") {
            CHECK(run(dispatcher,
                      otterbrix::session_id_t(),
                      "INSERT INTO ocdb.items (id, v, w) VALUES (3, 30, 100) ON CONFLICT (id) DO NOTHING RETURNING id, "
                      "v;")
                      ->is_error());
            CHECK(run(dispatcher,
                      otterbrix::session_id_t(),
                      "INSERT INTO ocdb.items (id, v, w) VALUES (3, 30, 100) ON CONFLICT (id) DO UPDATE SET v = 0 "
                      "RETURNING id, v;")
                      ->is_error());
            CHECK(read_items(dispatcher) == seeded);
        }
        SECTION("an update that moves a non-target key onto another row fails") {
            CHECK(run(dispatcher,
                      otterbrix::session_id_t(),
                      "INSERT INTO ocdb.items (id, v, w) VALUES (2, 0, 0) ON CONFLICT (id) DO UPDATE SET w = 100;")
                      ->is_error());
            CHECK(read_items(dispatcher) == seeded);
        }
        SECTION("inside a transaction the failed statement takes the transaction's earlier rows with it") {
            auto session = otterbrix::session_id_t();
            REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());
            REQUIRE(run(dispatcher, session, "INSERT INTO ocdb.items (id, v, w) VALUES (4, 40, 400);")->is_success());
            CHECK(run(dispatcher,
                      session,
                      "INSERT INTO ocdb.items (id, v, w) VALUES (5, 50, 100) ON CONFLICT (id) DO NOTHING;")
                      ->is_error());
            std::ignore = run(dispatcher, session, "ROLLBACK;");
            CHECK(read_items(dispatcher) == seeded);
        }
        SECTION("a NULL key never conflicts") {
            CHECK(returned(dispatcher,
                           otterbrix::session_id_t(),
                           "INSERT INTO ocdb.items (id, v, w) VALUES (6, 60, NULL), (7, 70, NULL) "
                           "ON CONFLICT (w) DO NOTHING RETURNING id, v;") == items_t{{6, 60}, {7, 70}});
            CHECK(returned(dispatcher,
                           otterbrix::session_id_t(),
                           "INSERT INTO ocdb.items (id, v, w) VALUES (8, 80, NULL) "
                           "ON CONFLICT (w) DO UPDATE SET v = 0 RETURNING id, v;") == items_t{{8, 80}});
            CHECK(read_items(dispatcher) == items_t{{1, 10}, {2, 20}, {6, 60}, {7, 70}, {8, 80}});
        }
        SECTION("a non-primary target") {
            CHECK(returned(dispatcher,
                           otterbrix::session_id_t(),
                           "INSERT INTO ocdb.items (id, v, w) VALUES (9, 90, 100) ON CONFLICT (w) "
                           "DO UPDATE SET v = excluded.v RETURNING id, v;") == items_t{{1, 90}});
            CHECK(read_items(dispatcher) == items_t{{1, 90}, {2, 20}});
        }
        SECTION("no target skips a row that conflicts on either constraint") {
            CHECK(returned(dispatcher,
                           otterbrix::session_id_t(),
                           "INSERT INTO ocdb.items (id, v, w) VALUES (1, 99, 999), (8, 80, 100), (9, 90, 900) "
                           "ON CONFLICT DO NOTHING RETURNING id, v;") == items_t{{9, 90}});
            CHECK(read_items(dispatcher) == items_t{{1, 10}, {2, 20}, {9, 90}});
        }
        SECTION("a row skipped for one constraint does not hide a later row that duplicates it on another") {
            // (11, w = 100) conflicts with the stored row on w. (13, w = 300) and (13, w = 301) share id, so the
            // second is skipped — and (14, w = 301) then duplicates only a row that was never written.
            CHECK(returned(dispatcher,
                           otterbrix::session_id_t(),
                           "INSERT INTO ocdb.items (id, v, w) VALUES (11, 110, 100), (13, 130, 300), "
                           "(13, 131, 301), (14, 140, 301) ON CONFLICT DO NOTHING RETURNING id, v;") ==
                  items_t{{13, 130}, {14, 140}});
            CHECK(read_items(dispatcher) == items_t{{1, 10}, {2, 20}, {13, 130}, {14, 140}});
        }
    }

    SECTION("across flushes") {
        auto config = test_create_config(integration_fixture_path("insert_on_conflict/across_flushes"));
        test_clear_directory(config);
        config.execution.dml_flush_row_threshold = 1000;
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        require_ok(dispatcher, "CREATE DATABASE ocdb;");
        require_ok(dispatcher, "CREATE TABLE ocdb.items (id bigint, v bigint);");
        require_ok(dispatcher, "ALTER TABLE ocdb.items ADD CONSTRAINT pk_items_id PRIMARY KEY (id);");

        SECTION("DO NOTHING") {
            // 1500 rows over 1100 keys: rows 1100..1499 repeat keys 0..399, in the first flush's rows and in their own.
            const auto rows = returned(dispatcher,
                                       otterbrix::session_id_t(),
                                       "INSERT INTO ocdb.items (id, v) VALUES " + values_list(1500, 1100) +
                                           " ON CONFLICT (id) DO NOTHING RETURNING id, v;");
            CHECK(rows.size() == 1100);
            const auto items = read_items(dispatcher);
            REQUIRE(items.size() == 1100);
            CHECK(items == rows);
            bool first_rows_kept = true;
            for (const auto& [id, value] : items) {
                if (value != id) {
                    first_rows_kept = false;
                }
            }
            CHECK(first_rows_kept);
        }
        SECTION("DO UPDATE") {
            // 1100 stored keys, then 1500 distinct proposed keys: 1100 conflicts — more than one chunk of pairs.
            require_ok(dispatcher, "INSERT INTO ocdb.items (id, v) VALUES " + values_list(1100, 1100) + ";");
            const auto rows = returned(dispatcher,
                                       otterbrix::session_id_t(),
                                       "INSERT INTO ocdb.items (id, v) VALUES " + values_list(1500, 1500, 10000) +
                                           " ON CONFLICT (id) DO UPDATE SET v = excluded.v + items.v RETURNING id, v;");
            CHECK(rows.size() == 1500);
            const auto items = read_items(dispatcher);
            REQUIRE(items.size() == 1500);
            CHECK(items == rows);
            bool every_row_resolved = true;
            for (const auto& [id, value] : items) {
                // A stored key holds id, so its update is (id + 10000) + id; a new key holds id + 10000.
                const auto expected = id < 1100 ? id + 10000 + id : id + 10000;
                if (value != expected) {
                    every_row_resolved = false;
                }
            }
            CHECK(every_row_resolved);
        }
    }

    SECTION("restart") {
        auto config = test_create_config(integration_fixture_path("insert_on_conflict/restart"));
        test_clear_directory(config);
        {
            test_spaces space(config);
            auto* dispatcher = space.dispatcher();
            seed_items(dispatcher);
            require_ok(dispatcher,
                       "INSERT INTO ocdb.items (id, v) VALUES (2, 99), (4, 40), (4, 41) ON CONFLICT (id) DO NOTHING;");
            require_ok(dispatcher,
                       "INSERT INTO ocdb.items (id, v) VALUES (3, 3), (5, 50) ON CONFLICT (id) "
                       "DO UPDATE SET v = items.v + excluded.v;");
            REQUIRE(read_items(dispatcher) == items_t{{1, 10}, {2, 20}, {3, 33}, {4, 40}, {5, 50}});
        }
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        CHECK(read_items(dispatcher) == items_t{{1, 10}, {2, 20}, {3, 33}, {4, 40}, {5, 50}});
        CHECK(returned(dispatcher,
                       otterbrix::session_id_t(),
                       "INSERT INTO ocdb.items (id, v) VALUES (4, 99), (6, 60) ON CONFLICT (id) DO NOTHING "
                       "RETURNING id, v;") == items_t{{6, 60}});
        CHECK(returned(dispatcher,
                       otterbrix::session_id_t(),
                       "INSERT INTO ocdb.items (id, v) VALUES (5, 5) ON CONFLICT (id) DO UPDATE SET v = excluded.v "
                       "RETURNING id, v;") == items_t{{5, 5}});
        CHECK(read_items(dispatcher) == items_t{{1, 10}, {2, 20}, {3, 33}, {4, 40}, {5, 5}, {6, 60}});
    }
}
