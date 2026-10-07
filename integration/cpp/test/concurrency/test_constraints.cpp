#include "concurrency.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <map>
#include <tuple>

using namespace concurrency;
using ints = std::vector<int64_t>;

namespace {
    ints values(session_t& session) { return column<int64_t>(session.run("SELECT v FROM db.t ORDER BY id;")); }
} // namespace

TEST_CASE("integration::cpp::concurrency::unique::interleaved", "[interleaved]") {
    const std::string key = GENERATE(as<std::string>{}, "UNIQUE", "PRIMARY KEY");
    engine_t engine("unique_interleaved");
    session_t first(engine, "first");
    session_t second(engine, "second");
    require_ok(first, "CREATE DATABASE db;");
    require_ok(first, "CREATE TABLE db.t (id bigint, v bigint);");
    require_ok(first, "INSERT INTO db.t (id, v) VALUES (1, 10), (2, 20), (3, 30);");
    require_ok(first, "ALTER TABLE db.t ADD CONSTRAINT key_t_id " + key + " (id);");

    SECTION("2 inserts") {
        interleaved({
            {first, "BEGIN;"},
            {second, "BEGIN;"},
            {first, "INSERT INTO db.t (id, v) VALUES (7, 70);"},
            {second, "INSERT INTO db.t (id, v) VALUES (7, 71);"},
            {first, "COMMIT;"},
            {second, "COMMIT;", fails},
        });
        CHECK(values(first) == ints{10, 20, 30, 70});
    }
    SECTION("insert and update") {
        interleaved({
            {first, "BEGIN;"},
            {second, "BEGIN;"},
            {first, "UPDATE db.t SET id = 7 WHERE id = 1;"},
            {second, "INSERT INTO db.t (id, v) VALUES (7, 71);"},
            {first, "COMMIT;"},
            {second, "COMMIT;", fails},
        });
        CHECK(column<int64_t>(first.run("SELECT v FROM db.t WHERE id = 7;")) == ints{10});
    }
    SECTION("insert rollback insert") {
        interleaved({
            {first, "BEGIN;"},
            {second, "BEGIN;"},
            {first, "INSERT INTO db.t (id, v) VALUES (7, 70);"},
            {second, "INSERT INTO db.t (id, v) VALUES (7, 71);"},
            {first, "ROLLBACK;"},
            {second, "COMMIT;"},
        });
        CHECK(values(first) == ints{10, 20, 30, 71});
    }
    SECTION("delete and insert") {
        interleaved({
            {first, "BEGIN;"},
            {first, "DELETE FROM db.t WHERE id = 2;"},
            {second, "INSERT INTO db.t (id, v) VALUES (2, 22);", fails},
            {first, "COMMIT;"},
            {second, "INSERT INTO db.t (id, v) VALUES (2, 22);"},
        });
        CHECK(values(first) == ints{10, 22, 30});
    }
}

TEST_CASE("integration::cpp::concurrency::unique::overlapping", "[overlapping]") {
    engine_t engine("unique_overlapping");
    session_t first(engine, "first");
    session_t second(engine, "second");
    require_ok(first, "CREATE DATABASE db;");
    require_ok(first, "CREATE TABLE db.big (id bigint, v bigint);");
    insert_numbered_rows(first, "db.big", PAUSABLE_TABLE_ROWS);
    require_ok(first, "CREATE TABLE db.t (id bigint, v bigint);");
    require_ok(first, "ALTER TABLE db.t ADD CONSTRAINT uq_t_id UNIQUE (id);");

    const int64_t contested = GENERATE(int64_t{0}, PAUSABLE_TABLE_ROWS - 1);
    INFO("contested key " << contested << (contested == 0 ? ", already read" : ", not read yet"));
    paused_statement_t copy(first, "INSERT INTO db.t (id, v) SELECT id, v FROM db.big;");
    REQUIRE(copy.paused());
    const bool key_inserted =
        second.run("INSERT INTO db.t (id, v) VALUES (" + std::to_string(contested) + ", -1);")->is_success();
    REQUIRE(copy.still_running());
    const bool copied = copy.release()->is_success();

    INFO("INSERT ... SELECT " << (copied ? "committed" : "failed") << ", INSERT "
                              << (key_inserted ? "committed" : "failed"));
    CHECK(copied != key_inserted);
    CHECK(column<int64_t>(second.run("SELECT id FROM db.t WHERE id = " + std::to_string(contested) + ";")).size() == 1);
}

TEST_CASE("integration::cpp::concurrency::unique::parallel", "[parallel]") {
    constexpr std::size_t SESSIONS = 8;
    constexpr int64_t KEYS = 13;
    engine_t engine("unique_parallel");
    session_t admin(engine, "admin");
    require_ok(admin, "CREATE DATABASE db;");
    require_ok(admin, "CREATE TABLE db.t (id bigint, v bigint);");
    require_ok(admin, "ALTER TABLE db.t ADD CONSTRAINT uq_t_id UNIQUE (id);");

    // Every session inserts every key, each in its own transaction.
    std::vector<std::vector<int64_t>> won(SESSIONS);
    in_parallel(engine, SESSIONS, [&](std::size_t index, session_t& session) {
        for (int64_t key = 0; key < KEYS; ++key) {
            std::ignore = session.run("BEGIN;");
            const bool inserted =
                session.run("INSERT INTO db.t (id, v) VALUES (" + std::to_string(key) + ", 0);")->is_success();
            if (inserted && session.run("COMMIT;")->is_success()) {
                won[index].push_back(key);
            } else {
                std::ignore = session.run("ROLLBACK;");
            }
        }
    });
    CHECK(engine.most_calls_at_once() >= 2);
    std::map<int64_t, int> commits_per_key;
    for (const auto& keys : won) {
        for (const auto key : keys) {
            ++commits_per_key[key];
        }
    }
    for (int64_t key = 0; key < KEYS; ++key) {
        INFO("key " << key);
        CHECK(commits_per_key[key] == 1);
    }
    ints all_keys;
    for (int64_t key = 0; key < KEYS; ++key) {
        all_keys.push_back(key);
    }
    CHECK(column<int64_t>(admin.run("SELECT id FROM db.t ORDER BY id;")) == all_keys);
}

TEST_CASE("integration::cpp::concurrency::on_conflict::interleaved", "[interleaved]") {
    engine_t engine("on_conflict_interleaved");
    session_t first(engine, "first");
    session_t second(engine, "second");
    require_ok(first, "CREATE DATABASE db;");
    require_ok(first, "CREATE TABLE db.t (id bigint, v bigint);");
    require_ok(first, "INSERT INTO db.t (id, v) VALUES (1, 10), (2, 20), (3, 30);");
    require_ok(first, "ALTER TABLE db.t ADD CONSTRAINT uq_t_id UNIQUE (id);");

    SECTION("DO NOTHING twice") {
        interleaved({
            {first, "BEGIN;"},
            {second, "BEGIN;"},
            {first, "INSERT INTO db.t (id, v) VALUES (7, 70) ON CONFLICT (id) DO NOTHING;"},
            {second, "INSERT INTO db.t (id, v) VALUES (7, 71) ON CONFLICT (id) DO NOTHING;"},
            {first, "COMMIT;"},
            {second, "COMMIT;", fails},
        });
        CHECK(values(first) == ints{10, 20, 30, 70});
    }
    SECTION("DO NOTHING on a key committed after the snapshot") {
        interleaved({
            {second, "BEGIN;"},
            {second, "SELECT v FROM db.t;"},
            {first, "INSERT INTO db.t (id, v) VALUES (7, 70);"},
            {second, "INSERT INTO db.t (id, v) VALUES (7, 71) ON CONFLICT (id) DO NOTHING;"},
            {second, "COMMIT;", fails},
        });
        CHECK(values(first) == ints{10, 20, 30, 70});
    }
    SECTION("DO UPDATE of a row another transaction changed") {
        std::string change;
        ints after;
        SECTION("updated") {
            change = "UPDATE db.t SET v = 11 WHERE id = 1;";
            after = {11, 20, 30};
        }
        SECTION("deleted") {
            change = "DELETE FROM db.t WHERE id = 1;";
            after = {20, 30};
        }
        SECTION("upserted") {
            change = "INSERT INTO db.t (id, v) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET v = t.v + excluded.v;";
            after = {11, 20, 30};
        }
        interleaved({
            {first, "BEGIN;"},
            {second, "BEGIN;"},
            {first, change},
            {second,
             "INSERT INTO db.t (id, v) VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET v = t.v + excluded.v;",
             fails},
            {first, "COMMIT;"},
            {second, "COMMIT;", fails},
        });
        CHECK(values(first) == after);
    }
}

TEST_CASE("integration::cpp::concurrency::foreign_key::interleaved", "[interleaved]") {
    const std::string action = GENERATE(as<std::string>{}, "RESTRICT", "CASCADE");
    engine_t engine("foreign_key_interleaved");
    session_t parent(engine, "parent");
    session_t child(engine, "child");
    require_ok(parent, "CREATE DATABASE db;");
    require_ok(parent, "CREATE TABLE db.parents (id bigint);");
    require_ok(parent, "INSERT INTO db.parents (id) VALUES (1), (2);");
    require_ok(parent, "CREATE TABLE db.children (id bigint, parent_id bigint);");
    require_ok(parent,
               "ALTER TABLE db.children ADD CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES db.parents (id) "
               "ON DELETE " +
                   action + ";");

    SECTION("the parent's delete writes first") {
        interleaved({
            {parent, "BEGIN;"},
            {child, "BEGIN;"},
            {parent, "DELETE FROM db.parents WHERE id = 1;"},
            {child, "INSERT INTO db.children (id, parent_id) VALUES (1, 1);"},
            {parent, "COMMIT;"},
            {child, "COMMIT;", fails},
        });
        CHECK(column<int64_t>(child.run("SELECT id FROM db.parents ORDER BY id;")) == ints{2});
        CHECK(child.run("SELECT id FROM db.children;")->size() == 0);
    }
    SECTION("the child's insert writes first") {
        interleaved({
            {child, "BEGIN;"},
            {parent, "BEGIN;"},
            {child, "INSERT INTO db.children (id, parent_id) VALUES (1, 1);"},
            {parent, "DELETE FROM db.parents WHERE id = 1;"},
            {child, "COMMIT;"},
            {parent, "COMMIT;", fails},
        });
        CHECK(column<int64_t>(child.run("SELECT id FROM db.parents ORDER BY id;")) == ints{1, 2});
        CHECK(column<int64_t>(child.run("SELECT parent_id FROM db.children;")) == ints{1});
    }
}
