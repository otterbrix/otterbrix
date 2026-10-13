#include "concurrency.hpp"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <string>

using namespace concurrency;
using ints = std::vector<int64_t>;

ints values(session_t& session) { return column<int64_t>(session.run("SELECT v FROM db.t ORDER BY id;")); }

TEST_CASE("integration::cpp::concurrency::savepoint::interleaved", "[interleaved]") {
    engine_t engine("savepoints_interleaved");
    session_t first(engine, "first");
    session_t second(engine, "second");
    require_ok(first, "CREATE DATABASE db;");
    require_ok(first, "CREATE TABLE db.t (id bigint, v bigint);");
    require_ok(first, "INSERT INTO db.t (id, v) VALUES (1, 10), (2, 20), (3, 30);");

    SECTION("rollback to releases only the rows written after the savepoint") {
        interleaved({
            {first, "BEGIN;"},
            {first, "DELETE FROM db.t WHERE id = 1;"},
            {first, "SAVEPOINT after_first_delete;"},
            {first, "DELETE FROM db.t WHERE id = 2;"},
            {second, "DELETE FROM db.t WHERE id = 2;", fails},
            {first, "ROLLBACK TO SAVEPOINT after_first_delete;"},
            {second, "DELETE FROM db.t WHERE id = 2;"},
            {second, "DELETE FROM db.t WHERE id = 1;", fails},
            {first, "COMMIT;"},
        });
        CHECK(values(second) == ints{30});
    }

    SECTION("readers see neither side of a rollback to before the commit") {
        interleaved({
            {first, "BEGIN;"},
            {first, "SAVEPOINT before_changes;"},
            {first, "UPDATE db.t SET v = 11 WHERE id = 1;"},
            {first, "INSERT INTO db.t (id, v) VALUES (4, 40);"},
        });
        CHECK(values(second) == ints{10, 20, 30});
        interleaved({
            {first, "ROLLBACK TO SAVEPOINT before_changes;"},
            {first, "UPDATE db.t SET v = 21 WHERE id = 2;"},
        });
        CHECK(values(second) == ints{10, 20, 30});
        require_ok(first, "COMMIT;");
        CHECK(values(second) == ints{10, 21, 30});
    }

    SECTION("another session appends after the rows a rollback to removes") {
        interleaved({
            {first, "BEGIN;"},
            {first, "INSERT INTO db.t (id, v) VALUES (4, 40);"},
            {first, "SAVEPOINT before_append;"},
            {first, "INSERT INTO db.t (id, v) VALUES (5, 50);"},
            {second, "INSERT INTO db.t (id, v) VALUES (6, 60);"},
            {first, "ROLLBACK TO SAVEPOINT before_append;"},
        });
        // 50 is no longer the tail, so it is marked aborted in place; 60 committed after first's snapshot.
        CHECK(values(first) == ints{10, 20, 30, 40});
        require_ok(first, "COMMIT;");
        CHECK(values(second) == ints{10, 20, 30, 40, 60});
    }

    SECTION("a table created after the savepoint frees its name") {
        interleaved({
            {first, "BEGIN;"},
            {first, "SAVEPOINT before_create;"},
            {first, "CREATE TABLE db.contested (id bigint);"},
            {first, "ROLLBACK TO SAVEPOINT before_create;"},
            {second, "CREATE TABLE db.contested (id bigint);"},
            {first, "COMMIT;"},
        });
        require_ok(second, "INSERT INTO db.contested (id) VALUES (1);");
    }
}

TEST_CASE("integration::cpp::concurrency::savepoint::ddl_and_dml", "[interleaved]") {
    engine_t engine("savepoint_ddl_and_dml");
    session_t first(engine, "first");
    session_t second(engine, "second");
    require_ok(first, "CREATE DATABASE db;");
    require_ok(first, "CREATE TABLE db.t (id bigint, v bigint);");
    require_ok(first, "INSERT INTO db.t (id, v) VALUES (1, 10), (2, 20), (3, 30);");

    SECTION("a write into a table whose drop was rolled back to commits") {
        std::string write;
        ints after;
        SECTION("insert") {
            write = "INSERT INTO db.t (id, v) VALUES (4, 40);";
            after = {10, 20, 30, 40};
        }
        SECTION("update") {
            write = "UPDATE db.t SET v = 11 WHERE id = 1;";
            after = {11, 20, 30};
        }
        SECTION("delete") {
            write = "DELETE FROM db.t WHERE id = 1;";
            after = {20, 30};
        }
        interleaved({
            {second, "BEGIN;"},
            {second, write},
            {first, "BEGIN;"},
            {first, "SAVEPOINT before_drop;"},
            {first, "DROP TABLE db.t;"},
            {first, "ROLLBACK TO SAVEPOINT before_drop;"},
            {first, "COMMIT;"},
            {second, "COMMIT;"},
        });
        CHECK(values(second) == after);
    }

    SECTION("a column whose drop was rolled back to keeps the writes beside it") {
        interleaved({
            {first, "BEGIN;"},
            {first, "SAVEPOINT before_drop;"},
            {first, "ALTER TABLE db.t DROP COLUMN v;"},
            {second, "UPDATE db.t SET v = 21 WHERE id = 2;"},
            {first, "ROLLBACK TO SAVEPOINT before_drop;"},
            {first, "UPDATE db.t SET v = 11 WHERE id = 1;"},
            {first, "COMMIT;"},
        });
        CHECK(values(second) == ints{11, 21, 30});
    }

    SECTION("rows written beside an added column rolled back to keep only their own columns") {
        interleaved({
            {first, "BEGIN;"},
            {first, "SAVEPOINT before_add;"},
            {first, "ALTER TABLE db.t ADD COLUMN extra bigint;"},
            {first, "INSERT INTO db.t (id, v, extra) VALUES (4, 40, 400);"},
            {second, "INSERT INTO db.t (id, v) VALUES (5, 50);"},
            {second, "UPDATE db.t SET v = 21 WHERE id = 2;"},
            {first, "ROLLBACK TO SAVEPOINT before_add;"},
            {first, "INSERT INTO db.t (id, v) VALUES (6, 60);"},
            {first, "COMMIT;"},
            {second, "SELECT extra FROM db.t;", fails},
        });
        CHECK(values(second) == ints{10, 21, 30, 50, 60});
    }

    SECTION("a name taken after the savepoint is free to another once rolled back to") {
        std::string ddl;
        SECTION("table") { ddl = "CREATE TABLE db.contested (id bigint);"; }
        SECTION("column") { ddl = "ALTER TABLE db.t ADD COLUMN contested bigint;"; }
        SECTION("table dropped") { ddl = "DROP TABLE db.t;"; }
        SECTION("column dropped") { ddl = "ALTER TABLE db.t DROP COLUMN v;"; }
        interleaved({
            {first, "BEGIN;"},
            {first, "SAVEPOINT before_ddl;"},
            {first, ddl},
            {second, ddl, fails},
            {first, "ROLLBACK TO SAVEPOINT before_ddl;"},
            {second, ddl},
            {first, "COMMIT;"},
        });
    }

    SECTION("DDL before the savepoint stays held, DDL after it is released") {
        interleaved({
            {first, "BEGIN;"},
            {first, "ALTER TABLE db.t ADD COLUMN kept bigint;"},
            {first, "SAVEPOINT after_kept;"},
            {first, "ALTER TABLE db.t ADD COLUMN released bigint;"},
            {first, "ROLLBACK TO SAVEPOINT after_kept;"},
            {second, "ALTER TABLE db.t ADD COLUMN kept bigint;", fails},
            {second, "ALTER TABLE db.t ADD COLUMN released bigint;"},
            {first, "COMMIT;"},
            {second, "SELECT kept, released FROM db.t;"},
        });
    }

    SECTION("rows written after the savepoint beside another's committed ADD COLUMN") {
        interleaved({
            {first, "BEGIN;"},
            {first, "INSERT INTO db.t (id, v) VALUES (4, 40);"},
            {first, "SAVEPOINT before_rows;"},
            {first, "INSERT INTO db.t (id, v) VALUES (5, 50);"},
            {first, "DELETE FROM db.t WHERE id = 1;"},
            {second, "ALTER TABLE db.t ADD COLUMN extra bigint DEFAULT 7;"},
            {first, "ROLLBACK TO SAVEPOINT before_rows;"},
            {first, "COMMIT;"},
        });
        CHECK(values(second) == ints{10, 20, 30, 40});
        CHECK(column<int64_t>(second.run("SELECT id FROM db.t WHERE extra = 7 ORDER BY id;")) == ints{1, 2, 3, 4});
    }
}

TEST_CASE("integration::cpp::concurrency::savepoint::parallel", "[parallel]") {
    engine_t engine("savepoints_parallel");
    constexpr std::size_t sessions = 4;
    {
        session_t setup(engine, "setup");
        require_ok(setup, "CREATE DATABASE db;");
        require_ok(setup, "CREATE TABLE db.t (id bigint, v bigint);");
    }

    // Each session keeps what it wrote before its savepoint and rolls back what it wrote after. Catch2 isn't
    // thread-safe, so the sessions only count refusals; the main thread asserts.
    std::atomic<int> refused{0};
    in_parallel(engine, sessions, [&refused](std::size_t index, session_t& session) {
        const auto base = static_cast<int64_t>(index) * 1000;
        const auto run = [&](const std::string& sql) { refused += session.run(sql)->is_success() ? 0 : 1; };
        const auto insert = [&](int64_t first, int64_t count) {
            std::string sql = "INSERT INTO db.t (id, v) VALUES ";
            for (int64_t id = first; id < first + count; ++id) {
                sql += "(" + std::to_string(id) + ", " + std::to_string(id) + ")";
                sql += id + 1 == first + count ? ";" : ", ";
            }
            run(sql);
        };
        run("BEGIN;");
        insert(base, 10);
        run("SAVEPOINT before_rest;");
        insert(base + 100, 10);
        run("DELETE FROM db.t WHERE id >= " + std::to_string(base) + " AND id < " + std::to_string(base + 5) + ";");
        run("ROLLBACK TO SAVEPOINT before_rest;");
        run("COMMIT;");
    });
    CHECK(refused.load() == 0);
    // Serial runs would make every rolled-back append the table's tail.
    CHECK(engine.most_calls_at_once() >= 2);

    session_t reader(engine, "reader");
    ints expected;
    for (std::size_t index = 0; index < sessions; ++index) {
        for (int64_t offset = 0; offset < 10; ++offset) {
            expected.push_back(static_cast<int64_t>(index) * 1000 + offset);
        }
    }
    CHECK(values(reader) == expected);
}

TEST_CASE("integration::cpp::concurrency::savepoint::parallel_ddl_and_dml", "[parallel]") {
    engine_t engine("savepoint_parallel_ddl_and_dml");
    constexpr std::size_t sessions = 4;
    constexpr int rounds = 5;
    {
        session_t setup(engine, "setup");
        require_ok(setup, "CREATE DATABASE db;");
        require_ok(setup, "CREATE TABLE db.t (id bigint, v bigint);");
    }

    // Session 0 adds a column after a savepoint and rolls back to it, round after round; the others write the
    // same table around it. The ADD may be refused under contention: ROLLBACK TO must recover the transaction
    // either way. Catch2 isn't thread-safe, so the sessions count; the main thread asserts.
    std::atomic<int> refused{0};
    std::atomic<int> added{0};
    in_parallel(engine, sessions, [&refused, &added](std::size_t index, session_t& session) {
        const auto run = [&](const std::string& sql) { refused += session.run(sql)->is_success() ? 0 : 1; };
        for (int round = 0; round < rounds; ++round) {
            run("BEGIN;");
            run("SAVEPOINT before_rest;");
            if (index == 0) {
                added += session.run("ALTER TABLE db.t ADD COLUMN extra bigint;")->is_success() ? 1 : 0;
            } else {
                const auto id = static_cast<int64_t>(index) * 1000 + round;
                run("INSERT INTO db.t (id, v) VALUES (" + std::to_string(id + 100) + ", 0);");
                run("ROLLBACK TO SAVEPOINT before_rest;");
                run("INSERT INTO db.t (id, v) VALUES (" + std::to_string(id) + ", " + std::to_string(id) + ");");
                run("SAVEPOINT before_delete;");
                run("DELETE FROM db.t WHERE id = " + std::to_string(id) + ";");
                run("ROLLBACK TO SAVEPOINT before_delete;");
            }
            if (index == 0) {
                run("ROLLBACK TO SAVEPOINT before_rest;");
            }
            run("COMMIT;");
        }
    });
    CHECK(refused.load() == 0);
    CHECK(added.load() > 0);
    CHECK(engine.most_calls_at_once() >= 2);

    session_t reader(engine, "reader");
    ints expected;
    for (std::size_t index = 1; index < sessions; ++index) {
        for (int round = 0; round < rounds; ++round) {
            expected.push_back(static_cast<int64_t>(index) * 1000 + round);
        }
    }
    CHECK(values(reader) == expected);
    CHECK(reader.run("SELECT extra FROM db.t;")->is_error());
}
