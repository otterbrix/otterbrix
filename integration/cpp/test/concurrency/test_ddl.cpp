#include "concurrency.hpp"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <thread>
#include <tuple>

using namespace concurrency;
using ints = std::vector<int64_t>;

namespace {
    ints values(session_t& session) { return column<int64_t>(session.run("SELECT v FROM db.t ORDER BY id;")); }
} // namespace

TEST_CASE("integration::cpp::concurrency::ddl_visibility::interleaved", "[interleaved]") {
    engine_t engine("ddl_visibility_interleaved");
    session_t alterer(engine, "alterer");
    session_t other(engine, "other");
    require_ok(alterer, "CREATE DATABASE db;");
    require_ok(alterer, "CREATE TABLE db.t (id bigint, v bigint);");
    require_ok(alterer, "INSERT INTO db.t (id, v) VALUES (1, 10), (2, 20), (3, 30);");

    SECTION("an open CREATE TABLE is invisible") {
        interleaved({
            {alterer, "BEGIN;"},
            {alterer, "CREATE TABLE db.fresh (id bigint);"},
            {alterer, "INSERT INTO db.fresh (id) VALUES (1);"},
            {other, "SELECT id FROM db.fresh;", fails},
            {other, "INSERT INTO db.fresh (id) VALUES (2);", fails},
        });
        SECTION("until it commits, with its rows") {
            require_ok(alterer, "COMMIT;");
            CHECK(column<int64_t>(other.run("SELECT id FROM db.fresh;")) == ints{1});
        }
        SECTION("and a rollback frees its name") {
            require_ok(alterer, "ROLLBACK;");
            require_ok(other, "CREATE TABLE db.fresh (id bigint);");
            CHECK(other.run("SELECT id FROM db.fresh;")->size() == 0);
        }
    }
    SECTION("an open DROP TABLE leaves the table to others") {
        interleaved({
            {alterer, "BEGIN;"},
            {alterer, "DROP TABLE db.t;"},
            {other, "SELECT v FROM db.t;"},
        });
        SECTION("until it commits") {
            require_ok(alterer, "COMMIT;");
            CHECK(other.run("SELECT v FROM db.t;")->is_error());
        }
        SECTION("and after a rollback") {
            require_ok(alterer, "ROLLBACK;");
            CHECK(values(other) == ints{10, 20, 30});
        }
    }
    SECTION("an open DROP COLUMN leaves the column to others until it commits") {
        interleaved({
            {alterer, "BEGIN;"},
            {alterer, "ALTER TABLE db.t DROP COLUMN v;"},
            {other, "SELECT v FROM db.t;"},
            {alterer, "COMMIT;"},
            {other, "SELECT v FROM db.t;", fails},
        });
    }
    SECTION("rows written beside an open ADD COLUMN take its default") {
        std::string add;
        std::string filled;
        SECTION("none") {
            add = "ALTER TABLE db.t ADD COLUMN extra bigint;";
            filled = "extra IS NULL";
        }
        SECTION("DEFAULT 7") {
            add = "ALTER TABLE db.t ADD COLUMN extra bigint DEFAULT 7;";
            filled = "extra = 7";
        }
        interleaved({
            {alterer, "BEGIN;"},
            {alterer, add},
            {alterer, "INSERT INTO db.t (id, v, extra) VALUES (4, 40, 400);"},
            {other, "INSERT INTO db.t (id, v) VALUES (5, 50);"},
            {other, "UPDATE db.t SET v = 21 WHERE id = 2;"},
            {other, "SELECT extra FROM db.t;", fails},
            {alterer, "COMMIT;"},
        });
        CHECK(values(other) == ints{10, 21, 30, 40, 50});
        CHECK(column<int64_t>(other.run("SELECT id FROM db.t WHERE " + filled + " ORDER BY id;")) == ints{1, 2, 3, 5});
        CHECK(column<int64_t>(other.run("SELECT extra FROM db.t WHERE id = 4;")) == ints{400});
    }
}

TEST_CASE("integration::cpp::concurrency::ddl_conflicts::interleaved", "[interleaved]") {
    engine_t engine("ddl_conflicts_interleaved");
    session_t first(engine, "first");
    session_t second(engine, "second");
    require_ok(first, "CREATE DATABASE db;");
    require_ok(first, "CREATE TABLE db.t (id bigint, v bigint);");
    require_ok(first, "INSERT INTO db.t (id, v) VALUES (1, 10), (2, 20), (3, 30);");

    SECTION("a name an open transaction takes is refused to another") {
        std::string first_ddl;
        std::string second_ddl;
        SECTION("table") { first_ddl = second_ddl = "CREATE TABLE db.contested (id bigint);"; }
        SECTION("database") { first_ddl = second_ddl = "CREATE DATABASE contested;"; }
        SECTION("index") { first_ddl = second_ddl = "CREATE INDEX contested ON db.t (v);"; }
        SECTION("column") { first_ddl = second_ddl = "ALTER TABLE db.t ADD COLUMN contested bigint;"; }
        SECTION("column renamed onto it") {
            first_ddl = "ALTER TABLE db.t ADD COLUMN contested bigint;";
            second_ddl = "ALTER TABLE db.t RENAME COLUMN v TO contested;";
        }
        SECTION("table dropped") { first_ddl = second_ddl = "DROP TABLE db.t;"; }
        SECTION("column dropped") { first_ddl = second_ddl = "ALTER TABLE db.t DROP COLUMN v;"; }
        interleaved({
            {first, "BEGIN;"},
            {second, "BEGIN;"},
            {first, first_ddl},
            {second, second_ddl, fails},
            {first, "COMMIT;"},
            {second, "COMMIT;", fails},
        });
    }

    SECTION("a dropped column's name") {
        SECTION("is free to the transaction that dropped it") {
            interleaved({
                {first, "BEGIN;"},
                {first, "ALTER TABLE db.t DROP COLUMN v;"},
                {first, "ALTER TABLE db.t ADD COLUMN v bigint;"},
                {first, "COMMIT;"},
            });
        }
        SECTION("is free once the drop committed") {
            interleaved({
                {first, "ALTER TABLE db.t DROP COLUMN v;"},
                {second, "ALTER TABLE db.t ADD COLUMN v bigint;"},
            });
        }
        SECTION("is taken while the drop is open") {
            interleaved({
                {first, "BEGIN;"},
                {first, "ALTER TABLE db.t DROP COLUMN v;"},
                {second, "ALTER TABLE db.t ADD COLUMN v bigint;", fails},
                {first, "ROLLBACK;"},
            });
        }
    }

    SECTION("a write into a table dropped under it is refused at COMMIT") {
        std::string write;
        SECTION("insert") { write = "INSERT INTO db.t (id, v) VALUES (4, 40);"; }
        SECTION("update") { write = "UPDATE db.t SET v = 11 WHERE id = 1;"; }
        SECTION("delete") { write = "DELETE FROM db.t WHERE id = 1;"; }
        interleaved({
            {first, "BEGIN;"},
            {first, write},
            {second, "DROP TABLE db.t;"},
            {first, "COMMIT;", fails},
            {second, "SELECT v FROM db.t;", fails},
        });
    }

    SECTION("NOT NULL without a default") {
        SECTION("is refused over committed rows") {
            interleaved({
                {first, "ALTER TABLE db.t ADD COLUMN extra bigint NOT NULL;", fails},
                {first, "SELECT extra FROM db.t;", fails},
            });
        }
        SECTION("and an open insert cannot both commit") {
            require_ok(first, "CREATE TABLE db.empty (id bigint);");
            SECTION("the ADD commits first") {
                interleaved({
                    {first, "BEGIN;"},
                    {first, "ALTER TABLE db.empty ADD COLUMN extra bigint NOT NULL;"},
                    {second, "BEGIN;"},
                    {second, "INSERT INTO db.empty (id) VALUES (1);"},
                    {first, "COMMIT;"},
                    {second, "COMMIT;", fails},
                    {second, "SELECT extra FROM db.empty;"},
                });
                CHECK(second.run("SELECT id FROM db.empty;")->size() == 0);
            }
            SECTION("the INSERT commits first") {
                interleaved({
                    {first, "BEGIN;"},
                    {first, "ALTER TABLE db.empty ADD COLUMN extra bigint NOT NULL;"},
                    {second, "BEGIN;"},
                    {second, "INSERT INTO db.empty (id) VALUES (1);"},
                    {second, "COMMIT;"},
                    {first, "COMMIT;", fails},
                    {second, "SELECT extra FROM db.empty;", fails},
                });
                CHECK(second.run("SELECT id FROM db.empty;")->size() == 1);
            }
        }
    }
}

TEST_CASE("integration::cpp::concurrency::ddl_conflicts::parallel", "[parallel]") {
    constexpr std::size_t SESSIONS = 8;
    engine_t engine("ddl_conflicts_parallel");
    session_t admin(engine, "admin");
    require_ok(admin, "CREATE DATABASE db;");
    require_ok(admin, "CREATE TABLE db.t (id bigint, v bigint);");

    SECTION("one name created by every session at once is created once") {
        std::string create;
        std::string use_it;
        SECTION("table") {
            create = "CREATE TABLE db.contested (id bigint);";
            use_it = "SELECT id FROM db.contested;";
        }
        SECTION("database") {
            create = "CREATE DATABASE contested;";
            use_it = "CREATE TABLE contested.inside (id bigint);";
        }
        SECTION("index") {
            create = "CREATE INDEX contested ON db.t (v);";
            use_it = "DROP INDEX db.t.contested;";
        }
        SECTION("column") {
            create = "ALTER TABLE db.t ADD COLUMN contested bigint;";
            use_it = "SELECT contested FROM db.t;";
        }
        std::atomic<int> created{0};
        in_parallel(engine, SESSIONS, [&](std::size_t, session_t& session) {
            created += session.run(create)->is_success() ? 1 : 0;
        });
        CHECK(engine.most_calls_at_once() >= 2);
        CHECK(created.load() == 1);
        require_ok(admin, use_it);
    }

    SECTION("a table dropped under writers stays dropped") {
        // Session 0 drops once every other session has its inserts under way.
        std::atomic<std::size_t> writing{0};
        in_parallel(engine, SESSIONS, [&](std::size_t index, session_t& session) {
            if (index == 0) {
                while (writing.load() < SESSIONS - 1) {
                    std::this_thread::yield();
                }
                std::ignore = session.run("DROP TABLE db.t;");
                return;
            }
            for (int statement = 0; statement < 31; ++statement) {
                writing += statement == 1 ? 1 : 0;
                const auto id = std::to_string(1000 * static_cast<int64_t>(index) + statement);
                std::ignore = session.run("INSERT INTO db.t (id, v) VALUES (" + id + ", 0);");
            }
        });
        CHECK(engine.most_calls_at_once() >= 2);
        CHECK(admin.run("SELECT id FROM db.t;")->is_error());
        require_ok(admin, "CREATE TABLE db.t (id bigint, v bigint);");
        CHECK(admin.run("SELECT id FROM db.t;")->size() == 0);
    }
}
