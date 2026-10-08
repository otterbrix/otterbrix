#include "concurrency.hpp"

#include <catch2/catch_test_macros.hpp>

#include <set>
#include <tuple>

using namespace concurrency;
using ints = std::vector<int64_t>;

namespace {
    ints values(session_t& session) { return column<int64_t>(session.run("SELECT v FROM db.t ORDER BY id;")); }
} // namespace

TEST_CASE("integration::cpp::concurrency::visibility::interleaved", "[interleaved]") {
    engine_t engine("visibility_interleaved");
    session_t writer(engine, "writer");
    session_t reader(engine, "reader");
    require_ok(writer, "CREATE DATABASE db;");
    require_ok(writer, "CREATE TABLE db.t (id bigint, v bigint);");
    require_ok(writer, "INSERT INTO db.t (id, v) VALUES (1, 10), (2, 20), (3, 30);");

    SECTION("open writes are invisible, committed ones visible") {
        std::string write;
        ints written;
        SECTION("insert") {
            write = "INSERT INTO db.t (id, v) VALUES (4, 40);";
            written = {10, 20, 30, 40};
        }
        SECTION("update") {
            write = "UPDATE db.t SET v = 99 WHERE id = 2;";
            written = {10, 99, 30};
        }
        SECTION("delete") {
            write = "DELETE FROM db.t WHERE id = 2;";
            written = {10, 30};
        }
        require_ok(writer, "BEGIN;");
        require_ok(writer, write);
        CHECK(values(writer) == written);
        CHECK(values(reader) == ints{10, 20, 30});
        require_ok(writer, "COMMIT;");
        CHECK(values(reader) == written);
    }

    SECTION("rolled-back writes never show, and leave the rows writable") {
        require_ok(writer, "BEGIN;");
        require_ok(writer, "INSERT INTO db.t (id, v) VALUES (4, 40);");
        require_ok(writer, "UPDATE db.t SET v = 99 WHERE id = 2;");
        require_ok(writer, "DELETE FROM db.t WHERE id = 3;");
        require_ok(writer, "ROLLBACK;");
        CHECK(values(reader) == ints{10, 20, 30});
        require_ok(reader, "UPDATE db.t SET v = 21 WHERE id = 2;");
        require_ok(reader, "DELETE FROM db.t WHERE id = 3;");
        CHECK(values(writer) == ints{10, 21});
    }

    SECTION("a snapshot does not move") {
        require_ok(reader, "BEGIN;");
        CHECK(values(reader) == ints{10, 20, 30});
        require_ok(writer, "INSERT INTO db.t (id, v) VALUES (4, 40);");
        require_ok(writer, "UPDATE db.t SET v = 99 WHERE id = 2;");
        require_ok(writer, "DELETE FROM db.t WHERE id = 3;");
        CHECK(values(reader) == ints{10, 20, 30});
        CHECK(column<int64_t>(reader.run("SELECT id FROM db.t WHERE v > 15 ORDER BY id;")) == ints{2, 3});
        require_ok(reader, "COMMIT;");
        CHECK(values(reader) == ints{10, 99, 40});
    }

    SECTION("own writes show, a concurrent commit does not") {
        require_ok(reader, "BEGIN;");
        CHECK(values(reader) == ints{10, 20, 30});
        require_ok(reader, "UPDATE db.t SET v = 11 WHERE id = 1;");
        require_ok(writer, "INSERT INTO db.t (id, v) VALUES (5, 50);");
        CHECK(values(reader) == ints{11, 20, 30});
        require_ok(reader, "COMMIT;");
        CHECK(values(writer) == ints{11, 20, 30, 50});
    }

    SECTION("commits show in commit order") {
        session_t observer(engine, "observer");
        require_ok(writer, "BEGIN;");
        require_ok(writer, "INSERT INTO db.t (id, v) VALUES (4, 40);");
        require_ok(reader, "BEGIN;");
        require_ok(reader, "INSERT INTO db.t (id, v) VALUES (5, 50);");
        CHECK(values(observer) == ints{10, 20, 30});
        require_ok(reader, "COMMIT;");
        CHECK(values(observer) == ints{10, 20, 30, 50});
        CHECK(values(writer) == ints{10, 20, 30, 40});
        require_ok(writer, "COMMIT;");
        CHECK(values(observer) == ints{10, 20, 30, 40, 50});
    }

    SECTION("an open writer does not hold readers back") {
        interleaved({
            {writer, "BEGIN;"},
            {writer, "UPDATE db.t SET v = v + 1;"},
            {writer, "DELETE FROM db.t WHERE id = 1;"},
            {reader, "SELECT v FROM db.t;"},
        });
        CHECK(values(reader) == ints{10, 20, 30});
    }

    SECTION("a failed transaction leaves the other alone") {
        interleaved({
            {writer, "BEGIN;"},
            {reader, "BEGIN;"},
            {writer, "INSERT INTO db.t (id, v) VALUES (4, 40);"},
            {reader, "INSERT INTO db.t (id, v) VALUES (5, 50);"},
            {writer, "INSERT INTO db.missing (id) VALUES (1);", fails},
            {reader, "UPDATE db.t SET v = 21 WHERE id = 2;"},
            {writer, "ROLLBACK;"},
            {reader, "COMMIT;"},
        });
        CHECK(values(writer) == ints{10, 21, 30, 50});
    }
}

TEST_CASE("integration::cpp::concurrency::visibility::overlapping", "[overlapping]") {
    engine_t engine("visibility_overlapping");
    session_t first(engine, "first");
    session_t second(engine, "second");
    require_ok(first, "CREATE DATABASE db;");
    require_ok(first, "CREATE TABLE db.big (id bigint, v bigint);");
    insert_numbered_rows(first, "db.big", PAUSABLE_TABLE_ROWS);
    const auto rows_where = [&](const std::string& condition) {
        return column<uint64_t>(second.run("SELECT COUNT(*) FROM db.big WHERE " + condition + ";")).front();
    };
    const auto all_rows = static_cast<uint64_t>(PAUSABLE_TABLE_ROWS);

    SECTION("a paused read keeps its snapshot while writes commit") {
        paused_statement_t read(first, "SELECT id, v FROM db.big;");
        REQUIRE(read.paused());
        require_ok(second, "UPDATE db.big SET v = -1 WHERE id >= 0;");
        require_ok(second, "DELETE FROM db.big WHERE id < 100;");
        require_ok(second, "INSERT INTO db.big (id, v) VALUES (" + std::to_string(PAUSABLE_TABLE_ROWS) + ", 0);");
        REQUIRE(read.still_running());

        const auto result = read.release();
        const auto ids = column<int64_t>(result, 0);
        const auto vs = column<int64_t>(result, 1);
        CHECK(ids.size() == all_rows);
        CHECK(std::set<int64_t>(ids.begin(), ids.end()).size() == ids.size());
        CHECK(ids == vs);
    }
    SECTION("a paused write is invisible until it ends") {
        paused_statement_t update(first, "UPDATE db.big SET v = v + 1;");
        REQUIRE(update.paused());
        CHECK(rows_where("v = id") == all_rows);
        REQUIRE(update.still_running());
        CHECK(update.release()->size() == all_rows);
        CHECK(rows_where("v = id + 1") == all_rows);
    }
    SECTION("a paused delete removes only its snapshot's rows") {
        paused_statement_t remove(first, "DELETE FROM db.big WHERE id >= 0;");
        REQUIRE(remove.paused());
        require_ok(second, "INSERT INTO db.big (id, v) VALUES (" + std::to_string(PAUSABLE_TABLE_ROWS) + ", 0);");
        REQUIRE(remove.still_running());
        CHECK(remove.release()->size() == all_rows);
        CHECK(column<int64_t>(second.run("SELECT id FROM db.big;")) == ints{PAUSABLE_TABLE_ROWS});
    }
}

TEST_CASE("integration::cpp::concurrency::visibility::parallel", "[parallel]") {
    engine_t engine("visibility_parallel");
    session_t admin(engine, "admin");
    require_ok(admin, "CREATE DATABASE db;");
    require_ok(admin, "CREATE TABLE db.t (id bigint, v bigint);");

    SECTION("a reader never sees part of a transaction") {
        // Writers commit 7 rows at a time; a reader counting anything but a multiple of 7 saw part of one.
        constexpr std::size_t WRITERS = 3;
        constexpr std::size_t READERS = 5;
        constexpr int TRANSACTIONS = 19;
        std::atomic<std::size_t> writing{WRITERS};
        std::atomic<int> torn_reads{0};
        std::atomic<int> failed_reads{0};
        in_parallel(engine, WRITERS + READERS, [&](std::size_t index, session_t& session) {
            if (index >= WRITERS) {
                while (writing.load() > 0) {
                    const auto read = session.run("SELECT id FROM db.t;");
                    if (!read->is_success()) {
                        ++failed_reads;
                    } else if (read->size() % 7 != 0) {
                        ++torn_reads;
                    }
                }
                return;
            }
            for (int transaction = 0; transaction < TRANSACTIONS; ++transaction) {
                std::ignore = session.run("BEGIN;");
                for (int row = 0; row < 7; ++row) {
                    const auto id = std::to_string(100'000 * static_cast<int64_t>(index + 1) + 7 * transaction + row);
                    std::ignore = session.run("INSERT INTO db.t (id, v) VALUES (" + id + ", 0);");
                }
                std::ignore = session.run("COMMIT;");
            }
            --writing;
        });
        CHECK(engine.most_calls_at_once() >= 2);
        CHECK(torn_reads.load() == 0);
        CHECK(failed_reads.load() == 0);
        CHECK(admin.run("SELECT id FROM db.t;")->size() == WRITERS * TRANSACTIONS * 7);
    }

    SECTION("commits and rollbacks keep exactly the committed rows") {
        // Every third transaction rolls back on purpose; nothing else may fail.
        constexpr std::size_t SESSIONS = 8;
        constexpr int TRANSACTIONS = 17;
        std::vector<std::vector<int64_t>> committed(SESSIONS);
        std::atomic<int> unexpected_failures{0};
        in_parallel(engine, SESSIONS, [&](std::size_t index, session_t& session) {
            for (int transaction = 0; transaction < TRANSACTIONS; ++transaction) {
                std::vector<int64_t> written;
                bool ok = session.run("BEGIN;")->is_success();
                for (int row = 0; row < 3 && ok; ++row) {
                    const int64_t id = 1000 * static_cast<int64_t>(index + 1) + 3 * transaction + row;
                    ok = session.run("INSERT INTO db.t (id, v) VALUES (" + std::to_string(id) + ", 0);")->is_success();
                    written.push_back(id);
                }
                if (transaction % 3 == 2 || !ok) {
                    std::ignore = session.run("ROLLBACK;");
                    unexpected_failures += ok ? 0 : 1;
                } else if (session.run("COMMIT;")->is_success()) {
                    committed[index].insert(committed[index].end(), written.begin(), written.end());
                } else {
                    ++unexpected_failures;
                }
            }
        });
        CHECK(engine.most_calls_at_once() >= 2);
        CHECK(unexpected_failures.load() == 0);
        std::multiset<int64_t> expected;
        for (const auto& ids : committed) {
            expected.insert(ids.begin(), ids.end());
        }
        const auto stored = column<int64_t>(admin.run("SELECT id FROM db.t;"));
        CHECK(std::multiset<int64_t>(stored.begin(), stored.end()) == expected);
    }
}
