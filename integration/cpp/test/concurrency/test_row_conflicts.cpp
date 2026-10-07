#include "concurrency.hpp"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <random>
#include <tuple>

using namespace concurrency;
using ints = std::vector<int64_t>;

namespace {
    ints values(session_t& session) { return column<int64_t>(session.run("SELECT v FROM db.t ORDER BY id;")); }
} // namespace

TEST_CASE("integration::cpp::concurrency::row_conflicts::interleaved", "[interleaved]") {
    engine_t engine("row_conflicts_interleaved");
    session_t first(engine, "first");
    session_t second(engine, "second");
    require_ok(first, "CREATE DATABASE db;");
    require_ok(first, "CREATE TABLE db.t (id bigint, v bigint);");
    require_ok(first, "INSERT INTO db.t (id, v) VALUES (1, 10), (2, 20), (3, 30);");

    SECTION("the second writer of a row is refused") {
        std::string first_write;
        std::string second_write;
        ints after;
        SECTION("update, update") {
            first_write = "UPDATE db.t SET v = v + 1 WHERE id = 1;";
            second_write = "UPDATE db.t SET v = v + 100 WHERE id = 1;";
            after = {11, 20, 30};
        }
        SECTION("delete, delete") {
            first_write = "DELETE FROM db.t WHERE id = 1;";
            second_write = "DELETE FROM db.t WHERE id = 1;";
            after = {20, 30};
        }
        SECTION("update, delete") {
            first_write = "UPDATE db.t SET v = 11 WHERE id = 1;";
            second_write = "DELETE FROM db.t WHERE id = 1;";
            after = {11, 20, 30};
        }
        SECTION("delete, update") {
            first_write = "DELETE FROM db.t WHERE id = 1;";
            second_write = "UPDATE db.t SET v = 11 WHERE id = 1;";
            after = {20, 30};
        }
        interleaved({
            {first, "BEGIN;"},
            {second, "BEGIN;"},
            {first, first_write},
            {second, second_write, fails},
            {first, "COMMIT;"},
            {second, "COMMIT;", fails},
        });
        CHECK(values(first) == after);
    }

    SECTION("writers of different rows both commit") {
        interleaved({
            {first, "BEGIN;"},
            {second, "BEGIN;"},
            {first, "UPDATE db.t SET v = 11 WHERE id = 1;"},
            {second, "UPDATE db.t SET v = 21 WHERE id = 2;"},
            {first, "DELETE FROM db.t WHERE id = 3;"},
            {second, "INSERT INTO db.t (id, v) VALUES (4, 40);"},
            {first, "COMMIT;"},
            {second, "COMMIT;"},
        });
        CHECK(values(first) == ints{11, 21, 40});
    }

    SECTION("a row changed after the snapshot is refused") {
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
        interleaved({
            {first, "BEGIN;"},
            {first, "SELECT v FROM db.t;"},
            {second, change},
            {first, "UPDATE db.t SET v = v + 100 WHERE id = 1;", fails},
            {first, "COMMIT;", fails},
        });
        CHECK(values(first) == after);
    }

    SECTION("a row frees when its first writer rolls back") {
        interleaved({
            {first, "BEGIN;"},
            {first, "UPDATE db.t SET v = 11 WHERE id = 1;"},
            {second, "UPDATE db.t SET v = 12 WHERE id = 1;", fails},
            {first, "ROLLBACK;"},
            {second, "UPDATE db.t SET v = 12 WHERE id = 1;"},
        });
        CHECK(values(first) == ints{12, 20, 30});
    }
}

TEST_CASE("integration::cpp::concurrency::row_conflicts::overlapping", "[overlapping]") {
    engine_t engine("row_conflicts_overlapping");
    session_t first(engine, "first");
    session_t second(engine, "second");
    require_ok(first, "CREATE DATABASE db;");
    require_ok(first, "CREATE TABLE db.big (id bigint, v bigint);");
    insert_numbered_rows(first, "db.big", PAUSABLE_TABLE_ROWS);

    const int64_t contested = GENERATE(int64_t{0}, PAUSABLE_TABLE_ROWS - 1);
    INFO("contested row " << contested << (contested == 0 ? ", already scanned" : ", not scanned yet"));
    std::string whole_table;
    SECTION("update") { whole_table = "UPDATE db.big SET v = v + 1;"; }
    SECTION("delete") { whole_table = "DELETE FROM db.big WHERE id >= 0;"; }

    paused_statement_t paused(first, whole_table);
    REQUIRE(paused.paused());
    const bool row_written =
        second.run("UPDATE db.big SET v = -1 WHERE id = " + std::to_string(contested) + ";")->is_success();
    REQUIRE(paused.still_running());
    const bool table_written = paused.release()->is_success();

    INFO("the whole-table statement " << (table_written ? "committed" : "failed") << ", the row update "
                                      << (row_written ? "committed" : "failed"));
    CHECK(table_written != row_written);
    const auto row_value =
        column<int64_t>(second.run("SELECT v FROM db.big WHERE id = " + std::to_string(contested) + ";"));
    if (row_written) {
        CHECK(row_value == ints{-1});
    } else if (whole_table.starts_with("UPDATE")) {
        CHECK(row_value == ints{contested + 1});
    } else {
        CHECK(row_value.empty());
    }
}

TEST_CASE("integration::cpp::concurrency::row_conflicts::parallel", "[parallel]") {
    engine_t engine("row_conflicts_parallel");
    session_t admin(engine, "admin");
    require_ok(admin, "CREATE DATABASE db;");
    constexpr std::size_t SESSIONS = 8;
    constexpr int ITERATIONS = 23;

    SECTION("increments of one row lose no commit") {
        require_ok(admin, "CREATE TABLE db.t (id bigint, v bigint);");
        require_ok(admin, "INSERT INTO db.t (id, v) VALUES (1, 0);");
        bool in_transactions = false;
        SECTION("explicit transactions") { in_transactions = true; }
        SECTION("autocommit") { in_transactions = false; }
        std::atomic<int> committed{0};
        in_parallel(engine, SESSIONS, [&](std::size_t, session_t& session) {
            for (int iteration = 0; iteration < ITERATIONS; ++iteration) {
                if (in_transactions) {
                    std::ignore = session.run("BEGIN;");
                }
                bool ok = session.run("UPDATE db.t SET v = v + 1 WHERE id = 1;")->is_success();
                if (in_transactions) {
                    ok = ok && session.run("COMMIT;")->is_success();
                    if (!ok) {
                        std::ignore = session.run("ROLLBACK;");
                    }
                }
                committed += ok ? 1 : 0;
            }
        });
        CHECK(engine.most_calls_at_once() >= 2);
        CHECK(committed.load() > 0);
        CHECK(values(admin) == ints{committed.load()});
    }

    SECTION("increments of separate rows all commit") {
        require_ok(admin, "CREATE TABLE db.t (id bigint, v bigint);");
        insert_numbered_rows(admin, "db.t", static_cast<int64_t>(SESSIONS));
        std::atomic<int> refused{0};
        in_parallel(engine, SESSIONS, [&](std::size_t index, session_t& session) {
            for (int iteration = 0; iteration < ITERATIONS; ++iteration) {
                refused +=
                    session.run("UPDATE db.t SET v = v + 1 WHERE id = " + std::to_string(index) + ";")->is_success()
                        ? 0
                        : 1;
            }
        });
        CHECK(engine.most_calls_at_once() >= 2);
        CHECK(refused.load() == 0);
        ints expected;
        for (std::size_t index = 0; index < SESSIONS; ++index) {
            expected.push_back(static_cast<int64_t>(index) + ITERATIONS);
        }
        CHECK(values(admin) == expected);
    }

    SECTION("transfers keep the total in every snapshot") {
        // Sessions move money between 11 accounts; one more session audits the total in every snapshot it reads.
        constexpr int64_t ACCOUNTS = 11;
        constexpr int64_t TOTAL = ACCOUNTS * 1000;
        require_ok(admin, "CREATE TABLE db.accounts (id bigint, balance bigint);");
        for (int64_t account = 0; account < ACCOUNTS; ++account) {
            require_ok(admin, "INSERT INTO db.accounts (id, balance) VALUES (" + std::to_string(account) + ", 1000);");
        }
        const auto total_of = [](const cursor_ptr& balances) {
            int64_t total = 0;
            for (std::size_t row = 0; row < balances->size(); ++row) {
                total += balances->value(0, row).value<int64_t>();
            }
            return total;
        };
        std::atomic<std::size_t> transferring{SESSIONS};
        std::atomic<int> committed{0};
        std::atomic<int> audits{0};
        std::atomic<int> audits_off{0};
        in_parallel(engine, SESSIONS + 1, [&](std::size_t index, session_t& session) {
            if (index == SESSIONS) {
                while (transferring.load() > 0) {
                    std::ignore = session.run("BEGIN;");
                    const auto first_read = session.run("SELECT balance FROM db.accounts;");
                    const auto second_read = session.run("SELECT balance FROM db.accounts;");
                    std::ignore = session.run("COMMIT;");
                    const bool adds_up = first_read->is_success() && second_read->is_success() &&
                                         total_of(first_read) == TOTAL && total_of(second_read) == TOTAL;
                    audits_off += adds_up ? 0 : 1;
                    ++audits;
                }
                return;
            }
            std::minstd_rand random(static_cast<unsigned>(index + 1));
            std::uniform_int_distribution<int64_t> pick_account(0, ACCOUNTS - 1);
            for (int iteration = 0; iteration < ITERATIONS; ++iteration) {
                const auto from = pick_account(random);
                const auto to = (from + 1 + pick_account(random) % (ACCOUNTS - 1)) % ACCOUNTS;
                bool ok = session.run("BEGIN;")->is_success();
                ok = ok &&
                     session
                         .run("UPDATE db.accounts SET balance = balance - 7 WHERE id = " + std::to_string(from) + ";")
                         ->is_success();
                ok = ok &&
                     session.run("UPDATE db.accounts SET balance = balance + 7 WHERE id = " + std::to_string(to) + ";")
                         ->is_success();
                ok = ok && session.run("COMMIT;")->is_success();
                if (!ok) {
                    std::ignore = session.run("ROLLBACK;");
                }
                committed += ok ? 1 : 0;
            }
            --transferring;
        });
        CHECK(engine.most_calls_at_once() >= 2);
        INFO("committed transfers: " << committed.load() << ", audits: " << audits.load());
        CHECK(committed.load() > 0);
        CHECK(audits_off.load() == 0);
        CHECK(total_of(admin.run("SELECT balance FROM db.accounts;")) == TOTAL);
    }

    SECTION("deletes of the same rows delete each row once") {
        constexpr int64_t ROWS = 37;
        require_ok(admin, "CREATE TABLE db.t (id bigint, v bigint);");
        insert_numbered_rows(admin, "db.t", ROWS);
        std::atomic<std::size_t> deleted{0};
        in_parallel(engine, SESSIONS, [&](std::size_t, session_t& session) {
            std::ignore = session.run("BEGIN;");
            const auto removed = session.run("DELETE FROM db.t WHERE id >= 0;");
            if (removed->is_success() && session.run("COMMIT;")->is_success()) {
                deleted += removed->size();
            } else {
                std::ignore = session.run("ROLLBACK;");
            }
        });
        CHECK(engine.most_calls_at_once() >= 2);
        CHECK(deleted.load() == static_cast<std::size_t>(ROWS));
        CHECK(values(admin).empty());
    }
}
