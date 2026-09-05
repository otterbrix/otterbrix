// A hashed index's keydir keeps only the LAST row id per key (disk_hash_table_t::get_all);
// only bitcask_index_disk_t::find replays the full row list from the snapshot record. A read
// through the keydir alone silently drops every earlier duplicate.

#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

namespace {

    using components::cursor::cursor_t_ptr;

    std::string plan_text(const cursor_t_ptr& cur) {
        std::string out;
        for (std::size_t r = 0; r < cur->size(); ++r) {
            auto v = cur->value(0, r);
            out += std::string(v.value<std::string_view>());
            out += '\n';
        }
        return out;
    }

    // Column 0 as a sorted set, not a count: a lookup returning the right NUMBER of
    // wrong rows must still fail.
    std::vector<int64_t> ids_of(const cursor_t_ptr& cur) {
        std::vector<int64_t> out;
        out.reserve(cur->size());
        for (std::size_t r = 0; r < cur->size(); ++r) {
            out.push_back(cur->value(0, r).value<int64_t>());
        }
        std::sort(out.begin(), out.end());
        return out;
    }

} // namespace

// A hashed index over a column with REPEATED values. `WHERE k = <repeated>` must
// return every row carrying that key.
//
// EXPLAIN is asserted first: without it, a planner regression to full scan would pass
// this test while testing nothing.
TEST_CASE("integration::cpp::index_read_through_agent::hash_lookup_returns_every_duplicate") {
    auto config = test_create_config(integration_fixture_path("test_index_read_through_agent/duplicates"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;

    // Scoped: the restart round below needs this instance torn down first.
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        REQUIRE(exec("CREATE DATABASE dupdb;")->is_success());
        REQUIRE(exec("CREATE TABLE dupdb.t (id bigint, k bigint);")->is_success());
        REQUIRE(exec("CREATE INDEX t_k ON dupdb.t USING hash (k);")->is_success());

        // k = 9 is a singleton: a last-row-only reader would still get it right, so it
        // separates "the lookup is broken" from "the index is empty".
        REQUIRE(
            exec("INSERT INTO dupdb.t (id, k) VALUES (1, 7), (2, 8), (3, 7), (4, 9), (5, 8), (6, 7);")->is_success());

        {
            auto plan = exec("EXPLAIN SELECT id FROM dupdb.t WHERE k = 7;");
            REQUIRE(plan->is_success());
            auto text = plan_text(plan);
            INFO("plan:\n" << text);
            INFO("without an Index Scan this test would be a full-scan test in disguise");
            REQUIRE(text.find("Index Scan") != std::string::npos);
        }

        {
            auto cur = exec("SELECT id FROM dupdb.t WHERE k = 7;");
            REQUIRE(cur->is_success());
            INFO("three rows carry k = 7; a keydir-only read reports only the last one");
            CHECK(ids_of(cur) == std::vector<int64_t>{1, 3, 6});
        }
        {
            auto cur = exec("SELECT id FROM dupdb.t WHERE k = 8;");
            REQUIRE(cur->is_success());
            CHECK(ids_of(cur) == std::vector<int64_t>{2, 5});
        }
        {
            auto cur = exec("SELECT id FROM dupdb.t WHERE k = 9;");
            REQUIRE(cur->is_success());
            INFO("the singleton control: correct even for a reader that loses duplicates");
            CHECK(ids_of(cur) == std::vector<int64_t>{4});
        }

        // A committed DELETE of ONE duplicate rewrites the snapshot record with a shorter
        // row list, proving the read follows the record and not a cached single id.
        REQUIRE(exec("DELETE FROM dupdb.t WHERE id = 3;")->is_success());
        {
            auto cur = exec("SELECT id FROM dupdb.t WHERE k = 7;");
            REQUIRE(cur->is_success());
            CHECK(ids_of(cur) == std::vector<int64_t>{1, 6});
        }

        // CHECKPOINT rebuilds the index from a full table scan; a rebuild that keeps only
        // one row per key would show up here without any crash or restart needed.
        REQUIRE(exec("CHECKPOINT;")->is_success());
        {
            auto cur = exec("SELECT id FROM dupdb.t WHERE k = 7;");
            REQUIRE(cur->is_success());
            INFO("the index rebuild must keep every row of a repeated key");
            CHECK(ids_of(cur) == std::vector<int64_t>{1, 6});
        }
        {
            auto cur = exec("SELECT id FROM dupdb.t WHERE k = 8;");
            REQUIRE(cur->is_success());
            CHECK(ids_of(cur) == std::vector<int64_t>{2, 5});
        }
    }

    // Duplicates must survive a restart too, since the index is rebuilt at every start.
    {
        test_spaces restarted(config);
        auto* rd = restarted.dispatcher();
        auto rexec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return rd->execute_sql(session, sql);
        };
        auto cur = rexec("SELECT id FROM dupdb.t WHERE k = 7;");
        REQUIRE(cur->is_success());
        INFO("duplicates must still be there after the start-up rebuild");
        CHECK(ids_of(cur) == std::vector<int64_t>{1, 6});
    }
}

// A transaction's own uncommitted insert must be visible to ITSELF through the
// index, and to nobody else. Uncommitted entries live only in the index's
// per-transaction bucket: a disk-only read misses them, a read that folds in
// every transaction's bucket leaks them to others.
TEST_CASE("integration::cpp::index_read_through_agent::own_uncommitted_insert_is_visible_only_to_its_txn") {
    auto config = test_create_config(integration_fixture_path("test_index_read_through_agent/visibility"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();

    auto writer = otterbrix::session_id_t();
    auto reader = otterbrix::session_id_t();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE visdb;")->is_success());
    REQUIRE(exec("CREATE TABLE visdb.t (id bigint, k bigint);")->is_success());
    REQUIRE(exec("CREATE INDEX t_k ON visdb.t USING hash (k);")->is_success());
    // A committed baseline row under a DIFFERENT key: the lookups below must keep
    // answering from disk while an uncommitted entry exists beside them.
    REQUIRE(exec("INSERT INTO visdb.t (id, k) VALUES (1, 10);")->is_success());

    {
        auto plan = exec("EXPLAIN SELECT id FROM visdb.t WHERE k = 20;");
        REQUIRE(plan->is_success());
        auto text = plan_text(plan);
        INFO("plan:\n" << text);
        REQUIRE(text.find("Index Scan") != std::string::npos);
    }

    REQUIRE(d->execute_sql(writer, "BEGIN;")->is_success());
    REQUIRE(d->execute_sql(writer, "INSERT INTO visdb.t (id, k) VALUES (2, 20);")->is_success());

    {
        auto own = d->execute_sql(writer, "SELECT id FROM visdb.t WHERE k = 20;");
        REQUIRE(own->is_success());
        INFO("the inserting transaction must find its own uncommitted row through the index");
        CHECK(ids_of(own) == std::vector<int64_t>{2});
    }
    {
        auto other = d->execute_sql(reader, "SELECT id FROM visdb.t WHERE k = 20;");
        REQUIRE(other->is_success());
        INFO("no other transaction may see an uncommitted index entry");
        CHECK(other->size() == 0);
    }
    {
        auto committed = d->execute_sql(reader, "SELECT id FROM visdb.t WHERE k = 10;");
        REQUIRE(committed->is_success());
        INFO("the committed row must stay reachable while an uncommitted entry exists");
        CHECK(ids_of(committed) == std::vector<int64_t>{1});
    }

    REQUIRE(d->execute_sql(writer, "COMMIT;")->is_success());

    {
        auto other = d->execute_sql(reader, "SELECT id FROM visdb.t WHERE k = 20;");
        REQUIRE(other->is_success());
        INFO("after the commit the row is everyone's");
        CHECK(ids_of(other) == std::vector<int64_t>{2});
    }

    // The delete side of the same rule. An uncommitted DELETE must disappear from
    // the deleting transaction's own index lookups and from nobody else's.
    auto deleter = otterbrix::session_id_t();
    REQUIRE(d->execute_sql(deleter, "BEGIN;")->is_success());
    REQUIRE(d->execute_sql(deleter, "DELETE FROM visdb.t WHERE k = 20;")->is_success());
    {
        auto own = d->execute_sql(deleter, "SELECT id FROM visdb.t WHERE k = 20;");
        REQUIRE(own->is_success());
        INFO("the deleting transaction must not find the row it just removed");
        CHECK(own->size() == 0);
    }
    REQUIRE(d->execute_sql(deleter, "COMMIT;")->is_success());
    {
        auto after = d->execute_sql(reader, "SELECT id FROM visdb.t WHERE k = 20;");
        REQUIRE(after->is_success());
        CHECK(after->size() == 0);
    }
}
