// disk_hash_table_t::get_all keeps only the LAST row id per key; bitcask_index_disk_t::find replays
// the full list from the snapshot record. EXPLAIN is asserted first so a full-scan regression can't pass silently.

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

TEST_CASE("integration::cpp::index_read_through_agent::hash_lookup_returns_every_duplicate") {
    auto config = test_create_config(integration_fixture_path("test_index_read_through_agent/duplicates"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;

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

        REQUIRE(exec("DELETE FROM dupdb.t WHERE id = 3;")->is_success());
        {
            auto cur = exec("SELECT id FROM dupdb.t WHERE k = 7;");
            REQUIRE(cur->is_success());
            CHECK(ids_of(cur) == std::vector<int64_t>{1, 6});
        }

        // CHECKPOINT rebuilds the index from a full table scan; a rebuild keeping one row per key would show up here.
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

// Uncommitted entries live only in the per-transaction bucket: disk-only misses them, folding all buckets leaks them.
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
