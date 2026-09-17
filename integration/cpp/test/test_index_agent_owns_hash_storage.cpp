// The index agent opens its own hash-storage file to avoid sharing a store handle across actors,
// though manager_index_t/bootstrap_indexes_sync derive the identical path anyway. Both cases
// restart the instance so pending in-memory buckets can't answer without touching the store.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <services/index/manager_index.hpp>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

namespace {

    using components::cursor::cursor_t_ptr;

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

TEST_CASE("integration::cpp::index_agent_owns_hash_storage::existing_hash_index_answers_after_restart") {
    auto config = test_create_config(integration_fixture_path("test_index_agent_owns_hash_storage/existing"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;

    // Scoped: the writing instance must tear down before the reading one starts.
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        REQUIRE(exec("CREATE DATABASE hsdb;")->is_success());
        REQUIRE(exec("CREATE TABLE hsdb.t (id bigint, k bigint);")->is_success());
        REQUIRE(exec("CREATE INDEX t_k ON hsdb.t USING hash (k);")->is_success());
        REQUIRE(exec("INSERT INTO hsdb.t (id, k) VALUES (1, 7), (2, 8), (3, 7), (4, 9);")->is_success());
    }

    {
        test_spaces restarted(config);
        auto* rd = restarted.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return rd->execute_sql(session, sql);
        };

        auto plan = exec("EXPLAIN SELECT id FROM hsdb.t WHERE k = 7;");
        REQUIRE(plan->is_success());
        std::string text;
        for (std::size_t r = 0; r < plan->size(); ++r) {
            text += std::string(plan->value(0, r).value<std::string_view>());
            text += '\n';
        }
        INFO("plan:\n" << text);
        REQUIRE(text.find("Index Scan") != std::string::npos);

        services::index::reset_index_agent_reads();
        auto cur = exec("SELECT id FROM hsdb.t WHERE k = 7;");
        REQUIRE(cur->is_success());
        INFO("zero agent reads means the answer came from somewhere other than the store the agent opened");
        CHECK(services::index::index_agent_reads() >= 1);
        CHECK(ids_of(cur) == std::vector<int64_t>{1, 3});

        // Singleton control: a failure above is then a storage failure, not duplicate handling.
        auto single = exec("SELECT id FROM hsdb.t WHERE k = 9;");
        REQUIRE(single->is_success());
        CHECK(ids_of(single) == std::vector<int64_t>{4});
    }
}

// Keys whose encoded form exceeds disk_hash_table_t::inline_key_limit (64 bytes) are stored
// truncated; losing the full-key hook that resolves them loses every long key silently (SELECT
// succeeds, returns nothing).
TEST_CASE("integration::cpp::index_agent_owns_hash_storage::long_key_hash_index_answers_after_restart") {
    auto config = test_create_config(integration_fixture_path("test_index_agent_owns_hash_storage/long_key"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;

    // 200 chars puts the encoded key well past the 64-byte inline limit; the keydir stores a
    // 32-byte prefix of it.
    const std::string long_key(200, 'q');
    // Shares the whole 32-byte prefix with long_key and differs only past it, defeating a
    // prefix-only comparison.
    const std::string sibling_key = std::string(120, 'q') + std::string(120, 'z');
    // Stays inline: the control separating "long keys are lost" from "the index is empty".
    const std::string short_key = "short";

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        REQUIRE(exec("CREATE DATABASE lkdb;")->is_success());
        REQUIRE(exec("CREATE TABLE lkdb.t (id bigint, k text);")->is_success());
        REQUIRE(exec("CREATE INDEX t_k ON lkdb.t USING hash (k);")->is_success());
        REQUIRE(exec("INSERT INTO lkdb.t (id, k) VALUES (1, '" + long_key + "'), (2, '" + sibling_key + "'), (3, '" +
                     short_key + "'), (4, '" + long_key + "');")
                    ->is_success());
    }

    {
        test_spaces restarted(config);
        auto* rd = restarted.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return rd->execute_sql(session, sql);
        };

        services::index::reset_index_agent_reads();
        auto cur = exec("SELECT id FROM lkdb.t WHERE k = '" + long_key + "';");
        REQUIRE(cur->is_success());
        INFO("a truncated keydir entry is resolved through the full-key hook; without it this is empty");
        CHECK(services::index::index_agent_reads() >= 1);
        CHECK(ids_of(cur) == std::vector<int64_t>{1, 4});

        auto sibling = exec("SELECT id FROM lkdb.t WHERE k = '" + sibling_key + "';");
        REQUIRE(sibling->is_success());
        CHECK(ids_of(sibling) == std::vector<int64_t>{2});

        auto short_rows = exec("SELECT id FROM lkdb.t WHERE k = '" + short_key + "';");
        REQUIRE(short_rows->is_success());
        CHECK(ids_of(short_rows) == std::vector<int64_t>{3});
    }
}
