// ============================================================================
// THE AGENT OPENS ITS OWN HASH STORAGE.
//
// The keydir file <disk>/<table_oid>/<indexrelid>/hash_index.bin is opened BY THE AGENT
// ITSELF. Opening it outside the actor -- in manager_index_t::create_index at runtime, in
// base_spaces::bootstrap_indexes_sync at startup -- and handing it both to the bitcask store
// and to the index facade makes it a cross-actor handle (rule 10), and buys nothing: the
// agent derives the very same path from the very same two oids.
//
// Neither half of that is visible to a grep, nor to a test that only counts rows on a live
// instance: the in-memory pending buckets answer a freshly-inserted key with or without any
// storage at all. Both gates below therefore RESTART, so the only thing that can answer is
// what the agent found on disk when it opened the file for itself.
//
//   (1) an EXISTING hash index survives the restart and answers a lookup, and the answer
//       provably comes out of the agent (index_agent_reads moves);
//   (2) a hash index over a key LONGER THAN 64 BYTES does the same — the guard on the trap.
//       disk_hash_table_t stores only a 32-byte PREFIX of an encoded key longer than
//       inline_key_limit = 64, plus the (segment, offset) of the record holding the whole
//       key, and resolves such an entry through the full-key hook. Removing that hook while
//       removing the sharing -- the obvious reading of "no cross-actor callbacks" -- makes
//       keys_equal() answer false for EVERY long key, so the lookup returns nothing and says
//       nothing. A 64-byte-or-shorter key never touches that path, which is why case (1)
//       cannot catch it.
// ============================================================================

#include "test_config.hpp"
#include "integration_fixture_path.hpp"

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
    config.wal.on = true;
    config.log.level = log_t::level::off;

    // Scoped: one otterbrix instance per directory, so the writing round has to be
    // torn down before the reading one starts.
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

        // Without an Index Scan this would be a full-scan test wearing an index's name.
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

        // The singleton control: right even for a reader that keeps one row per key, so a
        // failure above is a failure of the STORAGE, not of duplicate handling.
        auto single = exec("SELECT id FROM hsdb.t WHERE k = 9;");
        REQUIRE(single->is_success());
        CHECK(ids_of(single) == std::vector<int64_t>{4});
    }
}

// The trap. A hashed index over a key whose ENCODED form exceeds
// disk_hash_table_t::inline_key_limit (64 bytes) is stored truncated, and only the
// full-key hook can decide whether such an entry matches. Losing the hook loses every
// long key SILENTLY: the SELECT succeeds and returns nothing.
TEST_CASE("integration::cpp::index_agent_owns_hash_storage::long_key_hash_index_answers_after_restart") {
    auto config = test_create_config(integration_fixture_path("test_index_agent_owns_hash_storage/long_key"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;

    // 200 characters: the encoded key is 1 type byte + 4 length bytes + 200, well past
    // the 64-byte inline limit, so the keydir entry is truncated to a 32-byte prefix.
    const std::string long_key(200, 'q');
    // Shares the whole 32-byte prefix with long_key and differs only past it, so a
    // reader that compares prefixes and stops confuses the two.
    const std::string sibling_key = std::string(120, 'q') + std::string(120, 'z');
    // Short enough to stay INLINE: the control that separates "long keys are lost" from
    // "the index is empty".
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
        REQUIRE(exec("INSERT INTO lkdb.t (id, k) VALUES (1, '" + long_key + "'), (2, '" + sibling_key +
                     "'), (3, '" + short_key + "'), (4, '" + long_key + "');")
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

        // The prefix sibling must NOT come back with it: resolving a truncated entry means
        // comparing the WHOLE key, not the stored prefix.
        auto sibling = exec("SELECT id FROM lkdb.t WHERE k = '" + sibling_key + "';");
        REQUIRE(sibling->is_success());
        CHECK(ids_of(sibling) == std::vector<int64_t>{2});

        // The inline control: this one is answered without the hook at all.
        auto short_rows = exec("SELECT id FROM lkdb.t WHERE k = '" + short_key + "';");
        REQUIRE(short_rows->is_success());
        CHECK(ids_of(short_rows) == std::vector<int64_t>{3});
    }
}
