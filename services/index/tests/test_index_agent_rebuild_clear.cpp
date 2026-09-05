// A REBUILD'S clear() MUST NOT DESTROY ANOTHER TRANSACTION'S STAGED BATCH.
//
// manager_index_t::repopulate_table (VACUUM / CHECKPOINT) posts clear -> stage_inserts(0) ->
// commit_inserts(0) to every agent of the table in one uninterrupted burst. The burst is FIFO
// against the agent's mailbox, but a writer transaction that staged BEFORE it and commits AFTER
// it straddles the whole burst -- and clear() wipes pending_inserts_/pending_deletes_ WHOLESALE,
// every bucket of every transaction, not just the rebuild's own bucket 0.
//
// What that costs is not a superset. The rebuild feed is a visibility-filtered scan taken under
// the maintenance statement's snapshot, so an uncommitted writer's rows are NOT in it; and the
// writer's own commit finds an empty bucket, takes the `journal.empty()` road and reports
// no_error. The heap ends up with the row and the index without it -- an index scan then answers
// with FEWER rows than a sequential scan, silently, with every statement reporting success.
//
// The mirror case costs the other direction: a staged DELETE wiped by the clear leaves the row in
// the rebuilt index after the deleting transaction commits.
//
// The rule these cases pin is the same one test_index_agent_commit_retry.cpp pins one level down:
// a bucket belongs to the transaction that staged it, and nothing but that transaction's own
// commit, revert or drop may take it away.
//
// BOTH CASES WERE WRITTEN RED and carried [!shouldfail] until the owner ruled, on 2026-09-05,
// that clear() must narrow to its own bucket. The tag is gone; these now pin the fixed
// behaviour. The failure they used to show, per family:
//     :143  CHECK( read(42, onlooker) == std::vector<int64_t>{7} )   ->  { } == { 7 }
//     :175  CHECK( read(42, onlooker).empty() )                      ->  false
//
// The staged physical row ids are provably still valid across the round: a pending txn id is
// >= TRANSACTION_ID_START and so above every compact watermark, so
// table_storage_t::has_versions_above defers the compaction for the whole round
// (agent_disk_t::checkpoint_inner). The narrowing inverted one standing assertion --
// test_index_agent_buffer.cpp, SECTION("clear() wipes the buckets as well as the tree") --
// which was rewritten to the new contract under the same ruling; it now checks that the
// COMMITTED row goes and the onlooker's OWN staged row stays.

// clang-format off
// <actor-zeta/spawn.hpp> requires std::unique_ptr, but does not include it itself
#include <memory>
#include <memory_resource>
#include <actor-zeta/spawn.hpp>
// clang-format on

#include <catch2/catch_test_macros.hpp>

#include <components/expressions/compare_expression.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/table/row_version_manager.hpp>
#include <components/types/logical_value.hpp>
#include <core/executor.hpp>
#include <core/pmr.hpp>
#include <core/result_wrapper.hpp>

#include <services/index/bitcask_index_agent.hpp>
#include <services/index/btree_index_agent.hpp>
#include <services/index/index_agent_contract.hpp>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <set>
#include <utility>
#include <vector>

#include "index_fixture_path.hpp"

using components::expressions::compare_type;
using components::session::session_id_t;
using components::table::TRANSACTION_ID_START;
using components::types::logical_value_t;
using services::index::bitcask_index_agent_t;
using services::index::btree_index_agent_t;
using services::index::index_agent_contract;

namespace {

    constexpr components::catalog::oid_t kTableOid = 17500;
    constexpr components::catalog::oid_t kIndexOid = 17501;

    std::filesystem::path fresh_index_root(const char* name) {
        const std::filesystem::path path{services::index::tests::index_fixture_path(name)};
        std::filesystem::remove_all(path);
        std::filesystem::create_directories(path / std::to_string(static_cast<unsigned>(kTableOid)) /
                                            std::to_string(static_cast<unsigned>(kIndexOid)));
        return path;
    }

    std::vector<std::pair<logical_value_t, size_t>> entries(std::pmr::memory_resource* resource,
                                                            std::initializer_list<std::pair<int64_t, size_t>> rows) {
        std::vector<std::pair<logical_value_t, size_t>> values;
        for (const auto& [key, row_id] : rows) {
            values.emplace_back(logical_value_t(resource, key), row_id);
        }
        return values;
    }

    // One message, pumped to completion, its reply taken -- the same single-resume shape
    // test_index_agent_commit_retry.cpp uses, and sound for the same reason: every handler
    // under test is a straight-line coroutine with no cross-actor await.
    template<auto Handler, typename Agent, typename... Args>
    auto ask(Agent& agent, Args&&... args) {
        auto [needs_sched, future] =
            actor_zeta::otterbrix::send<Handler>(agent->address(), std::forward<Args>(args)...);
        agent->resume(1);
        REQUIRE(future.is_ready());
        return std::move(future).take_ready();
    }

    std::vector<int64_t> sorted(std::pmr::vector<int64_t> rows) {
        std::vector<int64_t> out(rows.begin(), rows.end());
        std::sort(out.begin(), out.end());
        return out;
    }

    // The two families answer the same contract, so the body below is written once and
    // instantiated for each. Only construction differs.
    template<typename Agent>
    void rebuild_clear_keeps_other_transactions_buckets(Agent& agent, std::pmr::memory_resource* resource) {
        const auto session = session_id_t::generate_uid();
        // A live writer, and a second id that never stages anything -- the reader below asks as
        // the second one so nothing it sees can come out of the writer's own pending bucket.
        const uint64_t writer = TRANSACTION_ID_START + 1;
        const uint64_t onlooker = TRANSACTION_ID_START + 2;

        auto read = [&](int64_t key, uint64_t txn_id) {
            auto answer = ask<&index_agent_contract::read_rows>(agent,
                                                                session,
                                                                compare_type::eq,
                                                                logical_value_t(resource, key),
                                                                txn_id);
            REQUIRE_FALSE(answer.has_error());
            return sorted(std::move(answer.value()));
        };

        SECTION("a staged insert survives a rebuild's clear and the writer's commit publishes it") {
            // The writer stages before the maintenance round starts.
            REQUIRE_FALSE(
                ask<&index_agent_contract::stage_inserts>(agent, session, writer, entries(resource, {{42, 7}}))
                    .contains_error());

            // ---- repopulate_table's burst, in its exact order --------------------------
            REQUIRE_FALSE(ask<&index_agent_contract::clear>(agent, session).contains_error());
            // The rebuild feed is the scan's own rows. Row 7 is NOT among them: the scan ran
            // under the maintenance snapshot and the writer has not committed.
            REQUIRE_FALSE(
                ask<&index_agent_contract::stage_inserts>(agent, session, uint64_t{0}, entries(resource, {{99, 3}}))
                    .contains_error());
            REQUIRE_FALSE(ask<&index_agent_contract::commit_inserts>(agent, session, uint64_t{0}).contains_error());
            // ---------------------------------------------------------------------------

            // The writer commits. Its batch was staged, never reverted and never dropped, so
            // this commit owes the index row 7.
            REQUIRE_FALSE(ask<&index_agent_contract::commit_inserts>(agent, session, writer).contains_error());

            // What this catches: a wiped bucket turning commit_inserts into a no-op that still
            // reports success, so the heap keeps the row and the index does not.
            CHECK(read(42, onlooker) == std::vector<int64_t>{7});
            // The rebuild's own rows are there either way -- if they were not, the failure above
            // would be about the clear, not about whose bucket it took.
            CHECK(read(99, onlooker) == std::vector<int64_t>{3});
        }

        SECTION("a staged delete survives a rebuild's clear and the writer's commit applies it") {
            // Row 7 is durable and committed for everyone.
            REQUIRE_FALSE(
                ask<&index_agent_contract::stage_inserts>(agent, session, uint64_t{0}, entries(resource, {{42, 7}}))
                    .contains_error());
            REQUIRE_FALSE(ask<&index_agent_contract::commit_inserts>(agent, session, uint64_t{0}).contains_error());
            REQUIRE(read(42, onlooker) == std::vector<int64_t>{7});

            // The writer stages the delete and has not committed it.
            REQUIRE_FALSE(
                ask<&index_agent_contract::stage_deletes>(agent, session, writer, entries(resource, {{42, 7}}))
                    .contains_error());

            // ---- repopulate_table's burst. Row 7 IS in the feed: the delete is uncommitted,
            // so the maintenance snapshot still sees the row. -----------------------------
            REQUIRE_FALSE(ask<&index_agent_contract::clear>(agent, session).contains_error());
            REQUIRE_FALSE(
                ask<&index_agent_contract::stage_inserts>(agent, session, uint64_t{0}, entries(resource, {{42, 7}}))
                    .contains_error());
            REQUIRE_FALSE(ask<&index_agent_contract::commit_inserts>(agent, session, uint64_t{0}).contains_error());
            // ---------------------------------------------------------------------------

            REQUIRE_FALSE(ask<&index_agent_contract::commit_deletes>(agent, session, writer).contains_error());

            // What this catches: the delete reported as landed while the row stays in the
            // rebuilt index for every later reader.
            CHECK(read(42, onlooker).empty());
        }
    }

} // namespace

TEST_CASE("services::index::bitcask_index_agent_t rebuild clear keeps other transactions' staged batches") {
    auto resource = core::pmr::otterbrix_resource();
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    const auto path = fresh_index_root("otterbrix_test_index_agent_rebuild_clear_bitcask");

    auto agent_result = bitcask_index_agent_t::create(&resource,
                                                      path,
                                                      kTableOid,
                                                      kIndexOid,
                                                      /*flush_threshold=*/1000,
                                                      /*segment_record_limit=*/100,
                                                      log,
                                                      std::pmr::set<std::uint64_t>(&resource));
    REQUIRE_FALSE(agent_result.has_error());
    auto agent = std::move(agent_result.value());

    rebuild_clear_keeps_other_transactions_buckets(agent, &resource);

    std::filesystem::remove_all(path);
}

TEST_CASE("services::index::btree_index_agent_t rebuild clear keeps other transactions' staged batches") {
    auto resource = core::pmr::otterbrix_resource();
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    const auto path = fresh_index_root("otterbrix_test_index_agent_rebuild_clear_btree");

    auto agent_result = btree_index_agent_t::create(&resource,
                                                    path,
                                                    kTableOid,
                                                    kIndexOid,
                                                    /*flush_threshold=*/1000,
                                                    log);
    REQUIRE_FALSE(agent_result.has_error());
    auto agent = std::move(agent_result.value());

    rebuild_clear_keeps_other_transactions_buckets(agent, &resource);

    std::filesystem::remove_all(path);
}
