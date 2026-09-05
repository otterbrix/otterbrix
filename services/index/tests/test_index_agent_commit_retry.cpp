// THE BUCKET IS CLEARED ONLY AFTER THE JOURNAL SAYS YES.
//
// commit_inserts/commit_deletes (txn != 0) used to run `take(txn_id)` -- which erased the
// pending bucket INSIDE itself -- and only then ask the store to journal the batch
// (apply_txn_inserts / apply_txn_deletes). A journal IO refusal therefore LOST the staged
// batch: the statement heard about the error, but a RETRY of the same commit found an empty
// bucket and reported success over nothing. These cases pin the branch lesson "state is
// cleared only AFTER the operation succeeds" at the agent level: a refused commit keeps the
// bucket, and the retried commit actually publishes it.

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
#include <services/index/index_agent_contract.hpp>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "index_fixture_path.hpp"

using components::expressions::compare_type;
using components::session::session_id_t;
using components::table::TRANSACTION_ID_START;
using components::types::logical_value_t;
using services::index::bitcask_index_agent_t;
using services::index::index_agent_contract;

namespace {

    constexpr components::catalog::oid_t kTableOid = 17400;
    constexpr components::catalog::oid_t kIndexOid = 17401;

    std::filesystem::path fresh_index_root(const char* name) {
        const std::filesystem::path path{services::index::tests::index_fixture_path(name)};
        std::filesystem::remove_all(path);
        std::filesystem::create_directories(path / std::to_string(static_cast<unsigned>(kTableOid)) /
                                            std::to_string(static_cast<unsigned>(kIndexOid)));
        return path;
    }

    // The store's own directory and the txn log inside it (bitcask.txn.log is the store's
    // constant; the log is opened LAZILY by the first journalled commit, which is what makes
    // the pre-commit sabotage below possible at all).
    std::filesystem::path store_dir(const std::filesystem::path& root) {
        return root / std::to_string(static_cast<unsigned>(kTableOid)) /
               std::to_string(static_cast<unsigned>(kIndexOid));
    }
    std::filesystem::path txn_log_path(const std::filesystem::path& root) {
        return store_dir(root) / "bitcask.txn.log";
    }

    std::vector<std::pair<logical_value_t, size_t>>
    entries(std::pmr::memory_resource* resource, std::initializer_list<std::pair<int64_t, size_t>> rows) {
        std::vector<std::pair<logical_value_t, size_t>> values;
        for (const auto& [key, row_id] : rows) {
            values.emplace_back(logical_value_t(resource, key), row_id);
        }
        return values;
    }

    // One message, pumped to completion, its reply taken. Every handler under test is a
    // straight-line coroutine with no cross-actor await, so ONE resume finishes it.
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

} // namespace

TEST_CASE("services::index::bitcask_index_agent_t keeps the staged bucket across a refused commit") {
    auto resource = core::pmr::otterbrix_resource();
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    const auto path = fresh_index_root("otterbrix_test_index_agent_commit_retry");

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

    const auto session = session_id_t::generate_uid();
    const uint64_t txn1 = TRANSACTION_ID_START + 1;
    const uint64_t txn2 = TRANSACTION_ID_START + 2;
    const logical_value_t val42(&resource, int64_t{42});

    auto read = [&](uint64_t txn_id) {
        auto answer = ask<&index_agent_contract::read_rows>(agent,
                                                            session,
                                                            compare_type::eq,
                                                            logical_value_t(&resource, val42),
                                                            txn_id);
        REQUIRE_FALSE(answer.has_error());
        return sorted(std::move(answer.value()));
    };

    // A directory squatting on the txn log's path makes the lazy open refuse, so the
    // journalled commit fails AFTER the agent has taken the bucket -- which is exactly the
    // window the fix closes. Removing the directory heals the store for the retry.
    auto sabotage = [&] { REQUIRE(std::filesystem::create_directory(txn_log_path(path))); };
    auto heal = [&] { REQUIRE(std::filesystem::remove(txn_log_path(path))); };

    SECTION("a refused commit_inserts keeps the batch, and the retry publishes it") {
        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_inserts>(agent, session, txn1, entries(&resource, {{42, 7}}))
                .contains_error());

        sabotage();
        auto refused = ask<&index_agent_contract::commit_inserts>(agent, session, txn1);
        INFO("the journal refusal must reach the statement");
        REQUIRE(refused.contains_error());

        heal();
        auto retried = ask<&index_agent_contract::commit_inserts>(agent, session, txn1);
        REQUIRE_FALSE(retried.contains_error());

        // RED before the fix: the retry reported success over an ALREADY-ERASED bucket, so
        // nothing was ever journalled and the row is absent for everyone.
        CHECK(read(0) == std::vector<int64_t>{7});
        CHECK(read(txn2) == std::vector<int64_t>{7});
    }

    SECTION("a refused commit_deletes keeps the batch, and the retry removes the row") {
        // Seed the durable row over the txn==0 route (publish, no journal): the txn log's
        // lazy open must still be ahead of us, or the directory squat below cannot refuse it.
        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_inserts>(agent, session, uint64_t{0}, entries(&resource, {{42, 7}}))
                .contains_error());
        REQUIRE_FALSE(ask<&index_agent_contract::commit_inserts>(agent, session, uint64_t{0}).contains_error());
        REQUIRE(read(0) == std::vector<int64_t>{7});

        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_deletes>(agent, session, txn2, entries(&resource, {{42, 7}}))
                .contains_error());

        sabotage();
        auto refused = ask<&index_agent_contract::commit_deletes>(agent, session, txn2);
        INFO("the journal refusal must reach the statement");
        REQUIRE(refused.contains_error());

        heal();
        auto retried = ask<&index_agent_contract::commit_deletes>(agent, session, txn2);
        REQUIRE_FALSE(retried.contains_error());

        // RED before the fix: the retried delete succeeded over an empty bucket, so the row
        // stayed in the index while the statement was told the delete landed.
        CHECK(read(0).empty());
    }

    std::filesystem::remove_all(path);
}

// ЗАПИСЬ #353: the txn==0 leg (rebuild / repopulate feed, no journal) erased the bucket
// AFTER applying it to the store but BEFORE force_flush() answered. The batch itself was
// not lost (the keydir held it), but a commit whose flush refused had already cleared its
// bucket — so the RETRY of that commit published nothing, found nothing parked, and
// reported success without ever re-asking the store for durability. Same branch lesson as
// the txn!=0 fix above: state is cleared only AFTER the operation succeeds. Re-publishing
// a kept bucket is safe in this family: bitcask's insert/remove doors are idempotent on
// the (key, row) pair.
TEST_CASE("services::index::bitcask_index_agent_t txn==0 publish keeps the bucket until the flush verdict") {
    auto resource = core::pmr::otterbrix_resource();
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    const auto path = fresh_index_root("otterbrix_test_index_agent_publish_retry");

    // segment_record_limit=4 so the FIRST non-bulk append after the 5-row seed asks for a
    // rotation; the squat below makes that rotation's open refuse, which leaves the store
    // with no active segment and every later append refusing (parked, handed over by
    // force_flush) — an environmental refusal on exactly the txn==0 publish leg.
    auto agent_result = bitcask_index_agent_t::create(&resource,
                                                      path,
                                                      kTableOid,
                                                      kIndexOid,
                                                      /*flush_threshold=*/1000,
                                                      /*segment_record_limit=*/4,
                                                      log,
                                                      std::pmr::set<std::uint64_t>(&resource));
    REQUIRE_FALSE(agent_result.has_error());
    auto agent = std::move(agent_result.value());

    const auto session = session_id_t::generate_uid();
    const logical_value_t val42(&resource, int64_t{42});

    auto read = [&](uint64_t txn_id) {
        auto answer = ask<&index_agent_contract::read_rows>(agent,
                                                            session,
                                                            compare_type::eq,
                                                            logical_value_t(&resource, val42),
                                                            txn_id);
        REQUIRE_FALSE(answer.has_error());
        return sorted(std::move(answer.value()));
    };

    // Seed five committed-for-everyone rows over the txn==0 route. Bulk mode suppresses
    // rotation, so the active segment ends the seed holding 5 >= 4 records.
    REQUIRE_FALSE(ask<&index_agent_contract::stage_inserts>(
                      agent,
                      session,
                      uint64_t{0},
                      entries(&resource, {{42, 7}, {43, 8}, {44, 9}, {45, 10}, {46, 11}}))
                      .contains_error());
    REQUIRE_FALSE(ask<&index_agent_contract::commit_inserts>(agent, session, uint64_t{0}).contains_error());
    REQUIRE(read(0) == std::vector<int64_t>{7});

    // A directory squatting on the NEXT segment's path (fresh store: active id 2, next 3)
    // makes the rotation's open refuse mid-publish.
    const auto next_segment = store_dir(path) / "bitcask.000003.data";
    REQUIRE(std::filesystem::create_directory(next_segment));

    SECTION("a refused txn==0 commit_deletes keeps its bucket, and the retry re-asks durability") {
        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_deletes>(agent, session, uint64_t{0}, entries(&resource, {{42, 7}}))
                .contains_error());

        auto refused = ask<&index_agent_contract::commit_deletes>(agent, session, uint64_t{0});
        INFO("the rotation refusal must reach the statement");
        REQUIRE(refused.contains_error());

        // RED before the fix: the bucket was erased ahead of the flush verdict, so this
        // retry published nothing, found nothing parked, and answered no_error — success
        // over a delete that never reached the device.
        auto retried = ask<&index_agent_contract::commit_deletes>(agent, session, uint64_t{0});
        REQUIRE(retried.contains_error());

        // The kept bucket keeps the SEMANTICS straight too: bucket 0 is committed for
        // everyone (readers subtract it), merely not yet durable — so the reader already
        // sees the delete while the statement keeps hearing "not durable" until a retry
        // lands. Before the fix the erased bucket made the reader UN-see a committed
        // delete that was then never going to be published.
        CHECK(read(0).empty());
    }

    SECTION("a refused txn==0 commit_inserts keeps its bucket, and the retry re-asks durability") {
        // Break the active segment first (same rotation squat, via a one-row delete).
        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_deletes>(agent, session, uint64_t{0}, entries(&resource, {{43, 8}}))
                .contains_error());
        REQUIRE(ask<&index_agent_contract::commit_deletes>(agent, session, uint64_t{0}).contains_error());

        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_inserts>(agent, session, uint64_t{0}, entries(&resource, {{50, 20}}))
                .contains_error());

        auto refused = ask<&index_agent_contract::commit_inserts>(agent, session, uint64_t{0});
        INFO("the append refusal must reach the statement");
        REQUIRE(refused.contains_error());

        // RED before the fix: same vacuous success as the delete leg.
        auto retried = ask<&index_agent_contract::commit_inserts>(agent, session, uint64_t{0});
        REQUIRE(retried.contains_error());
    }

    std::filesystem::remove_all(path);
}
