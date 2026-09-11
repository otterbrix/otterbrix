// manager_index_t::repopulate_table posts clear -> stage_inserts(0) -> commit_inserts(0) to every
// agent as one uninterrupted burst, so clear() may only wipe bucket 0, not a straddling writer's.

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

    // Offset from txn id, not equal to it: reusing one number for both would let a recycled txn
    // id's earlier COMMIT marker vouch for a new frame (bitcask_index_disk.cpp).
    constexpr std::uint64_t commit_id_of(std::uint64_t txn_id) { return txn_id + 500000; }

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

    // Single resume suffices: every handler here is a straight-line coroutine with no cross-actor await.
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

    // Same contract for both agent families, so the body is written once; only construction differs.
    template<typename Agent>
    void rebuild_clear_keeps_other_transactions_buckets(Agent& agent, std::pmr::memory_resource* resource) {
        const auto session = session_id_t::generate_uid();
        // The reader asks as `onlooker`, which never stages anything.
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
            REQUIRE_FALSE(
                ask<&index_agent_contract::stage_inserts>(agent, session, writer, entries(resource, {{42, 7}}))
                    .contains_error());

            // Row 7 is NOT in the rebuild feed: the scan ran under the maintenance snapshot.
            REQUIRE_FALSE(ask<&index_agent_contract::clear>(agent, session).contains_error());
            REQUIRE_FALSE(
                ask<&index_agent_contract::stage_inserts>(agent, session, uint64_t{0}, entries(resource, {{99, 3}}))
                    .contains_error());
            REQUIRE_FALSE(
                ask<&index_agent_contract::commit_inserts>(agent, session, uint64_t{0}, uint64_t{0}).contains_error());

            REQUIRE_FALSE(
                ask<&index_agent_contract::commit_inserts>(agent, session, writer, commit_id_of(writer))
                    .contains_error());

            // What this catches: a wiped bucket turning commit_inserts into a silent no-op.
            CHECK(read(42, onlooker) == std::vector<int64_t>{7});
            CHECK(read(99, onlooker) == std::vector<int64_t>{3});
        }

        SECTION("a staged delete survives a rebuild's clear and the writer's commit applies it") {
            REQUIRE_FALSE(
                ask<&index_agent_contract::stage_inserts>(agent, session, uint64_t{0}, entries(resource, {{42, 7}}))
                    .contains_error());
            REQUIRE_FALSE(
                ask<&index_agent_contract::commit_inserts>(agent, session, uint64_t{0}, uint64_t{0}).contains_error());
            REQUIRE(read(42, onlooker) == std::vector<int64_t>{7});

            REQUIRE_FALSE(
                ask<&index_agent_contract::stage_deletes>(agent, session, writer, entries(resource, {{42, 7}}))
                    .contains_error());

            // Row 7 IS in the feed here: the delete is uncommitted, so the snapshot still sees it.
            REQUIRE_FALSE(ask<&index_agent_contract::clear>(agent, session).contains_error());
            REQUIRE_FALSE(
                ask<&index_agent_contract::stage_inserts>(agent, session, uint64_t{0}, entries(resource, {{42, 7}}))
                    .contains_error());
            REQUIRE_FALSE(
                ask<&index_agent_contract::commit_inserts>(agent, session, uint64_t{0}, uint64_t{0}).contains_error());

            REQUIRE_FALSE(
                ask<&index_agent_contract::commit_deletes>(agent, session, writer, commit_id_of(writer))
                    .contains_error());

            // What this catches: the delete reported as landed while the row stays in the rebuilt index.
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
