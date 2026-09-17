// A write is two messages (stage_* into a bucket, commit_* that publishes it); both must refuse after drop.

// clang-format off
// <actor-zeta/spawn.hpp> requires std::unique_ptr, but does not include it itself
#include <memory>
#include <memory_resource>
#include <actor-zeta/spawn.hpp>
// clang-format on

#include <catch2/catch_test_macros.hpp>

#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/types/logical_value.hpp>
#include <core/executor.hpp>
#include <core/pmr.hpp>
#include <core/result_wrapper.hpp>

#include <services/index/bitcask_index_agent.hpp>
#include <services/index/btree_index_agent.hpp>
#include <services/index/index_agent_contract.hpp>

#include <cstdint>
#include <filesystem>
#include <set>
#include <utility>
#include <vector>

#include "index_fixture_path.hpp"

using components::session::session_id_t;
using components::types::logical_value_t;
using services::index::bitcask_index_agent_t;
using services::index::btree_index_agent_t;
using services::index::index_agent_contract;

namespace {

    // +500000 keeps txn_id and commit_id apart: reusing one number let an earlier COMMIT vouch for a later frame.
    constexpr std::uint64_t commit_id_of(std::uint64_t txn_id) { return txn_id + 500000; }

    constexpr components::catalog::oid_t kTableOid = 17200;
    constexpr components::catalog::oid_t kIndexOid = 17201;

    std::filesystem::path fresh_index_root(const char* name) {
        const std::filesystem::path path{services::index::tests::index_fixture_path(name)};
        std::filesystem::remove_all(path);
        std::filesystem::create_directories(path / std::to_string(static_cast<unsigned>(kTableOid)) /
                                            std::to_string(static_cast<unsigned>(kIndexOid)));
        return path;
    }

    std::vector<std::pair<logical_value_t, size_t>> one_entry(std::pmr::memory_resource* resource) {
        std::vector<std::pair<logical_value_t, size_t>> values;
        values.emplace_back(logical_value_t(resource, int64_t{42}), size_t{7});
        return values;
    }

} // namespace

TEST_CASE("services::index::btree_index_agent_t refuses writes after its drop") {
    auto resource = core::pmr::otterbrix_resource();
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    const auto path = fresh_index_root("otterbrix_test_index_agent_dropped_writes_btree");

    auto agent_result =
        btree_index_agent_t::create(&resource, path, kTableOid, kIndexOid, /*flush_threshold=*/1000, log);
    REQUIRE_FALSE(agent_result.has_error());
    auto agent = std::move(agent_result.value());

    const auto session = session_id_t::generate_uid();

    auto [drop_sched, drop_future] =
        actor_zeta::otterbrix::send<&index_agent_contract::drop>(agent->address(), session);
    agent->resume(1);
    REQUIRE(drop_future.is_ready());

    auto [stage_sched, stage_future] =
        actor_zeta::otterbrix::send<&index_agent_contract::stage_inserts>(agent->address(),
                                                                          session,
                                                                          uint64_t{0},
                                                                          one_entry(&resource));
    agent->resume(1);
    REQUIRE(stage_future.is_ready());
    auto stage_error = std::move(stage_future).take_ready();
    INFO("staging into a dropped ordered index must be refused, not buffered where nothing will read it");
    REQUIRE(stage_error.contains_error());
    REQUIRE(stage_error.type == core::error_code_t::index_not_exists);

    auto [commit_sched, commit_future] =
        actor_zeta::otterbrix::send<&index_agent_contract::commit_inserts>(agent->address(),
                                                                           session,
                                                                           uint64_t{0},
                                                                           uint64_t{0});
    agent->resume(1);
    REQUIRE(commit_future.is_ready());
    auto commit_error = std::move(commit_future).take_ready();
    INFO("and so must the commit that would publish it into a null tree");
    REQUIRE(commit_error.contains_error());
    REQUIRE(commit_error.type == core::error_code_t::index_not_exists);

    auto [stage_del_sched, stage_del_future] =
        actor_zeta::otterbrix::send<&index_agent_contract::stage_deletes>(agent->address(),
                                                                          session,
                                                                          uint64_t{0},
                                                                          one_entry(&resource));
    agent->resume(1);
    REQUIRE(stage_del_future.is_ready());
    auto stage_del_error = std::move(stage_del_future).take_ready();
    INFO("a delete against a dropped ordered index must be refused for the same reason");
    REQUIRE(stage_del_error.contains_error());
    REQUIRE(stage_del_error.type == core::error_code_t::index_not_exists);

    auto [commit_del_sched, commit_del_future] =
        actor_zeta::otterbrix::send<&index_agent_contract::commit_deletes>(agent->address(),
                                                                           session,
                                                                           uint64_t{0},
                                                                           uint64_t{0});
    agent->resume(1);
    REQUIRE(commit_del_future.is_ready());
    auto commit_del_error = std::move(commit_del_future).take_ready();
    REQUIRE(commit_del_error.contains_error());
    REQUIRE(commit_del_error.type == core::error_code_t::index_not_exists);

    // clear() must also refuse, so a repopulate of a dropped index cannot report success.
    auto [clear_sched, clear_future] =
        actor_zeta::otterbrix::send<&index_agent_contract::clear>(agent->address(), session);
    agent->resume(1);
    REQUIRE(clear_future.is_ready());
    auto clear_error = std::move(clear_future).take_ready();
    REQUIRE(clear_error.contains_error());
    REQUIRE(clear_error.type == core::error_code_t::index_not_exists);

    std::filesystem::remove_all(path);
}

TEST_CASE("services::index::bitcask_index_agent_t refuses writes after its drop") {
    auto resource = core::pmr::otterbrix_resource();
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    const auto path = fresh_index_root("otterbrix_test_index_agent_dropped_writes_bitcask");

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

    auto [drop_sched, drop_future] =
        actor_zeta::otterbrix::send<&index_agent_contract::drop>(agent->address(), session);
    agent->resume(1);
    REQUIRE(drop_future.is_ready());

    auto [stage_sched, stage_future] =
        actor_zeta::otterbrix::send<&index_agent_contract::stage_inserts>(agent->address(),
                                                                          session,
                                                                          uint64_t{99},
                                                                          one_entry(&resource));
    agent->resume(1);
    REQUIRE(stage_future.is_ready());
    auto stage_error = std::move(stage_future).take_ready();
    INFO("staging into a dropped hashed index must be refused");
    REQUIRE(stage_error.contains_error());
    REQUIRE(stage_error.type == core::error_code_t::index_not_exists);

    auto [txn_sched, txn_future] = actor_zeta::otterbrix::send<&index_agent_contract::commit_inserts>(agent->address(),
                                                                                                      session,
                                                                                                      uint64_t{99},
                                                                                                      commit_id_of(99));
    agent->resume(1);
    REQUIRE(txn_future.is_ready());
    auto txn_error = std::move(txn_future).take_ready();
    INFO("a committed insert into a dropped hashed index must be refused, not journalled");
    REQUIRE(txn_error.contains_error());
    REQUIRE(txn_error.type == core::error_code_t::index_not_exists);

    auto [bulk_sched, bulk_future] =
        actor_zeta::otterbrix::send<&index_agent_contract::commit_inserts>(agent->address(),
                                                                           session,
                                                                           uint64_t{0},
                                                                           uint64_t{0});
    agent->resume(1);
    REQUIRE(bulk_future.is_ready());
    auto bulk_error = std::move(bulk_future).take_ready();
    INFO("and so must a rebuild feed, which takes the other route into the same freed store");
    REQUIRE(bulk_error.contains_error());
    REQUIRE(bulk_error.type == core::error_code_t::index_not_exists);

    auto [stage_del_sched, stage_del_future] =
        actor_zeta::otterbrix::send<&index_agent_contract::stage_deletes>(agent->address(),
                                                                          session,
                                                                          uint64_t{99},
                                                                          one_entry(&resource));
    agent->resume(1);
    REQUIRE(stage_del_future.is_ready());
    auto stage_del_error = std::move(stage_del_future).take_ready();
    REQUIRE(stage_del_error.contains_error());
    REQUIRE(stage_del_error.type == core::error_code_t::index_not_exists);

    auto [remove_sched, remove_future] =
        actor_zeta::otterbrix::send<&index_agent_contract::commit_deletes>(agent->address(),
                                                                           session,
                                                                           uint64_t{99},
                                                                           commit_id_of(99));
    agent->resume(1);
    REQUIRE(remove_future.is_ready());
    auto remove_error = std::move(remove_future).take_ready();
    INFO("a delete against a dropped hashed index must be refused for the same reason");
    REQUIRE(remove_error.contains_error());
    REQUIRE(remove_error.type == core::error_code_t::index_not_exists);

    std::filesystem::remove_all(path);
}
