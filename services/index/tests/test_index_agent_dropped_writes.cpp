// A DROPPED AGENT MUST REFUSE A WRITE, NOT PERFORM ONE.
//
// drop() releases the agent's store: the ordered family resets its btree_t (db_ becomes
// null) and the hashed one resets its segment handle, its txn-log handle and its keydir.
// Every write path below that point dereferences one of those. The read side has said so
// since the read went through the mailbox -- read_rows() checks is_dropped_ and answers
// with an error -- and force_flush() checks it too. The WRITE handlers never did:
//
//     btree      insert_bulk_unchecked -> db_->append(...)          // db_ == nullptr
//     bitcask    apply_txn_inserts     -> append_snapshot -> file_->seek_position()
//                insert_bulk_unchecked -> ... -> hash_index_->put   // both reset
//
// A write is TWO messages now -- stage_* records it in the transaction's bucket, commit_*
// publishes that bucket into the store -- and BOTH must refuse. Staging into a dropped
// agent would not crash, which is exactly why it has to be checked: the bucket would take
// the rows and nothing would ever read it again, so the statement would be told its rows
// are indexed by an index that no longer exists.
//
// The agent keeps a LIVE ADDRESS after its drop is handled -- it is destroyed by its
// owner, which is a separate step -- so a message posted before that owner lets go still
// arrives at these handlers. Nothing above them can filter it out: the manager can only
// stop NEW sends, and the whole reason drop_index and the GC sweeps await the drop reply
// is that a message already in the mailbox cannot be recalled.
//
// The answer is an ERROR, not a silent skip: reporting no_error would tell a statement
// its rows are in the index when the index no longer exists, and commit_inserts folds
// exactly this value into what it returns.
//
// The test drives each agent by hand (cooperative_actor::resume(1)) so "drop handled,
// write posted behind it" is a state it chooses rather than races for.

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

    // No scheduler at all: the agent is pumped by hand below
    // (cooperative_actor::resume(1)), so nothing runs behind the test's back.
    auto agent_result =
        btree_index_agent_t::create(&resource, path, kTableOid, kIndexOid, /*flush_threshold=*/1000, log);
    REQUIRE_FALSE(agent_result.has_error());
    auto agent = std::move(agent_result.value());

    const auto session = session_id_t::generate_uid();

    auto [drop_sched, drop_future] =
        actor_zeta::otterbrix::send<&index_agent_contract::drop>(agent->address(), session);
    agent->resume(1);
    REQUIRE(drop_future.is_ready());

    // The writes posted AFTER the drop was handled. The address is still live, so they
    // arrive; the store behind them is gone.
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
        actor_zeta::otterbrix::send<&index_agent_contract::commit_inserts>(agent->address(), session, uint64_t{0});
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
        actor_zeta::otterbrix::send<&index_agent_contract::commit_deletes>(agent->address(), session, uint64_t{0});
    agent->resume(1);
    REQUIRE(commit_del_future.is_ready());
    auto commit_del_error = std::move(commit_del_future).take_ready();
    REQUIRE(commit_del_error.contains_error());
    REQUIRE(commit_del_error.type == core::error_code_t::index_not_exists);

    // clear() releases nothing but would wipe a store that is already gone; it refuses
    // too, so a repopulate of a dropped index cannot report success.
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

    // No scheduler at all: the agent is pumped by hand, as above.
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

    // Staging first: a refusal here is what keeps a statement from being told its rows
    // are indexed while they sit in a bucket nothing will ever publish.
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

    // txn_id != 0: the JOURNALLED route, the one only this family has. It opens the
    // txn-log file and then walks into append_snapshot, which writes through the segment
    // handle drop() reset.
    auto [txn_sched, txn_future] =
        actor_zeta::otterbrix::send<&index_agent_contract::commit_inserts>(agent->address(), session, uint64_t{99});
    agent->resume(1);
    REQUIRE(txn_future.is_ready());
    auto txn_error = std::move(txn_future).take_ready();
    INFO("a committed insert into a dropped hashed index must be refused, not journalled");
    REQUIRE(txn_error.contains_error());
    REQUIRE(txn_error.type == core::error_code_t::index_not_exists);

    // txn_id == 0: the direct bulk route, through the keydir drop() released.
    auto [bulk_sched, bulk_future] =
        actor_zeta::otterbrix::send<&index_agent_contract::commit_inserts>(agent->address(), session, uint64_t{0});
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
        actor_zeta::otterbrix::send<&index_agent_contract::commit_deletes>(agent->address(), session, uint64_t{99});
    agent->resume(1);
    REQUIRE(remove_future.is_ready());
    auto remove_error = std::move(remove_future).take_ready();
    INFO("a delete against a dropped hashed index must be refused for the same reason");
    REQUIRE(remove_error.contains_error());
    REQUIRE(remove_error.type == core::error_code_t::index_not_exists);

    std::filesystem::remove_all(path);
}
