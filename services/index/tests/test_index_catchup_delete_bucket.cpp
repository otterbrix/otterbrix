// THE CREATE INDEX CATCHUP MAY ADD TO THE INDEX. IT MAY NOT TAKE ANYTHING AWAY.
//
// operator_create_index_backfill scans the table under the build transaction's snapshot
// and then re-reads the journal past the build's start watermark, so rows committed while
// the scan ran are not missed. It hands each record to manager_index_t::apply_wal_record_for_index,
// and that handler used to have TWO legs: PHYSICAL_INSERT/UPDATE staged inserts, and
// PHYSICAL_DELETE staged DELETES into pending_deletes_[the CREATE INDEX txn].
//
// The delete leg is not a mirror of the insert leg, for the reason C5c wrote down about
// commits and repeats about replays. A physical record reaches the journal BEFORE its
// transaction has decided anything -- operator_delete writes physical_delete ahead of the
// storage mark and says so ("uncommitted deletes are filtered by replay") -- and the
// filter it names is the COMMIT marker, which crash recovery reads and this catchup does
// not. So the same undecided record fails in opposite directions on the two legs:
//   INSERT leg  names a row nobody can see  -> a SUPERSET, and storage_fetch drops it
//                                              under the reader's own snapshot.
//   DELETE leg  takes an id off a LIVE row  -> a SUBSET, and nothing downstream can put
//                                              back an id the index never named.
//
// On top of that the bucket had no exit. A build publishes through commit_inserts (the
// executor's CREATE INDEX back-channel); the batch commit_deletes keys off the base-table
// DELETE ranges, which a build has none of. So the staged deletes were neither published
// nor reverted -- they stayed in the agent for the life of the index, and read_rows merges
// the ASKING transaction's own bucket, so the one reader they were visible to was the
// build itself, which they answered SHORT.
//
// Both test cases below drive the manager's handlers directly with the agent pumped by
// hand, the way test_index_delete_horizon.cpp does, so the interleaving is chosen rather
// than raced for.

// clang-format off
// <actor-zeta/spawn.hpp> requires std::unique_ptr, but does not include it itself
#include <memory>
#include <memory_resource>
#include <actor-zeta/spawn.hpp>
// clang-format on

#include <catch2/catch_test_macros.hpp>

#include <components/context/execution_context.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/key.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/session/session.hpp>
#include <components/table/row_version_manager.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/executor.hpp>
#include <core/pmr.hpp>

#include <services/index/index_agent_contract.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/record.hpp>

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
using services::index::index_agent_contract;
using services::index::manager_index_t;

namespace {

    constexpr components::catalog::oid_t kTableOid = 17500;
    constexpr components::catalog::oid_t kIndexOid = 17501;

    // Resume the coroutine `fut` is suspended in, the way the manager's own loop thread
    // does. Copied from test_index_delete_horizon.cpp deliberately, for the reason stated
    // there: a shared helper header for three test files would be the start of a test
    // framework nobody asked for.
    template<typename T>
    bool resume_awaited(const actor_zeta::unique_future<T>& fut) {
        auto handle = fut.coroutine_handle();
        if (!handle || handle.done()) {
            return false;
        }
        auto* cont_ptr = handle.promise().awaited_continuation_;
        if (!cont_ptr) {
            return false;
        }
        auto cont = cont_ptr->exchange(nullptr, std::memory_order_acq_rel);
        if (!cont) {
            return false;
        }
        cont.resume();
        return true;
    }

    // Drive a manager handler to completion, pumping the one agent it can talk to.
    template<typename T, typename Agent>
    void settle(actor_zeta::unique_future<T>& fut, Agent* agent) {
        for (int attempt = 0; attempt < 8 && !fut.is_ready(); ++attempt) {
            agent->resume(1);
            resume_awaited(fut);
        }
        REQUIRE(fut.is_ready());
    }

    // One message to the agent, pumped to completion, its reply taken.
    template<auto Handler, typename Agent, typename... Args>
    auto ask(Agent* agent, Args&&... args) {
        auto [needs_sched, future] =
            actor_zeta::otterbrix::send<Handler>(agent->address(), std::forward<Args>(args)...);
        agent->resume(1);
        REQUIRE(future.is_ready());
        return std::move(future).take_ready();
    }

    components::index::keys_base_storage_t one_key(std::pmr::memory_resource* resource) {
        components::index::keys_base_storage_t keys(resource);
        keys.emplace_back(components::expressions::key_t{resource, "id"});
        return keys;
    }

    // One chunk carrying the index's key column under the alias the manager resolves by.
    std::pmr::vector<components::vector::data_chunk_t> chunk_of(std::pmr::memory_resource* resource,
                                                                const std::vector<int64_t>& keys) {
        std::pmr::vector<components::types::complex_logical_type> types(resource);
        types.emplace_back(components::types::logical_type::BIGINT, "id");
        components::vector::data_chunk_t chunk(resource, types, keys.size());
        for (std::size_t i = 0; i < keys.size(); ++i) {
            chunk.set_value(uint64_t{0}, static_cast<uint64_t>(i), keys[i]);
        }
        chunk.set_cardinality(keys.size());
        std::pmr::vector<components::vector::data_chunk_t> chunks(resource);
        chunks.push_back(std::move(chunk));
        return chunks;
    }

    std::filesystem::path fresh_index_root(const char* name) {
        const std::filesystem::path path{services::index::tests::index_fixture_path(name)};
        std::filesystem::remove_all(path);
        std::filesystem::create_directories(path / std::to_string(static_cast<unsigned>(kTableOid)) /
                                            std::to_string(static_cast<unsigned>(kIndexOid)));
        return path;
    }

    components::execution_context_t ctx_for(session_id_t session, uint64_t txn_id) {
        return components::execution_context_t{session, components::table::transaction_data{txn_id, 0}, {}};
    }

} // namespace

// THE RED ONE.
//
// Three rows are backfilled by the scan leg (keys 10/20/30 at physical ids 0/1/2). The
// journal then shows one PHYSICAL_DELETE for the middle row -- a record with no commit
// marker behind it, which is every physical record the catchup can ever see. A full scan
// of the table at this point still answers all three rows: nothing committed that delete.
// The index must answer with AT LEAST those three ids, to every reader, the build included.
//
// Before the fix the build's own read came back {0, 2}: the staged delete sat in
// pending_deletes_[build txn] and read_rows merges the asking transaction's own bucket.
TEST_CASE("services::index::a CREATE INDEX catchup delete never shrinks the built index") {
    auto resource = core::pmr::otterbrix_resource();
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    const auto path = fresh_index_root("otterbrix_test_index_catchup_delete_bucket");

    // Never started: everything below is driven by hand.
    auto scheduler = std::make_unique<actor_zeta::shared_work>(1, 100);

    auto manager = actor_zeta::spawn<manager_index_t>(&resource,
                                                      scheduler.get(),
                                                      log,
                                                      path,
                                                      /*bitcask_flush_threshold=*/1000,
                                                      /*bitcask_segment_record_limit=*/100,
                                                      /*btree_flush_threshold=*/1000);

    manager->bootstrap_engine_sync(kTableOid);
    REQUIRE_FALSE(manager
                      ->bootstrap_index_sync(kTableOid,
                                             kIndexOid,
                                             components::logical_plan::index_type::single,
                                             one_key(&resource),
                                             std::pmr::set<std::uint64_t>(&resource))
                      .contains_error());
    auto agents = manager->owned_btree_agents_sync();
    REQUIRE(agents.size() == 1);
    auto* agent = agents.front();

    const auto session = session_id_t::generate_uid();
    const uint64_t build_txn = TRANSACTION_ID_START + 11;
    const uint64_t onlooker_txn = TRANSACTION_ID_START + 12;
    const uint64_t build_commit_id = 300;

    // What the table holds, and what a full scan of it therefore answers for `id >= 10`.
    const std::vector<int64_t> live_rows{0, 1, 2};

    // 1. The backfill scan leg: three rows, contiguous from physical id 0.
    {
        auto fut = manager->insert_rows(ctx_for(session, build_txn),
                                        kTableOid,
                                        chunk_of(&resource, {10, 20, 30}),
                                        /*start_row_id=*/0,
                                        /*count=*/3);
        settle(fut, agent);
        REQUIRE_FALSE(std::move(fut).take_ready().contains_error());
    }

    // 2. The catchup leg, exactly as operator_create_index_backfill sends it: one
    //    PHYSICAL_DELETE naming physical row 1, carrying the OLD chunk the operator
    //    recovered with a RAW storage_fetch, tagged with the CREATE INDEX transaction.
    const auto deferred_before = services::index::index_deferred_deletes();
    {
        std::pmr::vector<int64_t> row_ids(&resource);
        row_ids.push_back(1);
        auto fut = manager->apply_wal_record_for_index(
            session,
            kTableOid,
            kIndexOid,
            /*wal_record_id=*/7,
            static_cast<uint8_t>(services::wal::wal_record_type::PHYSICAL_DELETE),
            std::move(row_ids),
            chunk_of(&resource, {20}),
            /*physical_row_start=*/0,
            build_txn,
            core::date::timezone_offset_t{});
        settle(fut, agent);
    }

    // 3. The build publishes, the only way a build ever does: commit_inserts for the
    //    indexed table (executor.cpp, the CREATE INDEX back-channel). There is no
    //    commit_deletes here and there cannot be one -- the build wrote no base-table
    //    DELETE ranges.
    {
        std::pmr::vector<components::catalog::oid_t> oids(&resource);
        oids.emplace_back(kTableOid);
        auto fut = manager->commit_inserts(ctx_for(session, build_txn), std::move(oids), build_commit_id);
        settle(fut, agent);
        REQUIRE_FALSE(std::move(fut).take_ready().contains_error());
    }

    const auto probe = [&](uint64_t txn_id) {
        auto answer = ask<&index_agent_contract::read_rows>(agent,
                                                            session,
                                                            compare_type::gte,
                                                            logical_value_t(&resource, int64_t{10}),
                                                            txn_id);
        REQUIRE_FALSE(answer.has_error());
        auto rows = std::move(answer.value());
        std::vector<int64_t> out(rows.begin(), rows.end());
        std::sort(out.begin(), out.end());
        return out;
    };

    INFO("every other reader must be handed the whole live set");
    CHECK(probe(onlooker_txn) == live_rows);

    INFO("and so must the transaction that built the index -- an undecided journal delete "
         "may not be subtracted from anyone's answer");
    // RED before the fix: {0, 2}. The build's own bucket ate physical row 1.
    CHECK(probe(build_txn) == live_rows);

    INFO("the catchup must not have queued an erase either: a held-back erase is a "
         "COMMITTED delete waiting for the horizon, and this one has not committed");
    CHECK(services::index::index_deferred_deletes() == deferred_before);

    manager.reset();
    std::filesystem::remove_all(path);
}

// THE DIRECTION GUARD.
//
// The bucket the build left behind cannot be fixed by publishing it later, so this pins
// the other end: a horizon that has run past every commit id in sight must not erase
// anything the catchup saw. Nothing committed those deletes -- the horizon has no opinion
// about them and must not be given one.
TEST_CASE("services::index::the horizon does not erase what a CREATE INDEX catchup read") {
    auto resource = core::pmr::otterbrix_resource();
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    const auto path = fresh_index_root("otterbrix_test_index_catchup_delete_horizon");

    auto scheduler = std::make_unique<actor_zeta::shared_work>(1, 100);

    auto manager = actor_zeta::spawn<manager_index_t>(&resource,
                                                      scheduler.get(),
                                                      log,
                                                      path,
                                                      /*bitcask_flush_threshold=*/1000,
                                                      /*bitcask_segment_record_limit=*/100,
                                                      /*btree_flush_threshold=*/1000);

    manager->bootstrap_engine_sync(kTableOid);
    REQUIRE_FALSE(manager
                      ->bootstrap_index_sync(kTableOid,
                                             kIndexOid,
                                             components::logical_plan::index_type::single,
                                             one_key(&resource),
                                             std::pmr::set<std::uint64_t>(&resource))
                      .contains_error());
    auto agents = manager->owned_btree_agents_sync();
    REQUIRE(agents.size() == 1);
    auto* agent = agents.front();

    const auto session = session_id_t::generate_uid();
    const uint64_t build_txn = TRANSACTION_ID_START + 21;
    const uint64_t reader_txn = TRANSACTION_ID_START + 22;

    {
        auto fut = manager->insert_rows(ctx_for(session, build_txn),
                                        kTableOid,
                                        chunk_of(&resource, {10, 20, 30}),
                                        /*start_row_id=*/0,
                                        /*count=*/3);
        settle(fut, agent);
        REQUIRE_FALSE(std::move(fut).take_ready().contains_error());
    }
    {
        std::pmr::vector<int64_t> row_ids(&resource);
        row_ids.push_back(1);
        auto fut = manager->apply_wal_record_for_index(
            session,
            kTableOid,
            kIndexOid,
            /*wal_record_id=*/9,
            static_cast<uint8_t>(services::wal::wal_record_type::PHYSICAL_DELETE),
            std::move(row_ids),
            chunk_of(&resource, {20}),
            /*physical_row_start=*/0,
            build_txn,
            core::date::timezone_offset_t{});
        settle(fut, agent);
    }
    {
        std::pmr::vector<components::catalog::oid_t> oids(&resource);
        oids.emplace_back(kTableOid);
        auto fut = manager->commit_inserts(ctx_for(session, build_txn), std::move(oids), /*commit_id=*/400);
        settle(fut, agent);
        REQUIRE_FALSE(std::move(fut).take_ready().contains_error());
    }

    auto sweep = manager->on_horizon_advanced(/*new_horizon=*/100000);
    settle(sweep, agent);

    auto answer = ask<&index_agent_contract::read_rows>(agent,
                                                        session,
                                                        compare_type::gte,
                                                        logical_value_t(&resource, int64_t{10}),
                                                        reader_txn);
    REQUIRE_FALSE(answer.has_error());
    auto rows = std::move(answer.value());
    std::vector<int64_t> out(rows.begin(), rows.end());
    std::sort(out.begin(), out.end());
    CHECK(out == std::vector<int64_t>{0, 1, 2});

    manager.reset();
    std::filesystem::remove_all(path);
}
