// MANAGER-LEVEL GATES (wave entries #121/#122/#123/#167/#229).
//
// Each case drives manager_index_t's own handlers directly with the one agent pumped by
// hand, the way test_index_delete_horizon.cpp and test_index_catchup_delete_bucket.cpp
// do. The helpers are copied from there deliberately, for the reason stated there: a
// shared helper header for these files would be the start of a test framework nobody
// asked for.

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
using services::index::tests::index_fixture_path;

namespace {

    constexpr components::catalog::oid_t kTableOid = 18500;
    constexpr components::catalog::oid_t kIndexOid = 18501;

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

    template<typename T, typename Agent>
    void settle(actor_zeta::unique_future<T>& fut, Agent* agent) {
        for (int attempt = 0; attempt < 8 && !fut.is_ready(); ++attempt) {
            agent->resume(1);
            resume_awaited(fut);
        }
        REQUIRE(fut.is_ready());
    }

    template<auto Handler, typename Agent, typename... Args>
    auto ask(Agent* agent, Args&&... args) {
        auto [needs_sched, future] =
            actor_zeta::otterbrix::send<Handler>(agent->address(), std::forward<Args>(args)...);
        agent->resume(1);
        REQUIRE(future.is_ready());
        return std::move(future).take_ready();
    }

    components::index::keys_base_storage_t keys_of(std::pmr::memory_resource* resource,
                                                   std::initializer_list<const char*> names) {
        components::index::keys_base_storage_t keys(resource);
        for (const char* name : names) {
            keys.emplace_back(components::expressions::key_t{resource, name});
        }
        return keys;
    }

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
        const auto path = index_fixture_path(name);
        std::filesystem::remove_all(path);
        std::filesystem::create_directories(path / std::to_string(static_cast<unsigned>(kTableOid)) /
                                            std::to_string(static_cast<unsigned>(kIndexOid)));
        return path;
    }

    components::execution_context_t ctx_for(session_id_t session, uint64_t txn_id) {
        return components::execution_context_t{session, components::table::transaction_data{txn_id, 0}, {}};
    }

} // namespace

// Wave entry #229. create_index refuses a duplicate (keys, type) pair -- a second index
// over the same keys answered by the same backend is a pure cost, maintained twice on
// every DML. bootstrap_index_sync only checked the index oid, so a catalog that carried
// two such rows raised two full agents over two stores and the table paid for both.
TEST_CASE("services::index::manager::bootstrap refuses a duplicate keys+type pair") {
    auto resource = core::pmr::otterbrix_resource();
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    const auto path = fresh_index_root("index_manager_bootstrap_duplicate");

    auto scheduler = std::make_unique<actor_zeta::shared_work>(1, 100);
    auto manager = actor_zeta::spawn<manager_index_t>(&resource, scheduler.get(), log, path, 1000, 100, 1000);

    manager->bootstrap_engine_sync(kTableOid);
    REQUIRE_FALSE(manager
                      ->bootstrap_index_sync(kTableOid,
                                             kIndexOid,
                                             components::logical_plan::index_type::single,
                                             keys_of(&resource, {"id"}),
                                             std::pmr::set<std::uint64_t>(&resource))
                      .contains_error());

    // Same keys, same type, a different indexrelid: exactly what create_index refuses.
    auto duplicate = manager->bootstrap_index_sync(kTableOid,
                                                   kIndexOid + 1,
                                                   components::logical_plan::index_type::single,
                                                   keys_of(&resource, {"id"}),
                                                   std::pmr::set<std::uint64_t>(&resource));
    INFO("a second index over the same (keys, type) is a duplicate however it arrives");
    REQUIRE(duplicate.contains_error());

    manager.reset();
    std::filesystem::remove_all(path);
}

// Wave entry #121. A multi-column CREATE INDEX used to be accepted end to end while
// resolve_key_column returned only the FIRST key's column -- the index then stored and
// probed one column while its registered key set claimed several. Until a multi-column
// backend exists, the honest answer to the statement is a refusal at the gate.
TEST_CASE("services::index::manager::a multi-column key set is refused, not narrowed") {
    auto resource = core::pmr::otterbrix_resource();
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    const auto path = fresh_index_root("index_manager_multi_column_refusal");

    auto scheduler = std::make_unique<actor_zeta::shared_work>(1, 100);
    auto manager = actor_zeta::spawn<manager_index_t>(&resource, scheduler.get(), log, path, 1000, 100, 1000);

    manager->bootstrap_engine_sync(kTableOid);

    INFO("the bootstrap door");
    auto wired = manager->bootstrap_index_sync(kTableOid,
                                               kIndexOid,
                                               components::logical_plan::index_type::single,
                                               keys_of(&resource, {"id", "name"}),
                                               std::pmr::set<std::uint64_t>(&resource));
    REQUIRE(wired.contains_error());

    INFO("the statement door");
    const auto session = session_id_t::generate_uid();
    auto fut = manager->create_index(session,
                                     kTableOid,
                                     kIndexOid + 1,
                                     keys_of(&resource, {"id", "name"}),
                                     components::logical_plan::index_type::single,
                                     core::date::timezone_offset_t{});
    REQUIRE(fut.is_ready());
    REQUIRE(std::move(fut).take_ready().contains_error());

    manager.reset();
    std::filesystem::remove_all(path);
}

// Wave entry #122. A CREATE INDEX catchup record for a table the registry does not know
// used to be traced and DROPPED -- the build then published an index missing those rows
// and reported success. The handler's contract returns void (the operator cannot be
// taught a new signature from here), so the refusal is recorded against the build's
// transaction and surfaces where the build publishes: its commit_inserts.
TEST_CASE("services::index::manager::a catchup record the registry cannot place fails the build's commit") {
    auto resource = core::pmr::otterbrix_resource();
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    const auto path = fresh_index_root("index_manager_catchup_lost_record");

    auto scheduler = std::make_unique<actor_zeta::shared_work>(1, 100);
    auto manager = actor_zeta::spawn<manager_index_t>(&resource, scheduler.get(), log, path, 1000, 100, 1000);

    const auto session = session_id_t::generate_uid();
    const uint64_t build_txn = TRANSACTION_ID_START + 31;

    // No bootstrap_engine_sync for this oid: the registry cannot place the record.
    {
        std::pmr::vector<int64_t> row_ids(&resource);
        auto fut = manager->apply_wal_record_for_index(
            session,
            kTableOid,
            kIndexOid,
            /*wal_record_id=*/3,
            static_cast<uint8_t>(services::wal::wal_record_type::PHYSICAL_INSERT),
            std::move(row_ids),
            chunk_of(&resource, {10}),
            /*physical_row_start=*/0,
            build_txn,
            core::date::timezone_offset_t{});
        REQUIRE(fut.is_ready());
    }

    // The build publishes. Before the fix this returned no_error over a record that was
    // silently dropped from the index being built.
    {
        std::pmr::vector<components::catalog::oid_t> oids(&resource);
        oids.emplace_back(kTableOid);
        auto fut = manager->commit_inserts(ctx_for(session, build_txn), std::move(oids), /*commit_id=*/700);
        REQUIRE(fut.is_ready());
        auto commit = std::move(fut).take_ready();
        INFO("a build whose catchup lost a record must not publish as if it had not");
        REQUIRE(commit.contains_error());
    }

    // The abort mirror clears the recorded refusal: a NEW transaction is not haunted.
    {
        auto revert = manager->revert_insert(ctx_for(session, build_txn), kTableOid);
        REQUIRE(revert.is_ready());
        std::pmr::vector<components::catalog::oid_t> oids(&resource);
        oids.emplace_back(kTableOid);
        auto fut = manager->commit_inserts(ctx_for(session, build_txn + 1), std::move(oids), /*commit_id=*/701);
        REQUIRE(fut.is_ready());
        REQUIRE_FALSE(std::move(fut).take_ready().contains_error());
    }

    manager.reset();
    std::filesystem::remove_all(path);
}

// THE STAGING LEG MUST FEED THE INDEX IT NAMES, AND REFUSE WHEN IT CANNOT FIND IT.
//
// apply_wal_record_for_index takes an indexrelid and its declaration says it "locates the
// engine for (table_oid, index_oid)". It did not: the body looked the TABLE up and then
// staged the batch into EVERY record registered for that oid, using index_oid in its log
// lines and nowhere else. Two consequences, and the second is why this case exists:
//
//   * a build fed the table's other indexes rows they already held -- not a wrong answer,
//     because both stores dedup a repeated (key, row id) pair, but a full extra staging and
//     publication per pre-existing index (measured at the SQL surface by
//     integration/cpp/test/test_create_index_backfill_addressing.cpp);
//   * and an indexrelid the registry does NOT hold was INDISTINGUISHABLE from one it does.
//     A record naming an index that is not registered means the build's rows are going
//     nowhere, and the fan-out answered it by staging into the table's other indexes and
//     returning quietly. The handler's contract returns void, so the refusal goes where the
//     #122 one goes: recorded against the build's transaction, surfaced at the commit_inserts
//     the build must pass to publish.
//
// This is the channel the MAIN backfill leg now depends on as well -- operator_create_index_
// backfill_t stages its scan runs through this same addressed door.
TEST_CASE("services::index::manager::a staging record naming an unregistered index fails the build's commit") {
    auto resource = core::pmr::otterbrix_resource();
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    const auto path = fresh_index_root("index_manager_unaddressed_staging");

    auto scheduler = std::make_unique<actor_zeta::shared_work>(1, 100);
    auto manager = actor_zeta::spawn<manager_index_t>(&resource, scheduler.get(), log, path, 1000, 100, 1000);

    // The table IS registered and DOES carry an index -- so the #122 gate (no registry entry
    // for the table) cannot be what answers here. The record below names a DIFFERENT index.
    manager->bootstrap_engine_sync(kTableOid);
    REQUIRE_FALSE(manager
                      ->bootstrap_index_sync(kTableOid,
                                             kIndexOid,
                                             components::logical_plan::index_type::single,
                                             keys_of(&resource, {"id"}),
                                             std::pmr::set<std::uint64_t>(&resource))
                      .contains_error());

    const auto session = session_id_t::generate_uid();
    const uint64_t build_txn = TRANSACTION_ID_START + 53;

    {
        std::pmr::vector<int64_t> row_ids(&resource);
        auto fut = manager->apply_wal_record_for_index(
            session,
            kTableOid,
            kIndexOid + 7, // registered nowhere
            /*wal_record_id=*/11,
            static_cast<uint8_t>(services::wal::wal_record_type::PHYSICAL_INSERT),
            std::move(row_ids),
            chunk_of(&resource, {10}),
            /*physical_row_start=*/0,
            build_txn,
            core::date::timezone_offset_t{});
        REQUIRE(fut.is_ready());
    }

    {
        std::pmr::vector<components::catalog::oid_t> oids(&resource);
        oids.emplace_back(kTableOid);
        auto fut = manager->commit_inserts(ctx_for(session, build_txn), std::move(oids), /*commit_id=*/900);
        REQUIRE(fut.is_ready());
        auto commit = std::move(fut).take_ready();
        INFO("a build whose rows were addressed to an index nobody registered may not publish");
        REQUIRE(commit.contains_error());
    }

    // The abort mirror clears it, exactly as it does for the #122 refusal. This table DOES
    // carry a live agent, so the revert is a real round trip and has to be pumped.
    {
        auto agents = manager->owned_btree_agents_sync();
        REQUIRE(agents.size() == 1);
        auto revert = manager->revert_insert(ctx_for(session, build_txn), kTableOid);
        settle(revert, agents.front());
        std::pmr::vector<components::catalog::oid_t> oids(&resource);
        oids.emplace_back(kTableOid);
        auto fut = manager->commit_inserts(ctx_for(session, build_txn + 1), std::move(oids), /*commit_id=*/901);
        settle(fut, agents.front());
        REQUIRE_FALSE(std::move(fut).take_ready().contains_error());
    }

    manager.reset();
    std::filesystem::remove_all(path);
}

// Wave entry #123. A catchup staging the agent REFUSES used to be only logged, because
// the handler's contract return type is void -- the build then published an index that
// never took those rows. The refusal must be recorded and must fail the build's commit,
// even when the index itself is gone by then.
TEST_CASE("services::index::manager::a catchup staging the agent refused fails the build's commit") {
    auto resource = core::pmr::otterbrix_resource();
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    const auto path = fresh_index_root("index_manager_catchup_refused_staging");

    auto scheduler = std::make_unique<actor_zeta::shared_work>(1, 100);
    auto manager = actor_zeta::spawn<manager_index_t>(&resource, scheduler.get(), log, path, 1000, 100, 1000);

    manager->bootstrap_engine_sync(kTableOid);
    REQUIRE_FALSE(manager
                      ->bootstrap_index_sync(kTableOid,
                                             kIndexOid,
                                             components::logical_plan::index_type::single,
                                             keys_of(&resource, {"id"}),
                                             std::pmr::set<std::uint64_t>(&resource))
                      .contains_error());
    auto agents = manager->owned_btree_agents_sync();
    REQUIRE(agents.size() == 1);
    auto* agent = agents.front();

    const auto session = session_id_t::generate_uid();
    const uint64_t build_txn = TRANSACTION_ID_START + 41;

    // The agent is dropped BEHIND the registry -- the shape a DROP INDEX racing the
    // build's catchup produces: the record still routes, the agent refuses.
    {
        auto [needs_sched, fut] = actor_zeta::otterbrix::send<&index_agent_contract::drop>(agent->address(), session);
        agent->resume(1);
        REQUIRE(fut.is_ready());
    }

    // The catchup leg: the staging is refused by the dropped agent.
    {
        std::pmr::vector<int64_t> row_ids(&resource);
        auto fut = manager->apply_wal_record_for_index(
            session,
            kTableOid,
            kIndexOid,
            /*wal_record_id=*/5,
            static_cast<uint8_t>(services::wal::wal_record_type::PHYSICAL_INSERT),
            std::move(row_ids),
            chunk_of(&resource, {10}),
            /*physical_row_start=*/0,
            build_txn,
            core::date::timezone_offset_t{});
        settle(fut, agent);
    }

    // The index leaves the registry the ordinary way; the recorded refusal must survive
    // it -- it belongs to the TRANSACTION, not to the index.
    {
        auto fut = manager->drop_index(session, kTableOid, kIndexOid);
        settle(fut, agent);
    }

    // The build publishes into a registry with no record left. Before the fix that was a
    // silent no_error; the staged rows the agent refused were simply gone.
    {
        std::pmr::vector<components::catalog::oid_t> oids(&resource);
        oids.emplace_back(kTableOid);
        auto fut = manager->commit_inserts(ctx_for(session, build_txn), std::move(oids), /*commit_id=*/800);
        REQUIRE(fut.is_ready());
        auto commit = std::move(fut).take_ready();
        INFO("a build whose staging was refused must not publish as if it had landed");
        REQUIRE(commit.contains_error());
    }

    manager.reset();
    std::filesystem::remove_all(path);
}

// Wave entry #167. A deferred index erase the agent refuses used to leave the queue
// anyway -- the entry was erased BEFORE the await, so the erase was never retried and the
// index kept naming deleted rows until a repopulate happened to rebuild it. State leaves
// the queue only AFTER the erase succeeded; a refusal re-queues the entry for the next
// horizon.
TEST_CASE("services::index::manager::a refused deferred erase is re-queued, not forgotten") {
    auto resource = core::pmr::otterbrix_resource();
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    const auto path = fresh_index_root("index_manager_deferred_erase_requeue");

    auto scheduler = std::make_unique<actor_zeta::shared_work>(1, 100);
    auto manager = actor_zeta::spawn<manager_index_t>(&resource, scheduler.get(), log, path, 1000, 100, 1000);

    manager->bootstrap_engine_sync(kTableOid);
    REQUIRE_FALSE(manager
                      ->bootstrap_index_sync(kTableOid,
                                             kIndexOid,
                                             components::logical_plan::index_type::single,
                                             keys_of(&resource, {"id"}),
                                             std::pmr::set<std::uint64_t>(&resource))
                      .contains_error());
    auto agents = manager->owned_btree_agents_sync();
    REQUIRE(agents.size() == 1);
    auto* agent = agents.front();

    const auto session = session_id_t::generate_uid();
    const uint64_t insert_txn = TRANSACTION_ID_START + 51;
    const uint64_t delete_txn = TRANSACTION_ID_START + 52;
    const uint64_t reader_txn = TRANSACTION_ID_START + 53;

    // Three committed rows, flushed -- this also creates the tree's metadata file.
    {
        auto fut = manager->insert_rows(ctx_for(session, insert_txn),
                                        kTableOid,
                                        chunk_of(&resource, {10, 20, 30}),
                                        /*start_row_id=*/0,
                                        /*count=*/3);
        settle(fut, agent);
        REQUIRE_FALSE(std::move(fut).take_ready().contains_error());
    }
    {
        std::pmr::vector<components::catalog::oid_t> oids(&resource);
        oids.emplace_back(kTableOid);
        auto fut = manager->commit_inserts(ctx_for(session, insert_txn), std::move(oids), /*commit_id=*/100);
        settle(fut, agent);
        REQUIRE_FALSE(std::move(fut).take_ready().contains_error());
    }

    // One committed delete, held back for the horizon.
    {
        std::pmr::vector<int64_t> row_ids(&resource);
        row_ids.push_back(1);
        auto fut = manager->delete_rows(ctx_for(session, delete_txn),
                                        kTableOid,
                                        chunk_of(&resource, {20}),
                                        std::move(row_ids));
        settle(fut, agent);
        REQUIRE_FALSE(std::move(fut).take_ready().contains_error());
    }
    const auto queued_before = services::index::index_deferred_deletes();
    {
        std::pmr::vector<components::catalog::oid_t> oids(&resource);
        oids.emplace_back(kTableOid);
        auto fut = manager->commit_deletes(ctx_for(session, delete_txn), std::move(oids), /*commit_id=*/200);
        settle(fut, agent);
        REQUIRE_FALSE(std::move(fut).take_ready().contains_error());
    }
    REQUIRE(services::index::index_deferred_deletes() == queued_before + 1);

    // The publish will fail: the tree's metadata file refuses the write.
    const auto metadata_path = path / std::to_string(static_cast<unsigned>(kTableOid)) /
                               std::to_string(static_cast<unsigned>(kIndexOid)) / "metadata";
    REQUIRE(std::filesystem::exists(metadata_path));
    std::filesystem::permissions(metadata_path, std::filesystem::perms::owner_read);

    {
        auto sweep = manager->on_horizon_advanced(/*new_horizon=*/1000);
        settle(sweep, agent);
    }
    INFO("an erase the agent refused must still be OWED: the entry stays queued for the next horizon");
    // RED before the fix: the entry left the queue before the refusal arrived, so the
    // meter dropped back and nothing would ever retry the erase.
    REQUIRE(services::index::index_deferred_deletes() == queued_before + 1);

    // The device heals; the next horizon publishes what is owed.
    std::filesystem::permissions(metadata_path,
                                 std::filesystem::perms::owner_read | std::filesystem::perms::owner_write);
    {
        auto sweep = manager->on_horizon_advanced(/*new_horizon=*/1001);
        settle(sweep, agent);
    }
    REQUIRE(services::index::index_deferred_deletes() == queued_before);

    // And the erase LANDED: the index no longer names the deleted row.
    {
        auto answer = ask<&index_agent_contract::read_rows>(agent,
                                                            session,
                                                            compare_type::gte,
                                                            logical_value_t(&resource, int64_t{10}),
                                                            reader_txn);
        REQUIRE_FALSE(answer.has_error());
        auto rows = std::move(answer.value());
        std::vector<int64_t> out(rows.begin(), rows.end());
        std::sort(out.begin(), out.end());
        REQUIRE(out == std::vector<int64_t>{0, 2});
    }

    manager.reset();
    std::filesystem::remove_all(path);
}
