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
#include <cstdlib>
#include <filesystem>
#include <string>
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

    // Kept far from txn_id: reusing one number for both let a COMMIT marker from an earlier
    // incarnation vouch for a later frame under a recycled txn id (bitcask_index_disk.cpp).
    constexpr std::uint64_t commit_id_of(std::uint64_t txn_id) { return txn_id + 500000; }

    constexpr components::catalog::oid_t kTableOid = 17300;
    constexpr components::catalog::oid_t kIndexOid = 17301;

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

    // Every handler under test is a straight-line coroutine with no cross-actor await,
    // so one resume finishes it.
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

    // Restores the previous value rather than unsetting blindly, so a case can't leak an
    // arming into the rest of the run.
    struct env_var_guard_t {
        std::string name;
        bool had_value{false};
        std::string prev;

        env_var_guard_t(std::string env_name, const std::string& value)
            : name(std::move(env_name)) {
            if (const char* current = std::getenv(name.c_str()); current != nullptr) {
                had_value = true;
                prev = current;
            }
            setenv(name.c_str(), value.c_str(), 1);
        }

        ~env_var_guard_t() {
            if (had_value) {
                setenv(name.c_str(), prev.c_str(), 1);
            } else {
                unsetenv(name.c_str());
            }
        }

        env_var_guard_t(const env_var_guard_t&) = delete;
        env_var_guard_t& operator=(const env_var_guard_t&) = delete;
    };

} // namespace

// --- The txn buffer, over the ORDERED family --------------------------------------

TEST_CASE("services::index::btree_index_agent_t buffers a transaction's own writes") {
    auto resource = core::pmr::otterbrix_resource();
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    const auto path = fresh_index_root("otterbrix_test_index_agent_buffer_btree");

    auto agent_result =
        btree_index_agent_t::create(&resource, path, kTableOid, kIndexOid, /*flush_threshold=*/1000, log);
    REQUIRE_FALSE(agent_result.has_error());
    auto agent = std::move(agent_result.value());

    const auto session = session_id_t::generate_uid();
    const uint64_t txn1 = TRANSACTION_ID_START + 1;
    const uint64_t txn2 = TRANSACTION_ID_START + 2;
    const logical_value_t val42(&resource, int64_t{42});

    auto read = [&](uint64_t txn_id, compare_type compare) {
        auto answer =
            ask<&index_agent_contract::read_rows>(agent, session, compare, logical_value_t(&resource, val42), txn_id);
        REQUIRE_FALSE(answer.has_error());
        return sorted(std::move(answer.value()));
    };

    SECTION("a transaction sees its own staged insert, and nobody else does") {
        REQUIRE_FALSE(ask<&index_agent_contract::stage_inserts>(agent, session, txn1, entries(&resource, {{42, 0}}))
                          .contains_error());
        CHECK(read(txn1, compare_type::eq) == std::vector<int64_t>{0});
        CHECK(read(txn2, compare_type::eq).empty());
    }

    SECTION("committing publishes the bucket into the tree, where everyone reads it") {
        REQUIRE_FALSE(ask<&index_agent_contract::stage_inserts>(agent, session, txn1, entries(&resource, {{42, 0}}))
                          .contains_error());
        REQUIRE_FALSE(
            ask<&index_agent_contract::commit_inserts>(agent, session, txn1, commit_id_of(txn1)).contains_error());
        CHECK(read(txn2, compare_type::eq) == std::vector<int64_t>{0});
        CHECK(read(0, compare_type::eq) == std::vector<int64_t>{0});
    }

    SECTION("aborting erases the bucket and touches nothing durable") {
        REQUIRE_FALSE(ask<&index_agent_contract::stage_inserts>(agent, session, txn1, entries(&resource, {{42, 0}}))
                          .contains_error());
        REQUIRE(read(txn1, compare_type::eq).size() == 1);
        REQUIRE_FALSE(ask<&index_agent_contract::revert_inserts>(agent, session, txn1).contains_error());
        CHECK(read(txn1, compare_type::eq).empty());
        CHECK(read(txn2, compare_type::eq).empty());
    }

    SECTION("a staged delete hides a committed row from the transaction that staged it") {
        REQUIRE_FALSE(ask<&index_agent_contract::stage_inserts>(agent, session, txn1, entries(&resource, {{42, 7}}))
                          .contains_error());
        REQUIRE_FALSE(
            ask<&index_agent_contract::commit_inserts>(agent, session, txn1, commit_id_of(txn1)).contains_error());
        REQUIRE(read(txn2, compare_type::eq) == std::vector<int64_t>{7});

        REQUIRE_FALSE(ask<&index_agent_contract::stage_deletes>(agent, session, txn2, entries(&resource, {{42, 7}}))
                          .contains_error());
        INFO("the deleting transaction must stop seeing the row");
        CHECK(read(txn2, compare_type::eq).empty());
        INFO("and everyone else must still see it, because the delete is not committed");
        CHECK(read(TRANSACTION_ID_START + 3, compare_type::eq) == std::vector<int64_t>{7});

        REQUIRE_FALSE(
            ask<&index_agent_contract::commit_deletes>(agent, session, txn2, commit_id_of(txn2)).contains_error());
        CHECK(read(TRANSACTION_ID_START + 3, compare_type::eq).empty());
    }

    SECTION("a row inserted AND deleted by one transaction ends up absent") {
        REQUIRE_FALSE(ask<&index_agent_contract::stage_inserts>(agent, session, txn1, entries(&resource, {{42, 5}}))
                          .contains_error());
        REQUIRE_FALSE(ask<&index_agent_contract::stage_deletes>(agent, session, txn1, entries(&resource, {{42, 5}}))
                          .contains_error());
        CHECK(read(txn1, compare_type::eq).empty());
    }

    SECTION("clear() wipes the tree and the REBUILD's bucket, and nobody else's") {
        REQUIRE_FALSE(ask<&index_agent_contract::stage_inserts>(agent, session, txn1, entries(&resource, {{42, 1}}))
                          .contains_error());
        REQUIRE_FALSE(
            ask<&index_agent_contract::commit_inserts>(agent, session, txn1, commit_id_of(txn1)).contains_error());
        REQUIRE_FALSE(ask<&index_agent_contract::stage_inserts>(agent, session, txn2, entries(&resource, {{42, 2}}))
                          .contains_error());
        REQUIRE(read(txn2, compare_type::eq).size() == 2);

        REQUIRE_FALSE(ask<&index_agent_contract::clear>(agent, session).contains_error());

        INFO("the committed row must not survive the clear");
        CHECK(read(txn1, compare_type::eq).empty());

        // clear() is scoped to bucket 0 (repopulate_table's own txn). Taking txn2's live bucket
        // too would let its later commit_inserts report success over an empty journal -- a
        // short answer, which is worse than the superset a surviving bucket risks. Owner's
        // decision, 2026-09-05; see test_index_agent_rebuild_clear.cpp.
        INFO("a live writer's staged keys are its own and must outlive a rebuild's clear");
        auto own = read(txn2, compare_type::eq);
        REQUIRE(own.size() == 1);
        CHECK(own.front() == 2);
    }

    std::filesystem::remove_all(path);
}

// Ordered agents answer all six predicates over staged rows, not just equality --
// a merge that only knew equality would drop a transaction's own write from a range query.
TEST_CASE("services::index::btree_index_agent_t answers every predicate over staged rows") {
    auto resource = core::pmr::otterbrix_resource();
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    const auto path = fresh_index_root("otterbrix_test_index_agent_buffer_btree_predicates");

    auto agent_result =
        btree_index_agent_t::create(&resource, path, kTableOid, kIndexOid, /*flush_threshold=*/1000, log);
    REQUIRE_FALSE(agent_result.has_error());
    auto agent = std::move(agent_result.value());

    const auto session = session_id_t::generate_uid();
    const uint64_t txn = TRANSACTION_ID_START + 11;
    REQUIRE_FALSE(
        ask<&index_agent_contract::stage_inserts>(agent, session, txn, entries(&resource, {{3, 30}, {5, 50}, {7, 70}}))
            .contains_error());

    auto probe = [&](compare_type compare, int64_t key) {
        auto answer =
            ask<&index_agent_contract::read_rows>(agent, session, compare, logical_value_t(&resource, key), txn);
        REQUIRE_FALSE(answer.has_error());
        return sorted(std::move(answer.value()));
    };

    CHECK(probe(compare_type::eq, 5) == std::vector<int64_t>{50});
    CHECK(probe(compare_type::ne, 5) == std::vector<int64_t>{30, 70});
    CHECK(probe(compare_type::lt, 5) == std::vector<int64_t>{30});
    CHECK(probe(compare_type::lte, 5) == std::vector<int64_t>{30, 50});
    CHECK(probe(compare_type::gt, 5) == std::vector<int64_t>{70});
    CHECK(probe(compare_type::gte, 5) == std::vector<int64_t>{50, 70});

    std::filesystem::remove_all(path);
}

// --- The txn buffer, over the HASHED family ---------------------------------------

TEST_CASE("services::index::bitcask_index_agent_t buffers a transaction's own writes") {
    auto resource = core::pmr::otterbrix_resource();
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    const auto path = fresh_index_root("otterbrix_test_index_agent_buffer_bitcask");

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

    auto read = [&](uint64_t txn_id, const logical_value_t& key) {
        auto answer = ask<&index_agent_contract::read_rows>(agent,
                                                            session,
                                                            compare_type::eq,
                                                            logical_value_t(&resource, key),
                                                            txn_id);
        REQUIRE_FALSE(answer.has_error());
        return sorted(std::move(answer.value()));
    };

    SECTION("own staged insert, then commit, then a staged delete") {
        const logical_value_t val42(&resource, int64_t{42});
        REQUIRE_FALSE(ask<&index_agent_contract::stage_inserts>(agent, session, txn1, entries(&resource, {{42, 7}}))
                          .contains_error());
        CHECK(read(txn1, val42) == std::vector<int64_t>{7});
        CHECK(read(txn2, val42).empty());

        REQUIRE_FALSE(
            ask<&index_agent_contract::commit_inserts>(agent, session, txn1, commit_id_of(txn1)).contains_error());
        CHECK(read(txn2, val42) == std::vector<int64_t>{7});

        REQUIRE_FALSE(ask<&index_agent_contract::stage_deletes>(agent, session, txn2, entries(&resource, {{42, 7}}))
                          .contains_error());
        CHECK(read(txn2, val42).empty());
        CHECK(read(TRANSACTION_ID_START + 3, val42) == std::vector<int64_t>{7});

        REQUIRE_FALSE(
            ask<&index_agent_contract::commit_deletes>(agent, session, txn2, commit_id_of(txn2)).contains_error());
        CHECK(read(TRANSACTION_ID_START + 3, val42).empty());
    }

    SECTION("aborting erases the bucket") {
        const logical_value_t val77(&resource, int64_t{77});
        REQUIRE_FALSE(ask<&index_agent_contract::stage_inserts>(agent, session, txn1, entries(&resource, {{77, 9}}))
                          .contains_error());
        REQUIRE(read(txn1, val77).size() == 1);
        REQUIRE_FALSE(ask<&index_agent_contract::revert_inserts>(agent, session, txn1).contains_error());
        CHECK(read(txn1, val77).empty());
    }

    // A hashed key is normalized to BIGINT/UBIGINT before keying, in both the agent's encoder
    // and the store's key_bytes_for_hash -- checked here across both halves.
    SECTION("a SMALLINT probe matches a BIGINT-stored key, in the bucket and in the store") {
        REQUIRE_FALSE(ask<&index_agent_contract::stage_inserts>(agent, session, txn1, entries(&resource, {{4242, 1}}))
                          .contains_error());
        const logical_value_t probe_small(&resource, int16_t{4242});
        INFO("the staged half must be found by a narrower probe");
        CHECK(read(txn1, probe_small) == std::vector<int64_t>{1});

        REQUIRE_FALSE(
            ask<&index_agent_contract::commit_inserts>(agent, session, txn1, commit_id_of(txn1)).contains_error());
        INFO("and so must the committed half, or the two halves would key differently");
        CHECK(read(txn2, probe_small) == std::vector<int64_t>{1});

        CHECK(read(txn2, logical_value_t(&resource, int16_t{4243})).empty());
    }

    // A range refusal must be a VALUE, not an empty result: an empty range is
    // indistinguishable from "no row carries this key".
    SECTION("a range predicate is refused, loudly") {
        for (auto compare :
             {compare_type::ne, compare_type::lt, compare_type::lte, compare_type::gt, compare_type::gte}) {
            auto answer = ask<&index_agent_contract::read_rows>(agent,
                                                                session,
                                                                compare,
                                                                logical_value_t(&resource, int64_t{42}),
                                                                txn1);
            INFO("compare=" << static_cast<int>(compare));
            REQUIRE(answer.has_error());
            REQUIRE(answer.error().type == core::error_code_t::index_not_exists);
        }
    }

    std::filesystem::remove_all(path);
}

// manager_index_t routes on these statics; a family that stopped declaring them would take
// a range predicate all the way down to a store with no ordering to answer it.
TEST_CASE("services::index::each agent family states its backend and its ordering") {
    STATIC_REQUIRE(btree_index_agent_t::index_type_v == components::logical_plan::index_type::single);
    STATIC_REQUIRE(btree_index_agent_t::supports_ordered_probe_v);
    STATIC_REQUIRE(bitcask_index_agent_t::index_type_v == components::logical_plan::index_type::hashed);
    STATIC_REQUIRE_FALSE(bitcask_index_agent_t::supports_ordered_probe_v);
}

// A NULL key must never be staged: convert() maps NULL to the NA physical_value
// (numeric_limits<physical_value>::max()), so a stored NULL would sort after every real
// key and pollute every upper-bound/gte answer.
TEST_CASE("services::index::a NULL key is neither staged nor matched") {
    auto resource = core::pmr::otterbrix_resource();
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    const auto path = fresh_index_root("otterbrix_test_index_agent_buffer_null_key");

    auto agent_result =
        btree_index_agent_t::create(&resource, path, kTableOid, kIndexOid, /*flush_threshold=*/1000, log);
    REQUIRE_FALSE(agent_result.has_error());
    auto agent = std::move(agent_result.value());

    const auto session = session_id_t::generate_uid();
    const uint64_t txn = TRANSACTION_ID_START + 21;

    std::vector<std::pair<logical_value_t, size_t>> with_null;
    with_null.emplace_back(logical_value_t(&resource, nullptr), size_t{1});
    with_null.emplace_back(logical_value_t(&resource, int64_t{9}), size_t{2});
    REQUIRE_FALSE(
        ask<&index_agent_contract::stage_inserts>(agent, session, txn, std::move(with_null)).contains_error());

    auto null_probe = ask<&index_agent_contract::read_rows>(agent,
                                                            session,
                                                            compare_type::eq,
                                                            logical_value_t(&resource, nullptr),
                                                            txn);
    REQUIRE_FALSE(null_probe.has_error());
    CHECK(null_probe.value().empty());

    auto gte_probe = ask<&index_agent_contract::read_rows>(agent,
                                                           session,
                                                           compare_type::gte,
                                                           logical_value_t(&resource, int64_t{9}),
                                                           txn);
    REQUIRE_FALSE(gte_probe.has_error());
    CHECK(sorted(std::move(gte_probe.value())) == std::vector<int64_t>{2});

    std::filesystem::remove_all(path);
}

// If clear() swallowed the store's error and returned no_error() unconditionally, a
// CHECKPOINT whose index rebuild refused would report a clean rebuild. Buckets must also
// be dropped on refusal, or the FIFO-queued commit right behind it would publish rows
// into an index this call failed to empty.
TEST_CASE("services::index::bitcask_index_agent_t hands back the store's refusal to clear") {
    auto resource = core::pmr::otterbrix_resource();
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    const auto path = fresh_index_root("otterbrix_test_index_agent_refused_clear");

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

    REQUIRE_FALSE(ask<&index_agent_contract::stage_inserts>(agent, session, txn1, entries(&resource, {{42, 1}}))
                      .contains_error());
    REQUIRE_FALSE(
        ask<&index_agent_contract::commit_inserts>(agent, session, txn1, commit_id_of(txn1)).contains_error());
    REQUIRE_FALSE(ask<&index_agent_contract::stage_inserts>(agent, session, txn1, entries(&resource, {{42, 2}}))
                      .contains_error());

    {
        env_var_guard_t armed("OTTERBRIX_DISK_HASH_RESET_FAILPOINT", "1");
        const auto clear_error = ask<&index_agent_contract::clear>(agent, session);
        INFO("a rebuild the store could not finish has to reach the manager that awaits this");
        REQUIRE(clear_error.contains_error());

        const auto after = ask<&index_agent_contract::read_rows>(agent,
                                                                 session,
                                                                 compare_type::eq,
                                                                 logical_value_t(&resource, int64_t{42}),
                                                                 txn1);
        INFO("a read over a store whose rebuild refused is a refusal, not an empty answer");
        REQUIRE(after.has_error());

        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_inserts>(agent, session, uint64_t{0}, entries(&resource, {{42, 3}}))
                .contains_error());
        REQUIRE(ask<&index_agent_contract::commit_inserts>(agent, session, uint64_t{0}, uint64_t{0}).contains_error());
    }

    // Disarmed: the agent is repairable in place.
    REQUIRE_FALSE(ask<&index_agent_contract::clear>(agent, session).contains_error());
    REQUIRE_FALSE(ask<&index_agent_contract::stage_inserts>(agent, session, uint64_t{0}, entries(&resource, {{42, 4}}))
                      .contains_error());
    REQUIRE_FALSE(
        ask<&index_agent_contract::commit_inserts>(agent, session, uint64_t{0}, uint64_t{0}).contains_error());
    {
        auto answer = ask<&index_agent_contract::read_rows>(agent,
                                                            session,
                                                            compare_type::eq,
                                                            logical_value_t(&resource, int64_t{42}),
                                                            uint64_t{0});
        REQUIRE_FALSE(answer.has_error());
        CHECK(sorted(std::move(answer.value())) == std::vector<int64_t>{4});
    }

    std::filesystem::remove_all(path);
}

// A merge refusal (pay_merge_debt -> merge_immutable_segments -> collect_segments) must
// surface in the round that met it, not get parked in pending_write_error_ for the next
// force_flush to report and mis-attribute.
TEST_CASE("services::index::bitcask_index_agent_t reports a merge refusal in the round that met it") {
    auto resource = core::pmr::otterbrix_resource();
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    const auto path = fresh_index_root("index_agent_merge_attribution");
    const auto index_dir =
        path / std::to_string(static_cast<unsigned>(kTableOid)) / std::to_string(static_cast<unsigned>(kIndexOid));

    auto agent_result = bitcask_index_agent_t::create(&resource,
                                                      path,
                                                      kTableOid,
                                                      kIndexOid,
                                                      /*flush_threshold=*/1000,
                                                      /*segment_record_limit=*/2,
                                                      log,
                                                      std::pmr::set<std::uint64_t>(&resource));
    REQUIRE_FALSE(agent_result.has_error());
    auto agent = std::move(agent_result.value());

    const auto session = session_id_t::generate_uid();
    const uint64_t txn1 = TRANSACTION_ID_START + 61;
    const uint64_t txn2 = TRANSACTION_ID_START + 62;

    // Round 1: enough records to rotate and arm a merge debt, paid while the directory
    // is still listable.
    REQUIRE_FALSE(
        ask<&index_agent_contract::stage_inserts>(agent,
                                                  session,
                                                  txn1,
                                                  entries(&resource, {{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}}))
            .contains_error());
    REQUIRE_FALSE(
        ask<&index_agent_contract::commit_inserts>(agent, session, txn1, commit_id_of(txn1)).contains_error());

    // Directory becomes unlistable but stays writable/executable, so every round-2 write
    // still lands and only collect_segments can refuse.
    std::filesystem::permissions(index_dir, std::filesystem::perms::owner_write | std::filesystem::perms::owner_exec);
    struct restore_perms_t {
        std::filesystem::path dir;
        ~restore_perms_t() { std::filesystem::permissions(dir, std::filesystem::perms::owner_all); }
    } restore{index_dir};

    REQUIRE_FALSE(
        ask<&index_agent_contract::stage_inserts>(agent, session, txn2, entries(&resource, {{6, 6}, {7, 7}, {8, 8}}))
            .contains_error());
    auto commit2 = ask<&index_agent_contract::commit_inserts>(agent, session, txn2, commit_id_of(txn2));
    INFO("the merge met the unlistable directory inside THIS commit; the reply must say so");
    REQUIRE(commit2.contains_error());

    std::filesystem::permissions(index_dir, std::filesystem::perms::owner_all);
    auto flush = ask<&index_agent_contract::force_flush>(agent, session);
    INFO("a refusal already delivered must not be re-delivered by a later round");
    REQUIRE_FALSE(flush.contains_error());
}
