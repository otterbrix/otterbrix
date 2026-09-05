// THE PER-TRANSACTION BUFFER, ASKED OF THE AGENT THAT NOW OWNS IT.
//
// These contracts came from components/index/test/test_index_mvcc.cpp and
// test_hash_single_field_index.cpp, where their subject was a "facade" object -- a
// registered index_t whose seven read/write doors all aborted because it held neither
// rows nor a search, and whose only real content was this buffer. The facade is gone and
// the buffer sits beside the store it belongs to, so the same contracts are asked HERE,
// through the mailbox, of both families:
//
//   * a transaction sees its OWN staged insert and no other transaction's;
//   * a staged delete hides a row from the transaction that staged it;
//   * an abort erases the bucket; a commit publishes it and clears it;
//   * a HASHED key is NORMALIZED before it is keyed, so a SMALLINT probe matches a
//     BIGINT-stored key;
//   * an ORDERED agent answers all six predicates over its buckets, a hashed one refuses
//     the five it has no ordering for -- with a value, not a signal.
//
// WHAT THE MOVE ADDS, and what could not be asked of the facade at all: the answer that
// comes back is BOTH halves. The facade had no store to read, so its cases could only
// ever see the buffer; here a committed row and a staged one arrive in one reply, which
// is what a statement actually gets.
//
// Each agent is driven by hand (cooperative_actor::resume(1)) so no scheduler decides an
// interleaving behind the test's back.

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

using components::expressions::compare_type;
using components::session::session_id_t;
using components::table::TRANSACTION_ID_START;
using components::types::logical_value_t;
using services::index::bitcask_index_agent_t;
using services::index::btree_index_agent_t;
using services::index::index_agent_contract;

namespace {

    constexpr components::catalog::oid_t kTableOid = 17300;
    constexpr components::catalog::oid_t kIndexOid = 17301;

    std::filesystem::path fresh_index_root(const char* name) {
        const std::filesystem::path path{std::filesystem::path{"/tmp"} / name};
        std::filesystem::remove_all(path);
        std::filesystem::create_directories(path / std::to_string(static_cast<unsigned>(kTableOid)) /
                                            std::to_string(static_cast<unsigned>(kIndexOid)));
        return path;
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

    // RAII around ONE environment variable, for the store's DEV_MODE seams. It restores the
    // previous value rather than unsetting blindly, so a case cannot leak an arming into the
    // rest of the run.
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
        auto answer = ask<&index_agent_contract::read_rows>(agent,
                                                            session,
                                                            compare,
                                                            logical_value_t(&resource, val42),
                                                            txn_id);
        REQUIRE_FALSE(answer.has_error());
        return sorted(std::move(answer.value()));
    };

    SECTION("a transaction sees its own staged insert, and nobody else does") {
        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_inserts>(agent, session, txn1, entries(&resource, {{42, 0}}))
                .contains_error());
        CHECK(read(txn1, compare_type::eq) == std::vector<int64_t>{0});
        // Another transaction's bucket is not even looked up: there is no stamp to compare
        // and no visibility predicate to get wrong.
        CHECK(read(txn2, compare_type::eq).empty());
    }

    SECTION("committing publishes the bucket into the tree, where everyone reads it") {
        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_inserts>(agent, session, txn1, entries(&resource, {{42, 0}}))
                .contains_error());
        REQUIRE_FALSE(ask<&index_agent_contract::commit_inserts>(agent, session, txn1).contains_error());
        // The row is no longer in a bucket AND it is still in the answer -- which is the
        // half the facade could never show, because it had no store to publish into.
        CHECK(read(txn2, compare_type::eq) == std::vector<int64_t>{0});
        CHECK(read(0, compare_type::eq) == std::vector<int64_t>{0});
    }

    SECTION("aborting erases the bucket and touches nothing durable") {
        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_inserts>(agent, session, txn1, entries(&resource, {{42, 0}}))
                .contains_error());
        REQUIRE(read(txn1, compare_type::eq).size() == 1);
        REQUIRE_FALSE(ask<&index_agent_contract::revert_inserts>(agent, session, txn1).contains_error());
        CHECK(read(txn1, compare_type::eq).empty());
        CHECK(read(txn2, compare_type::eq).empty());
    }

    SECTION("a staged delete hides a committed row from the transaction that staged it") {
        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_inserts>(agent, session, txn1, entries(&resource, {{42, 7}}))
                .contains_error());
        REQUIRE_FALSE(ask<&index_agent_contract::commit_inserts>(agent, session, txn1).contains_error());
        REQUIRE(read(txn2, compare_type::eq) == std::vector<int64_t>{7});

        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_deletes>(agent, session, txn2, entries(&resource, {{42, 7}}))
                .contains_error());
        INFO("the deleting transaction must stop seeing the row");
        CHECK(read(txn2, compare_type::eq).empty());
        INFO("and everyone else must still see it, because the delete is not committed");
        CHECK(read(TRANSACTION_ID_START + 3, compare_type::eq) == std::vector<int64_t>{7});

        REQUIRE_FALSE(ask<&index_agent_contract::commit_deletes>(agent, session, txn2).contains_error());
        CHECK(read(TRANSACTION_ID_START + 3, compare_type::eq).empty());
    }

    SECTION("a row inserted AND deleted by one transaction ends up absent") {
        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_inserts>(agent, session, txn1, entries(&resource, {{42, 5}}))
                .contains_error());
        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_deletes>(agent, session, txn1, entries(&resource, {{42, 5}}))
                .contains_error());
        CHECK(read(txn1, compare_type::eq).empty());
    }

    SECTION("clear() wipes the buckets as well as the tree") {
        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_inserts>(agent, session, txn1, entries(&resource, {{42, 1}}))
                .contains_error());
        REQUIRE_FALSE(ask<&index_agent_contract::commit_inserts>(agent, session, txn1).contains_error());
        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_inserts>(agent, session, txn2, entries(&resource, {{42, 2}}))
                .contains_error());
        REQUIRE(read(txn2, compare_type::eq).size() == 2);

        REQUIRE_FALSE(ask<&index_agent_contract::clear>(agent, session).contains_error());
        INFO("a clear that wiped only the durable half would leave a rebuilt index reporting phantom rows");
        CHECK(read(txn2, compare_type::eq).empty());
    }

    std::filesystem::remove_all(path);
}

// ALL SIX PREDICATES over the buckets, which is what separates the ordered family from
// the hashed one: a staged row keyed 3 belongs in the answer to `x < 5` and not in the
// answer to `x = 5`. A merge that only knew equality would silently drop it — a
// transaction failing to see its own write.
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
    REQUIRE_FALSE(ask<&index_agent_contract::stage_inserts>(agent,
                                                            session,
                                                            txn,
                                                            entries(&resource, {{3, 30}, {5, 50}, {7, 70}}))
                      .contains_error());

    auto probe = [&](compare_type compare, int64_t key) {
        auto answer = ask<&index_agent_contract::read_rows>(agent,
                                                            session,
                                                            compare,
                                                            logical_value_t(&resource, key),
                                                            txn);
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
        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_inserts>(agent, session, txn1, entries(&resource, {{42, 7}}))
                .contains_error());
        CHECK(read(txn1, val42) == std::vector<int64_t>{7});
        CHECK(read(txn2, val42).empty());

        REQUIRE_FALSE(ask<&index_agent_contract::commit_inserts>(agent, session, txn1).contains_error());
        CHECK(read(txn2, val42) == std::vector<int64_t>{7});

        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_deletes>(agent, session, txn2, entries(&resource, {{42, 7}}))
                .contains_error());
        CHECK(read(txn2, val42).empty());
        CHECK(read(TRANSACTION_ID_START + 3, val42) == std::vector<int64_t>{7});

        REQUIRE_FALSE(ask<&index_agent_contract::commit_deletes>(agent, session, txn2).contains_error());
        CHECK(read(TRANSACTION_ID_START + 3, val42).empty());
    }

    SECTION("aborting erases the bucket") {
        const logical_value_t val77(&resource, int64_t{77});
        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_inserts>(agent, session, txn1, entries(&resource, {{77, 9}}))
                .contains_error());
        REQUIRE(read(txn1, val77).size() == 1);
        REQUIRE_FALSE(ask<&index_agent_contract::revert_inserts>(agent, session, txn1).contains_error());
        CHECK(read(txn1, val77).empty());
    }

    // A HASHED key is NORMALIZED -- the integer family widened to BIGINT / UBIGINT --
    // before it is keyed, in the agent's encoder AND in the store's key_bytes_for_hash.
    // That is what makes the two halves of one answer key alike, so the case checks the
    // widening across BOTH: the row is committed into the store under a BIGINT key and
    // probed with a SMALLINT one.
    SECTION("a SMALLINT probe matches a BIGINT-stored key, in the bucket and in the store") {
        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_inserts>(agent, session, txn1, entries(&resource, {{4242, 1}}))
                .contains_error());
        const logical_value_t probe_small(&resource, int16_t{4242});
        INFO("the staged half must be found by a narrower probe");
        CHECK(read(txn1, probe_small) == std::vector<int64_t>{1});

        REQUIRE_FALSE(ask<&index_agent_contract::commit_inserts>(agent, session, txn1).contains_error());
        INFO("and so must the committed half, or the two halves would key differently");
        CHECK(read(txn2, probe_small) == std::vector<int64_t>{1});

        // The control: a key nothing carries stays unmatched, so the case cannot pass by
        // matching everything.
        CHECK(read(txn2, logical_value_t(&resource, int16_t{4243})).empty());
    }

    // A hash bucket has no ordering, so nothing but equality can be asked. It comes back
    // as a VALUE: an empty range would be indistinguishable from "no row carries this
    // key", which is a wrong answer dressed as a fast one. This is the last line of
    // defence — manager_index_t refuses the same predicate a round trip earlier, off the
    // record's `ordered` flag.
    SECTION("a range predicate is refused, loudly") {
        for (auto compare : {compare_type::ne,
                             compare_type::lt,
                             compare_type::lte,
                             compare_type::gt,
                             compare_type::gte}) {
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

// THE STATIC ANSWERS the manager routes on. They replaced two virtual accessors on the
// dead index_t, and the record manager_index_t keeps per index is a copy of them, so a
// family that stopped declaring them would take a range predicate all the way down to a
// store with no ordering to answer it.
TEST_CASE("services::index::each agent family states its backend and its ordering") {
    STATIC_REQUIRE(btree_index_agent_t::index_type_v == components::logical_plan::index_type::single);
    STATIC_REQUIRE(btree_index_agent_t::supports_ordered_probe_v);
    STATIC_REQUIRE(bitcask_index_agent_t::index_type_v == components::logical_plan::index_type::hashed);
    STATIC_REQUIRE_FALSE(bitcask_index_agent_t::supports_ordered_probe_v);
}

// A NULL key is never staged and never probed for -- ONE rule (index_key_is_null), called
// by both families. The consequence of losing it is specific and not a missing row: on the
// ordered side convert() maps a NULL to the NA physical_value, which is what
// numeric_limits<physical_value>::max() returns, so a stored NULL sorts after every real
// key and joins EVERY upper-bound and gte answer the tree gives.
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

    // `col <op> NULL` is UNKNOWN for every row, so it selects nothing.
    auto null_probe = ask<&index_agent_contract::read_rows>(agent,
                                                            session,
                                                            compare_type::eq,
                                                            logical_value_t(&resource, nullptr),
                                                            txn);
    REQUIRE_FALSE(null_probe.has_error());
    CHECK(null_probe.value().empty());

    // The non-null neighbour was staged, so the batch was not dropped whole; and the NULL
    // does not join the open ray above every real key.
    auto gte_probe = ask<&index_agent_contract::read_rows>(agent,
                                                           session,
                                                           compare_type::gte,
                                                           logical_value_t(&resource, int64_t{9}),
                                                           txn);
    REQUIRE_FALSE(gte_probe.has_error());
    CHECK(sorted(std::move(gte_probe.value())) == std::vector<int64_t>{2});

    std::filesystem::remove_all(path);
}

// ---------------------------------------------------------------------------------------
// THE STORE'S REFUSAL IS THE HANDLER'S ANSWER, and the buckets go with it either way.
//
// index_agent_contract::clear returns a core::error_t and manager_index_t::repopulate_table
// awaits it and folds it into its first_error -- but the hashed agent used to discard what
// the store said and co_return no_error unconditionally, so the fold was over a constant. A
// CHECKPOINT whose index rebuild refused therefore reported a clean rebuild, and the table and
// its index disagreed with nothing anywhere saying so.
//
// The second half is the ordering risk the same FIFO carries: stage_inserts and
// commit_inserts are queued right behind this clear, on the same agent, in the same
// repopulate. If the buckets survived a refused clear, that commit would publish rows into
// the index this call failed to empty -- so they are dropped even on the refusal, and the
// commit that follows meets the store's own refusal rather than turning it into a success.
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
    REQUIRE_FALSE(ask<&index_agent_contract::commit_inserts>(agent, session, txn1).contains_error());
    // A bucket left standing across the clear, which is what the second half is about.
    REQUIRE_FALSE(ask<&index_agent_contract::stage_inserts>(agent, session, txn1, entries(&resource, {{42, 2}}))
                      .contains_error());

    {
        env_var_guard_t armed("OTTERBRIX_DISK_HASH_RESET_FAILPOINT", "1");
        const auto clear_error = ask<&index_agent_contract::clear>(agent, session);
        INFO("a rebuild the store could not finish has to reach the manager that awaits this");
        REQUIRE(clear_error.contains_error());

        // The bucket went with the store. The staged row cannot come back from it.
        const auto after = ask<&index_agent_contract::read_rows>(agent,
                                                                 session,
                                                                 compare_type::eq,
                                                                 logical_value_t(&resource, int64_t{42}),
                                                                 txn1);
        INFO("a read over a store whose rebuild refused is a refusal, not an empty answer");
        REQUIRE(after.has_error());

        // And the repopulate's own next two messages do not turn the refusal into a success.
        REQUIRE_FALSE(ask<&index_agent_contract::stage_inserts>(agent, session, uint64_t{0}, entries(&resource, {{42, 3}}))
                          .contains_error());
        REQUIRE(ask<&index_agent_contract::commit_inserts>(agent, session, uint64_t{0}).contains_error());
    }

    // Disarmed: the agent is repairable in place, which is what keeps "loud" from meaning
    // "dead" for the index this agent stands for.
    REQUIRE_FALSE(ask<&index_agent_contract::clear>(agent, session).contains_error());
    REQUIRE_FALSE(ask<&index_agent_contract::stage_inserts>(agent, session, uint64_t{0}, entries(&resource, {{42, 4}}))
                      .contains_error());
    REQUIRE_FALSE(ask<&index_agent_contract::commit_inserts>(agent, session, uint64_t{0}).contains_error());
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
