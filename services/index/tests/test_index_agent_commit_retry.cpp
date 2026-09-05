// THE BUCKET IS CLEARED ONLY AFTER THE JOURNAL SAYS YES.
//
// If commit_inserts/commit_deletes (txn != 0) ran `take(txn_id)` -- erasing the pending bucket
// INSIDE itself -- and only then asked the store to journal the batch (apply_txn_inserts /
// apply_txn_deletes), a journal IO refusal would LOSE the staged batch: the statement hears about
// the error, but a RETRY of the same commit finds an empty bucket and reports success over nothing.
// These cases pin "state is cleared only AFTER the operation succeeds" at the agent level: a
// refused commit keeps the bucket, and the retried commit actually publishes it.

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
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
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

    // THE COMMIT ID A FIXTURE'S TRANSACTION COMMITTED AT, kept far from the txn id it is derived
    // from because the two are different id spaces. The txn id says WHICH BUCKET to publish; the
    // commit id is what the hashed family stamps into its durable txn-log frame and what the
    // recover gate judges the frame by (bitcask_index_disk.cpp). One number serving as both is
    // exactly the confusion that let a COMMIT marker of an earlier incarnation vouch for a later
    // one's frame under a recycled txn id. The rebuild feed (txn_id 0) journals nothing and
    // carries commit id 0.
    constexpr std::uint64_t commit_id_of(std::uint64_t txn_id) { return txn_id + 500000; }

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

    // THE STORE'S OWN TXN FRAME HEADER, mirrored field for field so that a reordering of it
    // cannot let this test pass in silence -- the same tie test_bitcask_index_disk.cpp makes
    // with crashed_txn_frame_header_t, and for the same reason. The declaration this mirrors
    // is txn_frame_header_t in bitcask_index_disk.cpp.
    struct txn_frame_view_t {
        uint32_t magic;
        uint32_t crc;
        uint64_t txn_id;
        uint64_t commit_id;
        uint8_t op_kind;
        uint64_t payload_size;
    };
    static_assert(sizeof(txn_frame_view_t) == 40, "the view must be the store's txn frame header, byte for byte");
    static_assert(offsetof(txn_frame_view_t, magic) == 0,
                  "the view must be the store's txn frame header, byte for byte");
    static_assert(offsetof(txn_frame_view_t, crc) == 4, "the view must be the store's txn frame header, byte for byte");
    static_assert(offsetof(txn_frame_view_t, txn_id) == 8,
                  "the view must be the store's txn frame header, byte for byte");
    static_assert(offsetof(txn_frame_view_t, commit_id) == 16,
                  "the view must be the store's txn frame header, byte for byte");
    static_assert(offsetof(txn_frame_view_t, op_kind) == 24,
                  "the view must be the store's txn frame header, byte for byte");
    static_assert(offsetof(txn_frame_view_t, payload_size) == 32,
                  "the view must be the store's txn frame header, byte for byte");

    // WHO OWNS EACH DURABLE FRAME, read off the log itself. The txn id in the header is what a
    // frame's rows belong to, and the commit id beside it is what recover_txn_log's gate
    // consults, so "which transaction was this batch journalled under" is a question only the
    // bytes can answer -- and it is the question the case below is about. The walk needs
    // nothing but the declared payload size: the log is [40-byte header][payload], appended and
    // never rewritten.
    std::vector<std::pair<uint64_t, uint8_t>> txn_log_frames(const std::filesystem::path& root) {
        std::vector<std::pair<uint64_t, uint8_t>> frames;
        const auto log_path = txn_log_path(root);
        // A log that was never opened is not an empty log by assumption -- it is the state a
        // commit that journalled nothing leaves, which is exactly what is being checked.
        if (!std::filesystem::exists(log_path) || !std::filesystem::is_regular_file(log_path)) {
            return frames;
        }
        std::ifstream input(log_path, std::ios::binary);
        REQUIRE(input.good());
        for (;;) {
            txn_frame_view_t header{};
            if (!input.read(reinterpret_cast<char*>(&header), sizeof(header))) {
                break;
            }
            REQUIRE(header.magic == 0x314E5854u); // TXN1, the txn_magic of bitcask_index_disk.cpp
            frames.emplace_back(header.txn_id, header.op_kind);
            input.seekg(static_cast<std::streamoff>(header.payload_size), std::ios::cur);
        }
        return frames;
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
        auto refused = ask<&index_agent_contract::commit_inserts>(agent, session, txn1, commit_id_of(txn1));
        INFO("the journal refusal must reach the statement");
        REQUIRE(refused.contains_error());

        heal();
        auto retried = ask<&index_agent_contract::commit_inserts>(agent, session, txn1, commit_id_of(txn1));
        REQUIRE_FALSE(retried.contains_error());

        // What this catches: a retry reporting success over an ALREADY-ERASED bucket, so
        // nothing is ever journalled and the row is absent for everyone.
        CHECK(read(0) == std::vector<int64_t>{7});
        CHECK(read(txn2) == std::vector<int64_t>{7});
    }

    SECTION("a refused commit_deletes keeps the batch, and the retry removes the row") {
        // Seed the durable row over the txn==0 route (publish, no journal): the txn log's
        // lazy open must still be ahead of us, or the directory squat below cannot refuse it.
        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_inserts>(agent, session, uint64_t{0}, entries(&resource, {{42, 7}}))
                .contains_error());
        REQUIRE_FALSE(
            ask<&index_agent_contract::commit_inserts>(agent, session, uint64_t{0}, uint64_t{0}).contains_error());
        REQUIRE(read(0) == std::vector<int64_t>{7});

        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_deletes>(agent, session, txn2, entries(&resource, {{42, 7}}))
                .contains_error());

        sabotage();
        auto refused = ask<&index_agent_contract::commit_deletes>(agent, session, txn2, commit_id_of(txn2));
        INFO("the journal refusal must reach the statement");
        REQUIRE(refused.contains_error());

        heal();
        auto retried = ask<&index_agent_contract::commit_deletes>(agent, session, txn2, commit_id_of(txn2));
        REQUIRE_FALSE(retried.contains_error());

        // What this catches: a retried delete succeeding over an empty bucket, so the row
        // stays in the index while the statement is told the delete landed.
        CHECK(read(0).empty());
    }

    std::filesystem::remove_all(path);
}

// The txn==0 leg (rebuild / repopulate feed, no journal) must not erase the bucket after
// applying it to the store but BEFORE force_flush() answers. The batch itself is not lost (the
// keydir holds it), but a commit whose flush refused would have cleared its bucket already —
// so the RETRY of that commit publishes nothing, finds nothing parked, and reports success
// without ever re-asking the store for durability. Same rule as the txn!=0 leg above: state is
// cleared only AFTER the operation succeeds. Re-publishing a kept bucket is safe in this
// family: bitcask's insert/remove doors are idempotent on the (key, row) pair.
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
    REQUIRE_FALSE(
        ask<&index_agent_contract::commit_inserts>(agent, session, uint64_t{0}, uint64_t{0}).contains_error());
    REQUIRE(read(0) == std::vector<int64_t>{7});

    // A directory squatting on the NEXT segment's path (fresh store: active id 2, next 3)
    // makes the rotation's open refuse mid-publish.
    const auto next_segment = store_dir(path) / "bitcask.000003.data";
    REQUIRE(std::filesystem::create_directory(next_segment));

    SECTION("a refused txn==0 commit_deletes keeps its bucket, and the retry re-asks durability") {
        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_deletes>(agent, session, uint64_t{0}, entries(&resource, {{42, 7}}))
                .contains_error());

        auto refused = ask<&index_agent_contract::commit_deletes>(agent, session, uint64_t{0}, uint64_t{0});
        INFO("the rotation refusal must reach the statement");
        REQUIRE(refused.contains_error());

        // What this catches: the bucket erased ahead of the flush verdict, so this retry
        // publishes nothing, finds nothing parked, and answers no_error — success over a
        // delete that never reached the device.
        auto retried = ask<&index_agent_contract::commit_deletes>(agent, session, uint64_t{0}, uint64_t{0});
        REQUIRE(retried.contains_error());

        // The kept bucket keeps the SEMANTICS straight too: bucket 0 is committed for
        // everyone (readers subtract it), merely not yet durable — so the reader already
        // sees the delete while the statement keeps hearing "not durable" until a retry
        // lands. An erased bucket would make the reader UN-see a committed delete that is
        // then never going to be published.
        CHECK(read(0).empty());
    }

    SECTION("a refused txn==0 commit_inserts keeps its bucket, and the retry re-asks durability") {
        // Break the active segment first (same rotation squat, via a one-row delete).
        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_deletes>(agent, session, uint64_t{0}, entries(&resource, {{43, 8}}))
                .contains_error());
        REQUIRE(ask<&index_agent_contract::commit_deletes>(agent, session, uint64_t{0}, uint64_t{0}).contains_error());

        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_inserts>(agent, session, uint64_t{0}, entries(&resource, {{50, 20}}))
                .contains_error());

        auto refused = ask<&index_agent_contract::commit_inserts>(agent, session, uint64_t{0}, uint64_t{0});
        INFO("the append refusal must reach the statement");
        REQUIRE(refused.contains_error());

        // What this catches: the same vacuous success as the delete leg.
        auto retried = ask<&index_agent_contract::commit_inserts>(agent, session, uint64_t{0}, uint64_t{0});
        REQUIRE(retried.contains_error());
    }

    std::filesystem::remove_all(path);
}

// A COMMITTING TRANSACTION MAY ONLY PUBLISH WHAT IT STAGED, AND BUCKET 0 IS NOT ITS BUCKET.
//
// commit_inserts/commit_deletes used to take bucket `txn_id` AND bucket 0 -- the rebuild's stage
// -- and journal both under `txn_id`. The rebuild feeds bucket 0 from manager_index_t's
// repopulate_table, which posts clear -> stage_inserts(0) -> commit_inserts(0) with no co_await
// between them, so no foreign commit can be processed in the middle of that burst TODAY. That is a
// property of one loop in a file this agent does not own, nothing in the contract records it, and
// the first cross-actor await added to that loop opens the window -- so the bound belongs here,
// where the buckets are, and not in a schedule.
//
// WHAT THE RIDE-ALONG COSTS WHEN THE WINDOW IS OPEN, stated as the log states it: the rebuild's
// rows -- committed for everyone, owned by no transaction -- land in a DURABLE txn-log frame
// stamped with the foreign transaction's id, and recover_txn_log applies a frame only when its
// txn_id is in the WAL's committed set. Their replay is then gated on a commit marker belonging to
// a transaction that never staged them. The same narrowing clear() received on 2026-09-05, applied
// to the other half of the pair: the comment there ("ONLY THE REBUILD'S OWN BUCKET") reads as a
// property of bucket 0 and was true of one handler out of three.
//
// The assertion is on the LOG BYTES because that is where the misattribution is durable. The
// deeper consequence is not observable from here and is deliberately not claimed: apply_txn_inserts
// writes the frame AND the segments in the same call, so the frame alone decides only inside a
// crash between two fsyncs, which this store has no seam to stage.
TEST_CASE("services::index::bitcask_index_agent_t a foreign commit does not journal the rebuild's bucket") {
    auto resource = core::pmr::otterbrix_resource();
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    const auto path = fresh_index_root("otterbrix_test_index_agent_foreign_commit_bucket_zero");

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
    // The foreign writer, and a third id that never stages anything so nothing the reader below
    // sees can come out of a bucket of its own.
    const uint64_t writer = TRANSACTION_ID_START + 1;
    const uint64_t onlooker = TRANSACTION_ID_START + 2;

    auto read = [&](int64_t key, uint64_t txn_id) {
        auto answer = ask<&index_agent_contract::read_rows>(agent,
                                                            session,
                                                            compare_type::eq,
                                                            logical_value_t(&resource, key),
                                                            txn_id);
        REQUIRE_FALSE(answer.has_error());
        return sorted(std::move(answer.value()));
    };

    SECTION("commit_inserts of a foreign transaction leaves bucket 0 for the rebuild's own commit") {
        // The rebuild has staged and has not committed yet.
        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_inserts>(agent, session, uint64_t{0}, entries(&resource, {{99, 3}}))
                .contains_error());

        // A writer that staged NOTHING on this index commits. That message is ordinary:
        // manager_index_t::commit_inserts fans out to every index record of every touched
        // table without looking at what was staged for any of them.
        REQUIRE_FALSE(
            ask<&index_agent_contract::commit_inserts>(agent, session, writer, commit_id_of(writer)).contains_error());

        // What this catches: the foreign commit taking bucket 0 and writing the rebuild's rows
        // into a durable frame under ITS transaction id.
        const auto frames = txn_log_frames(path);
        // Reported as a number rather than a presence: the owner is what the recover gate
        // reads, so "one frame, owned by the writer" is the whole finding.
        const uint64_t first_owner = frames.empty() ? 0u : frames.front().first;
        INFO("frames in bitcask.txn.log after a foreign commit that staged nothing: "
             << frames.size() << "; first frame owned by txn " << first_owner << "; the foreign writer is txn "
             << writer);
        CHECK(frames.size() == 0u);
        CHECK(first_owner != writer);

        // And the rebuild's own commit still publishes it, which is the half a narrowing could
        // break by stranding the bucket.
        REQUIRE_FALSE(
            ask<&index_agent_contract::commit_inserts>(agent, session, uint64_t{0}, uint64_t{0}).contains_error());
        CHECK(read(99, onlooker) == std::vector<int64_t>{3});
    }

    SECTION("commit_deletes of a foreign transaction leaves bucket 0 for the rebuild's own commit") {
        // Row 3 is durable and committed for everyone.
        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_inserts>(agent, session, uint64_t{0}, entries(&resource, {{99, 3}}))
                .contains_error());
        REQUIRE_FALSE(
            ask<&index_agent_contract::commit_inserts>(agent, session, uint64_t{0}, uint64_t{0}).contains_error());
        REQUIRE(read(99, onlooker) == std::vector<int64_t>{3});

        // The removal is staged as committed-for-everyone and not yet published.
        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_deletes>(agent, session, uint64_t{0}, entries(&resource, {{99, 3}}))
                .contains_error());

        REQUIRE_FALSE(
            ask<&index_agent_contract::commit_deletes>(agent, session, writer, commit_id_of(writer)).contains_error());

        const auto frames = txn_log_frames(path);
        const uint64_t first_owner = frames.empty() ? 0u : frames.front().first;
        const unsigned first_kind = frames.empty() ? 0u : static_cast<unsigned>(frames.front().second);
        INFO("frames in bitcask.txn.log after a foreign commit_deletes that staged nothing: "
             << frames.size() << "; first frame owned by txn " << first_owner << " with op_kind " << first_kind
             << " (2 = delete); the foreign writer is txn " << writer);
        CHECK(frames.size() == 0u);
        CHECK(first_owner != writer);

        REQUIRE_FALSE(
            ask<&index_agent_contract::commit_deletes>(agent, session, uint64_t{0}, uint64_t{0}).contains_error());
        CHECK(read(99, onlooker).empty());
    }

    std::filesystem::remove_all(path);
}
