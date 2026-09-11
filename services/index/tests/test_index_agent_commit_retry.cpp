// State must clear only after the journal write succeeds, or a retry reports success over an empty bucket.

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

    // Every handler under test is a straight-line coroutine with no cross-actor await, so one
    // resume finishes it.
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

    std::vector<std::pair<uint64_t, uint8_t>> txn_log_frames(const std::filesystem::path& root) {
        std::vector<std::pair<uint64_t, uint8_t>> frames;
        const auto log_path = txn_log_path(root);
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

        CHECK(read(0) == std::vector<int64_t>{7});
        CHECK(read(txn2) == std::vector<int64_t>{7});
    }

    SECTION("a refused commit_deletes keeps the batch, and the retry removes the row") {
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

        CHECK(read(0).empty());
    }

    std::filesystem::remove_all(path);
}

TEST_CASE("services::index::bitcask_index_agent_t txn==0 publish keeps the bucket until the flush verdict") {
    auto resource = core::pmr::otterbrix_resource();
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    const auto path = fresh_index_root("otterbrix_test_index_agent_publish_retry");

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

    REQUIRE_FALSE(ask<&index_agent_contract::stage_inserts>(
                      agent,
                      session,
                      uint64_t{0},
                      entries(&resource, {{42, 7}, {43, 8}, {44, 9}, {45, 10}, {46, 11}}))
                      .contains_error());
    REQUIRE_FALSE(
        ask<&index_agent_contract::commit_inserts>(agent, session, uint64_t{0}, uint64_t{0}).contains_error());
    REQUIRE(read(0) == std::vector<int64_t>{7});

    const auto next_segment = store_dir(path) / "bitcask.000003.data";
    REQUIRE(std::filesystem::create_directory(next_segment));

    SECTION("a refused txn==0 commit_deletes keeps its bucket, and the retry re-asks durability") {
        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_deletes>(agent, session, uint64_t{0}, entries(&resource, {{42, 7}}))
                .contains_error());

        auto refused = ask<&index_agent_contract::commit_deletes>(agent, session, uint64_t{0}, uint64_t{0});
        INFO("the rotation refusal must reach the statement");
        REQUIRE(refused.contains_error());

        auto retried = ask<&index_agent_contract::commit_deletes>(agent, session, uint64_t{0}, uint64_t{0});
        REQUIRE(retried.contains_error());

        // Bucket 0 is visible to readers immediately, independent of durability, until a retry confirms it.
        CHECK(read(0).empty());
    }

    SECTION("a refused txn==0 commit_inserts keeps its bucket, and the retry re-asks durability") {
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

        auto retried = ask<&index_agent_contract::commit_inserts>(agent, session, uint64_t{0}, uint64_t{0});
        REQUIRE(retried.contains_error());
    }

    std::filesystem::remove_all(path);
}

// A committing transaction may only journal what it staged; taking bucket 0 (the rebuild's stage) too would
// let recover_txn_log gate replay on a commit marker for a transaction that never staged those rows.
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
        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_inserts>(agent, session, uint64_t{0}, entries(&resource, {{99, 3}}))
                .contains_error());

        REQUIRE_FALSE(
            ask<&index_agent_contract::commit_inserts>(agent, session, writer, commit_id_of(writer)).contains_error());

        const auto frames = txn_log_frames(path);
        const uint64_t first_owner = frames.empty() ? 0u : frames.front().first;
        INFO("frames in bitcask.txn.log after a foreign commit that staged nothing: "
             << frames.size() << "; first frame owned by txn " << first_owner << "; the foreign writer is txn "
             << writer);
        CHECK(frames.size() == 0u);
        CHECK(first_owner != writer);

        REQUIRE_FALSE(
            ask<&index_agent_contract::commit_inserts>(agent, session, uint64_t{0}, uint64_t{0}).contains_error());
        CHECK(read(99, onlooker) == std::vector<int64_t>{3});
    }

    SECTION("commit_deletes of a foreign transaction leaves bucket 0 for the rebuild's own commit") {
        REQUIRE_FALSE(
            ask<&index_agent_contract::stage_inserts>(agent, session, uint64_t{0}, entries(&resource, {{99, 3}}))
                .contains_error());
        REQUIRE_FALSE(
            ask<&index_agent_contract::commit_inserts>(agent, session, uint64_t{0}, uint64_t{0}).contains_error());
        REQUIRE(read(99, onlooker) == std::vector<int64_t>{3});

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
