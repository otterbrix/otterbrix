#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

// ============================================================================
// THE MAILBOX IS THE ONLY THING THAT ORDERS A HASHED INDEX.
//
// bitcask_index_disk_t used to hold a shared_mutex, and it held one for a concrete
// reason: it started a std::thread of its own and pushed every segment merge onto it
// (rotate_active_segment -> enqueue_task). So an index had TWO serialization domains --
// its agent's mailbox and that mutex -- and the second one existed only to make the
// first one's guarantee false. The merge is the agent's own work now, run at the end of
// the write handler that caused the rotation, and the mutex is gone with the thread.
//
// A GREP PROVES NOTHING HERE. "No mutex left" is satisfied just as well by a forgotten
// door that reaches the store without going through the mailbox at all -- which is
// exactly the shape of the thing being removed. What has to be exercised is the real
// path: many CLIENT THREADS, through the dispatcher, reading and writing ONE index at
// once, hard enough that the merger actually runs.
//
// THE VOLUME IS PART OF THE ASSERTION. bitcask_segment_record_limit is configured down
// to a handful of records so the committed inserts below rotate the active segment many
// times over, and the fixture ASSERTS afterwards -- off CURRENT and the on-disk segment
// ids -- that they did and that a merged segment was published. Without that a green run
// would mean only that the merger never ran.
//
// The reader threads ask about ANCHOR keys, which are inserted once, before the storm,
// and which no writer ever touches. Their answer is therefore invariant, and any reader
// that sees something else has seen a torn read of a keydir or a segment set that a
// merge was rewriting underneath it. Catch2's assertion macros are not thread-safe, so
// the readers count violations into atomics and the main thread asserts after the join.
// ============================================================================

namespace {

    using components::cursor::cursor_t_ptr;

    std::vector<int64_t> ids_of(const cursor_t_ptr& cur) {
        std::vector<int64_t> out;
        out.reserve(cur->size());
        for (std::size_t r = 0; r < cur->size(); ++r) {
            out.push_back(cur->value(0, r).value<int64_t>());
        }
        std::sort(out.begin(), out.end());
        return out;
    }

    std::string plan_text(const cursor_t_ptr& cur) {
        std::string out;
        for (std::size_t r = 0; r < cur->size(); ++r) {
            auto v = cur->value(0, r);
            out += std::string(v.value<std::string_view>());
            out += '\n';
        }
        return out;
    }

    // The one index directory below the disk root: found by content (a bitcask CURRENT
    // marker), because the on-disk layout is oid-keyed and carries no index name.
    std::filesystem::path find_bitcask_index_dir(const std::filesystem::path& disk_root) {
        for (const auto& e : std::filesystem::recursive_directory_iterator(disk_root)) {
            if (e.is_directory() && std::filesystem::exists(e.path() / "CURRENT")) {
                return e.path();
            }
        }
        return {};
    }

    uint64_t current_segment_id(const std::filesystem::path& index_dir) {
        std::ifstream input(index_dir / "CURRENT");
        uint64_t id = 0;
        input >> id;
        return input.fail() ? 0 : id;
    }

    // Regular segments start at 2; the merger writes its output to one of the reserved
    // ids below that. A file with such an id is the proof that a merge was published.
    bool merged_segment_exists(const std::filesystem::path& index_dir) {
        for (const auto& e : std::filesystem::directory_iterator(index_dir)) {
            const auto name = e.path().filename().string();
            if (name == "bitcask.000000.data" || name == "bitcask.000001.data") {
                return true;
            }
        }
        return false;
    }

    constexpr uint64_t kFirstRegularSegmentId = 2;
    constexpr uint64_t kSegmentRecordLimit = 4;
    constexpr uint64_t kMinRotations = 24;

    constexpr int kAnchorKey = 900000;
    constexpr int kAnchorRows = 3;
    constexpr size_t kWriterThreads = 4;
    constexpr size_t kReaderThreads = 4;
    constexpr int kStatementsPerWriter = 30;
    constexpr int kProbesPerReader = 150;

} // namespace

TEST_CASE("integration::cpp::index_concurrent_merge::readers_and_writers_share_one_hashed_index") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index_concurrent_merge/one_index");
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;
    // Small enough that the committed inserts below rotate — and therefore merge — over
    // and over instead of once.
    config.disk.bitcask_segment_record_limit = kSegmentRecordLimit;

    std::filesystem::path index_dir;
    uint64_t rotations = 0;
    bool merged = false;

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) { return d->execute_sql(otterbrix::session_id_t(), sql); };

        REQUIRE(exec("CREATE DATABASE cm;")->is_success());
        REQUIRE(exec("CREATE TABLE cm.t (id bigint, k bigint);")->is_success());
        REQUIRE(exec("CREATE INDEX t_k ON cm.t USING hash (k);")->is_success());

        // The anchors: three rows under one key, never written again. Three rather than
        // one because a reader that keeps only the last row id per key answers a
        // singleton correctly and would slip through.
        for (int i = 0; i < kAnchorRows; ++i) {
            REQUIRE(exec("INSERT INTO cm.t (id, k) VALUES (" + std::to_string(i) + ", " +
                         std::to_string(kAnchorKey) + ");")
                        ->is_success());
        }

        const std::vector<int64_t> expected_anchor_ids{0, 1, 2};
        {
            auto plan = exec("EXPLAIN SELECT id FROM cm.t WHERE k = " + std::to_string(kAnchorKey) + ";");
            REQUIRE(plan->is_success());
            auto text = plan_text(plan);
            INFO("plan:\n" << text);
            INFO("without an Index Scan the readers below would never reach the index at all");
            REQUIRE(text.find("Index Scan") != std::string::npos);
        }
        {
            auto cur = exec("SELECT id FROM cm.t WHERE k = " + std::to_string(kAnchorKey) + ";");
            REQUIRE(cur->is_success());
            REQUIRE(ids_of(cur) == expected_anchor_ids);
        }

        std::atomic<size_t> read_failures{0};
        std::atomic<size_t> wrong_answers{0};
        std::atomic<size_t> write_failures{0};
        std::atomic<size_t> reads_done{0};

        // Each writer owns a disjoint id/key range, so the writers never contend for a
        // row and every failure the fixture reports is about the index, not about two
        // statements fighting over one tuple.
        auto writer = [&](size_t worker_id) {
            const int base = 1000 + static_cast<int>(worker_id) * 1000;
            for (int i = 0; i < kStatementsPerWriter; ++i) {
                const auto id = std::to_string(base + i);
                const auto key = std::to_string(base + i);
                if (!exec("INSERT INTO cm.t (id, k) VALUES (" + id + ", " + key + ");")->is_success()) {
                    write_failures.fetch_add(1, std::memory_order_relaxed);
                }
                if ((i % 3) == 2) {
                    if (!exec("DELETE FROM cm.t WHERE id = " + id + ";")->is_success()) {
                        write_failures.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
        };

        auto reader = [&](size_t) {
            for (int i = 0; i < kProbesPerReader; ++i) {
                auto cur = exec("SELECT id FROM cm.t WHERE k = " + std::to_string(kAnchorKey) + ";");
                if (!cur->is_success()) {
                    read_failures.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }
                if (ids_of(cur) != expected_anchor_ids) {
                    wrong_answers.fetch_add(1, std::memory_order_relaxed);
                }
                reads_done.fetch_add(1, std::memory_order_relaxed);
            }
        };

        std::vector<std::thread> threads;
        threads.reserve(kWriterThreads + kReaderThreads);
        for (size_t t = 0; t < kWriterThreads; ++t) {
            threads.emplace_back(writer, t);
        }
        for (size_t t = 0; t < kReaderThreads; ++t) {
            threads.emplace_back(reader, t);
        }
        for (auto& thread : threads) {
            thread.join();
        }

        CHECK(write_failures.load(std::memory_order_relaxed) == 0);
        CHECK(read_failures.load(std::memory_order_relaxed) == 0);
        INFO("an index scan that answered anything but the three anchor rows read a keydir "
             "or a segment set while a merge was rewriting it");
        CHECK(wrong_answers.load(std::memory_order_relaxed) == 0);
        CHECK(reads_done.load(std::memory_order_relaxed) ==
              static_cast<size_t>(kProbesPerReader) * kReaderThreads);

        // READ THE LAYOUT WHILE THE ENGINE IS STILL UP: shutdown runs a CHECKPOINT, which
        // repopulates every index with one bulk load, and a bulk load suppresses rotation
        // on purpose — a post-shutdown directory says nothing about how the index was
        // written.
        index_dir = find_bitcask_index_dir(config.disk.path);
        REQUIRE_FALSE(index_dir.empty());
        REQUIRE(current_segment_id(index_dir) >= kFirstRegularSegmentId);
        rotations = current_segment_id(index_dir) - kFirstRegularSegmentId;
        merged = merged_segment_exists(index_dir);

        // The whole point of the volume: if nothing rotated, nothing merged, and a green
        // run above would have proved only that the merger never got a chance to race.
        INFO("rotations observed: " << rotations);
        CHECK(rotations >= kMinRotations);
        INFO("a segment with a reserved id (0 or 1) is the merger's published output");
        CHECK(merged);

        // Still answering correctly after the storm, on this thread, with nothing else
        // running: separates "the readers happened to be lucky" from "the index is sound".
        {
            auto cur = exec("SELECT id FROM cm.t WHERE k = " + std::to_string(kAnchorKey) + ";");
            REQUIRE(cur->is_success());
            CHECK(ids_of(cur) == expected_anchor_ids);
        }
        for (size_t worker_id = 0; worker_id < kWriterThreads; ++worker_id) {
            const int base = 1000 + static_cast<int>(worker_id) * 1000;
            for (int i = 0; i < kStatementsPerWriter; ++i) {
                const auto key = std::to_string(base + i);
                auto cur = exec("SELECT id FROM cm.t WHERE k = " + key + ";");
                REQUIRE(cur->is_success());
                const std::vector<int64_t> expected =
                    (i % 3) == 2 ? std::vector<int64_t>{} : std::vector<int64_t>{base + i};
                INFO("key " << key << " after the concurrent run");
                CHECK(ids_of(cur) == expected);
            }
        }
    }

    // And after a restart, off the files the merger left behind.
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) { return d->execute_sql(otterbrix::session_id_t(), sql); };
        auto cur = exec("SELECT id FROM cm.t WHERE k = " + std::to_string(kAnchorKey) + ";");
        REQUIRE(cur->is_success());
        CHECK(ids_of(cur) == std::vector<int64_t>{0, 1, 2});
    }
}
