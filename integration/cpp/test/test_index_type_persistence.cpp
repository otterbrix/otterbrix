#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <string>
#include <vector>

// The index TYPE (btree vs hash) must survive a restart.
//
// pg_index carries indtype; bootstrap reads it back and hands it to
// manager_index_t::spawn_disk_agent, which picks the agent family from it: index_type::hashed
// -> bitcask LSM, everything else -> ordered B+tree. If the type were lost, a
// `USING hash` index would come back as a btree POINTED AT THE BITCASK DIRECTORY —
// the same files, a different reader.
//
// "It says hash" is not observable. What IS observable is which backend owns the
// directory, because the two backends leave disjoint artefacts in it:
//   bitcask -> CURRENT, bitcask.NNNNNN.data, hash_index.bin
//   b+tree  -> metadata
// So the witness is: post-restart writes land in the bitcask segments (their bytes
// grow) and NO b+tree `metadata` file ever appears beside them.

namespace {

    bool has_bitcask_artefacts(const std::filesystem::path& dir) {
        return std::filesystem::exists(dir / "CURRENT");
    }

    // Total bytes of the bitcask segment files. Grows when the bitcask backend
    // takes a write; stays put when some other backend owns the directory.
    std::uintmax_t bitcask_segment_bytes(const std::filesystem::path& dir) {
        std::uintmax_t total = 0;
        for (const auto& e : std::filesystem::directory_iterator(dir)) {
            if (!e.is_regular_file())
                continue;
            const auto name = e.path().filename().string();
            if (name.rfind("bitcask.", 0) == 0 && e.path().extension() == ".data") {
                total += e.file_size();
            }
        }
        return total;
    }

    // The one index directory below the disk root: the directory that holds a
    // bitcask CURRENT marker. Found by content, not by name — the on-disk layout
    // is oid-keyed and carries no index name.
    std::filesystem::path find_index_dir(const std::filesystem::path& disk_root) {
        for (const auto& e : std::filesystem::recursive_directory_iterator(disk_root)) {
            if (e.is_directory() && has_bitcask_artefacts(e.path())) {
                return e.path();
            }
        }
        return {};
    }

} // namespace

TEST_CASE("integration::cpp::test_index_type_persistence::hash_index_type_survives_restart") {
    auto config = test_create_config(integration_fixture_path("test_index_type_persistence/hash_restart"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;

    std::filesystem::path index_dir;
    std::uintmax_t bytes_before = 0;

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };
        REQUIRE(exec("CREATE DATABASE b;")->is_success());
        REQUIRE(exec("CREATE TABLE b.t (id bigint, k bigint);")->is_success());
        REQUIRE(exec("CREATE INDEX k_idx ON b.t USING hash (k);")->is_success());
        REQUIRE(exec("INSERT INTO b.t (id, k) VALUES (1, 10), (2, 20), (3, 30);")->is_success());
    }

    index_dir = find_index_dir(config.disk.path);
    INFO("a USING hash index must own a bitcask directory before the restart");
    REQUIRE_FALSE(index_dir.empty());
    bytes_before = bitcask_segment_bytes(index_dir);
    REQUIRE(bytes_before > 0);
    REQUIRE_FALSE(std::filesystem::exists(index_dir / "metadata"));

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };
        REQUIRE(exec("INSERT INTO b.t (id, k) VALUES (4, 40), (5, 50), (6, 60);")->is_success());
        auto cur = exec("SELECT id FROM b.t WHERE k = 40;");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 1);
    }

    INFO("post-restart writes must land in the bitcask segments the hash index owns");
    CHECK(bitcask_segment_bytes(index_dir) > bytes_before);

    INFO("no b+tree may take over the hash index directory across a restart");
    CHECK_FALSE(std::filesystem::exists(index_dir / "metadata"));
}

// The KEY TYPE must survive a restart, and DATE / TIME are where losing it is invisible
// until it is wrong.
//
// A DATE key is physically an INT32 day counter and a TIME key an INT64 microsecond
// counter; both are written by the binary key codec, which tags the logical type in the
// stored bytes, and both are probed by services::index::convert(), which must produce the
// same physical value from the column's own logical type. The catalog is what carries that
// logical type across a restart: pg_attribute's atttypid for the indexed column, read back
// at bootstrap and handed to the index agent with the rest of the key description.
//
// Lose it and nothing announces itself. An equality probe encoded under the wrong logical
// tag simply matches nothing, and a RANGE probe is worse than that: it returns a
// well-formed answer of the wrong rows, because the raw counters still order among
// themselves. So the witness here is not "the statement succeeded" -- it is that the
// post-restart index answers ranges and equalities EXACTLY, over rows written on both
// sides of the restart, with an unindexed twin holding the same data as the oracle.
namespace {

    std::string type_persistence_plan_text(const components::cursor::cursor_t_ptr& cur) {
        std::string out;
        for (std::size_t r = 0; r < cur->size(); ++r) {
            auto cell = cur->value(0, r);
            out += std::string(cell.value<std::string_view>());
            out += '\n';
        }
        return out;
    }

} // namespace

TEST_CASE("integration::cpp::test_index_type_persistence::temporal_key_type_survives_restart") {
    auto config = test_create_config(integration_fixture_path("test_index_type_persistence/temporal_restart"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;

    // Written BEFORE the restart. The dates and times are deliberately out of order so no
    // answer below can come from insertion order.
    const char* before[] = {
        "(1, DATE '2024-03-15', TIME '12:30:00')",
        "(2, DATE '2024-01-01', TIME '08:00:00')",
        "(3, DATE '2024-12-31', TIME '23:59:00')",
    };
    // Written AFTER it, interleaved with the values above rather than appended past them,
    // so a range answer has to mix rows from both sessions to be right.
    const char* after[] = {
        "(4, DATE '2024-02-01', TIME '09:15:00')",
        "(5, DATE '2024-06-30', TIME '18:45:00')",
    };

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };
        REQUIRE(exec("CREATE DATABASE t;")->is_success());
        REQUIRE(exec("CREATE TABLE t.ti (id BIGINT, d DATE, tm TIME);")->is_success());
        // The unindexed twin: same rows, no index. It is the oracle for every count below,
        // so a wrong answer cannot pass by both sides being wrong the same way.
        REQUIRE(exec("CREATE TABLE t.tp (id BIGINT, d DATE, tm TIME);")->is_success());
        REQUIRE(exec("CREATE INDEX i_d ON t.ti (d);")->is_success());
        REQUIRE(exec("CREATE INDEX i_tm ON t.ti (tm);")->is_success());

        for (const char* row : before) {
            for (const char* table : {"t.ti", "t.tp"}) {
                const std::string sql = std::string{"INSERT INTO "} + table + " (id, d, tm) VALUES " + row + ";";
                INFO(sql);
                REQUIRE(exec(sql)->is_success());
            }
        }
    }

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        // Post-restart writes go through the rehydrated index. If the key type came back
        // wrong these would be encoded under it and the two sessions' keys would not
        // compare -- which the mixed-range probes below are what catch.
        for (const char* row : after) {
            for (const char* table : {"t.ti", "t.tp"}) {
                const std::string sql = std::string{"INSERT INTO "} + table + " (id, d, tm) VALUES " + row + ";";
                INFO(sql);
                REQUIRE(exec(sql)->is_success());
            }
        }

        const auto probe = [&](const std::string& predicate, std::size_t expected) {
            {
                auto plan = exec("EXPLAIN SELECT id FROM t.ti WHERE " + predicate + ";");
                REQUIRE(plan->is_success());
                const auto text = type_persistence_plan_text(plan);
                INFO("predicate: " << predicate << "\nplan:\n" << text);
                INFO("a Seq Scan answers out of the heap and would pass with a broken index");
                REQUIRE(text.find("Index Scan") != std::string::npos);
            }
            auto indexed = exec("SELECT id FROM t.ti WHERE " + predicate + ";");
            REQUIRE(indexed->is_success());
            auto heap = exec("SELECT id FROM t.tp WHERE " + predicate + ";");
            REQUIRE(heap->is_success());
            INFO("predicate: " << predicate << " -- index " << indexed->size() << ", unindexed twin "
                               << heap->size() << ", expected " << expected);
            CHECK(heap->size() == expected);
            CHECK(indexed->size() == expected);
        };

        // EQUALITY on a row written BEFORE the restart: the key the pre-restart session
        // encoded and the probe this session encodes must be the same bytes.
        probe("d = DATE '2024-03-15'", 1);
        probe("tm = TIME '12:30:00'", 1);
        // ... and on one written AFTER it.
        probe("d = DATE '2024-06-30'", 1);
        probe("tm = TIME '18:45:00'", 1);
        // A key nothing carries, so the index cannot pass by matching everything.
        probe("d = DATE '2020-05-05'", 0);

        // RANGES spanning the restart. Each answer mixes rows from both sessions, which is
        // what a lost key type breaks without failing: 2024-02-01 (after) sorts between
        // 2024-01-01 and 2024-03-15 (both before).
        probe("d < DATE '2024-03-15'", 2);  // 2024-01-01, 2024-02-01
        probe("d >= DATE '2024-03-15'", 3); // 2024-03-15, 2024-06-30, 2024-12-31
        probe("tm > TIME '09:15:00'", 3);   // 12:30, 18:45, 23:59
        probe("tm <= TIME '09:15:00'", 2);  // 08:00, 09:15
    }
}
