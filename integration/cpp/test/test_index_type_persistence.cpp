#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <string>
#include <vector>

// The index TYPE (btree vs hash) must survive a restart.
//
// pg_index carries indtype; bootstrap reads it back and hands it to
// index_agent_disk_t, which picks the on-disk backend from it: index_type::hashed
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
    auto config = test_create_config("/tmp/otterbrix/integration/test_index_type_persistence/hash_restart");
    test_clear_directory(config);
    config.disk.on = true;
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
