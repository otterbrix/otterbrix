#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <fstream>
#include <string>

// A CONFIGURED INDEX THRESHOLD MUST HOLD FOR EVERY INDEX, NOT ONLY FOR THE ONES THAT
// EXISTED AT STARTUP.
//
// configuration::config_disk carries three index-storage knobs -- bitcask_flush_threshold,
// bitcask_segment_record_limit, btree_flush_threshold. They reached manager_index_t's
// constructor, which stored them in three fields... that nothing ever read. Bootstrap
// spawned its agents from config.disk.* directly (in base_spaces), while
// manager_index_t::create_index built its agent from bitcask_index_disk_t::default_* /
// btree_index_disk_t::default_* -- the backends' own static defaults. The result is one
// database holding two differently laid-out copies of the same kind of index, decided by
// nothing more than WHEN the index was created:
//
//     CREATE INDEX ... USING hash    -> segment_record_limit 10000 (the static default)
//     restart, same index            -> segment_record_limit whatever was configured
//
// This is observable because bitcask ROTATES its active data segment every
// segment_record_limit records, and the segment it is writing to is named in the CURRENT
// file in the index directory (a decimal id; regular segments start at 2 and the merger
// only ever reuses the reserved ids 0-1 below them). With the limit configured down to a
// handful of records, a dozen committed inserts must move CURRENT well past its starting
// id. With the 10000-record static default in force instead, it cannot move at all.
//
// THE READS HAPPEN WHILE THE ENGINE IS STILL UP, and that is not incidental: shutdown runs
// a CHECKPOINT, which repopulates every index -- clear() and then one txn_id==0 BULK load,
// and a bulk load deliberately suppresses rotation (bitcask_index_disk_t::set_bulk_mode).
// A post-shutdown directory therefore always holds exactly one segment whatever the limit
// is, and says nothing about how the index was written.
//
// Both roads are exercised in one case, because the claim is that they AGREE: the index a
// statement creates and the same index after a restart are the same index, and must be
// laid out the same way.

namespace {

    // The one index directory below the disk root: the directory that holds a bitcask
    // CURRENT marker. Found by content, not by name -- the on-disk layout is oid-keyed and
    // carries no index name.
    std::filesystem::path find_bitcask_index_dir(const std::filesystem::path& disk_root) {
        for (const auto& e : std::filesystem::recursive_directory_iterator(disk_root)) {
            if (e.is_directory() && std::filesystem::exists(e.path() / "CURRENT")) {
                return e.path();
            }
        }
        return {};
    }

    // The active segment id bitcask recorded. It starts at 2 on a fresh directory and only
    // ever grows, once per rotation.
    uint64_t current_segment_id(const std::filesystem::path& index_dir) {
        std::ifstream input(index_dir / "CURRENT");
        uint64_t id = 0;
        input >> id;
        return input.fail() ? 0 : id;
    }

    constexpr uint64_t kFirstRegularSegmentId = 2;
    constexpr uint64_t kSegmentRecordLimit = 2;
    constexpr unsigned kRows = 12;
    // Every committed insert appends one snapshot record, so kRows records at
    // kSegmentRecordLimit per segment must rotate this many times. One is subtracted to
    // stay clear of where the boundary falls relative to the run that preceded it.
    constexpr uint64_t kMinRotations = kRows / kSegmentRecordLimit - 1;

} // namespace

TEST_CASE("integration::cpp::test_index_threshold_config::every_road_honours_the_configured_segment_limit") {
    auto config = test_create_config(integration_fixture_path("test_index_threshold_config/segments"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;
    // Small enough that a dozen inserts must rotate several times.
    config.disk.bitcask_segment_record_limit = kSegmentRecordLimit;

    std::filesystem::path index_dir;
    uint64_t runtime_rotations = 0;

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };
        REQUIRE(exec("CREATE DATABASE b;")->is_success());
        REQUIRE(exec("CREATE TABLE b.t (id bigint, k bigint);")->is_success());
        // Created AT RUNTIME -- this is the road that ignored the configuration.
        REQUIRE(exec("CREATE INDEX k_idx ON b.t USING hash (k);")->is_success());
        for (unsigned i = 0; i < kRows; ++i) {
            REQUIRE(exec("INSERT INTO b.t (id, k) VALUES (" + std::to_string(i) + ", " + std::to_string(i) + ");")
                        ->is_success());
        }

        index_dir = find_bitcask_index_dir(config.disk.path);
        INFO("a USING hash index must own a bitcask directory");
        REQUIRE_FALSE(index_dir.empty());
        REQUIRE(current_segment_id(index_dir) >= kFirstRegularSegmentId);
        runtime_rotations = current_segment_id(index_dir) - kFirstRegularSegmentId;

        INFO("with bitcask_segment_record_limit=2 configured, a dozen committed inserts must "
             "have rotated the active segment; a CURRENT still naming the first segment means "
             "the runtime CREATE INDEX built its store from the backend's static default instead");
        CHECK(runtime_rotations >= kMinRotations);
    }

    {
        // The same index, now brought up by the BOOTSTRAP road -- the one that always read
        // the configuration. The shutdown checkpoint above bulk-reloaded the store, so this
        // run starts from whatever segment that left behind and counts from there.
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };
        const auto before = current_segment_id(index_dir);
        for (unsigned i = 0; i < kRows; ++i) {
            REQUIRE(exec("INSERT INTO b.t (id, k) VALUES (" + std::to_string(kRows + i) + ", " +
                         std::to_string(kRows + i) + ");")
                        ->is_success());
        }
        const auto bootstrap_rotations = current_segment_id(index_dir) - before;

        INFO("the same index, written through the same configuration, must rotate at the same "
             "rate whichever road raised its agent -- to within the one segment the two runs "
             "can differ by, because this run starts part-way through a segment the shutdown "
             "checkpoint's bulk reload left behind");
        CHECK(bootstrap_rotations >= kMinRotations);
        // Written as two additions rather than one subtraction: these are unsigned.
        CHECK(runtime_rotations + 1 >= bootstrap_rotations);
        CHECK(bootstrap_rotations + 1 >= runtime_rotations);
    }
}
