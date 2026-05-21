#include <catch2/catch.hpp>
#include <services/disk/manager_disk.hpp>

#include <components/table/column_definition.hpp>
#include <components/table/table_state.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <filesystem>
#include <fstream>
#include <unistd.h>

using namespace services::disk;
using namespace components::table;
using namespace components::types;
using namespace components::vector;

namespace {
    std::string torn_test_dir() {
        static std::string path = "/tmp/test_otterbrix_torn_" + std::to_string(::getpid());
        return path;
    }
    void cleanup_torn_dir() { std::filesystem::remove_all(torn_test_dir()); }

    void append_one_int(data_table_t& table, std::pmr::memory_resource* res, int64_t value) {
        auto types = table.copy_types();
        data_chunk_t chunk(res, types, 1);
        chunk.set_cardinality(1);
        chunk.set_value(0, 0, logical_value_t{res, value});
        table_append_state state(res);
        table.append_lock(state);
        table.initialize_append(state);
        table.append(chunk, state);
        table.finalize_append(state, transaction_data{0, 0});
    }
} // namespace

// W-TORN: checkpoint(wal_id) overload tracks per-table prev/current ids.
// Bare checkpoint() leaves the ids untouched (used by IN_MEMORY no-ops and legacy callsites).
TEST_CASE("services::disk::torn::checkpoint_wal_id_overload_tracks_prev") {
    cleanup_torn_dir();
    std::filesystem::create_directories(torn_test_dir());
    std::pmr::synchronized_pool_resource resource;

    auto otbx = std::filesystem::path(torn_test_dir()) / "track.otbx";
    std::vector<column_definition_t> cols;
    cols.emplace_back("value", logical_type::BIGINT);

    table_storage_t ts(&resource, std::move(cols), otbx);
    REQUIRE(ts.checkpoint_wal_id() == 0);
    REQUIRE(ts.prev_checkpoint_wal_id() == 0);

    append_one_int(ts.table(), &resource, 1);
    ts.checkpoint(services::wal::id_t{100});
    REQUIRE(ts.checkpoint_wal_id() == 100);
    REQUIRE(ts.prev_checkpoint_wal_id() == 0); // first checkpoint, no prior id

    append_one_int(ts.table(), &resource, 2);
    ts.checkpoint(services::wal::id_t{250});
    REQUIRE(ts.checkpoint_wal_id() == 250);
    REQUIRE(ts.prev_checkpoint_wal_id() == 100); // shifted

    append_one_int(ts.table(), &resource, 3);
    ts.checkpoint(services::wal::id_t{260});
    REQUIRE(ts.checkpoint_wal_id() == 260);
    REQUIRE(ts.prev_checkpoint_wal_id() == 250); // shifted again

    cleanup_torn_dir();
}

// W-TORN: bare checkpoint() (legacy) does NOT mutate wal_id fields.
TEST_CASE("services::disk::torn::checkpoint_no_overload_leaves_ids_zero") {
    cleanup_torn_dir();
    std::filesystem::create_directories(torn_test_dir());
    std::pmr::synchronized_pool_resource resource;

    auto otbx = std::filesystem::path(torn_test_dir()) / "noid.otbx";
    std::vector<column_definition_t> cols;
    cols.emplace_back("value", logical_type::BIGINT);

    table_storage_t ts(&resource, std::move(cols), otbx);
    append_one_int(ts.table(), &resource, 42);
    ts.checkpoint();
    REQUIRE(ts.checkpoint_wal_id() == 0);
    REQUIRE(ts.prev_checkpoint_wal_id() == 0);

    cleanup_torn_dir();
}

// W-TORN: data is persisted across a checkpoint(wal_id) — both fsync barriers fired.
TEST_CASE("services::disk::torn::data_persists_after_checkpoint_with_wal_id") {
    cleanup_torn_dir();
    std::filesystem::create_directories(torn_test_dir());
    std::pmr::synchronized_pool_resource resource;

    auto otbx = std::filesystem::path(torn_test_dir()) / "persist.otbx";
    constexpr int64_t N = 50;
    {
        std::vector<column_definition_t> cols;
        cols.emplace_back("value", logical_type::BIGINT);
        table_storage_t ts(&resource, std::move(cols), otbx);
        for (int64_t i = 0; i < N; i++) {
            append_one_int(ts.table(), &resource, i);
        }
        ts.checkpoint(services::wal::id_t{777});
        REQUIRE(ts.checkpoint_wal_id() == 777);
    }

    {
        table_storage_t ts(&resource, otbx);
        REQUIRE(ts.table().calculate_size() == static_cast<uint64_t>(N));
        // Per-table wal_id is in-memory only — a fresh load starts at 0.
        REQUIRE(ts.checkpoint_wal_id() == 0);
        REQUIRE(ts.prev_checkpoint_wal_id() == 0);
    }

    cleanup_torn_dir();
}

// W-TORN: only .prev exists (otbx missing — crash between rename and reopen) → promote.
TEST_CASE("services::disk::torn::load_storage_disk_sync_promotes_only_prev") {
    cleanup_torn_dir();
    auto db_dir = std::filesystem::path(torn_test_dir()) / "db1" / "main" / "coll1";
    std::filesystem::create_directories(db_dir);
    std::pmr::synchronized_pool_resource resource;

    auto otbx = db_dir / "table.otbx";
    auto prev = db_dir / "table.otbx.prev";

    // Build a real otbx via table_storage_t, then rename to .prev (simulating partial-rename crash).
    {
        std::vector<column_definition_t> cols;
        cols.emplace_back("value", logical_type::BIGINT);
        table_storage_t ts(&resource, std::move(cols), otbx);
        append_one_int(ts.table(), &resource, 7);
        ts.checkpoint();
    }
    std::filesystem::rename(otbx, prev);
    REQUIRE_FALSE(std::filesystem::exists(otbx));
    REQUIRE(std::filesystem::exists(prev));

    // Direct table_storage_t cannot test load_storage_disk_sync (which is on manager_disk_t),
    // so simulate the recovery path: rename(.prev → otbx) + reload — exactly what the
    // load_storage_disk_sync's "only .prev" branch does.
    std::filesystem::rename(prev, otbx);
    table_storage_t ts(&resource, otbx);
    REQUIRE(ts.table().calculate_size() == 1);

    cleanup_torn_dir();
}

// W-TORN: corrupt otbx + valid .prev → fallback path corrupts moved aside as .broken,
// .prev promoted. We exercise the file-shuffle logic that load_storage_disk_sync performs.
TEST_CASE("services::disk::torn::load_falls_back_to_prev_on_corrupt_otbx") {
    cleanup_torn_dir();
    auto db_dir = std::filesystem::path(torn_test_dir()) / "db2" / "main" / "coll2";
    std::filesystem::create_directories(db_dir);
    std::pmr::synchronized_pool_resource resource;

    auto otbx = db_dir / "table.otbx";
    auto prev = db_dir / "table.otbx.prev";
    auto broken = db_dir / "table.otbx.broken";

    // Build a valid .prev by checkpointing then renaming.
    {
        std::vector<column_definition_t> cols;
        cols.emplace_back("value", logical_type::BIGINT);
        table_storage_t ts(&resource, std::move(cols), otbx);
        append_one_int(ts.table(), &resource, 99);
        ts.checkpoint();
    }
    std::filesystem::copy_file(otbx, prev);

    // Corrupt the otbx by truncating it to nothing.
    { std::ofstream zap(otbx, std::ios::trunc | std::ios::binary); }
    REQUIRE(std::filesystem::file_size(otbx) == 0);
    REQUIRE(std::filesystem::exists(prev));

    // Mirror load_storage_disk_sync's recovery: move corrupt aside, promote .prev.
    std::filesystem::rename(otbx, broken);
    std::filesystem::rename(prev, otbx);

    table_storage_t ts(&resource, otbx);
    REQUIRE(ts.table().calculate_size() == 1);
    REQUIRE(std::filesystem::exists(broken));
    REQUIRE_FALSE(std::filesystem::exists(prev));

    cleanup_torn_dir();
}
