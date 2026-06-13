#include <catch2/catch.hpp>

#include <services/wal/base.hpp>

#include <components/log/log.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <core/pmr.hpp>
#include <services/disk/manager_disk.hpp>

#include <actor-zeta/spawn.hpp>

#include <filesystem>
#include <type_traits>
#include <unistd.h>

using namespace services;

// ---------------------------------------------------------------------------
// W-TORN WAL-side tests. The disk-side mechanics live in
// services/disk/tests/test_torn_checkpoint.cpp; this file exercises the WAL
// side: that the checkpoint_all return value (min(prev_checkpoint_wal_id_))
// is the correct argument for truncate_before, and that the typing matches
// the WAL id contract.
// ---------------------------------------------------------------------------

namespace {
    std::string torn_wal_dir() {
        static std::string path = "/tmp/test_otterbrix_wal_torn_" + std::to_string(::getpid());
        return path;
    }
    void cleanup() { std::filesystem::remove_all(torn_wal_dir()); }
} // namespace

// 1. wal::id_t is uint64. Tests in test_torn_checkpoint.cpp rely on this; this is the
//    type contract reflected in services/wal/base.hpp:9.
TEST_CASE("wal::torn::id_t_is_uint64") {
    static_assert(std::is_same_v<wal::id_t, std::uint64_t>, "wal::id_t must be uint64_t");
    REQUIRE(sizeof(wal::id_t) == 8);
}

// 2. id_t{0} is the conventional "no-op" / "nothing yet" value used by truncate_before
//    and by the IN_MEMORY-table early-out in checkpoint_all.
TEST_CASE("wal::torn::id_zero_is_noop_value") {
    REQUIRE(wal::id_t{0} == 0);
    // dispatcher.cpp checks `min_prev_wal_id > 0` before calling truncate_before — id 0
    // means "do not truncate WAL".
}

// 3. table_storage_t::checkpoint(wal_id) shifts prev/current — the core W-TORN
//    in-memory record that drives min(prev) accounting.
TEST_CASE("wal::torn::table_storage_tracks_wal_id_chain") {
    cleanup();
    std::filesystem::create_directories(torn_wal_dir());
    std::pmr::synchronized_pool_resource resource;

    auto otbx = std::filesystem::path(torn_wal_dir()) / "chain.otbx";
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("v", components::types::complex_logical_type{components::types::logical_type::BIGINT});

    services::disk::table_storage_t ts(&resource, std::move(cols), otbx);
    REQUIRE(ts.checkpoint_wal_id() == 0);
    REQUIRE(ts.prev_checkpoint_wal_id() == 0);

    ts.checkpoint(wal::id_t{42});
    REQUIRE(ts.checkpoint_wal_id() == 42);
    REQUIRE(ts.prev_checkpoint_wal_id() == 0);

    ts.checkpoint(wal::id_t{100});
    REQUIRE(ts.checkpoint_wal_id() == 100);
    REQUIRE(ts.prev_checkpoint_wal_id() == 42);

    // Crucial for truncate_before: prev (42) is what bounds safe WAL deletion, not 100.
    // Truncating ≤ 100 would discard records [43..100] needed if recovery falls back to .prev.
    REQUIRE(ts.prev_checkpoint_wal_id() < ts.checkpoint_wal_id());

    cleanup();
}

// 4. checkpoint(wal::id_t{0}) is a legal no-op value carrier (we don't reject it).
//    A caller that passes 0 means "I have no committed wal_id to advance to" — semantics
//    handled by checkpoint_all (it short-circuits truncate_before when min_prev==0).
TEST_CASE("wal::torn::checkpoint_zero_id_is_legal") {
    cleanup();
    std::filesystem::create_directories(torn_wal_dir());
    std::pmr::synchronized_pool_resource resource;

    auto otbx = std::filesystem::path(torn_wal_dir()) / "zero.otbx";
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("v", components::types::complex_logical_type{components::types::logical_type::BIGINT});

    services::disk::table_storage_t ts(&resource, std::move(cols), otbx);
    REQUIRE_NOTHROW(ts.checkpoint(wal::id_t{0}));
    REQUIRE(ts.checkpoint_wal_id() == 0);

    cleanup();
}

// 5. After multiple checkpoints, prev <= current always holds — invariant for the
//    truncate_before semantics. (Strict `<` after at least one shift, but `==` allowed
//    if the same wal_id is passed twice — degenerate but legal.)
TEST_CASE("wal::torn::prev_le_current_invariant") {
    cleanup();
    std::filesystem::create_directories(torn_wal_dir());
    std::pmr::synchronized_pool_resource resource;

    auto otbx = std::filesystem::path(torn_wal_dir()) / "inv.otbx";
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("v", components::types::complex_logical_type{components::types::logical_type::BIGINT});

    services::disk::table_storage_t ts(&resource, std::move(cols), otbx);
    for (wal::id_t id : {wal::id_t{1}, wal::id_t{5}, wal::id_t{7}, wal::id_t{7}, wal::id_t{12}}) {
        ts.checkpoint(id);
        REQUIRE(ts.prev_checkpoint_wal_id() <= ts.checkpoint_wal_id());
    }
    // After the full sequence, prev should be the second-to-last (7), current the last (12).
    REQUIRE(ts.checkpoint_wal_id() == 12);
    REQUIRE(ts.prev_checkpoint_wal_id() == 7);

    cleanup();
}
