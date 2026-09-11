#include <catch2/catch_test_macros.hpp>

#include <components/configuration/configuration.hpp>

#include <filesystem>
#include <type_traits>

// config_disk and config_wal deliberately share <base>/wal; renaming it would orphan every
// existing database instead of fixing anything.

TEST_CASE("config_disk_path_layout") {
    const std::filesystem::path base{"/otterbrix-config-layout-probe"};
    const auto config = configuration::config::create_config(base);

    REQUIRE(config.disk.path == base / "wal");
    REQUIRE(config.wal.path == base / "wal");
    REQUIRE(config.disk.path == config.wal.path);
    REQUIRE(config.log.path == base / "log");
    REQUIRE(config.main_path == base);

    REQUIRE_FALSE(std::is_aggregate_v<configuration::config_disk>);
    const configuration::config_disk defaulted;
    REQUIRE(defaulted.path == std::filesystem::current_path() / "wal");
    REQUIRE(defaulted.path == configuration::config_disk{std::filesystem::current_path()}.path);

    // Neither the WAL nor disk storage has an on/off switch any more: switching the WAL off left
    // the disk index (durable per commit) ahead of the table (durable per checkpoint) with nothing
    // to reconcile them, and it stamped a zero checkpoint floor that poisoned the NEXT run.
    REQUIRE_FALSE(std::is_aggregate_v<configuration::config_wal>);
    const configuration::config_wal wal{base};
    REQUIRE(wal.path == base / "wal");
    REQUIRE(wal.page_size == 4096u);
    REQUIRE(wal.max_segment_size == 4u * 1024 * 1024);
    REQUIRE(wal.auto_checkpoint_threshold_bytes == 16u * 1024 * 1024);
}

namespace {
    template<typename T, typename = void>
    struct has_sync_to_disk : std::false_type {};
    template<typename T>
    struct has_sync_to_disk<T, std::void_t<decltype(std::declval<const T&>().sync_to_disk)>> : std::true_type {};

    template<typename T, typename = void>
    struct has_on : std::false_type {};
    template<typename T>
    struct has_on<T, std::void_t<decltype(std::declval<const T&>().on)>> : std::true_type {};
} // namespace

// A runtime CHECK, not a static_assert: this compiles both before and after the field is gone,
// so it is red while the field is still there instead of refusing to build.
TEST_CASE("config_wal_has_no_boolean_switches") {
    // `sync_to_disk` was never read by the engine at all (the fsync mode is wal_sync_mode, an
    // argument of commit_txn); `on` decided something, and what it decided was broken. Neither
    // may come back quietly.
    CHECK_FALSE(has_sync_to_disk<configuration::config_wal>::value);
    CHECK_FALSE(has_on<configuration::config_wal>::value);
}
