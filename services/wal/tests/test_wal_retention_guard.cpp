// clang-format off
// <actor-zeta/spawn.hpp> requires std::unique_ptr, but does not include it itself
#include <memory>
#include <memory_resource>
#include <actor-zeta/spawn.hpp>
// clang-format on

#include <catch2/catch_test_macros.hpp>
#include <components/context/context.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <unistd.h>

#include <components/catalog/catalog_oids.hpp>
#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/tests/generaty.hpp>
#include <core/config.hpp>
#include <core/executor.hpp>
#include <core/pmr.hpp>
#include <services/wal/manager_wal_replicate.hpp>

// Three guards of the manager's own bookkeeping: (1) the CREATE INDEX retention set must be a
// multiset, not a deduplicating one — two builds registering the same start position would
// collapse into one entry, so the first unregister empties the clamp while the second build
// still needs it, and the second unregister then finds nothing to erase and ABORTS THE PROCESS;
// (2) startup classification of <wal>/<db> directories must not use std::stoul under catch(...),
// which half-parses "9zz" as database oid 9 and spawns a worker over the wrong directory; (3)
// total_wal_bytes() must sum only wal_* segments, not every file under <wal>/<db>/, or a foreign
// neighbour inflates the auto-checkpoint window.

using namespace services;
using namespace services::wal;
namespace catalog = components::catalog;

namespace {

    using session_id_t = components::session::session_id_t;
    using data_chunk_t = components::vector::data_chunk_t;

    constexpr auto kMainDb = catalog::well_known_oid::main_database;
    constexpr catalog::oid_t kTestTableOid = 16700;

    std::filesystem::path base_path() {
        static std::filesystem::path p =
            std::filesystem::temp_directory_path() / ("test_wal_retention_guard_" + std::to_string(::getpid()));
        return p;
    }

    template<typename F>
    decltype(auto) await_ready(F& fut) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (!fut.is_ready() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
        REQUIRE(fut.is_ready());
        return std::move(fut).take_ready();
    }

    std::pmr::vector<data_chunk_t> one_chunk(std::pmr::memory_resource* arena, size_t rows) {
        std::pmr::vector<data_chunk_t> chunks(arena);
        chunks.emplace_back(gen_data_chunk(rows, arena));
        return chunks;
    }

    std::string segment_name(uint32_t index) {
        std::string suffix = std::to_string(index);
        suffix.insert(suffix.begin(), 6 - suffix.size(), '0');
        return "wal_" + std::to_string(static_cast<unsigned>(kMainDb)) + "_" + suffix;
    }

    configuration::config_wal make_config(const std::filesystem::path& path, size_t max_segment_size) {
        std::filesystem::create_directories(path);
        configuration::config_wal config(path);
        config.on = true;
        if (max_segment_size != 0) {
            config.max_segment_size = max_segment_size;
        }
        return config;
    }

    struct wal_env_t {
        explicit wal_env_t(const std::filesystem::path& path, size_t max_segment_size = 0, bool wipe = true)
            : path_(path)
            , log_(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler_(new actor_zeta::shared_work(2, 1000))
            , config_([&] {
                if (wipe) {
                    std::filesystem::remove_all(path);
                }
                return make_config(path, max_segment_size);
            }())
            , manager_(actor_zeta::spawn<manager_wal_replicate_t>(&resource_,
                                                                  scheduler_.get(),
                                                                  config_,
                                                                  log_,
                                                                  components::pipeline::no_mailbox(),
                                                                  components::pipeline::no_mailbox())) {
            scheduler_->start();
        }

        ~wal_env_t() {
            scheduler_->stop();
            manager_.reset();
        }

        // Built on the fixture's own arena (core::pmr::otterbrix_resource, resource_tracer_t under
        // ASAN), mirroring production (agent_disk_t::storage_append_inner builds off resource()).
        // resource_ is declared FIRST so it outlives ~wal_env_t's teardown of manager_. Extracted
        // so a test can assert the ARENA of a REAL payload before it's moved into the message.
        std::pmr::vector<data_chunk_t> make_insert_batch(size_t rows) {
            return one_chunk(&resource_, rows);
        }

        auto send_insert(uint64_t txn_id, size_t rows, uint64_t row_start = 0) {
            auto [ns, fut] = actor_zeta::otterbrix::send(manager_->address(),
                                                         &manager_wal_replicate_t::write_physical_insert,
                                                         session_id_t::generate_uid(),
                                                         kTestTableOid,
                                                         make_insert_batch(rows),
                                                         row_start,
                                                         static_cast<uint64_t>(rows),
                                                         txn_id,
                                                         kMainDb);
            return std::move(fut);
        }

        auto send_commit(uint64_t txn_id) {
            auto [ns, fut] = actor_zeta::otterbrix::send(manager_->address(),
                                                         &manager_wal_replicate_t::commit_txn,
                                                         session_id_t::generate_uid(),
                                                         txn_id,
                                                         wal_sync_mode::NORMAL,
                                                         kMainDb,
                                                         uint64_t{0});
            return std::move(fut);
        }

        auto send_truncate_before(services::wal::id_t checkpoint_id) {
            auto [ns, fut] = actor_zeta::otterbrix::send(manager_->address(),
                                                         &manager_wal_replicate_t::truncate_before,
                                                         session_id_t::generate_uid(),
                                                         checkpoint_id);
            return std::move(fut);
        }

        auto send_current_wal_id() {
            auto [ns, fut] = actor_zeta::otterbrix::send(manager_->address(),
                                                         &manager_wal_replicate_t::current_wal_id,
                                                         session_id_t::generate_uid());
            return std::move(fut);
        }

        std::filesystem::path db_dir() const { return config_.path / std::to_string(static_cast<unsigned>(kMainDb)); }

        std::filesystem::path path_;
        core::pmr::otterbrix_resource resource_;
        log_t log_;
        actor_zeta::scheduler_ptr scheduler_;
        configuration::config_wal config_;
        std::unique_ptr<manager_wal_replicate_t, actor_zeta::pmr::deleter_t> manager_;
    };

    // Roll the journal into at least two segments so truncate_before has a closed candidate.
    void fill_two_segments(wal_env_t& env) {
        for (uint64_t i = 0; i < 12; ++i) {
            auto fut = env.send_insert(/*txn_id=*/300 + i, /*rows=*/40, i * 40);
            auto result = await_ready(fut);
            REQUIRE_FALSE(result.has_error());
        }
        auto commit_fut = env.send_commit(/*txn_id=*/311);
        auto commit_result = await_ready(commit_fut);
        REQUIRE_FALSE(commit_result.has_error());
        REQUIRE(std::filesystem::exists(env.db_dir() / segment_name(0)));
        REQUIRE(std::filesystem::exists(env.db_dir() / segment_name(1)));
    }

} // namespace

// Two builds at the same start position are two registrations: register(1) twice,
// unregister(1) once, and truncate_before must still clamp to 1.
// BEFORE: the set deduplicated the two registrations, the single unregister emptied it, and the
// segments the live catchup still needs were unlinked.
TEST_CASE("wal::retention::two_builds_at_the_same_start_position_hold_the_clamp") {
    wal_env_t env(base_path() / "dedup_clamp", /*max_segment_size=*/8192);
    fill_two_segments(env);

    const auto first_segment = env.db_dir() / segment_name(0);

    // Two concurrent CREATE INDEX builds, both starting at wal position 1.
    env.manager_->register_active_build_sync(1);
    env.manager_->register_active_build_sync(1);
    // The first build finishes.
    env.manager_->unregister_active_build_sync(1);

    auto cur_fut = env.send_current_wal_id();
    const auto checkpoint_id = await_ready(cur_fut);
    REQUIRE(checkpoint_id > 1);

    auto truncate_fut = env.send_truncate_before(checkpoint_id);
    auto truncate_error = await_ready(truncate_fut);
    REQUIRE_FALSE(truncate_error.contains_error());

    INFO("one build still runs from position 1, so the clamp must hold and segment 000000 must stay");
    REQUIRE(std::filesystem::exists(first_segment));

    // Balance the second registration so the test leaves no live clamp behind.
    env.manager_->unregister_active_build_sync(1);
}

// An unmatched unregister is a bug report, not a process exit: this path is fed by messages
// from another actor, so the input is reachable, not hypothetical.
// BEFORE: assert in Debug, explicit std::abort() in Release — the process died.
TEST_CASE("wal::retention::an_unmatched_unregister_does_not_abort_the_process") {
    wal_env_t env(base_path() / "unmatched_unregister");

    // No registration was ever made; the process must survive and the journal must go on
    // accepting writes.
    env.manager_->unregister_active_build_sync(42);

    auto fut = env.send_insert(/*txn_id=*/1, /*rows=*/4);
    auto r = await_ready(fut);
    REQUIRE_FALSE(r.has_error());
}

// A directory that is not a database oid is skipped loudly, not half-parsed: std::stoul("9zz")
// answers 9, spawning a worker for the WRONG directory and splitting the journal in two.
// BEFORE: <wal>/9 appeared next to <wal>/9zz.
TEST_CASE("wal::classification::a_non_oid_directory_does_not_spawn_a_worker") {
    const auto path = base_path() / "foreign_dir";
    std::filesystem::remove_all(path);
    // config_wal(path) roots the journal at <path>/wal — the foreign directory must sit
    // where the manager's startup scan actually walks.
    std::filesystem::create_directories(path / "wal" / "9zz");

    wal_env_t env(path, /*max_segment_size=*/0, /*wipe=*/false);

    INFO("'9zz' is not a database oid; no worker (and no directory '9') may be manufactured from it");
    REQUIRE_FALSE(std::filesystem::exists(path / "wal" / "9"));
}

// The auto-checkpoint window counts the journal, not neighbours sharing its root.
// BEFORE: any regular file directly under the database directory inflated the sum.
TEST_CASE("wal::classification::total_wal_bytes_counts_only_wal_segments") {
    wal_env_t env(base_path() / "total_bytes");

    {
        auto fut = env.send_insert(/*txn_id=*/1, /*rows=*/8);
        auto r = await_ready(fut);
        REQUIRE_FALSE(r.has_error());
    }
    {
        auto fut = env.send_commit(/*txn_id=*/1);
        auto r = await_ready(fut);
        REQUIRE_FALSE(r.has_error());
    }

    const auto before = env.manager_->total_wal_bytes();
    REQUIRE(before > 0);

    // A foreign neighbour lands directly under <wal>/<db>/.
    {
        std::ofstream stray(env.db_dir() / "stray.bin", std::ios::binary);
        std::string filler(4096, 'x');
        stray.write(filler.data(), static_cast<std::streamsize>(filler.size()));
    }
    REQUIRE(std::filesystem::file_size(env.db_dir() / "stray.bin") == 4096);

    INFO("a neighbour that is not a wal_* segment must not widen the auto-checkpoint window");
    REQUIRE(env.manager_->total_wal_bytes() == before);
}

// Insert payload built on the fixture's own arena (see make_insert_batch above); the batch is
// unobservable after send, so the assertion is made on make_insert_batch's own output.
TEST_CASE("wal::retention::the_insert_payload_is_built_on_the_fixture_arena") {
    const auto path = base_path() / "payload_arena";
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    wal_env_t env(path);

    auto batch = env.make_insert_batch(4);
    REQUIRE(batch.size() == 1);
    REQUIRE(batch.get_allocator().resource() == &env.resource_);
    REQUIRE(batch.front().resource() == &env.resource_);
}
