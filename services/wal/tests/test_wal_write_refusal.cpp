// clang-format off
// <actor-zeta/spawn.hpp> requires std::unique_ptr, but does not include it itself
#include <memory>
#include <memory_resource>
#include <actor-zeta/spawn.hpp>
// clang-format on

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <string>
#include <thread>
#include <unistd.h>

#include <components/catalog/catalog_oids.hpp>
#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/table/test/fault_injection_file.hpp>
#include <components/tests/generaty.hpp>
#include <core/config.hpp>
#include <core/executor.hpp>
#include <core/pmr.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/wal_page.hpp>
#include <services/wal/wal_reader.hpp>

// FIN-1 — THE JOURNAL MUST NOT REPORT WORK IT DID NOT DO.
//
// Four defects of one family lived here: the write path and the segment-read path had no
// error channel, so four different failures all came back looking like success.
//
//   1. Every wal_worker_t write handler dropped wal_page_writer_t::append's answer — the one
//      documented "false on write error (e.g. disk full)" — and returned the wal_id anyway.
//      The caller was handed the number of a record that is not in the journal.
//   2. commit_txn dropped flush_and_sync()'s answer, so under wal_sync_mode::FULL a FAILED
//      fsync still returned a wal_id: a transaction reporting a durable commit over a page
//      that never reached the device.
//   3. wal_page_reader_t left file_ null and file_size_ zero when the open failed, so
//      page_count() answered 0 — and truncate_before read that as "this segment is empty,
//      safe to remove" and UNLINKED the one segment nobody could read.
//   4. read_all_records answered an empty vector for the same unopened segment, so startup
//      replay came up silently missing every committed transaction it held.
//
// THE INJECTION. These files are opened by the WAL itself through core::filesystem::open_file,
// not by single_file_block_manager_t, so the existing T3 seam could not reach them. The WAL now
// carries the same shape of seam (services::wal::dev_set_wal_file_interposer, DEV_MODE only),
// and the interposer below drives it two ways:
//   - returning nullptr models a segment that WILL NOT OPEN. That is faithful, not a
//     stand-in: local_file_system.cpp's open_file answers nullptr for exactly this;
//   - wrapping with otterbrix_test::faulty_file_handle_t models a device that refuses writes
//     or fsyncs, driven by the same fault_plan_t the .otbx tests use.

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
            std::filesystem::temp_directory_path() / ("test_wal_refusal_" + std::to_string(::getpid()));
        return p;
    }

    // The seam is process-wide, so it is scoped by this RAII object and narrowed by path.
    // Both knobs are live data: a test arms them AFTER the setup traffic it wants to succeed.
    class wal_fault_scope_t final : public wal_file_interposer_t {
    public:
        wal_fault_scope_t() { dev_set_wal_file_interposer(this); }
        ~wal_fault_scope_t() override { dev_set_wal_file_interposer(nullptr); }

        wal_fault_scope_t(const wal_fault_scope_t&) = delete;
        wal_fault_scope_t& operator=(const wal_fault_scope_t&) = delete;

        // Segment files whose path contains this marker do not open at all.
        std::string refuse_open_marker;
        // Segment files whose path contains this marker get the T3 faulty handle below.
        std::string faulty_marker;
        otterbrix_test::fault_plan_t plan;

        std::unique_ptr<core::filesystem::file_handle_t>
        wrap(const std::filesystem::path& path, std::unique_ptr<core::filesystem::file_handle_t> inner) override {
            const auto name = path.string();
            if (!refuse_open_marker.empty() && name.find(refuse_open_marker) != std::string::npos) {
                return nullptr;
            }
            if (inner != nullptr && !faulty_marker.empty() && name.find(faulty_marker) != std::string::npos) {
                return std::make_unique<otterbrix_test::faulty_file_handle_t>(std::move(inner), plan);
            }
            return inner;
        }
    };

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

    // Name of the segment file the main-database worker writes first.
    std::string segment_name(uint32_t index) {
        std::string suffix = std::to_string(index);
        suffix.insert(suffix.begin(), 6 - suffix.size(), '0');
        return "wal_" + std::to_string(static_cast<unsigned>(kMainDb)) + "_" + suffix;
    }

    // The directory is prepared inside make_config so it exists before the manager is spawned
    // in the member-init list (its deleter has no default constructor, so it cannot be
    // assigned in the body).
    configuration::config_wal make_config(const std::filesystem::path& path, size_t max_segment_size) {
        std::filesystem::remove_all(path);
        std::filesystem::create_directories(path);
        configuration::config_wal config(path);
        config.on = true;
        if (max_segment_size != 0) {
            config.max_segment_size = max_segment_size;
        }
        return config;
    }

    struct wal_env_t {
        explicit wal_env_t(const std::filesystem::path& path, size_t max_segment_size = 0)
            : path_(path)
            , log_(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler_(new actor_zeta::shared_work(2, 1000))
            , config_(make_config(path, max_segment_size))
            , manager_(actor_zeta::spawn<manager_wal_replicate_t>(&resource_, scheduler_.get(), config_, log_)) {
            manager_->sync(wal_sync_pack_t{actor_zeta::address_t::empty_address(),
                                           actor_zeta::address_t::empty_address(),
                                           actor_zeta::address_t::empty_address()});
            scheduler_->start();
        }

        ~wal_env_t() {
            scheduler_->stop();
            manager_.reset();
        }

        auto send_insert(uint64_t txn_id, size_t rows, uint64_t row_start = 0) {
            auto* arena = std::pmr::new_delete_resource(); // must outlive async processing
            auto [ns, fut] = actor_zeta::otterbrix::send(manager_->address(),
                                                         &manager_wal_replicate_t::write_physical_insert,
                                                         session_id_t::generate_uid(),
                                                         kTestTableOid,
                                                         one_chunk(arena, rows),
                                                         row_start,
                                                         static_cast<uint64_t>(rows),
                                                         txn_id,
                                                         kMainDb);
            return std::move(fut);
        }

        auto send_commit(uint64_t txn_id, wal_sync_mode mode) {
            auto [ns, fut] = actor_zeta::otterbrix::send(manager_->address(),
                                                         &manager_wal_replicate_t::commit_txn,
                                                         session_id_t::generate_uid(),
                                                         txn_id,
                                                         mode,
                                                         kMainDb,
                                                         uint64_t{0});
            return std::move(fut);
        }

        auto send_current_wal_id() {
            auto [ns, fut] = actor_zeta::otterbrix::send(manager_->address(),
                                                         &manager_wal_replicate_t::current_wal_id,
                                                         session_id_t::generate_uid());
            return std::move(fut);
        }

        auto send_truncate_before(services::wal::id_t checkpoint_id) {
            auto [ns, fut] = actor_zeta::otterbrix::send(manager_->address(),
                                                         &manager_wal_replicate_t::truncate_before,
                                                         session_id_t::generate_uid(),
                                                         checkpoint_id);
            return std::move(fut);
        }

        std::filesystem::path path_;
        core::pmr::otterbrix_resource resource_;
        log_t log_;
        actor_zeta::scheduler_ptr scheduler_;
        configuration::config_wal config_;
        std::unique_ptr<manager_wal_replicate_t, actor_zeta::pmr::deleter_t> manager_;
    };

} // namespace

// ===========================================================================
// FIN-1 / item 1 — A REFUSED PAGE WRITE MUST NOT COME BACK AS A WAL ID.
//
// The record below is 500 rows wide, which is more than PAGE_DATA_SIZE, so append() has to
// flush a full page in the middle of the record rather than only buffering it — the exact
// call whose bool answer was discarded. The fault plan lets ONE write through (the segment
// file header, written when the worker opens the segment) and refuses every write after it.
//
// BEFORE: write_physical_insert returned the freshly allocated wal_id, with nothing on disk.
// ===========================================================================
TEST_CASE("wal::refusal::a_refused_page_write_is_not_reported_as_a_written_record") {
    wal_fault_scope_t fault;
    fault.faulty_marker = "wal_";
    // fail_after_writes counts ALLOWED successes: 1 lets the file header land and refuses
    // every page write that follows.
    fault.plan.fail_after_writes = 1;

    wal_env_t env(base_path() / "refused_page_write");

    auto fut = env.send_insert(/*txn_id=*/100, /*rows=*/500);
    auto result = await_ready(fut);

    INFO("a record the page writer refused must not be answered with its wal_id");
    REQUIRE(result.has_error());
    REQUIRE(result.error().type == core::error_code_t::io_error);
    // The refusal really came from a refused write, not from a segment that never opened.
    REQUIRE(fault.plan.writes_seen > 1);
}

// ===========================================================================
// FIN-1 / item 2 — A FAILED fsync UNDER FULL MUST REFUSE THE COMMIT.
//
// wal_sync_mode::FULL is the mode whose whole meaning is "this marker is on the device". The
// plan lets every write through and fails the first sync(), which is the one flush_and_sync
// issues for this commit.
//
// BEFORE: commit_txn returned the wal_id, i.e. reported a durable commit over an unsynced page.
// ===========================================================================
TEST_CASE("wal::refusal::a_failed_fsync_under_full_sync_refuses_the_commit") {
    wal_fault_scope_t fault;
    fault.faulty_marker = "wal_";
    fault.plan.fail_syncs_from = 1; // 1-based: the very first sync fails

    wal_env_t env(base_path() / "failed_fsync");

    auto insert_fut = env.send_insert(/*txn_id=*/200, /*rows=*/4);
    auto insert_result = await_ready(insert_fut);
    REQUIRE_FALSE(insert_result.has_error()); // the write path itself is healthy here

    auto commit_fut = env.send_commit(/*txn_id=*/200, wal_sync_mode::FULL);
    auto commit_result = await_ready(commit_fut);

    INFO("FULL sync means the marker is on the device; a failed fsync cannot report a commit");
    REQUIRE(commit_result.has_error());
    REQUIRE(commit_result.error().type == core::error_code_t::io_error);
    REQUIRE(fault.plan.syncs_seen >= 1);
}

// ===========================================================================
// FIN-1 / item 3 — TRUNCATION MUST REFUSE A SEGMENT IT CANNOT READ, NOT DELETE IT.
//
// "unreadable" and "empty" both arrived at truncate_before as page_count() == 0, and the
// branch for "empty" unlinks the file. So the ONE segment whose contents nobody could account
// for was the one that got destroyed. The assertion below is on the FILESYSTEM, not on the
// status: the file must still be there afterwards.
//
// BEFORE: the segment was removed and truncate_before reported nothing.
// ===========================================================================
TEST_CASE("wal::refusal::truncation_keeps_a_segment_it_cannot_read") {
    wal_fault_scope_t fault; // installed, but armed only after the segments exist

    // A tiny segment size forces a rotation, so segment 000000 is closed and therefore a
    // truncation candidate (the writer's CURRENT segment is always skipped).
    wal_env_t env(base_path() / "truncate_refusal", /*max_segment_size=*/8192);

    for (uint64_t i = 0; i < 12; ++i) {
        auto fut = env.send_insert(/*txn_id=*/300 + i, /*rows=*/40, i * 40);
        auto result = await_ready(fut);
        REQUIRE_FALSE(result.has_error());
    }

    // config_wal appends "wal" to the base path, so the segments live under config_.path.
    const auto db_dir = env.config_.path / std::to_string(static_cast<unsigned>(kMainDb));
    const auto first_segment = db_dir / segment_name(0);
    REQUIRE(std::filesystem::exists(first_segment));
    // The rotation must have happened, otherwise segment 000000 is the writer's current one
    // and truncate_before would skip it for an unrelated reason.
    REQUIRE(std::filesystem::exists(db_dir / segment_name(1)));

    auto cur_fut = env.send_current_wal_id();
    const auto checkpoint_id = await_ready(cur_fut);
    REQUIRE(checkpoint_id > 0);

    // Now make segment 000000 unopenable and ask for a truncation that covers it.
    fault.refuse_open_marker = segment_name(0);

    auto truncate_fut = env.send_truncate_before(checkpoint_id);
    auto truncate_error = await_ready(truncate_fut);

    // THE FILESYSTEM IS THE ASSERTION, and it is taken FIRST: the defect was a deleted file,
    // not a missing status. Snapshot before any REQUIRE so the state is the post-truncate one.
    fault.refuse_open_marker.clear();
    const bool segment_survived = std::filesystem::exists(first_segment);
    const auto surviving_size = segment_survived ? std::filesystem::file_size(first_segment) : 0;

    INFO("a segment that cannot be READ is not a segment that is EMPTY");
    REQUIRE(segment_survived);
    REQUIRE(surviving_size > 0);

    REQUIRE(truncate_error.contains_error());
    REQUIRE(truncate_error.type == core::error_code_t::io_error);
}

// ===========================================================================
// FIN-1 / item 4 — STARTUP REPLAY MUST REFUSE AN UNOPENABLE SEGMENT.
//
// The first half of the test reads the same directory with no fault and REQUIREs records, so
// the empty answer in the second half can only be caused by the refusal — not by a journal
// that happened to hold nothing.
//
// BEFORE: read_committed_records returned an empty vector and no error, and base_spaces
// carried on and brought the engine up without every transaction the segment held.
// ===========================================================================
TEST_CASE("wal::refusal::startup_replay_refuses_a_segment_that_will_not_open") {
    const auto path = base_path() / "replay_refusal";
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    core::pmr::otterbrix_resource resource;
    configuration::config_wal config(path);
    config.on = true;

    {
        wal_env_t env(path);
        auto insert_fut = env.send_insert(/*txn_id=*/400, /*rows=*/6);
        REQUIRE_FALSE(await_ready(insert_fut).has_error());
        auto commit_fut = env.send_commit(/*txn_id=*/400, wal_sync_mode::FULL);
        REQUIRE_FALSE(await_ready(commit_fut).has_error());
    }

    // Control: with the segment readable, the replay finds the committed transaction.
    {
        wal_reader_t reader(&resource, config, log);
        auto records = reader.read_committed_records(services::wal::id_t{0});
        REQUIRE_FALSE(records.has_error());
        REQUIRE_FALSE(records.value().empty());
    }

    // Now the same directory, with the segment refusing to open.
    {
        wal_fault_scope_t fault;
        fault.refuse_open_marker = segment_name(0);

        wal_reader_t reader(&resource, config, log);
        auto records = reader.read_committed_records(services::wal::id_t{0});

        INFO("replay that cannot read a segment must refuse, not answer 'there was nothing'");
        REQUIRE(records.has_error());
        REQUIRE(records.error().type == core::error_code_t::io_error);
    }
}
