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

// Injected via services::wal::dev_set_wal_file_interposer: nullptr models an unopenable segment
// (the same value local_file_system.cpp's open_file returns for it), and
// otterbrix_test::faulty_file_handle_t models a device refusing writes/fsyncs.

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

    class wal_fault_scope_t final : public wal_file_interposer_t {
    public:
        wal_fault_scope_t() { dev_set_wal_file_interposer(this); }
        ~wal_fault_scope_t() override { dev_set_wal_file_interposer(nullptr); }

        wal_fault_scope_t(const wal_fault_scope_t&) = delete;
        wal_fault_scope_t& operator=(const wal_fault_scope_t&) = delete;

        std::string refuse_open_marker;
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

    std::string segment_name(uint32_t index) {
        std::string suffix = std::to_string(index);
        suffix.insert(suffix.begin(), 6 - suffix.size(), '0');
        return "wal_" + std::to_string(static_cast<unsigned>(kMainDb)) + "_" + suffix;
    }

    configuration::config_wal make_config(const std::filesystem::path& path, size_t max_segment_size) {
        std::filesystem::remove_all(path);
        std::filesystem::create_directories(path);
        configuration::config_wal config(path);
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

        std::pmr::vector<data_chunk_t> make_insert_batch(size_t rows) { return one_chunk(&resource_, rows); }

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

// A refused page write must not surface as a wal_id: the record is 500 rows (over PAGE_DATA_SIZE)
// so append() flushes mid-record, the exact call whose bool answer must not be dropped.
TEST_CASE("wal::refusal::a_refused_page_write_is_not_reported_as_a_written_record") {
    wal_fault_scope_t fault;
    fault.faulty_marker = "wal_";
    fault.plan.fail_after_writes = 1; // counts allowed successes: 1 lets the header land, then refuses

    wal_env_t env(base_path() / "refused_page_write");

    auto fut = env.send_insert(/*txn_id=*/100, /*rows=*/500);
    auto result = await_ready(fut);

    INFO("a record the page writer refused must not be answered with its wal_id");
    REQUIRE(result.has_error());
    REQUIRE(result.error().type == core::error_code_t::io_error);
    REQUIRE(fault.plan.writes_seen > 1);
}

// A failed fsync under FULL must refuse the commit: a dropped flush_and_sync() would report a
// durable commit over an unsynced page, when FULL's whole meaning is that the marker is on disk.
TEST_CASE("wal::refusal::a_failed_fsync_under_full_sync_refuses_the_commit") {
    wal_fault_scope_t fault;
    fault.faulty_marker = "wal_";
    fault.plan.fail_syncs_from = 1; // 1-based: the very first sync fails

    wal_env_t env(base_path() / "failed_fsync");

    auto insert_fut = env.send_insert(/*txn_id=*/200, /*rows=*/4);
    auto insert_result = await_ready(insert_fut);
    REQUIRE_FALSE(insert_result.has_error());

    auto commit_fut = env.send_commit(/*txn_id=*/200, wal_sync_mode::FULL);
    auto commit_result = await_ready(commit_fut);

    INFO("FULL sync means the marker is on the device; a failed fsync cannot report a commit");
    REQUIRE(commit_result.has_error());
    REQUIRE(commit_result.error().type == core::error_code_t::io_error);
    REQUIRE(fault.plan.syncs_seen >= 1);
}

// Truncation must refuse a segment it cannot read, not delete it: "unreadable" and "empty" both
// read as page_count()==0, so the empty branch could unlink an unaccountable segment. The
// assertion is on the filesystem, not the returned status.
TEST_CASE("wal::refusal::truncation_keeps_a_segment_it_cannot_read") {
    wal_fault_scope_t fault;

    wal_env_t env(base_path() / "truncate_refusal", /*max_segment_size=*/8192);

    for (uint64_t i = 0; i < 12; ++i) {
        auto fut = env.send_insert(/*txn_id=*/300 + i, /*rows=*/40, i * 40);
        auto result = await_ready(fut);
        REQUIRE_FALSE(result.has_error());
    }

    const auto db_dir = env.config_.path / std::to_string(static_cast<unsigned>(kMainDb));
    const auto first_segment = db_dir / segment_name(0);
    REQUIRE(std::filesystem::exists(first_segment));
    REQUIRE(std::filesystem::exists(db_dir / segment_name(1)));

    auto cur_fut = env.send_current_wal_id();
    const auto checkpoint_id = await_ready(cur_fut);
    REQUIRE(checkpoint_id > 0);

    fault.refuse_open_marker = segment_name(0);

    auto truncate_fut = env.send_truncate_before(checkpoint_id);
    auto truncate_error = await_ready(truncate_fut);

    fault.refuse_open_marker.clear();
    const bool segment_survived = std::filesystem::exists(first_segment);
    const auto surviving_size = segment_survived ? std::filesystem::file_size(first_segment) : 0;

    INFO("a segment that cannot be READ is not a segment that is EMPTY");
    REQUIRE(segment_survived);
    REQUIRE(surviving_size > 0);

    REQUIRE(truncate_error.contains_error());
    REQUIRE(truncate_error.type == core::error_code_t::io_error);
}

// The control half (no fault) proves records exist, so an empty answer in the fault half can
// only be the refusal, not an empty journal.
TEST_CASE("wal::refusal::startup_replay_refuses_a_segment_that_will_not_open") {
    const auto path = base_path() / "replay_refusal";
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    core::pmr::otterbrix_resource resource;
    configuration::config_wal config(path);

    {
        wal_env_t env(path);
        auto insert_fut = env.send_insert(/*txn_id=*/400, /*rows=*/6);
        REQUIRE_FALSE(await_ready(insert_fut).has_error());
        auto commit_fut = env.send_commit(/*txn_id=*/400, wal_sync_mode::FULL);
        REQUIRE_FALSE(await_ready(commit_fut).has_error());
    }

    {
        wal_reader_t reader(&resource, config, log);
        auto records = reader.read_committed_records(services::wal::id_t{0});
        REQUIRE_FALSE(records.has_error());
        REQUIRE_FALSE(records.value().empty());
    }

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

TEST_CASE("wal::refusal::the_insert_payload_is_built_on_the_fixture_arena") {
    const auto path = base_path() / "payload_arena";
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    wal_env_t env(path);

    auto batch = env.make_insert_batch(4);
    REQUIRE(batch.size() == 1);
    REQUIRE(batch.get_allocator().resource() == &env.resource_);
    REQUIRE(batch.front().resource() == &env.resource_);
}
