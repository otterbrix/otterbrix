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
#include <components/tests/generaty.hpp>
#include <core/config.hpp>
#include <core/executor.hpp>
#include <core/file/file_handle.hpp>
#include <core/pmr.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/wal_page.hpp>

// ONE PAGE, ONE READ. Deciding a segment's fate from TWO reads of the same page —
// verify_page_checksum reads and verifies it, then read_page_header reads it AGAIN and
// swallows that read's answer, returning a zeroed header on failure — lets a read that
// succeeds the first time and fails the second (a device on its way out, a truncated file, an
// interposer — anything between the two reads) produce page_end_lsn == 0, which is <= every
// checkpoint id, so the branch UNLINKS a segment whose records sit ABOVE the checkpoint.
//
// The injection below allows the first read of the last data page and refuses the second —
// the exact window between the two calls.

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
            std::filesystem::temp_directory_path() / ("test_wal_truncate_header_race_" + std::to_string(::getpid()));
        return p;
    }

    // Fails the Nth and every later positional read that starts at one exact offset; all
    // other I/O delegates untouched. Counting is what fault_plan_t cannot express — its
    // fail_reads_at_location refuses EVERY read at the offset, which would fail the checksum
    // pass too and never open the window this file is about.
    class nth_read_at_offset_fails_t final : public core::filesystem::file_handle_t {
    public:
        nth_read_at_offset_fails_t(std::unique_ptr<core::filesystem::file_handle_t> inner,
                                   uint64_t target_offset,
                                   uint64_t fail_from_nth,
                                   uint64_t& reads_at_offset)
            : core::filesystem::file_handle_t(inner->fs_, inner->path())
            , inner_(std::move(inner))
            , target_offset_(target_offset)
            , fail_from_nth_(fail_from_nth)
            , reads_at_offset_(reads_at_offset) {}

        bool read(void* buffer, uint64_t nr_bytes, uint64_t location) override {
            if (location == target_offset_) {
                ++reads_at_offset_;
                if (reads_at_offset_ >= fail_from_nth_) {
                    return false;
                }
            }
            return inner_->read(buffer, nr_bytes, location);
        }

        int64_t read(void* buffer, uint64_t nr_bytes) override { return inner_->read(buffer, nr_bytes); }
        core::filesystem::write_result_t write(void* buffer, uint64_t nr_bytes) override {
            return inner_->write(buffer, nr_bytes);
        }
        bool write(void* buffer, uint64_t nr_bytes, uint64_t location) override {
            return inner_->write(buffer, nr_bytes, location);
        }
        bool seek(uint64_t location) override { return inner_->seek(location); }
        uint64_t seek_position() override { return inner_->seek_position(); }
        bool sync() override { return inner_->sync(); }
        bool truncate(int64_t new_size) override { return inner_->truncate(new_size); }
        bool trim(uint64_t offset_bytes, uint64_t length_bytes) override {
            return inner_->trim(offset_bytes, length_bytes);
        }
        uint64_t file_size() override { return inner_->file_size(); }
        void close() override { inner_->close(); }

    private:
        std::unique_ptr<core::filesystem::file_handle_t> inner_;
        uint64_t target_offset_;
        uint64_t fail_from_nth_;
        uint64_t& reads_at_offset_;
    };

    class second_read_fault_scope_t final : public wal_file_interposer_t {
    public:
        second_read_fault_scope_t() { dev_set_wal_file_interposer(this); }
        ~second_read_fault_scope_t() override { dev_set_wal_file_interposer(nullptr); }

        second_read_fault_scope_t(const second_read_fault_scope_t&) = delete;
        second_read_fault_scope_t& operator=(const second_read_fault_scope_t&) = delete;

        std::string marker;              // segment files matching this get the wrapper
        uint64_t target_offset{0};       // the last data page's byte offset
        uint64_t fail_from_nth{2};       // let the checksum pass through, refuse the re-read
        uint64_t reads_at_offset{0};     // diagnostics: how many reads hit the offset

        std::unique_ptr<core::filesystem::file_handle_t>
        wrap(const std::filesystem::path& path, std::unique_ptr<core::filesystem::file_handle_t> inner) override {
            if (inner != nullptr && !marker.empty() && path.string().find(marker) != std::string::npos) {
                return std::make_unique<nth_read_at_offset_fails_t>(std::move(inner),
                                                                    target_offset,
                                                                    fail_from_nth,
                                                                    reads_at_offset);
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

        // Built on the fixture's OWN arena, never the process-global new_delete_resource
        // singleton: this is real load, and off resource_ it never reaches
        // core::pmr::otterbrix_resource -- which under ASAN IS resource_tracer_t, the only thing
        // that would report a chunk still alive after the manager is gone. Production hands the manager
        // chunks off the calling actor's own arena (agent_disk_t::storage_append_inner builds them on
        // resource()); this is that shape. resource_ outlives the asynchronous processing three times
        // over: ~wal_env_t stops the scheduler and resets manager_
        // (destroying the mailbox and any message still holding this batch) inside its own body,
        // resource_ is declared FIRST so it is destroyed LAST, and otterbrix_resource is
        // thread-safe in both builds. Extracted so a test can assert the ARENA of a REAL payload:
        // the batch is moved into the message and is unobservable after send.
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

        std::filesystem::path db_dir() const { return config_.path / std::to_string(static_cast<unsigned>(kMainDb)); }

        std::filesystem::path path_;
        core::pmr::otterbrix_resource resource_;
        log_t log_;
        actor_zeta::scheduler_ptr scheduler_;
        configuration::config_wal config_;
        std::unique_ptr<manager_wal_replicate_t, actor_zeta::pmr::deleter_t> manager_;
    };

} // namespace

// ===========================================================================
// A HEADER RE-READ THAT FAILS BETWEEN VERIFY AND DECIDE MUST NOT UNLINK THE SEGMENT.
//
// checkpoint id 1 sits BELOW every id the closed segment holds, so the only correct outcomes
// are "read the verified header and keep the file" or "could not read, skip the file". A
// zeroed header instead answers page_end_lsn == 0 <= 1 and the file is destroyed.
//
// BEFORE: segment 000000 was unlinked with every record above the checkpoint in it.
// ===========================================================================
TEST_CASE("wal::truncate::a_failed_header_reread_does_not_unlink_a_live_segment") {
    second_read_fault_scope_t fault;

    // Small segments force a rotation so segment 000000 is closed (a truncation candidate).
    wal_env_t env(base_path() / "header_race", /*max_segment_size=*/8192);

    for (uint64_t i = 0; i < 12; ++i) {
        auto fut = env.send_insert(/*txn_id=*/100 + i, /*rows=*/40, i * 40);
        auto result = await_ready(fut);
        REQUIRE_FALSE(result.has_error());
    }
    {
        auto fut = env.send_commit(/*txn_id=*/112);
        auto result = await_ready(fut);
        REQUIRE_FALSE(result.has_error());
    }

    const auto first_segment = env.db_dir() / segment_name(0);
    REQUIRE(std::filesystem::exists(first_segment));
    REQUIRE(std::filesystem::exists(env.db_dir() / segment_name(1)));

    // The last data page of segment 000000: page indices count the file header page at 0.
    const auto seg_size = std::filesystem::file_size(first_segment);
    REQUIRE(seg_size > PAGE_SIZE);
    const uint64_t last_data_page = (seg_size / PAGE_SIZE) - 1;
    fault.target_offset = last_data_page * PAGE_SIZE;
    fault.fail_from_nth = 2; // the checksum pass reads it once; the re-read is refused
    fault.marker = segment_name(0);

    auto truncate_fut = env.send_truncate_before(/*checkpoint_wal_id=*/1);
    auto truncate_error = await_ready(truncate_fut);
    fault.marker.clear();

    INFO("reads that hit the last data page of segment 000000: " << fault.reads_at_offset);
    INFO("a header whose re-read failed is not a header of zeros; the segment holds ids above "
         "checkpoint 1 and must survive");
    REQUIRE(std::filesystem::exists(first_segment));
    REQUIRE_FALSE(truncate_error.contains_error());
}

// ===========================================================================
// THE INSERT PAYLOAD MUST BE BUILT ON THE FIXTURE'S OWN ARENA -- see the note on
// make_insert_batch above. The batch is moved into the message and is unobservable after
// send, so the assertion is made on the object make_insert_batch produces: the same call, on
// the same path, that send_insert makes -- not a value handed in by the test.
// ===========================================================================
TEST_CASE("wal::truncate::the_insert_payload_is_built_on_the_fixture_arena") {
    const auto path = base_path() / "payload_arena";
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    wal_env_t env(path);

    auto batch = env.make_insert_batch(4);
    REQUIRE(batch.size() == 1);
    REQUIRE(batch.get_allocator().resource() == &env.resource_);
    REQUIRE(batch.front().resource() == &env.resource_);
}
