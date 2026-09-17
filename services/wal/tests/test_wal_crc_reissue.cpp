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
#include <vector>
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
#include <services/wal/wal_page.hpp>
#include <services/wal/wal_page_reader.hpp>
#include <services/wal/wal_reader.hpp>

// A CRC break must not make the allocator forget what is on disk: recover_from_disk() took the id
// allocator's resume point from the same replay scan, so it resumed below ids still on disk and reissued them.

using namespace services;
using namespace services::wal;
namespace catalog = components::catalog;

namespace {

    using session_id_t = components::session::session_id_t;
    using data_chunk_t = components::vector::data_chunk_t;

    constexpr auto kMainDb = catalog::well_known_oid::main_database;
    constexpr catalog::oid_t kTestTableOid = 16711;

    std::filesystem::path base_path() {
        static std::filesystem::path p =
            std::filesystem::temp_directory_path() / ("test_wal_crc_reissue_" + std::to_string(::getpid()));
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

    std::filesystem::path db_dir_of(const std::filesystem::path& base) {
        return base / "wal" / std::to_string(static_cast<unsigned>(kMainDb));
    }

    configuration::config_wal reopen_config(const std::filesystem::path& path) {
        std::filesystem::create_directories(path);
        configuration::config_wal config(path);
        return config;
    }

    struct wal_env_t {
        explicit wal_env_t(const std::filesystem::path& path, size_t max_segment_size = 0)
            : log_(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler_(new actor_zeta::shared_work(2, 1000))
            , config_(reopen_config(path))
            , manager_(nullptr, actor_zeta::pmr::deleter_t(&resource_)) {
            if (max_segment_size != 0) {
                config_.max_segment_size = max_segment_size;
            }
            manager_ = actor_zeta::spawn<manager_wal_replicate_t>(&resource_,
                                                                  scheduler_.get(),
                                                                  config_,
                                                                  log_,
                                                                  components::pipeline::no_mailbox(),
                                                                  components::pipeline::no_mailbox());
            scheduler_->start();
        }

        ~wal_env_t() {
            scheduler_->stop();
            manager_.reset();
        }

        std::pmr::vector<data_chunk_t> make_insert_batch(size_t rows) { return one_chunk(&resource_, rows); }

        auto send_insert(uint64_t txn_id, size_t rows, uint64_t row_start) {
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

        wal::id_t commit_one(uint64_t txn_id, uint64_t row_start) {
            auto ins = send_insert(txn_id, 4, row_start);
            auto ins_result = await_ready(ins);
            REQUIRE_FALSE(ins_result.has_error());
            auto cm = send_commit(txn_id);
            auto cm_result = await_ready(cm);
            REQUIRE_FALSE(cm_result.has_error());
            return cm_result.value();
        }

        core::pmr::otterbrix_resource resource_;
        log_t log_;
        actor_zeta::scheduler_ptr scheduler_;
        configuration::config_wal config_;
        std::unique_ptr<manager_wal_replicate_t, actor_zeta::pmr::deleter_t> manager_;
    };

    std::vector<std::filesystem::path> segment_files(const std::filesystem::path& db_dir) {
        std::vector<std::filesystem::path> result;
        for (const auto& entry : std::filesystem::directory_iterator(db_dir)) {
            if (!entry.is_regular_file()) {
                continue;
            }
            const auto name = entry.path().filename().string();
            if (name.size() >= 4 && name.compare(0, 4, "wal_") == 0) {
                result.push_back(entry.path());
            }
        }
        std::sort(result.begin(), result.end());
        return result;
    }

    wal::id_t on_disk_max_wal_id(std::pmr::memory_resource* res, const std::filesystem::path& db_dir) {
        wal::id_t max_id = 0;
        for (const auto& seg : segment_files(db_dir)) {
            wal_page_reader_t reader(res, seg);
            REQUIRE(reader.is_open());
            for (size_t p = 1; p <= reader.page_count(); ++p) {
                if (!reader.verify_page_checksum(p)) {
                    continue;
                }
                const auto hdr = reader.read_page_header(p);
                if (hdr.page_end_lsn > max_id) {
                    max_id = hdr.page_end_lsn;
                }
            }
        }
        return max_id;
    }

    std::vector<wal::id_t> readable_ids_of(std::pmr::memory_resource* res, const std::filesystem::path& seg) {
        std::vector<wal::id_t> ids;
        wal_page_reader_t reader(res, seg);
        REQUIRE(reader.is_open());
        auto records = reader.read_all_records(0);
        REQUIRE_FALSE(records.has_error());
        for (const auto& r : records.value()) {
            if (r.is_valid()) {
                ids.push_back(r.id);
            }
        }
        return ids;
    }

    std::vector<wal::id_t> readable_ids(std::pmr::memory_resource* res, const std::filesystem::path& db_dir) {
        std::vector<wal::id_t> ids;
        for (const auto& seg : segment_files(db_dir)) {
            for (auto id : readable_ids_of(res, seg)) {
                ids.push_back(id);
            }
        }
        return ids;
    }

    bool contains(const std::vector<wal::id_t>& ids, wal::id_t id) {
        return std::find(ids.begin(), ids.end(), id) != ids.end();
    }

    wal::id_t max_of(const std::vector<wal::id_t>& ids) {
        wal::id_t m = 0;
        for (auto id : ids) {
            if (id > m) {
                m = id;
            }
        }
        return m;
    }

    void break_page_crc(const std::filesystem::path& seg, size_t data_page_index) {
        std::fstream file(seg, std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(file.is_open());
        const auto offset = static_cast<std::streamoff>(data_page_index * PAGE_SIZE + PAGE_HEADER_SIZE + 7);
        file.seekg(offset);
        char byte = 0;
        file.read(&byte, 1);
        REQUIRE(file.good());
        byte = static_cast<char>(byte ^ 0x5a);
        file.seekp(offset);
        file.write(&byte, 1);
        file.flush();
        REQUIRE(file.good());
    }

    void forge_page_end_lsn(const std::filesystem::path& seg, size_t data_page_index, uint64_t value) {
        std::fstream file(seg, std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(file.is_open());
        const auto offset = static_cast<std::streamoff>(data_page_index * PAGE_SIZE + 8);
        file.seekp(offset);
        file.write(reinterpret_cast<const char*>(&value), sizeof(value));
        file.flush();
        REQUIRE(file.good());
    }

    size_t data_page_count(std::pmr::memory_resource* res, const std::filesystem::path& seg) {
        wal_page_reader_t reader(res, seg);
        REQUIRE(reader.is_open());
        return reader.page_count();
    }

} // namespace

// The first id after a restart must not repeat one already in the journal.
TEST_CASE("wal::reissue::the_first_id_after_a_crc_break_is_not_one_the_journal_already_holds") {
    const auto path = base_path() / "reissue_interior_page";
    std::filesystem::remove_all(path);
    const auto db_dir = db_dir_of(path);
    core::pmr::otterbrix_resource witness;

    {
        wal_env_t env(path);
        for (uint64_t t = 1; t <= 12; ++t) {
            env.commit_one(t, (t - 1) * 4);
        }
    }

    const auto segments = segment_files(db_dir);
    REQUIRE(segments.size() == 1);
    const auto& segment = segments.front();
    const auto pages = data_page_count(&witness, segment);
    REQUIRE(pages >= 5);

    const auto intact_max = on_disk_max_wal_id(&witness, db_dir);
    REQUIRE(intact_max > 0);

    break_page_crc(segment, pages / 2);

    const auto after_break_on_disk = on_disk_max_wal_id(&witness, db_dir);
    const auto after_break_readable = max_of(readable_ids(&witness, db_dir));
    INFO("on disk: " << after_break_on_disk << " , reachable past the break: " << after_break_readable);
    REQUIRE(after_break_readable > 0);
    REQUIRE(after_break_on_disk > after_break_readable);

    wal::id_t first_id_after_restart = 0;
    {
        wal_env_t env(path);
        first_id_after_restart = env.commit_one(/*txn_id=*/100, /*row_start=*/0);
    }

    INFO("the allocator resumed at " << first_id_after_restart << " while the files hold up to "
                                     << after_break_on_disk);
    REQUIRE(first_id_after_restart > after_break_on_disk);

    wal::id_t first_id_after_second_restart = 0;
    {
        wal_env_t env(path);
        first_id_after_second_restart = env.commit_one(/*txn_id=*/200, /*row_start=*/0);
    }

    INFO("two restarts issued " << first_id_after_restart << " and " << first_id_after_second_restart);
    REQUIRE(first_id_after_second_restart > first_id_after_restart);
}

// A record the journal accepted must be readable from it: the same break left current_segment_index_
// at the corrupted segment, so ensure_writer() appended behind the corruption point, unreachable.
TEST_CASE("wal::reissue::a_record_written_after_a_crc_break_is_reachable_in_the_journal") {
    const auto path = base_path() / "write_behind_break";
    std::filesystem::remove_all(path);
    const auto db_dir = db_dir_of(path);
    core::pmr::otterbrix_resource witness;

    {
        wal_env_t env(path);
        for (uint64_t t = 1; t <= 12; ++t) {
            env.commit_one(t, (t - 1) * 4);
        }
    }

    const auto segments = segment_files(db_dir);
    REQUIRE(segments.size() == 1);
    const auto pages = data_page_count(&witness, segments.front());
    REQUIRE(pages >= 5);
    break_page_crc(segments.front(), pages / 2);

    wal::id_t written = 0;
    {
        wal_env_t env(path);
        written = env.commit_one(/*txn_id=*/300, /*row_start=*/0);
    }
    REQUIRE(written > 0);

    const auto reachable = readable_ids(&witness, db_dir);
    INFO("the journal answered with id " << written << " for a record no reader can reach");
    REQUIRE(contains(reachable, written));
}

TEST_CASE("wal::reissue::current_wal_id_counts_the_segments_after_a_broken_one") {
    const auto path = base_path() / "later_segments_ignored";
    std::filesystem::remove_all(path);
    const auto db_dir = db_dir_of(path);
    core::pmr::otterbrix_resource witness;

    {
        wal_env_t env(path, /*max_segment_size=*/4 * PAGE_SIZE);
        for (uint64_t t = 1; t <= 9; ++t) {
            env.commit_one(t, (t - 1) * 4);
        }
    }

    const auto segments = segment_files(db_dir);
    REQUIRE(segments.size() >= 3);
    const auto first_segment = db_dir / segment_name(0);
    REQUIRE(std::filesystem::exists(first_segment));
    const auto first_pages = data_page_count(&witness, first_segment);
    REQUIRE(first_pages >= 2);

    break_page_crc(first_segment, 1);

    const auto on_disk = on_disk_max_wal_id(&witness, db_dir);
    const auto reachable_in_first = max_of(readable_ids_of(&witness, first_segment));
    INFO("segment 000000 stops at " << reachable_in_first << " , the files still hold up to " << on_disk);
    REQUIRE(on_disk > reachable_in_first);

    wal::id_t reported = 0;
    {
        wal_env_t env(path);
        auto fut = env.send_current_wal_id();
        reported = await_ready(fut);
    }

    INFO("current_wal_id answered " << reported << " over a journal holding " << on_disk);
    REQUIRE(reported >= on_disk);
}

// Truncation must not delete a segment on a header field the checksum never vouched for: forging
// page_end_lsn low makes unlinking on <= checkpoint delete a segment full of records above it.
TEST_CASE("wal::reissue::truncation_keeps_a_segment_whose_last_header_is_corrupt") {
    const auto path = base_path() / "truncate_forged_header";
    std::filesystem::remove_all(path);
    const auto db_dir = db_dir_of(path);
    core::pmr::otterbrix_resource witness;

    {
        wal_env_t env(path, /*max_segment_size=*/4 * PAGE_SIZE);
        for (uint64_t t = 1; t <= 9; ++t) {
            env.commit_one(t, (t - 1) * 4);
        }
    }

    const auto first_segment = db_dir / segment_name(0);
    const auto second_segment = db_dir / segment_name(1);
    REQUIRE(std::filesystem::exists(first_segment));
    REQUIRE(std::filesystem::exists(second_segment));
    REQUIRE(std::filesystem::exists(db_dir / segment_name(2)));
    const auto second_pages = data_page_count(&witness, second_segment);
    REQUIRE(second_pages >= 1);

    wal::id_t real_high = 0;
    {
        wal_page_reader_t reader(&witness, second_segment);
        REQUIRE(reader.is_open());
        real_high = reader.read_page_header(second_pages).page_end_lsn;
    }

    const wal::id_t checkpoint_id = 1;
    REQUIRE(real_high > checkpoint_id);

    break_page_crc(first_segment, 1);
    forge_page_end_lsn(second_segment, second_pages, /*value=*/0);

    {
        wal_env_t env(path, /*max_segment_size=*/4 * PAGE_SIZE);
        auto fut = env.send_truncate_before(checkpoint_id);
        auto truncate_error = await_ready(fut);
        REQUIRE_FALSE(truncate_error.contains_error());
    }

    INFO("a forged page_end_lsn must not be enough to unlink a segment holding ids up to " << real_high);
    REQUIRE(std::filesystem::exists(second_segment));
}

TEST_CASE("wal::reissue::the_insert_payload_is_built_on_the_fixture_arena") {
    const auto path = base_path() / "payload_arena";
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    wal_env_t env(path);

    auto batch = env.make_insert_batch(4);
    REQUIRE(batch.size() == 1);
    REQUIRE(batch.get_allocator().resource() == &env.resource_);
    REQUIRE(batch.front().resource() == &env.resource_);
}
