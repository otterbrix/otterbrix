// clang-format off
// <actor-zeta/spawn.hpp> requires std::unique_ptr, but does not include it itself
#include <memory>
#include <memory_resource>
#include <actor-zeta/spawn.hpp>
// clang-format on

#include <catch2/catch_test_macros.hpp>

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

// FIN-2 — A CRC BREAK MUST NOT MAKE THE ALLOCATOR FORGET WHAT IS ON THE DISK.
//
// recover_from_disk() logged "truncating at corruption point" and then `break`ed out of the
// segment loop WITHOUT TRUNCATING ANYTHING. Two different answers were being taken from one
// scan:
//
//   - WHAT REPLAY MAY APPLY. That legitimately stops at the first CRC break (STOP-A): the
//     prefix before the break is complete, and replaying past a break would apply a range
//     with a HOLE in it. wal_reader_t is where that decision belongs and it is unchanged.
//   - WHERE THE ID ALLOCATOR MUST RESUME. That is a high-water mark over what the FILES
//     physically hold, and it has nothing to do with how far replay got. Taking it from the
//     replay scan put it BELOW ids that are still on disk, so the next write reissued them.
//
// The tests below assert on the ids the engine actually hands out and on the ids the segment
// files actually contain — never on a status.
//
// The corruption is done by the filesystem (one flipped byte inside a data page), which is
// what a bad sector does; dev_set_wal_file_interposer is the seam for failures of the OPEN
// and of the WRITE, and neither of those is the input here.

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

    // config_wal appends "wal" to the base path, so the segments live under <path>/wal/<db>.
    std::filesystem::path db_dir_of(const std::filesystem::path& base) {
        return base / "wal" / std::to_string(static_cast<unsigned>(kMainDb));
    }

    // A RESTART, not a fresh database: unlike the FIN-1 harness this one never wipes the
    // directory, because every test here is about what a SECOND open makes of what the first
    // one left behind.
    configuration::config_wal reopen_config(const std::filesystem::path& path) {
        std::filesystem::create_directories(path);
        configuration::config_wal config(path);
        config.on = true;
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
            manager_ = actor_zeta::spawn<manager_wal_replicate_t>(&resource_, scheduler_.get(), config_, log_);
            manager_->sync(wal_sync_pack_t{actor_zeta::address_t::empty_address(),
                                           actor_zeta::address_t::empty_address(),
                                           actor_zeta::address_t::empty_address()});
            scheduler_->start();
        }

        ~wal_env_t() {
            scheduler_->stop();
            manager_.reset();
        }

        auto send_insert(uint64_t txn_id, size_t rows, uint64_t row_start) {
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

        // One committed transaction. commit_txn under NORMAL flushes the page, so each call
        // closes the page it wrote into — which is what makes "corrupt an INTERIOR page and
        // leave live pages after it" constructible at all.
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

    // THE ANSWER THE FILES GIVE. Every data page whose checksum still verifies vouches for
    // its own page_end_lsn, and that includes pages sitting AFTER a corruption point — the
    // region read_all_records (STOP-A) stops short of. This helper deliberately uses only
    // wal_page_reader_t's long-standing public accessors, so it is an independent witness
    // rather than a mirror of the code under test.
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

    // The answer the STOP-A reader gives for ONE segment: ids reachable without crossing a
    // break inside it.
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

    // Flip one byte in the DATA AREA of a data page. Data page N starts at file offset
    // N * PAGE_SIZE (page 0 is the file header), so this breaks that page's checksum and
    // leaves every other page — including the ones after it — intact and verifiable.
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

    // Overwrite the page_end_lsn field (offset 8) of a data page header with `value`. This is
    // the same class of damage as break_page_crc — the page's checksum stops verifying — but
    // it is aimed at the ONE field truncate_before used to act on without checking it.
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

// ===========================================================================
// FIN-2 / item 1 — THE FIRST ID AFTER A RESTART MUST NOT BE ONE THE JOURNAL ALREADY HOLDS.
//
// One segment, an interior data page corrupted, live pages after it. The manager's startup
// scan derived global_id_ from read_all_records(), which stops at the break, so it came up
// with a maximum that ignored every page beyond it.
//
// BEFORE: on_disk_max was 24 and the very next write was issued id 9 — an id that four other
// still-verifiable pages already carry. Restarting again issued 9 a SECOND time, to a third
// record, because nothing the previous run wrote was visible to the scan either.
// ===========================================================================
TEST_CASE("wal::reissue::the_first_id_after_a_crc_break_is_not_one_the_journal_already_holds") {
    const auto path = base_path() / "reissue_interior_page";
    std::filesystem::remove_all(path);
    const auto db_dir = db_dir_of(path);
    core::pmr::otterbrix_resource witness;

    // --- 1. A journal of one-transaction-per-page, so an interior page can be picked. ---
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

    // --- 2. A bad sector in the middle of the segment. ---
    break_page_crc(segment, pages / 2);

    // THE PRECONDITION, checked rather than assumed: the break must leave live pages behind
    // it, otherwise this is the ordinary torn-tail case and proves nothing.
    const auto after_break_on_disk = on_disk_max_wal_id(&witness, db_dir);
    const auto after_break_readable = max_of(readable_ids(&witness, db_dir));
    INFO("on disk: " << after_break_on_disk << " , reachable past the break: " << after_break_readable);
    REQUIRE(after_break_readable > 0);            // a prefix survives
    REQUIRE(after_break_on_disk > after_break_readable); // and live pages sit beyond the break

    // --- 3. Restart and write. ---
    wal::id_t first_id_after_restart = 0;
    {
        wal_env_t env(path);
        first_id_after_restart = env.commit_one(/*txn_id=*/100, /*row_start=*/0);
    }

    INFO("the allocator resumed at " << first_id_after_restart << " while the files hold up to "
                                     << after_break_on_disk);
    REQUIRE(first_id_after_restart > after_break_on_disk);

    // --- 4. And it must not keep resuming from the same place on every later restart. ---
    wal::id_t first_id_after_second_restart = 0;
    {
        wal_env_t env(path);
        first_id_after_second_restart = env.commit_one(/*txn_id=*/200, /*row_start=*/0);
    }

    INFO("two restarts issued " << first_id_after_restart << " and " << first_id_after_second_restart);
    REQUIRE(first_id_after_second_restart > first_id_after_restart);
}

// ===========================================================================
// FIN-2 / item 2 — A RECORD THE JOURNAL ACCEPTED MUST BE READABLE FROM IT.
//
// The same `break` left current_segment_index_ at the CORRUPTED segment, so ensure_writer()
// reopened it and appended after its last page — behind the corruption point. Every reader in
// the tree stops at that point, so the record was written, reported durable, and unreadable.
//
// BEFORE: the id returned by the write was in no segment read_all_records could reach.
// ===========================================================================
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

// ===========================================================================
// FIN-2 / item 3 — current_wal_id MUST NOT UNDERSTATE THE JOURNAL BECAUSE OF AN EARLY BREAK.
//
// This is the literal shape named in recover_from_disk(): discover_segments() sorts ascending
// and the loop `break`s, so a break in segment 000000 meant segments 000001+ were never
// looked at. current_wal_id (the max of the workers' id_) is what operator_checkpoint pins
// the checkpoint boundary to and what operator_create_index_backfill starts a backfill from.
//
// BEFORE: three segments on disk carrying ids up to 24, and current_wal_id answered 4.
// ===========================================================================
TEST_CASE("wal::reissue::current_wal_id_counts_the_segments_after_a_broken_one") {
    const auto path = base_path() / "later_segments_ignored";
    std::filesystem::remove_all(path);
    const auto db_dir = db_dir_of(path);
    core::pmr::otterbrix_resource witness;

    // header page + 3 data pages per segment, one transaction per page.
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

    // Break an interior page of the EARLIEST segment; the later ones stay perfect.
    break_page_crc(first_segment, 1);

    // THE PRECONDITION: recover_from_disk walks the segments in ascending order and stopped
    // at the first break, so everything it could ever see is segment 000000's prefix. The
    // later segments hold strictly more.
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

// ===========================================================================
// FIN-2 / item 4 — TRUNCATION MUST NOT DELETE A SEGMENT ON THE STRENGTH OF A HEADER FIELD
// THE CHECKSUM NEVER VOUCHED FOR.
//
// truncate_before read page_end_lsn straight out of the LAST data page's header and unlinked
// the file when it came out <= the checkpoint. That field is inside the region the page CRC
// covers, and a corrupt page's header is exactly what the CRC failed to vouch for: forge it
// low and the branch deletes a segment full of records ABOVE the checkpoint.
//
// This is the same family as the refusal already in this function for a segment that will not
// OPEN — "unreadable is not empty" — one field down.
//
// THE SETUP NEEDS TWO DAMAGED SEGMENTS AND THAT IS NOT PADDING. truncate_before never touches
// the segment the writer is using, and the recover_from_disk this file also covers used to
// park the writer on the FIRST broken segment — so with only 000001 forged, 000001 WAS the
// writer's segment and got skipped for an unrelated reason, hiding the defect. Breaking
// 000000 is what moves the writer off the segment under test. 000001 is the one under test.
//
// BEFORE: segment 000001 was unlinked and the records between the checkpoint and its real
// page_end_lsn went with it.
// ===========================================================================
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
    REQUIRE(std::filesystem::exists(db_dir / segment_name(2))); // 000001 is closed, not current
    const auto second_pages = data_page_count(&witness, second_segment);
    REQUIRE(second_pages >= 1);

    // The real highest id in segment 000001, before anything is forged.
    wal::id_t real_high = 0;
    {
        wal_page_reader_t reader(&witness, second_segment);
        REQUIRE(reader.is_open());
        real_high = reader.read_page_header(second_pages).page_end_lsn;
    }

    // Checkpoint strictly below what the segment holds: keeping it is the only correct answer.
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
