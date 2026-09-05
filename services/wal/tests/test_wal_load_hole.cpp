// clang-format off
// <actor-zeta/spawn.hpp> requires std::unique_ptr, but does not include it itself
#include <memory>
#include <memory_resource>
#include <actor-zeta/spawn.hpp>
// clang-format on

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
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

// ONE READER OF THIS JOURNAL STOPPED AT A BREAK, THE OTHER READ STRAIGHT THROUGH IT.
//
// wal_reader_t (startup replay) stops at the first CRC break and says why: a range with a
// HOLE in it is worse than a short prefix, because the rows behind the hole get their later
// updates applied over a version that was never restored. wal_worker_t::load had no such
// check at all -- it concatenated the STOP-A prefix of segment k with the WHOLE of segment
// k+1 and handed the caller a range missing everything between them.
//
// Its only production caller is the CREATE INDEX catchup (operator_create_index_backfill),
// and the question that caller asks is a THIRD question, different from both the ones the
// CRC-break wave already answered:
//
//   * replay          -- "what may I APPLY?"        -> prefix, stop at the break;
//   * the id allocator -- "where do I RESUME?"      -> high-water over the FILES;
//   * the catchup      -- "is the window (after, high_water] WHOLE?" -> yes or refuse.
//
// The third question is BINARY: a catchup that applies a subset publishes an index that
// answers with a subset, and a silently incomplete index is the exact failure the whole
// index layer was rebuilt to remove. Partial success is therefore not in load's contract.
//
// The tests below assert on the SET OF IDS the catchup is handed, never on a status, and the
// corruption is done by the filesystem (one flipped byte inside a data page) because that is
// what a bad sector does.

using namespace services;
using namespace services::wal;
namespace catalog = components::catalog;

namespace {

    using session_id_t = components::session::session_id_t;
    using data_chunk_t = components::vector::data_chunk_t;

    constexpr auto kMainDb = catalog::well_known_oid::main_database;
    constexpr catalog::oid_t kTestTableOid = 16713;

    std::filesystem::path base_path() {
        static std::filesystem::path p =
            std::filesystem::temp_directory_path() / ("test_wal_load_hole_" + std::to_string(::getpid()));
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

    configuration::config_wal fresh_config(const std::filesystem::path& path) {
        std::filesystem::create_directories(path);
        configuration::config_wal config(path);
        config.on = true;
        return config;
    }

    // NO RESTART ANYWHERE IN THIS FILE. load() calls discover_segments() and opens a fresh
    // wal_page_reader_t per call, so a byte flipped in a closed segment is visible to the
    // very same live manager -- which is also the only shape the catchup can ever meet,
    // since it runs inside a live engine and never after a reopen.
    struct wal_env_t {
        explicit wal_env_t(const std::filesystem::path& path, size_t max_segment_size = 0)
            : log_(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler_(new actor_zeta::shared_work(2, 1000))
            , config_(fresh_config(path))
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

        // The catchup's own call, verbatim: manager_wal_replicate_t::load(session, after).
        auto send_load(wal::id_t after_wal_id) {
            auto [ns, fut] = actor_zeta::otterbrix::send(manager_->address(),
                                                         &manager_wal_replicate_t::load,
                                                         session_id_t::generate_uid(),
                                                         after_wal_id);
            return std::move(fut);
        }

        // One committed transaction. commit_txn under NORMAL flushes the page, so each call
        // closes the page it wrote into -- which is what makes "corrupt an INTERIOR page and
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

    // What ONE segment physically holds: every data page whose checksum still verifies
    // vouches for its own page_end_lsn, INCLUDING pages sitting past a corruption point.
    // Uses only long-standing public accessors, so it is an independent witness rather than
    // a mirror of the code under test.
    wal::id_t on_disk_max_of(std::pmr::memory_resource* res, const std::filesystem::path& seg) {
        wal::id_t max_id = 0;
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
        return max_id;
    }

    wal::id_t on_disk_max_wal_id(std::pmr::memory_resource* res, const std::filesystem::path& db_dir) {
        wal::id_t max_id = 0;
        for (const auto& seg : segment_files(db_dir)) {
            max_id = std::max(max_id, on_disk_max_of(res, seg));
        }
        return max_id;
    }

    std::vector<record_t> readable_records_of(std::pmr::memory_resource* res, const std::filesystem::path& seg) {
        wal_page_reader_t reader(res, seg);
        REQUIRE(reader.is_open());
        auto records = reader.read_all_records(0);
        REQUIRE_FALSE(records.has_error());
        return std::move(records.value());
    }

    // The answer the STOP-A reader gives for ONE segment: ids reachable without crossing a
    // break inside it.
    std::vector<wal::id_t> readable_ids_of(std::pmr::memory_resource* res, const std::filesystem::path& seg) {
        std::vector<wal::id_t> ids;
        for (const auto& r : readable_records_of(res, seg)) {
            if (r.is_valid()) {
                ids.push_back(r.id);
            }
        }
        return ids;
    }

    std::vector<wal::id_t> readable_commit_ids_of(std::pmr::memory_resource* res, const std::filesystem::path& seg) {
        std::vector<wal::id_t> ids;
        for (const auto& r : readable_records_of(res, seg)) {
            if (r.is_valid() && r.is_commit_marker()) {
                ids.push_back(r.id);
            }
        }
        return ids;
    }

    std::vector<wal::id_t> ids_of(const std::vector<record_t>& records) {
        std::vector<wal::id_t> ids;
        ids.reserve(records.size());
        for (const auto& r : records) {
            ids.push_back(r.id);
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
    // leaves every other page -- including the ones after it -- intact and verifiable.
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

    size_t data_page_count(std::pmr::memory_resource* res, const std::filesystem::path& seg) {
        wal_page_reader_t reader(res, seg);
        REQUIRE(reader.is_open());
        return reader.page_count();
    }

    // header page + 3 data pages per segment, one committed transaction per page.
    constexpr size_t kSmallSegment = 4 * PAGE_SIZE;

} // namespace

// ===========================================================================
// A -- THE CATCHUP MUST NEVER BE HANDED A RANGE WITH A HOLE IN IT.
//
// Segment 000000 has an INTERIOR page corrupted and live pages behind it; segments 000001+
// are perfect. read_all_records stops at the break (STOP-A), so segment 000000 contributes
// only its prefix -- and load then appended the WHOLE of the later segments on top of it.
//
// BEFORE: load(0) answered successfully with ids running up to the end of the journal while
// every id between the break and the next segment was missing. The catchup applied that,
// found nothing left on the next iteration, called it convergence and flipped the index to
// indisvalid.
// ===========================================================================
TEST_CASE("wal::load_hole::an_interior_break_must_not_be_answered_with_the_segments_behind_it") {
    const auto path = base_path() / "interior_break";
    std::filesystem::remove_all(path);
    const auto db_dir = db_dir_of(path);
    core::pmr::otterbrix_resource witness;

    wal_env_t env(path, kSmallSegment);
    for (uint64_t t = 1; t <= 9; ++t) {
        env.commit_one(t, (t - 1) * 4);
    }

    const auto segments = segment_files(db_dir);
    REQUIRE(segments.size() >= 2);
    const auto seg0 = db_dir / segment_name(0);
    REQUIRE(std::filesystem::exists(seg0));
    REQUIRE(data_page_count(&witness, seg0) >= 3);

    // Page 2 of 3: a prefix survives IN FRONT of it and live pages survive BEHIND it, which
    // is the only shape in which a segment can contribute a partial answer at all.
    break_page_crc(seg0, 2);

    // THE PRECONDITION, CHECKED RATHER THAN ASSUMED. Without live pages past the break this
    // is the ordinary torn tail and proves nothing.
    const auto prefix_max = max_of(readable_ids_of(&witness, seg0));
    const auto seg0_on_disk = on_disk_max_of(&witness, seg0);
    const auto on_disk = on_disk_max_wal_id(&witness, db_dir);
    INFO("segment 000000 reaches " << prefix_max << " , physically holds up to " << seg0_on_disk
                                   << " , the journal holds up to " << on_disk);
    REQUIRE(prefix_max > 0);
    REQUIRE(seg0_on_disk > prefix_max); // live pages sit beyond the break
    REQUIRE(on_disk > seg0_on_disk);    // and whole segments sit beyond those

    auto fut = env.send_load(0);
    auto answer = await_ready(fut);

    const auto answered = answer.has_error() ? std::vector<wal::id_t>{} : ids_of(answer.value());
    INFO("load answered " << answered.size() << " records reaching id " << max_of(answered)
                          << " while everything past " << prefix_max << " up to " << seg0_on_disk
                          << " is unreachable");
    // Either the window is refused, or it stops where the journal stops being whole. There is
    // no third answer: an id ABOVE the break in the reply proves the ids inside the break
    // were skipped over rather than cut off.
    REQUIRE((answer.has_error() || max_of(answered) <= prefix_max));
}

// ===========================================================================
// B -- A BREAK THAT IS ENTIRELY BELOW THE WATERMARK HIDES NOTHING, AND MUST NOT REFUSE.
//
// This is the anti-(a) case: copying wal_reader_t's unconditional "chain broken -> stop
// reading segments" into load would fail it. The catchup asks about (after, high_water], and
// here `after` is the on-disk high-water of the broken segment itself, so not one id the
// break swallowed is inside the window. Refusing would ban CREATE INDEX on a database whose
// damage is old, already below every future build_start -- and after ANY restart the
// allocator resumes above the whole file, so that is the COMMON shape, not the exotic one.
// ===========================================================================
TEST_CASE("wal::load_hole::a_break_below_the_watermark_is_read_straight_through") {
    const auto path = base_path() / "break_below_watermark";
    std::filesystem::remove_all(path);
    const auto db_dir = db_dir_of(path);
    core::pmr::otterbrix_resource witness;

    wal_env_t env(path, kSmallSegment);
    for (uint64_t t = 1; t <= 9; ++t) {
        env.commit_one(t, (t - 1) * 4);
    }

    const auto segments = segment_files(db_dir);
    REQUIRE(segments.size() >= 2);
    const auto seg0 = db_dir / segment_name(0);
    REQUIRE(data_page_count(&witness, seg0) >= 2);
    break_page_crc(seg0, 1);

    // The whole damaged segment sits below the window the catchup asks about.
    const auto after = on_disk_max_of(&witness, seg0);
    REQUIRE(after > 0);

    std::vector<wal::id_t> expected;
    for (const auto& seg : segments) {
        if (seg == seg0) {
            continue;
        }
        for (auto id : readable_commit_ids_of(&witness, seg)) {
            if (id > after) {
                expected.push_back(id);
            }
        }
    }
    REQUIRE_FALSE(expected.empty());

    auto fut = env.send_load(after);
    auto answer = await_ready(fut);

    INFO("load(after=" << after << ") over a journal whose damage is entirely below it: "
                       << (answer.has_error() ? answer.error().what.c_str() : "no error"));
    REQUIRE_FALSE(answer.has_error());

    const auto answered = ids_of(answer.value());
    for (auto id : expected) {
        INFO("commit marker " << id << " lives past the watermark and must be in the answer");
        REQUIRE(contains(answered, id));
    }
}

// ===========================================================================
// C -- A TORN TAIL IS NOT A HOLE, AND MUST NOT BAN CREATE INDEX FOREVER.
//
// The break is on the LAST page of the LAST segment: nothing verifies after it, so nothing
// is hidden between two things the reply contains. An ordinary crash leaves exactly this
// shape, and a refusal here would be a failure on a path that cannot be repaired from
// inside -- LOUD IS NOT THE SAME AS FATAL.
// ===========================================================================
TEST_CASE("wal::load_hole::a_torn_tail_is_not_a_hole_and_is_not_refused") {
    const auto path = base_path() / "torn_tail";
    std::filesystem::remove_all(path);
    const auto db_dir = db_dir_of(path);
    core::pmr::otterbrix_resource witness;

    wal_env_t env(path, kSmallSegment);
    for (uint64_t t = 1; t <= 9; ++t) {
        env.commit_one(t, (t - 1) * 4);
    }

    const auto segments = segment_files(db_dir);
    REQUIRE(segments.size() >= 2);
    const auto last_seg = segments.back();
    const auto last_pages = data_page_count(&witness, last_seg);
    REQUIRE(last_pages >= 1);
    break_page_crc(last_seg, last_pages); // the tail, with nothing behind it

    auto fut = env.send_load(0);
    auto answer = await_ready(fut);

    INFO("a torn tail must still answer: " << (answer.has_error() ? answer.error().what.c_str() : "no error"));
    REQUIRE_FALSE(answer.has_error());
    REQUIRE_FALSE(answer.value().empty());
}

// ===========================================================================
// D -- THE HOLE CAN OPEN AT A SEGMENT BOUNDARY, WHERE NOTHING INSIDE THE SEGMENT SHOWS IT.
//
// The break is on the LAST page of segment 000000, so within that segment it looks exactly
// like case C: nothing verifies after it. What makes it a hole is the segment that FOLLOWS
// -- load reads it in full, so the reply jumps straight over the ids the broken page held.
// A per-segment test cannot see this; only carrying the open hole into the next segment can.
// ===========================================================================
TEST_CASE("wal::load_hole::a_break_at_a_segment_boundary_is_still_a_hole") {
    const auto path = base_path() / "boundary_break";
    std::filesystem::remove_all(path);
    const auto db_dir = db_dir_of(path);
    core::pmr::otterbrix_resource witness;

    wal_env_t env(path, kSmallSegment);
    for (uint64_t t = 1; t <= 9; ++t) {
        env.commit_one(t, (t - 1) * 4);
    }

    const auto segments = segment_files(db_dir);
    REQUIRE(segments.size() >= 2);
    const auto seg0 = db_dir / segment_name(0);
    const auto seg0_pages = data_page_count(&witness, seg0);
    REQUIRE(seg0_pages >= 1);
    break_page_crc(seg0, seg0_pages); // last page of a segment that is NOT the last segment

    const auto prefix_max = max_of(readable_ids_of(&witness, seg0));
    const auto seg0_on_disk = on_disk_max_of(&witness, seg0);
    INFO("segment 000000 reaches " << prefix_max << " and physically held up to " << seg0_on_disk);
    REQUIRE(seg0_on_disk >= prefix_max);

    auto fut = env.send_load(0);
    auto answer = await_ready(fut);

    const auto answered = answer.has_error() ? std::vector<wal::id_t>{} : ids_of(answer.value());
    INFO("load answered up to " << max_of(answered) << " with the ids after " << prefix_max
                                << " on the last page of segment 000000 skipped");
    REQUIRE((answer.has_error() || max_of(answered) <= prefix_max));
}
