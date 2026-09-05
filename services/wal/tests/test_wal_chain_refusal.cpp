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
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

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
#include <services/wal/wal_page_reader.hpp>

// THE CHAIN AND THE BUFFER MUST DESCRIBE THE JOURNAL, NOT THE INTENTION.
//
// Three defects of one family lived on the write path:
//
//   1. last_crc_ advanced when the record was ENCODED, before anything was written. A write
//      that was then refused (an unopenable rotation target, a refused device write) left the
//      chain pointing at a record that is not in the journal, and the next successful record
//      was stamped with that phantom link.
//   2. A refused page flush in the middle of append() left the partially copied record IN the
//      buffered page (flags included). The next record was appended BEHIND those bytes, and
//      every reader parses the page from the front — the next record was swallowed into a
//      span that never completes.
//   3. The same refusal after at least one page of the spanning record had already been
//      flushed left a continuation-only page in the buffer; the next record landed on a page
//      still flagged PARTIAL_CONT and was read as continuation bytes, not as a record.
//
// The assertions below read the segment files back with wal_page_reader_t and assert on the
// DECODED records: which ids are present, and whose crc each record's last_crc32 names.

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
            std::filesystem::temp_directory_path() / ("test_wal_chain_refusal_" + std::to_string(::getpid()));
        return p;
    }

    // Same seam-scoping shape as test_wal_write_refusal.cpp: both knobs are live data, armed
    // only around the one operation that must meet the fault.
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

    // A record wide enough to span several pages: the whole batch is concatenated into ONE
    // WAL record, and each chunk stays under DEFAULT_VECTOR_CAPACITY (1024 rows).
    std::pmr::vector<data_chunk_t> wide_batch(std::pmr::memory_resource* arena, size_t chunk_count, size_t rows) {
        std::pmr::vector<data_chunk_t> chunks(arena);
        for (size_t i = 0; i < chunk_count; ++i) {
            chunks.emplace_back(gen_data_chunk(rows, arena));
        }
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

        auto send_wide_insert(uint64_t txn_id, size_t chunk_count, size_t rows) {
            auto* arena = std::pmr::new_delete_resource(); // must outlive async processing
            auto [ns, fut] = actor_zeta::otterbrix::send(manager_->address(),
                                                         &manager_wal_replicate_t::write_physical_insert,
                                                         session_id_t::generate_uid(),
                                                         kTestTableOid,
                                                         wide_batch(arena, chunk_count, rows),
                                                         uint64_t{0},
                                                         static_cast<uint64_t>(chunk_count * rows),
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

        std::filesystem::path db_dir() const {
            return config_.path / std::to_string(static_cast<unsigned>(kMainDb));
        }

        std::filesystem::path path_;
        core::pmr::otterbrix_resource resource_;
        log_t log_;
        actor_zeta::scheduler_ptr scheduler_;
        configuration::config_wal config_;
        std::unique_ptr<manager_wal_replicate_t, actor_zeta::pmr::deleter_t> manager_;
    };

    // Decode EVERY record from every segment in the directory (no committed-txn filter, no
    // after-id filter: the assertion is about the journal's bytes, not about replay policy).
    std::vector<record_t> read_journal(const std::filesystem::path& db_dir, std::pmr::memory_resource* resource) {
        std::vector<std::filesystem::path> segments;
        for (const auto& entry : std::filesystem::directory_iterator(db_dir)) {
            if (entry.is_regular_file() && entry.path().filename().string().compare(0, 4, "wal_") == 0) {
                segments.push_back(entry.path());
            }
        }
        std::sort(segments.begin(), segments.end());

        std::vector<record_t> all;
        for (const auto& seg : segments) {
            wal_page_reader_t reader(resource, seg);
            REQUIRE(reader.is_open());
            auto records = reader.read_all_records(0);
            REQUIRE_FALSE(records.has_error());
            for (auto& r : records.value()) {
                all.push_back(std::move(r));
            }
        }
        std::sort(all.begin(), all.end(), [](const record_t& a, const record_t& b) { return a.id < b.id; });
        return all;
    }

    const record_t* find_id(const std::vector<record_t>& records, wal::id_t id) {
        for (const auto& r : records) {
            if (r.id == id) {
                return &r;
            }
        }
        return nullptr;
    }

} // namespace

// ===========================================================================
// item 1 — A REFUSED WRITE MUST NOT ADVANCE THE CHAIN.
//
// The write that meets the refusal here never touches the device at all: the rotation target
// will not OPEN (the one refusal a buffered small write can actually meet). The record was
// only ENCODED — and encoding used to be the moment the chain moved.
//
// BEFORE: the first record of the new segment carried last_crc32 = crc of the refused record,
// a link into a record that is not in the journal.
// ===========================================================================
TEST_CASE("wal::chain::a_refused_write_does_not_advance_the_crc_chain") {
    wal_fault_scope_t fault;

    // max_segment_size 8192 = header page + one data page: the first flushed segment is
    // immediately full, so the NEXT write must rotate into segment 000001.
    wal_env_t env(base_path() / "refused_rotation", /*max_segment_size=*/8192);

    // id 1: insert A (buffered). id 2: commit txn 1 (flushes -> seg 000000 reaches 8192).
    {
        auto fut = env.send_insert(/*txn_id=*/1, /*rows=*/4);
        auto r = await_ready(fut);
        REQUIRE_FALSE(r.has_error());
    }
    {
        auto fut = env.send_commit(/*txn_id=*/1, wal_sync_mode::NORMAL);
        auto r = await_ready(fut);
        REQUIRE_FALSE(r.has_error());
    }
    REQUIRE(std::filesystem::file_size(env.db_dir() / segment_name(0)) >= 8192);

    // id 3: the refused record — the rotation target does not open.
    fault.refuse_open_marker = segment_name(1);
    {
        auto fut = env.send_insert(/*txn_id=*/2, /*rows=*/4);
        auto r = await_ready(fut);
        INFO("the write whose rotation target would not open must be refused");
        REQUIRE(r.has_error());
    }
    fault.refuse_open_marker.clear();

    // id 4: the first record that lands in segment 000001. id 5: its commit (flushes).
    {
        auto fut = env.send_insert(/*txn_id=*/3, /*rows=*/4);
        auto r = await_ready(fut);
        REQUIRE_FALSE(r.has_error());
    }
    {
        auto fut = env.send_commit(/*txn_id=*/3, wal_sync_mode::NORMAL);
        auto r = await_ready(fut);
        REQUIRE_FALSE(r.has_error());
    }

    core::pmr::otterbrix_resource resource;
    auto records = read_journal(env.db_dir(), &resource);

    const auto* commit1 = find_id(records, 2); // last record that actually reached the journal
    const auto* next = find_id(records, 4);    // first record written after the refusal
    REQUIRE(commit1 != nullptr);
    REQUIRE(next != nullptr);
    REQUIRE(find_id(records, 3) == nullptr); // the refused record is not in the journal

    INFO("record id 4 must chain to the last record IN the journal (id 2), not to the refused id 3");
    REQUIRE(next->last_crc32 == commit1->crc32);
}

// ===========================================================================
// item 2 — A RECORD WHOSE FIRST PAGE FLUSH WAS REFUSED MUST LEAVE THE BUFFERED PAGE AS IT WAS.
//
// The spanning record's first chunk is copied into the page that already holds record A, and
// the flush of that page is refused. The record was reported refused — so record A and every
// record written AFTER the refusal must still be readable.
//
// BEFORE: the refused record's prefix stayed in the buffered page (PARTIAL_CONT included), the
// next record landed behind it, and the reader swallowed it into a span that never completes.
// ===========================================================================
TEST_CASE("wal::chain::a_refused_first_page_flush_rolls_the_record_out_of_the_buffer") {
    wal_fault_scope_t fault;
    fault.faulty_marker = "wal_";

    wal_env_t env(base_path() / "refused_first_flush");

    // id 1: small insert A — buffered, meets no device write.
    {
        auto fut = env.send_insert(/*txn_id=*/1, /*rows=*/4);
        auto r = await_ready(fut);
        REQUIRE_FALSE(r.has_error());
    }

    // id 2: a record wider than one page. Its append must flush the page holding A — and that
    // write (the 2nd on the handle; the 1st was the segment file header) is refused.
    fault.plan.fail_writes_from = 2;
    {
        auto fut = env.send_insert(/*txn_id=*/2, /*rows=*/600);
        auto r = await_ready(fut);
        INFO("a record whose page flush was refused must be reported refused");
        REQUIRE(r.has_error());
        REQUIRE(fault.plan.writes_seen >= 2);
    }
    fault.plan.fail_writes_from = 0;

    // id 3: the record written after the refusal. id 4: commit (flushes everything).
    {
        auto fut = env.send_insert(/*txn_id=*/3, /*rows=*/4);
        auto r = await_ready(fut);
        REQUIRE_FALSE(r.has_error());
    }
    {
        auto fut = env.send_commit(/*txn_id=*/3, wal_sync_mode::NORMAL);
        auto r = await_ready(fut);
        REQUIRE_FALSE(r.has_error());
    }

    core::pmr::otterbrix_resource resource;
    auto records = read_journal(env.db_dir(), &resource);

    INFO("A (id 1) was accepted before the refusal and must be readable");
    REQUIRE(find_id(records, 1) != nullptr);
    INFO("the refused record (id 2) must not be readable");
    REQUIRE(find_id(records, 2) == nullptr);
    INFO("the record written AFTER the refusal (id 3) must be readable");
    REQUIRE(find_id(records, 3) != nullptr);
    INFO("and it must chain to A, the last record in the journal before it");
    REQUIRE(find_id(records, 3)->last_crc32 == find_id(records, 1)->crc32);
}

// ===========================================================================
// item 3 — THE SAME REFUSAL AFTER A PAGE OF THE RECORD ALREADY LANDED.
//
// The first page of the spanning record reaches the disk, the second is refused. The pages
// already flushed are ORPHAN continuation pages — the reader abandons the span when the next
// page does not continue it — but the BUFFERED page still held continuation bytes and the
// CONT flag, and the next record used to be appended into it.
//
// BEFORE: the record written after the refusal was read as continuation bytes of the refused
// record and never came back.
// ===========================================================================
TEST_CASE("wal::chain::a_refused_mid_record_flush_discards_the_continuation_buffer") {
    wal_fault_scope_t fault;
    fault.faulty_marker = "wal_";

    wal_env_t env(base_path() / "refused_mid_flush");

    // id 1: small insert A — buffered.
    {
        auto fut = env.send_insert(/*txn_id=*/1, /*rows=*/4);
        auto r = await_ready(fut);
        REQUIRE_FALSE(r.has_error());
    }

    // id 2: a record spanning at least three pages. Write 1 = file header, write 2 = the page
    // holding A + the record's first chunk (allowed), write 3 = the second chunk (refused).
    fault.plan.fail_writes_from = 3;
    {
        auto fut = env.send_wide_insert(/*txn_id=*/2, /*chunks=*/3, /*rows=*/600);
        auto r = await_ready(fut);
        INFO("a record whose mid-record page flush was refused must be reported refused");
        REQUIRE(r.has_error());
        REQUIRE(fault.plan.writes_seen >= 3);
    }
    fault.plan.fail_writes_from = 0;

    // id 3: the record written after the refusal. id 4: commit (flushes).
    {
        auto fut = env.send_insert(/*txn_id=*/3, /*rows=*/4);
        auto r = await_ready(fut);
        REQUIRE_FALSE(r.has_error());
    }
    {
        auto fut = env.send_commit(/*txn_id=*/3, wal_sync_mode::NORMAL);
        auto r = await_ready(fut);
        REQUIRE_FALSE(r.has_error());
    }

    core::pmr::otterbrix_resource resource;
    auto records = read_journal(env.db_dir(), &resource);

    INFO("A (id 1) was accepted before the refusal and must be readable");
    REQUIRE(find_id(records, 1) != nullptr);
    INFO("the refused record (id 2) must not be readable");
    REQUIRE(find_id(records, 2) == nullptr);
    INFO("the record written AFTER the refusal (id 3) must be readable");
    REQUIRE(find_id(records, 3) != nullptr);
}
