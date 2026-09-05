#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/table/test/fault_injection_file.hpp>
#include <services/wal/wal_page.hpp>

#include <filesystem>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>

// FIN-1 — THE STATEMENT ABOVE THE JOURNAL MUST SEE A REFUSED WRITE.
//
// The unit-level proofs of the four defects live in services/wal/tests/test_wal_write_refusal.cpp.
// This file covers the two that only a real statement can show:
//
//   - a page write the device refuses must FAIL THE STATEMENT rather than let it report rows
//     inserted over a journal record that does not exist. Every wal_worker_t write handler
//     used to drop wal_page_writer_t::append's answer and return the wal_id regardless;
//   - a segment that WILL NOT OPEN must stop startup rather than let the engine come up
//     missing every committed transaction the segment held. read_all_records answered an
//     empty vector for that case, indistinguishable from "there is nothing to replay".
//
// THE INJECTION. WAL segments are opened by the WAL itself through core::filesystem::open_file,
// so the .otbx seam (single_file_block_manager_t::dev_set_file_interposer) never saw them; the
// WAL now carries its own DEV_MODE seam. Returning nullptr from wrap() is what an unopenable
// file looks like to the caller — open_file's own failure answer IS nullptr — and the write
// failures ride the same otterbrix_test::fault_plan_t the .otbx tests use.

namespace {

    // Process-wide seam, so it is scoped by this object and narrowed to WAL segment files by
    // path. Both knobs are live data: a test arms them AFTER the setup traffic it needs to
    // succeed, which is why the plan starts switched off.
    class wal_fault_scope_t final : public services::wal::wal_file_interposer_t {
    public:
        wal_fault_scope_t() { services::wal::dev_set_wal_file_interposer(this); }
        ~wal_fault_scope_t() override { services::wal::dev_set_wal_file_interposer(nullptr); }

        wal_fault_scope_t(const wal_fault_scope_t&) = delete;
        wal_fault_scope_t& operator=(const wal_fault_scope_t&) = delete;

        std::string refuse_open_marker; // these segment files do not open at all
        std::string faulty_marker;      // these get the T3 faulty handle driven by `plan`
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

    // A single INSERT wide enough that its WAL record cannot fit in one 4 KiB page, so
    // wal_page_writer_t::append has to flush a full page mid-record — the write whose answer
    // used to be discarded. A statement that only buffers would not exercise it.
    std::string wide_insert_sql(int rows) {
        std::ostringstream sql;
        sql << "INSERT INTO refusal.t (id, payload) VALUES ";
        for (int i = 0; i < rows; ++i) {
            if (i != 0) {
                sql << ", ";
            }
            sql << "(" << i << ", '" << std::string(48, 'x') << "')";
        }
        sql << ";";
        return sql.str();
    }

} // namespace

// ===========================================================================
// FIN-1 / item 1 — AN INSERT WHOSE JOURNAL RECORD WAS REFUSED MUST FAIL.
//
// BEFORE: write_physical_insert answered with the freshly allocated wal_id, storage_append
// materialized the rows on the strength of it, and the statement reported them inserted with
// nothing in the journal to replay them from.
// ===========================================================================
TEST_CASE("integration::cpp::test_wal_write_refusal::insert_fails_when_the_wal_page_write_is_refused") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_wal_write_refusal/insert"),
                                                 /*wal_on=*/true);
    config.log.level = log_t::level::off;

    wal_fault_scope_t fault;
    fault.faulty_marker = "wal_"; // WAL segment files only; the .otbx files stay untouched

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE refusal;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE refusal.t (id bigint, payload text);")->is_success());

    // Arm only now: the DDL above must reach the journal, so the refusal below can only be
    // about the INSERT.
    fault.plan.fail_writes_from = fault.plan.writes_seen + 1;
    const auto writes_before = fault.plan.writes_seen;

    auto cur = test_helpers::exec(dispatcher, wide_insert_sql(200));

    INFO("an INSERT whose WAL record the device refused must FAIL, not report rows inserted");
    REQUIRE(cur->is_error());
    // The refusal really travelled through a refused write.
    REQUIRE(fault.plan.writes_seen > writes_before);
}

// ===========================================================================
// FIN-1 / item 4 — A SEGMENT THAT WILL NOT OPEN MUST STOP STARTUP.
//
// Why startup refusal and not a first-statement refusal: the scan that reads these segments is
// also the one that recovers the wal id allocator (manager_wal_replicate_t's constructor sets
// global_id_ from it; wal_worker_t::recover_from_disk sets id_ and last_crc_). Records it could
// not see leave both BELOW ids already on disk, so the first write after startup reuses them
// and the page_lsn ordering, the CRC chain and read_all_records(after_id) all start comparing
// against duplicated ids. That is the one outcome nothing later undoes. Refusing to start
// writes nothing and deletes nothing — truncation now refuses on the same segment instead of
// unlinking it — so the segment is still there for the next attempt.
//
// BEFORE: the engine opened, and the transactions in that segment were simply not there.
// ===========================================================================
TEST_CASE("integration::cpp::test_wal_write_refusal::startup_refuses_a_wal_segment_that_will_not_open") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_wal_write_refusal/startup"),
                                                 /*wal_on=*/true);
    config.log.level = log_t::level::off;

    // First lifetime: write something the journal has to hold.
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE refusal;")->is_success());
        REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE refusal.t (id bigint);")->is_success());
        REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO refusal.t (id) VALUES (1), (2), (3);")->is_success());
    }

    // The segment file exists and is non-empty; the refusal below is about reading it, not
    // about a journal that was never written.
    bool found_segment = false;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(config.wal.path)) {
        if (entry.is_regular_file() && entry.path().filename().string().rfind("wal_", 0) == 0 &&
            entry.file_size() > 0) {
            found_segment = true;
            break;
        }
    }
    REQUIRE(found_segment);

    // Second lifetime, same directory, with every WAL segment refusing to open.
    wal_fault_scope_t fault;
    fault.refuse_open_marker = "wal_";

    bool refused = false;
    std::string reason;
    // NOTE: the config path must differ from the first lifetime's registration only in that
    // the first instance is already destroyed — base_otterbrix_t erases its path on destruction,
    // so a refusal here can only come from the WAL. The message is checked for that reason.
    try {
        test_spaces space(config);
    } catch (const std::runtime_error& e) {
        refused = true;
        reason = e.what();
    }

    INFO("a startup that cannot read a WAL segment must refuse, not come up without it");
    REQUIRE(refused);
    REQUIRE(reason.find("WAL replay") != std::string::npos);
}
