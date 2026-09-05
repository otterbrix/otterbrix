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

// Unit-level proofs of the four defects live in services/wal/tests/test_wal_write_refusal.cpp;
// this file covers the two only a real statement can show: a refused page write must fail the
// statement (wal_page_writer_t::append's answer must reach the caller), and a segment that
// won't open must stop startup. WAL segments open via core::filesystem::open_file, not the
// .otbx seam, so the WAL carries its own DEV_MODE seam; wrap() returning nullptr mimics
// open_file's own failure.

namespace {

    // Process-wide, scoped by this object's lifetime. Arm the plan AFTER setup traffic
    // succeeds — it starts switched off for that reason.
    class wal_fault_scope_t final : public services::wal::wal_file_interposer_t {
    public:
        wal_fault_scope_t() { services::wal::dev_set_wal_file_interposer(this); }
        ~wal_fault_scope_t() override { services::wal::dev_set_wal_file_interposer(nullptr); }

        wal_fault_scope_t(const wal_fault_scope_t&) = delete;
        wal_fault_scope_t& operator=(const wal_fault_scope_t&) = delete;

        std::string refuse_open_marker; // these segment files do not open at all
        std::string faulty_marker;      // these get the faulty handle driven by `plan`
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

    // Wide enough that the WAL record can't fit one 4 KiB page, forcing a flush mid-record —
    // the write whose answer must not be discarded.
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

// If write_physical_insert answered with the wal_id regardless, storage_append would
// materialize the rows with nothing in the journal to replay them from.
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

// Startup must refuse rather than continue: the same segment scan recovers the wal id
// allocator (manager_wal_replicate_t / wal_worker_t::recover_from_disk), so missed records
// leave the allocator BELOW ids already on disk — the next write reuses them and corrupts the
// CRC chain. Refusing leaves the segment untouched, so a retry can still read it.
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

    // Non-empty on disk, so the refusal below is about reading it, not an empty journal.
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
    // Reusing the same config.wal.path is safe: base_otterbrix_t erases its path on
    // destruction, so this refusal can only come from the WAL — checked via the message.
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
