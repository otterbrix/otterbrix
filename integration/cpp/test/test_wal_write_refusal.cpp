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

// Unit-level defect proofs live in services/wal/tests/test_wal_write_refusal.cpp; WAL segments
// open through their own DEV_MODE seam (core::filesystem::open_file), not the .otbx one.

namespace {

    // Process-wide for this object's lifetime; starts switched off, so arm the plan only after setup traffic succeeds.
    class wal_fault_scope_t final : public services::wal::wal_file_interposer_t {
    public:
        wal_fault_scope_t() { services::wal::dev_set_wal_file_interposer(this); }
        ~wal_fault_scope_t() override { services::wal::dev_set_wal_file_interposer(nullptr); }

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

    // Wide enough that the WAL record can't fit one 4 KiB page, forcing a flush mid-record.
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
    auto config = test_helpers::make_test_config(integration_fixture_path("test_wal_write_refusal/insert"));
    config.log.level = log_t::level::off;

    wal_fault_scope_t fault;
    fault.faulty_marker = "wal_";

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE refusal;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE refusal.t (id bigint, payload text);")->is_success());

    fault.plan.fail_writes_from = fault.plan.writes_seen + 1;
    const auto writes_before = fault.plan.writes_seen;

    auto cur = test_helpers::exec(dispatcher, wide_insert_sql(200));

    INFO("an INSERT whose WAL record the device refused must FAIL, not report rows inserted");
    REQUIRE(cur->is_error());
    REQUIRE(fault.plan.writes_seen > writes_before);
}

// The same segment scan recovers the wal id allocator (wal_worker_t::recover_from_disk); missed
// records would leave it below ids already on disk, so the next write reuses them and corrupts
// the CRC chain.
TEST_CASE("integration::cpp::test_wal_write_refusal::startup_refuses_a_wal_segment_that_will_not_open") {
    auto config = test_helpers::make_test_config(integration_fixture_path("test_wal_write_refusal/startup"));
    config.log.level = log_t::level::off;

    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE refusal;")->is_success());
        REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE refusal.t (id bigint);")->is_success());
        REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO refusal.t (id) VALUES (1), (2), (3);")->is_success());
    }

    bool found_segment = false;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(config.wal.path)) {
        if (entry.is_regular_file() && entry.path().filename().string().rfind("wal_", 0) == 0 &&
            entry.file_size() > 0) {
            found_segment = true;
            break;
        }
    }
    REQUIRE(found_segment);

    wal_fault_scope_t fault;
    fault.refuse_open_marker = "wal_";

    bool refused = false;
    std::string reason;
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
