#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/test/fault_injection_file.hpp>

#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

// A DDL STATEMENT MUST NOT REPORT SUCCESS OVER A CATALOG ROW IT DID NOT WRITE.
//
// A pg_catalog_append_range_t carries no error channel of its own, so an
// agent_disk_t::append_pg_catalog_row_inner that answered with it alone could only leave
// start_row/count at 0 and log — and a zero-count range is exactly what a legitimate no-op
// append looks like, which makes every caller read the two cases the same way. CREATE TABLE
// writes pg_class, pg_attribute and pg_depend rows through that door: one of them failing
// leaves the statement reporting success over a catalog that does not describe the table it
// just claimed to create.
//
// THE INJECTION, and why it lands where it does. The fault seam (fault_injection_file.hpp)
// wraps the file handle a single_file_block_manager_t opens; the interposer below narrows it
// to ONE table's directory by path, and the plan is armed BEFORE the engine starts. The header
// write that create_new_database issues for pg_depend's .otbx therefore fails and the storage
// is never emplaced.
//
// WHY THIS CASE ASSERTS ON THE BOOTSTRAP AND NOT ON THE STATEMENT. Letting the engine come up
// over that missing pg_depend and asserting the CREATE TABLE failure instead needs exactly the
// start the bootstrap refusal forbids: an engine holding an incomplete pg_catalog over live
// storage mints fresh oids on top of it at the next DDL. Catching the injection one floor
// EARLIER is strictly stronger — the write it guards cannot be attempted at all — while the
// guarantee that append_pg_catalog_row refuses instead of answering with a zero-count range is
// carried by its own error channel.
//
// The recoverability half is asserted too, because a refusal that cannot be undone would be a
// worse defect than the one it replaces: with the fault removed the same directory opens and
// the same CREATE TABLE succeeds.
//
// pg_depend rather than pg_class on purpose: it is the table CREATE TABLE only ever WRITES, so
// nothing else in the statement path could be blamed for the outcome.

namespace {

    // The seam is process-wide and this engine opens one .otbx per catalog table plus one per
    // user table, so filter by path: every handle whose path does not carry the marker is
    // returned unwrapped, i.e. not interposed at all. Same shape as the one_table_fault_scope_t
    // in services/disk/tests/test_persistence.cpp.
    class one_table_fault_scope_t final
        : public components::table::storage::single_file_block_manager_t::file_handle_interposer_t {
    public:
        one_table_fault_scope_t(otterbrix_test::fault_plan_t& plan, std::string path_marker)
            : plan_(plan)
            , marker_(std::move(path_marker)) {
            components::table::storage::single_file_block_manager_t::dev_set_file_interposer(this);
        }
        ~one_table_fault_scope_t() override {
            components::table::storage::single_file_block_manager_t::dev_set_file_interposer(nullptr);
        }

        std::unique_ptr<core::filesystem::file_handle_t>
        wrap(std::unique_ptr<core::filesystem::file_handle_t> inner) override {
            if (inner == nullptr || inner->path().string().find(marker_) == std::string::npos) {
                return inner;
            }
            return std::make_unique<otterbrix_test::faulty_file_handle_t>(std::move(inner), plan_);
        }

    private:
        otterbrix_test::fault_plan_t& plan_;
        std::string marker_;
    };

} // namespace

TEST_CASE("integration::cpp::test_catalog_write_refusal::create_table_fails_when_a_catalog_row_cannot_be_written") {
    const std::filesystem::path dir = integration_fixture_path("test_catalog_write_refusal/create_table");
    auto config = test_helpers::make_test_config(dir, /*wal_on=*/true);
    config.log.level = log_t::level::off;

    const auto marker =
        "/" + std::to_string(static_cast<unsigned>(components::catalog::well_known_oid::pg_depend_table)) + "/";

    {
        otterbrix_test::fault_plan_t plan;
        // fail_writes_from is compared with >=, so 1 fails every write on the wrapped handle
        // from the first one on — including the header write that creates the file.
        plan.fail_writes_from = 1;
        one_table_fault_scope_t fault(plan, marker);

        INFO("an engine whose pg_depend could not be created must not open at all");
        REQUIRE_THROWS_AS(test_spaces(config), std::runtime_error);
    }

    // THE REFUSAL IS RECOVERABLE, which is the whole licence for refusing: the fault is gone,
    // nothing was left behind that blocks a retry, and the statement the case is named after
    // now runs and succeeds.
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE refusal;")->is_success());
        auto cur = test_helpers::exec(dispatcher, "CREATE TABLE refusal.t (id bigint);");
        REQUIRE(cur->is_success());
    }
}
