#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/test/fault_injection_file.hpp>

#include <filesystem>
#include <memory>
#include <string>
#include <utility>

// FIN-0 / item 1 — A DDL STATEMENT MUST NOT REPORT SUCCESS OVER A CATALOG ROW IT DID NOT WRITE.
//
// agent_disk_t::append_pg_catalog_row_inner answered with a pg_catalog_append_range_t and
// nothing else, and said so in a comment: "returns a pg_catalog_append_range_t with NO ERROR
// CHANNEL; on a failure leave start_row/count at 0 and log — the caller treats a zero-count
// range as a no-op append". A zero-count range is what a legitimate no-op looks like, so
// every caller read the two cases the same way. CREATE TABLE writes pg_class, pg_attribute
// and pg_depend rows through that door: one of them failing left the statement reporting
// success over a catalog that does not describe the table it just claimed to create.
//
// THE INJECTION, and why it lands where it does. The T3 fault seam (fault_injection_file.hpp)
// wraps the file handle a single_file_block_manager_t opens; the interposer below narrows it
// to ONE table's directory by path, and the plan is armed BEFORE the engine starts. The
// header write that create_new_database issues for pg_depend's .otbx therefore fails, the
// storage is never emplaced, and the CREATE TABLE below meets a catalog table whose owning
// agent holds no storage — the "oid not owned / empty" leg, reached through a real statement.
//
// pg_depend rather than pg_class on purpose: CREATE TABLE WRITES a pg_depend row and never
// READS one, so the refusal this case is about is the only thing standing between the
// statement and success. Break pg_class instead and the resolve ahead of the write fails
// first, which would make the test green for the wrong reason.

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
    auto config = test_helpers::make_test_config(
        "/tmp/otterbrix/integration/test_catalog_write_refusal/create_table",
        /*wal_on=*/true);
    config.log.level = log_t::level::off;

    otterbrix_test::fault_plan_t plan;
    // fail_writes_from is compared with >=, so 1 fails every write on the wrapped handle from
    // the first one on — including the header write that creates the file.
    plan.fail_writes_from = 1;
    const auto marker =
        "/" + std::to_string(static_cast<unsigned>(components::catalog::well_known_oid::pg_depend_table)) + "/";
    one_table_fault_scope_t fault(plan, marker);

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    // The database lives in pg_database / pg_namespace, neither of which is interposed.
    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE refusal;")->is_success());

    auto cur = test_helpers::exec(dispatcher, "CREATE TABLE refusal.t (id bigint);");
    INFO("a CREATE TABLE whose pg_depend row could not be written must FAIL, not report success");
    REQUIRE(cur->is_error());
}
