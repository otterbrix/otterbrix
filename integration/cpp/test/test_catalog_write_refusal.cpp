#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/test/fault_injection_file.hpp>

#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

// pg_catalog_append_range_t has no error channel, so a failed catalog write is caught by
// refusing to boot rather than by the statement -- an engine that booted over an incomplete
// catalog would mint fresh oids on top of it at the next DDL. pg_depend is the only table
// CREATE TABLE writes exactly once, so failing it unambiguously blames this seam.

namespace {

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
    auto config = test_helpers::make_test_config(dir);
    config.log.level = log_t::level::off;

    const auto marker =
        "/" + std::to_string(static_cast<unsigned>(components::catalog::well_known_oid::pg_depend_table)) + "/";

    {
        otterbrix_test::fault_plan_t plan;
        // fail_writes_from is compared with >=, so 1 fails the header write too.
        plan.fail_writes_from = 1;
        one_table_fault_scope_t fault(plan, marker);

        INFO("an engine whose pg_depend could not be created must not open at all");
        REQUIRE_THROWS_AS(test_spaces(config), std::runtime_error);
    }

    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE refusal;")->is_success());
        auto cur = test_helpers::exec(dispatcher, "CREATE TABLE refusal.t (id bigint);");
        REQUIRE(cur->is_success());
    }
}
