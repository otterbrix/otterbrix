#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/log/test/test_log.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/test/fault_injection_file.hpp>

#include <cstddef>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
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
        REQUIRE(otterbrix::base_otterbrix_t::open(config).has_error());
    }

    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        REQUIRE(test_helpers::exec(dispatcher, "CREATE SCHEMA refusal;")->is_success());
        auto cur = test_helpers::exec(dispatcher, "CREATE TABLE refusal.t (id bigint);");
        REQUIRE(cur->is_success());
    }
}

TEST_CASE("integration::cpp::test_catalog_write_refusal::create_table_fails_when_its_storage_cannot_be_created") {
    const std::filesystem::path dir = integration_fixture_path("test_catalog_write_refusal/storage_not_created");
    auto config = test_helpers::make_test_config(dir);
    config.log.level = log_t::level::warn;
    std::filesystem::path obstacle;

    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        REQUIRE(test_helpers::exec(dispatcher, "CREATE SCHEMA blocked;")->is_success());

        auto oid_cur =
            test_helpers::exec(dispatcher, "SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'blocked';");
        REQUIRE(oid_cur->is_success());
        REQUIRE(oid_cur->size() == 1);
        const auto namespace_oid = oid_cur->chunks().front().get_value<std::uint32_t>(0, 0);

        // A table's directory is ${disk path}/${schema oid}/${table oid} (the schema oid, not the database's: #834),
        // made by its first table; a plain file in the schema's place cannot become a directory.
        obstacle = config.disk.path / std::to_string(namespace_oid);
        REQUIRE_FALSE(std::filesystem::exists(obstacle));
        {
            std::ofstream file(obstacle);
            REQUIRE(file.good());
        }

        const std::string create = "CREATE TABLE blocked.t (id bigint);";

        auto refused = test_helpers::exec(dispatcher, create);
        REQUIRE(refused->is_error());
        CHECK(refused->get_error().type == core::error_code_t::io_error);
        CHECK(std::string{refused->get_error().what.c_str()}.find(obstacle.string()) != std::string::npos);
        auto after_refusal =
            test_helpers::exec(dispatcher, "SELECT relname FROM pg_catalog.pg_class WHERE relname = 't';");
        REQUIRE(after_refusal->is_success());
        CHECK(after_refusal->size() == 0);

        REQUIRE(std::filesystem::remove(obstacle));

        REQUIRE(test_helpers::exec(dispatcher, create)->is_success());
        REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO blocked.t (id) VALUES (7);")->is_success());
        auto selected = test_helpers::exec(dispatcher, "SELECT id FROM blocked.t;");
        REQUIRE(selected->is_success());
        REQUIRE(selected->size() == 1);
        CHECK(selected->chunks().front().get_value<std::int64_t>(0, 0) == 7);
    }

    INFO("one refusal is one log record: the disk agent writes it with the path, the manager adds nothing");
    std::istringstream lines(log_text(config.log.path));
    std::size_t records = 0;
    for (std::string line; std::getline(lines, line);) {
        if (line.find(obstacle.string()) != std::string::npos) {
            ++records;
        }
    }
    CHECK(records == 1);
}
