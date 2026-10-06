// The physical DROP COLUMN (collection_t::remove_column, reached from SQL by VACUUM on a computed
// table) seals the table's append packer first. A refused seal used to be printed and dropped;
// it is the statement's error now, and the storage keeps the column.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/table/storage/partial_block_manager.hpp>
#include <string>

namespace {
    using test_helpers::exec;

    std::string error_text(const components::cursor::cursor_t& cur) {
        return cur.is_error() ? std::string{cur.get_error().what.begin(), cur.get_error().what.end()}
                              : std::string{"<no error: statement reported success>"};
    }
} // namespace

TEST_CASE("integration::cpp::vacuum_drop_column_seal_refusal::a_refused_seal_is_the_statements_error") {
    auto config = test_create_config(integration_fixture_path("test_vacuum_drop_column_seal_refusal"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE cdc;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE cdc.docs();")->is_success());
    REQUIRE(exec(d, "INSERT INTO cdc.docs (id, n) VALUES (1, 42);")->is_success());
    REQUIRE(exec(d, "ALTER TABLE cdc.docs DROP COLUMN n;")->is_success());

    components::table::storage::partial_block_manager_t::dev_refuse_next_seal();
    auto vacuum = exec(d, "VACUUM;");
    INFO("VACUUM: " << error_text(*vacuum));
    REQUIRE(vacuum->is_error());
    CHECK(error_text(*vacuum).find("seal") != std::string::npos);

    // The seam is spent; the next VACUUM drops the column for real and the table stays readable.
    auto again = exec(d, "VACUUM;");
    INFO("second VACUUM: " << error_text(*again));
    REQUIRE(again->is_success());
    auto cur = exec(d, "SELECT * FROM cdc.docs;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
}
