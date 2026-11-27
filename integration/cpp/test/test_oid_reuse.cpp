// VACUUM gives back the oids nothing names any more; one is handed out again only once the journal holds none
// of its records, and that wait survives a restart.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>

#include <memory>
#include <string>

using namespace test_helpers;
using components::catalog::INVALID_OID;
using components::catalog::oid_t;

namespace {

    components::cursor::cursor_t_ptr run_ok(otterbrix::wrapper_dispatcher_t* d, const std::string& sql) {
        auto cur = exec(d, sql);
        INFO("statement: " << sql);
        INFO("error: " << (cur->is_error() ? std::string{cur->get_error().what.begin(), cur->get_error().what.end()}
                                           : std::string{"none"}));
        REQUIRE(cur->is_success());
        return cur;
    }

    oid_t create(otterbrix::wrapper_dispatcher_t* d, const std::string& name) {
        auto cur = run_ok(d, "CREATE TABLE r." + name + " (id bigint);");
        REQUIRE(cur->created_oid() != INVALID_OID);
        return cur->created_oid();
    }

    // Enough journal to fill several of the small segments below, so a truncate can retire whole ones.
    void fill(otterbrix::wrapper_dispatcher_t* d, int round) {
        for (int batch = 0; batch < 10; ++batch) {
            std::string sql = "INSERT INTO r.keep (id, name) VALUES ";
            for (int row = 0; row < 50; ++row) {
                if (row != 0) {
                    sql += ", ";
                }
                sql += "(" + std::to_string(row) + ", 'round " + std::to_string(round) + " batch " +
                       std::to_string(batch) + " padding padding padding padding padding')";
            }
            run_ok(d, sql + ";");
        }
    }

} // namespace

TEST_CASE("integration::cpp::oid_reuse::a_dropped_tables_oid") {
    auto config = test_create_config(integration_fixture_path("test_oid_reuse"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    config.wal.max_segment_size = 8 * 1024;
    // only the statements below truncate the journal
    config.wal.auto_checkpoint_threshold_bytes = 1024ull * 1024ull * 1024ull;

    auto space = std::make_unique<test_spaces>(config);
    auto* d = space->dispatcher();
    run_ok(d, "CREATE SCHEMA r;");
    const oid_t dropped = create(d, "dropped");
    const oid_t keep = run_ok(d, "CREATE TABLE r.keep (id bigint, name string);")->created_oid();
    REQUIRE(dropped < keep);
    run_ok(d, "DROP TABLE r.dropped;");

    run_ok(d, "VACUUM;");
    INFO("the journal still holds the dropped table's records");
    CHECK(create(d, "early") > keep);

    SECTION("in one run") {}
    SECTION("across a restart") {
        space.reset();
        space = std::make_unique<test_spaces>(config);
        d = space->dispatcher();
    }

    INFO("the checkpoint keeps the interval before it, so two rounds retire the segments the drop wrote to");
    for (int round = 0; round < 2; ++round) {
        fill(d, round);
        run_ok(d, "VACUUM;");
    }
    CHECK(create(d, "reused") < keep);
}
