#include "test_config.hpp"

#include <catch2/catch.hpp>
#include <services/disk/agent_disk.hpp>

// End-to-end coverage for aggregate-pushdown-to-owning-agent, run over BOTH
// relation kinds via one shared query/assert battery:
//   relkind 'r' — a STATIC schema (CREATE TABLE t(g bigint, v bigint)).
//   relkind 'g' — a COMPUTED / dynamic-schema collection (empty CREATE TABLE
//                 docs(); columns g,v registered Mongo-style on the first INSERT).
//
// The optimizer's pushdown_aggregate rule stamps EVERY pushable single-owned-table
// mergeable aggregate (SUM/COUNT/MIN/MAX/AVG, scalar or grouped, with/without a
// builtin WHERE) purely BY SHAPE — it has no catalog access, so it cannot see
// relkind. physgen then routes a stamped aggregate to the pushdown fragment; the
// coordinator lowers the full_scan with the SAME relkind-aware chunk_position
// projection the base scan would use (create_plan_aggregate::relkind_projected_cols),
// so the owning agent runs the reduce over its own slice and streams back the FINAL
// aggregated rows. The disk manager is always spawned in the test harness, so
// disk_address_ is non-empty and the capability gate is satisfied even with
// disk.on == false: these queries route through the live pushed path.
//
// The assertions pin CONCRETE values (correctness is transparent — identical
// whichever path runs) AND, for the non-empty aggregates, assert
// services::disk::pushdown_reply_rows() > 0: a DEV_MODE process-global counter
// bumped inside the agent-side reduce OPEN, proving the aggregate WAS pushed to the
// owning agent rather than computed coordinator-side. Without this guard the value
// REQUIREs would still pass even if pushdown silently stopped firing, hiding a
// coverage loss.

using namespace test_helpers;

namespace {

    // Create db + table for `relkind` and seed the same 5-row data either way:
    //   g=1: v=10,20        -> count 2, sum 30,  min 10, max 20, avg 15
    //   g=2: v=30,50,40     -> count 3, sum 120, min 30, max 50, avg 40
    //   whole table: count 5, sum 150, min 10, max 50, avg 30.
    // Returns the fully-qualified table name.
    std::string seed(otterbrix::wrapper_dispatcher_t* dispatcher, char relkind) {
        exec(dispatcher, "CREATE DATABASE TestDatabase;");
        const std::string table = relkind == 'g' ? "TestDatabase.docs" : "TestDatabase.TestCollection";
        if (relkind == 'g') {
            // Empty column list => relkind='g' (computed / dynamic schema); g,v are
            // registered on the first INSERT (integer literals => bigint).
            REQUIRE(exec(dispatcher, "CREATE TABLE " + table + "();")->is_success());
        } else {
            REQUIRE(exec(dispatcher, "CREATE TABLE " + table + " (g bigint, v bigint);")->is_success());
        }
        auto cur =
            exec(dispatcher, "INSERT INTO " + table + " (g, v) VALUES (1, 10), (1, 20), (2, 30), (2, 50), (2, 40);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 5);
        return table;
    }

    void run_scalar(char relkind) {
        auto config = make_test_config(std::string("/tmp/test_aggregate_pushdown_e2e/scalar_") + relkind);
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        const std::string table = seed(dispatcher, relkind);

        INFO("SUM/COUNT/MIN/MAX/AVG over the whole table") {
            services::disk::reset_pushdown_reply_rows();
            auto cur = exec(dispatcher,
                            "SELECT SUM(v) AS s, COUNT(*) AS c_all, COUNT(v) AS c_v, "
                            "MIN(v) AS mn, MAX(v) AS mx, AVG(v) AS av FROM " +
                                table + ";");
            INFO("scalar error: " << (cur->is_error() ? cur->get_error().what : "none"));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->column_count() == 6);
            REQUIRE(cur->value(0, 0).value<int64_t>() == 150);
            REQUIRE(cur->value(1, 0).value<uint64_t>() == 5);
            REQUIRE(cur->value(2, 0).value<uint64_t>() == 5);
            REQUIRE(cur->value(3, 0).value<int64_t>() == 10);
            REQUIRE(cur->value(4, 0).value<int64_t>() == 50);
            REQUIRE(cur->value(5, 0).value<int64_t>() == 30); // 150 / 5
            REQUIRE(services::disk::pushdown_reply_rows() > 0); // was-actually-pushed proof
        }

        INFO("scalar aggregates WITH a builtin WHERE (v >= 30 -> rows 30,50,40)") {
            services::disk::reset_pushdown_reply_rows();
            auto cur = exec(dispatcher,
                            "SELECT SUM(v) AS s, COUNT(*) AS c, MIN(v) AS mn, MAX(v) AS mx, AVG(v) AS av "
                            "FROM " +
                                table + " WHERE v >= 30;");
            INFO("scalar-where error: " << (cur->is_error() ? cur->get_error().what : "none"));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->value(0, 0).value<int64_t>() == 120);
            REQUIRE(cur->value(1, 0).value<uint64_t>() == 3);
            REQUIRE(cur->value(2, 0).value<int64_t>() == 30);
            REQUIRE(cur->value(3, 0).value<int64_t>() == 50);
            REQUIRE(cur->value(4, 0).value<int64_t>() == 40); // 120 / 3
            REQUIRE(services::disk::pushdown_reply_rows() > 0);
        }

        INFO("empty slice (WHERE matches nothing): SUM/MIN/MAX/AVG NULL, COUNT 0") {
            auto cur = exec(dispatcher,
                            "SELECT SUM(v) AS s, COUNT(*) AS c_all, COUNT(v) AS c_v, "
                            "MIN(v) AS mn, MAX(v) AS mx, AVG(v) AS av FROM " +
                                table + " WHERE v > 1000;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->value(0, 0).is_null());              // SUM over no rows
            REQUIRE(cur->value(1, 0).value<uint64_t>() == 0); // COUNT(*) over no rows
            REQUIRE(cur->value(2, 0).value<uint64_t>() == 0); // COUNT(v) over no rows
            REQUIRE(cur->value(3, 0).is_null());              // MIN over no rows
            REQUIRE(cur->value(4, 0).is_null());              // MAX over no rows
            REQUIRE(cur->value(5, 0).is_null());              // AVG over no rows
        }
    }

    void run_grouped(char relkind) {
        auto config = make_test_config(std::string("/tmp/test_aggregate_pushdown_e2e/grouped_") + relkind);
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        const std::string table = seed(dispatcher, relkind);

        INFO("GROUP BY g with SUM/COUNT/MIN/MAX/AVG, ORDER BY g") {
            services::disk::reset_pushdown_reply_rows();
            auto cur = exec(dispatcher,
                            "SELECT g, SUM(v) AS s, COUNT(*) AS c, MIN(v) AS mn, MAX(v) AS mx, AVG(v) AS av "
                            "FROM " +
                                table + " GROUP BY g ORDER BY g ASC;");
            INFO("grouped error: " << (cur->is_error() ? cur->get_error().what : "none"));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
            REQUIRE(cur->column_count() == 6);

            // group g=1: v {10,20}
            REQUIRE(cur->value(0, 0).value<int64_t>() == 1);
            REQUIRE(cur->value(1, 0).value<int64_t>() == 30);
            REQUIRE(cur->value(2, 0).value<uint64_t>() == 2);
            REQUIRE(cur->value(3, 0).value<int64_t>() == 10);
            REQUIRE(cur->value(4, 0).value<int64_t>() == 20);
            REQUIRE(cur->value(5, 0).value<int64_t>() == 15); // 30 / 2

            // group g=2: v {30,50,40}
            REQUIRE(cur->value(0, 1).value<int64_t>() == 2);
            REQUIRE(cur->value(1, 1).value<int64_t>() == 120);
            REQUIRE(cur->value(2, 1).value<uint64_t>() == 3);
            REQUIRE(cur->value(3, 1).value<int64_t>() == 30);
            REQUIRE(cur->value(4, 1).value<int64_t>() == 50);
            REQUIRE(cur->value(5, 1).value<int64_t>() == 40); // 120 / 3
            REQUIRE(services::disk::pushdown_reply_rows() > 0);
        }

        INFO("GROUP BY g with a builtin WHERE (v >= 20) -> g=1 {20}, g=2 {30,50,40}") {
            services::disk::reset_pushdown_reply_rows();
            auto cur = exec(dispatcher,
                            "SELECT g, SUM(v) AS s, COUNT(*) AS c "
                            "FROM " +
                                table + " WHERE v >= 20 GROUP BY g ORDER BY g ASC;");
            INFO("grouped-where error: " << (cur->is_error() ? cur->get_error().what : "none"));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
            REQUIRE(cur->value(0, 0).value<int64_t>() == 1);
            REQUIRE(cur->value(1, 0).value<int64_t>() == 20);
            REQUIRE(cur->value(2, 0).value<uint64_t>() == 1);
            REQUIRE(cur->value(0, 1).value<int64_t>() == 2);
            REQUIRE(cur->value(1, 1).value<int64_t>() == 120);
            REQUIRE(cur->value(2, 1).value<uint64_t>() == 3);
            REQUIRE(services::disk::pushdown_reply_rows() > 0);
        }
    }

} // namespace

TEST_CASE("integration::cpp::aggregate_pushdown_e2e::scalar_static_schema") { run_scalar('r'); }
TEST_CASE("integration::cpp::aggregate_pushdown_e2e::scalar_computed_schema") { run_scalar('g'); }
TEST_CASE("integration::cpp::aggregate_pushdown_e2e::grouped_static_schema") { run_grouped('r'); }
TEST_CASE("integration::cpp::aggregate_pushdown_e2e::grouped_computed_schema") { run_grouped('g'); }
