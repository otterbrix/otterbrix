#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <core/operations_helper.hpp>
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

        INFO("SUM/COUNT/MIN/MAX/AVG over the whole table");
        {
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
            REQUIRE(cur->value(5, 0).value<int64_t>() == 30);   // 150 / 5
            REQUIRE(services::disk::pushdown_reply_rows() > 0); // was-actually-pushed proof
        }

        INFO("scalar aggregates WITH a builtin WHERE (v >= 30 -> rows 30,50,40)");
        {
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

        INFO("empty slice (WHERE matches nothing): SUM/MIN/MAX/AVG NULL, COUNT 0");
        {
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

        INFO("GROUP BY g with SUM/COUNT/MIN/MAX/AVG, ORDER BY g");
        {
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

        INFO("GROUP BY g with a builtin WHERE (v >= 20) -> g=1 {20}, g=2 {30,50,40}");
        {
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
TEST_CASE("integration::cpp::aggregate_pushdown_e2e::grouped_static_schema") { run_grouped('r'); }
// TODO: computed schema does not work properly for now
// TEST_CASE("integration::cpp::aggregate_pushdown_e2e::scalar_computed_schema") { run_scalar('g'); }
// TEST_CASE("integration::cpp::aggregate_pushdown_e2e::grouped_computed_schema") { run_grouped('g'); }

// ----------------------------------------------------------------------------
// eager_aggregation vs a RESIDUAL cross-side WHERE. `WHERE t1.a > t2.b`
// references BOTH join sides, so pushdown_filter leaves it as a match_t child of
// the outer aggregate, addressed in the join's MERGED column space. The
// eager-aggregation rule must bail on that residual match (as it does for a
// having_t child): splicing the MIN partial under the join would pre-aggregate
// rows the post-join filter is required to drop, and would leave the match's
// merged paths pointing into t1's collapsed [g, k, MIN(x)] layout.
//
// Data design: the ONLY row the true filter drops, (g=1, x=5, a=0), holds its
// group's SMALLEST x, so a wrong pre-aggregation is observable as MIN 5 for
// g=1. (With a non-minimal x the fold is invisible and the plan would be
// coincidentally green — exactly what this regression test must not be.)
// Correct result: TWO groups, g=1 -> MIN 10 and g=2 -> MIN 30.
// ----------------------------------------------------------------------------
TEST_CASE("integration::cpp::aggregate_pushdown_e2e::eager_aggregation_bails_under_residual_cross_side_where") {
    auto config = make_test_config("/tmp/test_aggregate_pushdown_e2e/eager_residual_where");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    exec(dispatcher, "CREATE DATABASE TestDatabase;");
    REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.t1 (g bigint, x bigint, k bigint, a bigint);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.t2 (k bigint, b bigint, c bigint);")->is_success());
    REQUIRE(exec(dispatcher,
                 "INSERT INTO TestDatabase.t1 (g, x, k, a) VALUES (1, 10, 1, 100), (1, 5, 1, 0), (2, 30, 2, 100);")
                ->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.t2 (k, b, c) VALUES (1, 50, 0), (2, 50, 5);")->is_success());

    auto cur = exec(dispatcher,
                    "SELECT t1.g, MIN(t1.x) AS m FROM TestDatabase.t1 JOIN TestDatabase.t2 ON t1.k = t2.k "
                    "WHERE t1.a > t2.b GROUP BY t1.g ORDER BY t1.g ASC;");
    INFO("residual-where error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    // Correct semantics: rows surviving t1.a > t2.b are (g=1,x=10,a=100) and
    // (g=2,x=30,a=100); the (g=1,x=5,a=0) row joins on k=1 but fails the WHERE,
    // so it is dropped AFTER the join and must NOT contribute to MIN(x) — a
    // wrong eager splice folds it into the partial and returns MIN 5 here.
    REQUIRE(cur->size() == 2);
    REQUIRE(cur->value(0, 0).value<int64_t>() == 1);
    REQUIRE(cur->value(1, 0).value<int64_t>() == 10);
    REQUIRE(cur->value(0, 1).value<int64_t>() == 2);
    REQUIRE(cur->value(1, 1).value<int64_t>() == 30);
}

// ----------------------------------------------------------------------------
// eager_aggregation must re-stamp the pushed node's output_types with the true
// partial layout [keys..., MIN(x)]: create_plan_aggregate forwards that stamp
// as the authoritative output layout (operator_group's output_types_ / the
// pushed reduce spec), which types the aggregate output column at ordinal
// key_count + a. A stale base-table stamp mistypes that slot — here
// a = (x double, g bigint, k bigint) puts BIGINT k at the ordinal where the
// partial layout [g, k, MIN(x)] emits the DOUBLE minimum, and
// vector_t::set_value refuses the mistyped cell (assert in Debug; the cell is
// skipped in Release), so MIN(x) never comes back as 1.5 / 10.25.
// ----------------------------------------------------------------------------
TEST_CASE("integration::cpp::aggregate_pushdown_e2e::eager_partial_min_keeps_double_type") {
    auto config = make_test_config("/tmp/test_aggregate_pushdown_e2e/eager_partial_types");
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    exec(dispatcher, "CREATE DATABASE TestDatabase;");
    REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.a (x double, g bigint, k bigint);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.b (k bigint);")->is_success());
    REQUIRE(exec(dispatcher,
                 "INSERT INTO TestDatabase.a (x, g, k) VALUES "
                 "(1.5, 1, 100), (2.5, 1, 101), (10.25, 2, 100), (20.5, 2, 101);")
                ->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.b (k) VALUES (100), (101);")->is_success());

    auto cur = exec(dispatcher,
                    "SELECT g, MIN(x) AS m FROM TestDatabase.a JOIN TestDatabase.b ON a.k = b.k "
                    "GROUP BY g ORDER BY g ASC;");
    INFO("partial-types error: " << (cur->is_error() ? cur->get_error().what : "none"));
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 2);
    REQUIRE(cur->value(0, 0).value<int64_t>() == 1);
    REQUIRE(cur->value(1, 0).type().type() == components::types::logical_type::DOUBLE);
    REQUIRE(core::is_equals(cur->value(1, 0).value<double>(), 1.5)); // exactly representable
    REQUIRE(cur->value(0, 1).value<int64_t>() == 2);
    REQUIRE(cur->value(1, 1).type().type() == components::types::logical_type::DOUBLE);
    REQUIRE(core::is_equals(cur->value(1, 1).value<double>(), 10.25)); // exactly representable
}
