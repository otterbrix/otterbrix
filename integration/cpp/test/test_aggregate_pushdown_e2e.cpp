#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>
#include <core/operations_helper.hpp>
#include <services/disk/agent_disk.hpp>

// The value REQUIREs pass whichever path runs, so pushdown_reply_rows() > 0 is the only
// thing proving the aggregate reached the owning agent.

using namespace test_helpers;

namespace {

    std::string seed(otterbrix::wrapper_dispatcher_t* dispatcher, char relkind) {
        exec(dispatcher, "CREATE DATABASE TestDatabase;");
        const std::string table = relkind == 'g' ? "TestDatabase.docs" : "TestDatabase.TestCollection";
        if (relkind == 'g') {
            // Empty column list => relkind='g' (dynamic schema); g,v register on the first INSERT.
            REQUIRE(exec(dispatcher, "CREATE TABLE " + table + "();")->is_success());
        } else {
            REQUIRE(exec(dispatcher, "CREATE TABLE " + table + " (g bigint, v bigint);")->is_success());
        }
        auto cur =
            exec(dispatcher, "INSERT INTO " + table + " (g, v) VALUES (1, 10), (1, 20), (2, 30), (2, 50), (2, 40);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->affected_rows() == 5);
        return table;
    }

    void run_scalar(char relkind) {
        auto config =
            make_test_config(integration_fixture_path(std::string("test_aggregate_pushdown_e2e/scalar_") + relkind));
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
            auto cell0 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell0.has_error());
            REQUIRE(cell0.value() == 150);
            auto cell1 = cur->get<uint64_t>(1, 0);
            REQUIRE_FALSE(cell1.has_error());
            REQUIRE(cell1.value() == 5);
            auto cell2 = cur->get<uint64_t>(2, 0);
            REQUIRE_FALSE(cell2.has_error());
            REQUIRE(cell2.value() == 5);
            auto cell3 = cur->get<int64_t>(3, 0);
            REQUIRE_FALSE(cell3.has_error());
            REQUIRE(cell3.value() == 10);
            auto cell4 = cur->get<int64_t>(4, 0);
            REQUIRE_FALSE(cell4.has_error());
            REQUIRE(cell4.value() == 50);
            auto cell5 = cur->get<double>(5, 0);
            REQUIRE_FALSE(cell5.has_error());
            REQUIRE(cell5.value() == Catch::Approx(30.0));      // 150 / 5
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
            auto cell6 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell6.has_error());
            REQUIRE(cell6.value() == 120);
            auto cell7 = cur->get<uint64_t>(1, 0);
            REQUIRE_FALSE(cell7.has_error());
            REQUIRE(cell7.value() == 3);
            auto cell8 = cur->get<int64_t>(2, 0);
            REQUIRE_FALSE(cell8.has_error());
            REQUIRE(cell8.value() == 30);
            auto cell9 = cur->get<int64_t>(3, 0);
            REQUIRE_FALSE(cell9.has_error());
            REQUIRE(cell9.value() == 50);
            auto cell10 = cur->get<double>(4, 0);
            REQUIRE_FALSE(cell10.has_error());
            REQUIRE(cell10.value() == Catch::Approx(40.0)); // 120 / 3, AVG over BIGINT answers DOUBLE
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
            auto cell11 = cur->is_null(0, 0);
            REQUIRE_FALSE(cell11.has_error());
            REQUIRE(cell11.value()); // SUM over no rows
            auto cell12 = cur->get<uint64_t>(1, 0);
            REQUIRE_FALSE(cell12.has_error());
            REQUIRE(cell12.value() == 0); // COUNT(*) over no rows
            auto cell13 = cur->get<uint64_t>(2, 0);
            REQUIRE_FALSE(cell13.has_error());
            REQUIRE(cell13.value() == 0); // COUNT(v) over no rows
            auto cell14 = cur->is_null(3, 0);
            REQUIRE_FALSE(cell14.has_error());
            REQUIRE(cell14.value()); // MIN over no rows
            auto cell15 = cur->is_null(4, 0);
            REQUIRE_FALSE(cell15.has_error());
            REQUIRE(cell15.value()); // MAX over no rows
            auto cell16 = cur->is_null(5, 0);
            REQUIRE_FALSE(cell16.has_error());
            REQUIRE(cell16.value()); // AVG over no rows
        }
    }

    void run_grouped(char relkind) {
        auto config =
            make_test_config(integration_fixture_path(std::string("test_aggregate_pushdown_e2e/grouped_") + relkind));
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

            auto cell17 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell17.has_error());
            REQUIRE(cell17.value() == 1);
            auto cell18 = cur->get<int64_t>(1, 0);
            REQUIRE_FALSE(cell18.has_error());
            REQUIRE(cell18.value() == 30);
            auto cell19 = cur->get<uint64_t>(2, 0);
            REQUIRE_FALSE(cell19.has_error());
            REQUIRE(cell19.value() == 2);
            auto cell20 = cur->get<int64_t>(3, 0);
            REQUIRE_FALSE(cell20.has_error());
            REQUIRE(cell20.value() == 10);
            auto cell21 = cur->get<int64_t>(4, 0);
            REQUIRE_FALSE(cell21.has_error());
            REQUIRE(cell21.value() == 20);
            auto cell22 = cur->get<double>(5, 0);
            REQUIRE_FALSE(cell22.has_error());
            REQUIRE(cell22.value() == Catch::Approx(15.0)); // 30 / 2, AVG over BIGINT answers DOUBLE

            auto cell23 = cur->get<int64_t>(0, 1);
            REQUIRE_FALSE(cell23.has_error());
            REQUIRE(cell23.value() == 2);
            auto cell24 = cur->get<int64_t>(1, 1);
            REQUIRE_FALSE(cell24.has_error());
            REQUIRE(cell24.value() == 120);
            auto cell25 = cur->get<uint64_t>(2, 1);
            REQUIRE_FALSE(cell25.has_error());
            REQUIRE(cell25.value() == 3);
            auto cell26 = cur->get<int64_t>(3, 1);
            REQUIRE_FALSE(cell26.has_error());
            REQUIRE(cell26.value() == 30);
            auto cell27 = cur->get<int64_t>(4, 1);
            REQUIRE_FALSE(cell27.has_error());
            REQUIRE(cell27.value() == 50);
            auto cell28 = cur->get<double>(5, 1);
            REQUIRE_FALSE(cell28.has_error());
            REQUIRE(cell28.value() == Catch::Approx(40.0)); // 120 / 3
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
            auto cell29 = cur->get<int64_t>(0, 0);
            REQUIRE_FALSE(cell29.has_error());
            REQUIRE(cell29.value() == 1);
            auto cell30 = cur->get<int64_t>(1, 0);
            REQUIRE_FALSE(cell30.has_error());
            REQUIRE(cell30.value() == 20);
            auto cell31 = cur->get<uint64_t>(2, 0);
            REQUIRE_FALSE(cell31.has_error());
            REQUIRE(cell31.value() == 1);
            auto cell32 = cur->get<int64_t>(0, 1);
            REQUIRE_FALSE(cell32.has_error());
            REQUIRE(cell32.value() == 2);
            auto cell33 = cur->get<int64_t>(1, 1);
            REQUIRE_FALSE(cell33.has_error());
            REQUIRE(cell33.value() == 120);
            auto cell34 = cur->get<uint64_t>(2, 1);
            REQUIRE_FALSE(cell34.has_error());
            REQUIRE(cell34.value() == 3);
            REQUIRE(services::disk::pushdown_reply_rows() > 0);
        }
    }

} // namespace

TEST_CASE("integration::cpp::aggregate_pushdown_e2e::scalar_static_schema") { run_scalar('r'); }
TEST_CASE("integration::cpp::aggregate_pushdown_e2e::grouped_static_schema") { run_grouped('r'); }
// TODO: computed schema does not work properly for now
// TEST_CASE("integration::cpp::aggregate_pushdown_e2e::scalar_computed_schema") { run_scalar('g'); }
// TEST_CASE("integration::cpp::aggregate_pushdown_e2e::grouped_computed_schema") { run_grouped('g'); }

// WHERE t1.a > t2.b references both join sides, so eager_aggregation must bail on it (like a having_t child)
// instead of splicing MIN under the join, which would fold in rows the post-join filter must drop. The dropped
// row (g=1, x=5, a=0) holds its group's smallest x, so a wrong fold is observable as MIN 5 instead of MIN 10.
TEST_CASE("integration::cpp::aggregate_pushdown_e2e::eager_aggregation_bails_under_residual_cross_side_where") {
    auto config = make_test_config(integration_fixture_path("test_aggregate_pushdown_e2e/eager_residual_where"));
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
    REQUIRE(cur->size() == 2);
    auto cell35 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell35.has_error());
    REQUIRE(cell35.value() == 1);
    auto cell36 = cur->get<int64_t>(1, 0);
    REQUIRE_FALSE(cell36.has_error());
    REQUIRE(cell36.value() == 10);
    auto cell37 = cur->get<int64_t>(0, 1);
    REQUIRE_FALSE(cell37.has_error());
    REQUIRE(cell37.value() == 2);
    auto cell38 = cur->get<int64_t>(1, 1);
    REQUIRE_FALSE(cell38.has_error());
    REQUIRE(cell38.value() == 30);
}

// eager_aggregation must re-stamp the pushed node's output_types with the true partial layout; a stale base-table
// stamp would mistype the MIN(x) ordinal (DOUBLE) as the next base column (BIGINT k), silently dropping the value.
TEST_CASE("integration::cpp::aggregate_pushdown_e2e::eager_partial_min_keeps_double_type") {
    auto config = make_test_config(integration_fixture_path("test_aggregate_pushdown_e2e/eager_partial_types"));
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
    auto cell39 = cur->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell39.has_error());
    REQUIRE(cell39.value() == 1);
    REQUIRE(cur->type_data()[1].type() == components::types::logical_type::DOUBLE);
    auto cell40 = cur->get<double>(1, 0);
    REQUIRE_FALSE(cell40.has_error());
    REQUIRE(core::is_equals(cell40.value(), 1.5)); // exactly representable
    auto cell41 = cur->get<int64_t>(0, 1);
    REQUIRE_FALSE(cell41.has_error());
    REQUIRE(cell41.value() == 2);
    REQUIRE(cur->type_data()[1].type() == components::types::logical_type::DOUBLE);
    auto cell42 = cur->get<double>(1, 1);
    REQUIRE_FALSE(cell42.has_error());
    REQUIRE(core::is_equals(cell42.value(), 10.25)); // exactly representable
}
