#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <string>

// encode_type_spec must refuse anything the decoder would reject (DECIMAL outside width 0..38,
// scale > width, or nesting past the format depth limit); an accepted-but-unreadable value only
// surfaces as data_corruption on the next restart, which is why these cases restart.

using namespace test_helpers;

namespace {

    std::string chained_type_ddl(unsigned n) {
        if (n == 0) {
            return "CREATE TYPE nest0 AS (a bigint);";
        }
        return "CREATE TYPE nest" + std::to_string(n) + " AS (a nest" + std::to_string(n - 1) + ");";
    }

    // 123456789 * 10^20 is 29 digits, past int64 scaled storage, so this needs int128.
    const components::types::int128_t WIDE_SCALED_PAYLOAD = [] {
        components::types::int128_t v{123456789};
        for (int i = 0; i < 20; ++i) {
            v *= 10;
        }
        return v;
    }();

    // Raw scaled integers, not decoded values: a truncated high word or 64-bit read would show as
    // a wrong value rather than a wrong scale. `sign` flips the mirrored row.
    void check_wide_row(const components::cursor::cursor_t_ptr& cursor, uint64_t row, int sign) {
        using components::types::int128_t;
        INFO("w.widest row " << row);
        // NUMERIC(38,38), NUMERIC(38,0), NUMERIC(38,20), NUMERIC(19,0) -- in that column order.
        CHECK(cursor->value(0, row).value<int128_t>() == int128_t{0});
        CHECK(cursor->value(1, row).value<int128_t>() == int128_t{sign * 2000000000LL});
        CHECK(cursor->value(2, row).value<int128_t>() == WIDE_SCALED_PAYLOAD * sign);
        CHECK(cursor->value(3, row).value<int128_t>() == int128_t{sign * 1234567890LL});
    }

} // namespace

TEST_CASE("integration::cpp::test_type_spec_write_gate::decimal_outside_the_window_is_refused_at_ddl") {
    auto config = make_test_config(integration_fixture_path("test_type_spec_write_gate/decimal_window"), true);
    config.log.level = log_t::level::off;

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());

        INFO("width 0 holds no digit");
        CHECK(exec(d, "CREATE TABLE w.zero_width (c NUMERIC(0,0));")->is_error());
        INFO("width 39 has no scaled-integer storage — int128 tops out at 38 digits");
        CHECK(exec(d, "CREATE TABLE w.too_wide (c NUMERIC(39,0));")->is_error());
        INFO("scale above width means more fraction digits than digits");
        CHECK(exec(d, "CREATE TABLE w.scale_over_width (c NUMERIC(5,7));")->is_error());
        // 256 wraps to width 0 through the narrowing cast to uint8.
        INFO("a width that wraps on narrowing is still out of range");
        CHECK(exec(d, "CREATE TABLE w.wrapped_width (c NUMERIC(256,0));")->is_error());

        CHECK(exec(d, "SELECT * FROM w.zero_width;")->is_error());
        REQUIRE(exec(d, "CREATE TABLE w.ok (id BIGINT, small NUMERIC(1,0), wide NUMERIC(18,4));")->is_success());
        REQUIRE(exec(d, "INSERT INTO w.ok (id, small, wide) VALUES (1, 7, 12345.6789);")->is_success());
        REQUIRE(exec(d, "CHECKPOINT;")->is_success());
    }

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto cur = exec(d, "SELECT id FROM w.ok;");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 1);
        REQUIRE(exec(d, "INSERT INTO w.ok (id, small, wide) VALUES (2, 8, 1.0);")->is_success());
        auto after = exec(d, "SELECT id FROM w.ok;");
        REQUIRE(after->is_success());
        CHECK(after->size() == 2);
    }
}

TEST_CASE("integration::cpp::test_type_spec_write_gate::legal_decimal_boundaries_survive_checkpoint_and_restart") {
    auto config = make_test_config(integration_fixture_path("test_type_spec_write_gate/decimal_boundaries"), true);
    config.log.level = log_t::level::off;

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());

        // Width 19..38 is a 128-bit scaled integer; without an int128 arm in
        // column_segment_t::scan/scan_partial, the checkpoint's compaction scan throws and kills the process.
        REQUIRE(exec(d,
                     "CREATE TABLE w.widest (id BIGINT, d38 NUMERIC(38,38), d38z NUMERIC(38,0), "
                     "d38s NUMERIC(38,20), d19 NUMERIC(19,0));")
                    ->is_success());
        REQUIRE(exec(d,
                     "CREATE TABLE w.edges (id BIGINT, d1 NUMERIC(1,0), d1s NUMERIC(1,1), d4 NUMERIC(4,4), "
                     "d9 NUMERIC(9,0), d18 NUMERIC(18,18), d18z NUMERIC(18,0));")
                    ->is_success());
        REQUIRE(exec(d, "INSERT INTO w.edges (id, d1, d18z) VALUES (1, 3, 100), (2, 4, 200);")->is_success());
        REQUIRE(exec(d,
                     "INSERT INTO w.widest (id, d38, d38z, d38s, d19) VALUES "
                     "(1, 0, 2000000000, 123456789, 1234567890), "
                     "(2, 0, -2000000000, -123456789, -1234567890);")
                    ->is_success());
        {
            auto cur = exec(d, "SELECT d38, d38z, d38s, d19 FROM w.widest ORDER BY id;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
            REQUIRE(cur->value(2, 0).type().type() == components::types::logical_type::DECIMAL);
            check_wide_row(cur, 0, 1);
            check_wide_row(cur, 1, -1);
        }
        REQUIRE(exec(d, "CHECKPOINT;")->is_success());
    }

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto cur = exec(d, "SELECT id FROM w.edges;");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 2);
        REQUIRE(exec(d, "INSERT INTO w.edges (id, d1, d18z) VALUES (3, 5, 300);")->is_success());
        auto after = exec(d, "SELECT id FROM w.edges;");
        REQUIRE(after->is_success());
        CHECK(after->size() == 3);

        {
            auto wide = exec(d, "SELECT d38, d38z, d38s, d19 FROM w.widest ORDER BY id;");
            REQUIRE(wide->is_success());
            REQUIRE(wide->size() == 2);
            REQUIRE(wide->value(2, 0).type().type() == components::types::logical_type::DECIMAL);
            check_wide_row(wide, 0, 1);
            check_wide_row(wide, 1, -1);
        }
        REQUIRE(exec(d, "INSERT INTO w.widest (id, d38, d38z, d38s, d19) VALUES (3, 0, 1, 1, 1);")->is_success());
        auto grown = exec(d, "SELECT id FROM w.widest;");
        REQUIRE(grown->is_success());
        CHECK(grown->size() == 3);
    }
}

TEST_CASE("integration::cpp::test_type_spec_write_gate::nesting_past_the_format_limit_is_refused_at_create_type") {
    auto config = make_test_config(integration_fixture_path("test_type_spec_write_gate/nesting_limit"), true);
    config.log.level = log_t::level::off;

    unsigned deepest_accepted = 0;
    bool refused = false;

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());

        // An unrefused chain would be persisted by a checkpoint but refused by a load no statement can retry.
        for (unsigned n = 0; n < 90; ++n) {
            auto cur = exec(d, chained_type_ddl(n));
            if (cur->is_error()) {
                refused = true;
                break;
            }
            deepest_accepted = n;
        }
        INFO("deepest accepted CREATE TYPE index: " << deepest_accepted);
        REQUIRE(refused);
        // A refusal at depth 0 would mean the gate ate the whole feature.
        REQUIRE(deepest_accepted > 8);

        REQUIRE(
            exec(d, "CREATE TABLE w.deep (id BIGINT, c nest" + std::to_string(deepest_accepted) + ");")->is_success());
        REQUIRE(exec(d, "INSERT INTO w.deep (id) VALUES (1);")->is_success());
        REQUIRE(exec(d, "CHECKPOINT;")->is_success());
    }

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto cur = exec(d, "SELECT id FROM w.deep;");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 1);
    }
}
