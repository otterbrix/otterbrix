#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <string>

// THE WRITE SIDE MUST VALIDATE THE WINDOW THE READ SIDE ACCEPTS.
//
// A column type is made durable by components::types::encode_type_spec — the table
// checkpoint writes it into the .otbx metadata stream and every WAL chunk header carries
// it. Its decoder refuses a DECIMAL whose width is 0 or above 38 or whose scale exceeds
// its width, and refuses nesting past the format depth limit, both as data_corruption.
//
// The encoder used to refuse NEITHER. So both were CONSTRUCTIBLE FROM PLAIN SQL, and both
// failed only on the way back in:
//
//   CREATE TABLE t (c NUMERIC(0,0));   -- accepted
//   INSERT INTO t ...;                 -- accepted, WAL record written
//   <restart>                          -- data_corruption, forever
//
// That asymmetry is the whole defect, and it is why these cases RESTART. A case that only
// asserted "the statement failed" would also pass against a fix that merely moved the
// failure somewhere else; what has to be true is that a refusal costs one statement and
// the database still opens, reads and answers afterwards.
//
// The mirror matters just as much: a fix that NARROWED the window would be a different bug
// wearing the same shape. So the legal boundary values are carried through the same
// checkpoint and restart and required to come back intact.

using namespace test_helpers;

namespace {

    // "CREATE TYPE t_n AS (a t_{n-1})" inlines t_{n-1} WHOLE, so each statement in the
    // chain adds exactly one nesting level to the type the next one persists. This is the
    // only route the SQL surface has to deep nesting at all — no single statement can spell
    // a 60-deep type by hand.
    std::string chained_type_ddl(unsigned n) {
        if (n == 0) {
            return "CREATE TYPE nest0 AS (a bigint);";
        }
        return "CREATE TYPE nest" + std::to_string(n) + " AS (a nest" + std::to_string(n - 1) + ");";
    }

    // The scaled payload the NUMERIC(38,20) rows below must carry, byte for byte:
    // 123456789 * 10^20, which is 29 digits and so cannot be held by the int64 storage a
    // narrower DECIMAL would use.
    //
    // The literal itself stays inside int32 ON PURPOSE. The scanner's int32 overflow guard
    // (process_integer_literal in components/sql/parser/scan.l) sits behind an #ifdef
    // HAVE_LONG_INT_64 that nothing in this project defines, so an integer literal outside
    // int32 is silently truncated through the scanner's `int ival` — 9223372036854775807
    // arrives as -1. That is a separate parser defect; a DECIMAL's SCALE is the route to a
    // 128-bit payload that does not depend on it.
    const components::types::int128_t WIDE_SCALED_PAYLOAD = [] {
        components::types::int128_t v{123456789};
        for (int i = 0; i < 20; ++i) {
            v *= 10;
        }
        return v;
    }();

    // One row of w.widest, read through the cursor as raw scaled integers: a truncated high
    // word or a 64-bit read shows up as a wrong value, not as a wrong scale. `sign` is +1 for
    // the positive row and -1 for its mirror.
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

        // Each of these is a type the checkpoint would write and the next startup would
        // refuse. The refusal has to land HERE, while there is still a statement to fail.
        INFO("width 0 holds no digit");
        CHECK(exec(d, "CREATE TABLE w.zero_width (c NUMERIC(0,0));")->is_error());
        INFO("width 39 has no scaled-integer storage — int128 tops out at 38 digits");
        CHECK(exec(d, "CREATE TABLE w.too_wide (c NUMERIC(39,0));")->is_error());
        INFO("scale above width means more fraction digits than digits");
        CHECK(exec(d, "CREATE TABLE w.scale_over_width (c NUMERIC(5,7));")->is_error());
        // The narrowing cast on the way to uint8 used to WRAP these into the middle of the
        // window: 256 -> width 0.
        INFO("a width that wraps on narrowing is still out of range");
        CHECK(exec(d, "CREATE TABLE w.wrapped_width (c NUMERIC(256,0));")->is_error());

        // A refused DDL leaves nothing behind, and the session stays usable.
        CHECK(exec(d, "SELECT * FROM w.zero_width;")->is_error());
        REQUIRE(exec(d, "CREATE TABLE w.ok (id BIGINT, small NUMERIC(1,0), wide NUMERIC(18,4));")->is_success());
        REQUIRE(exec(d, "INSERT INTO w.ok (id, small, wide) VALUES (1, 7, 12345.6789);")->is_success());
        REQUIRE(exec(d, "CHECKPOINT;")->is_success());
    }

    // The point of the restart: what was made durable above must still be readable, and the
    // engine must still accept writes. This is where the refused types used to land.
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

        // The far end of the window must still be ACCEPTED — the gate refuses what the
        // decoder refuses and nothing beyond it. The wide half (width 19..38, stored as a
        // 128-bit scaled integer) is carried through the SAME checkpoint and restart as the
        // narrow half: without an int128 arm in column_segment_t::scan and ::scan_partial the
        // checkpoint's compaction scan throws std::logic_error across an actor coroutine and
        // kills the process, so leaving the wide half out here would hide exactly that.
        REQUIRE(exec(d,
                     "CREATE TABLE w.widest (id BIGINT, d38 NUMERIC(38,38), d38z NUMERIC(38,0), "
                     "d38s NUMERIC(38,20), d19 NUMERIC(19,0));")
                    ->is_success());
        REQUIRE(exec(d,
                     "CREATE TABLE w.edges (id BIGINT, d1 NUMERIC(1,0), d1s NUMERIC(1,1), d4 NUMERIC(4,4), "
                     "d9 NUMERIC(9,0), d18 NUMERIC(18,18), d18z NUMERIC(18,0));")
                    ->is_success());
        REQUIRE(exec(d, "INSERT INTO w.edges (id, d1, d18z) VALUES (1, 3, 100), (2, 4, 200);")->is_success());
        // Every wide storage class at once: NUMERIC(38,20) carries a payload no int64 can
        // hold (the scale multiplies the literal by 10^20), while NUMERIC(38,0) and
        // NUMERIC(19,0) hold small values whose STORAGE is 128-bit purely because of the
        // declared width — 19 is the first width past int64.
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
        // Every storage class of the window came back and still takes writes.
        REQUIRE(exec(d, "INSERT INTO w.edges (id, d1, d18z) VALUES (3, 5, 300);")->is_success());
        auto after = exec(d, "SELECT id FROM w.edges;");
        REQUIRE(after->is_success());
        CHECK(after->size() == 3);

        // The 128-bit half, element by element, after the checkpoint that used to abort the
        // process and the restart that reads the segment back off disk.
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

    // The deepest chain index CREATE TYPE accepted. Used after the restart to prove the
    // accepted side of the boundary is genuinely usable, not merely un-refused.
    unsigned deepest_accepted = 0;
    bool refused = false;

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());

        // Walk well past the format's depth limit. Exactly one of two things can happen:
        // the chain is refused at some depth, or it is not — and "not" is the bug, because
        // the type is then persisted by a checkpoint that succeeds and refused by a load
        // that no statement can retry.
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

        // The accepted side of the boundary must still back a real column, and that
        // column's type must survive being written and read back.
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
