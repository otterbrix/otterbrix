#include "test_config.hpp"

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

} // namespace

TEST_CASE("integration::cpp::test_type_spec_write_gate::decimal_outside_the_window_is_refused_at_ddl") {
    auto config = make_test_config("/tmp/otterbrix/integration/test_type_spec_write_gate/decimal_window", true);
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
    auto config = make_test_config("/tmp/otterbrix/integration/test_type_spec_write_gate/decimal_boundaries", true);
    config.log.level = log_t::level::off;

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        REQUIRE(exec(d, "CREATE DATABASE w;")->is_success());

        // The far end of the window must still be ACCEPTED — the gate refuses what the
        // decoder refuses and nothing beyond it. These two are not carried through the
        // checkpoint below only because a DECIMAL wider than 18 digits is stored as int128
        // and components::table::column_segment_t::scan_partial has no int128 arm (it
        // throws std::logic_error out of the checkpoint's compaction scan) — an unrelated
        // pre-existing storage gap, not a property of the type window.
        REQUIRE(exec(d, "CREATE TABLE w.widest (id BIGINT, d38 NUMERIC(38,38), d38z NUMERIC(38,0));")->is_success());
        REQUIRE(exec(d,
                     "CREATE TABLE w.edges (id BIGINT, d1 NUMERIC(1,0), d1s NUMERIC(1,1), d4 NUMERIC(4,4), "
                     "d9 NUMERIC(9,0), d18 NUMERIC(18,18), d18z NUMERIC(18,0));")
                    ->is_success());
        REQUIRE(exec(d, "INSERT INTO w.edges (id, d1, d18z) VALUES (1, 3, 100), (2, 4, 200);")->is_success());
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
    }
}

TEST_CASE("integration::cpp::test_type_spec_write_gate::nesting_past_the_format_limit_is_refused_at_create_type") {
    auto config = make_test_config("/tmp/otterbrix/integration/test_type_spec_write_gate/nesting_limit", true);
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
