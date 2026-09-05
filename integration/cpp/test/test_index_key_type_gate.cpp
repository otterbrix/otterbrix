#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <set>
#include <string>

// CREATE INDEX must refuse a key type the encoders can't represent: convert() has no error
// channel, so an unencodable key aborts (Debug) or, under NDEBUG, silently returns NA and
// serves wrong rows -- only the abort half is exercised here (Debug+DEV_MODE). Tables stay
// EMPTY: a populated CREATE INDEX backfills, reaching the encoder before the gate's verdict.

using namespace test_helpers;

namespace {

    // Must fail as index_create_fail specifically, not any parse/schema error.
    void refused(otterbrix::wrapper_dispatcher_t* d, const std::string& sql) {
        auto cur = exec(d, sql);
        INFO(sql);
        REQUIRE(cur);
        REQUIRE(cur->is_error());
        CHECK(cur->get_error().type == core::error_code_t::index_create_fail);
    }

} // namespace

TEST_CASE("integration::cpp::test_index_key_type_gate::unrepresentable_key_types_are_refused") {
    auto config = make_test_config(integration_fixture_path("test_index_key_type_gate/refused"), true);
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE g;")->is_success());
    // INTERVAL/TIMETZ are physically STRUCT; HUGEINT/UHUGEINT are 16-byte ints the codec has
    // no case for — none of the four can be an index key.
    REQUIRE(exec(d, "CREATE TABLE g.t (id BIGINT, iv INTERVAL, ttz TIMETZ, h HUGEINT, uh UHUGEINT);")->is_success());

    refused(d, "CREATE INDEX i_iv ON g.t (iv);");
    refused(d, "CREATE INDEX i_ttz ON g.t (ttz);");
    refused(d, "CREATE INDEX i_h ON g.t (h);");
    refused(d, "CREATE INDEX i_uh ON g.t (uh);");

    // Same verdict for a hash index: the bitcask/hash encoders share the codec.
    refused(d, "CREATE INDEX i_iv_h ON g.t USING hash (iv);");
    refused(d, "CREATE INDEX i_h_h ON g.t USING hash (h);");

    // The refusal is the gate's, not a blanket "no index on this table".
    REQUIRE(exec(d, "CREATE INDEX i_id ON g.t (id);")->is_success());
}

// Mirror case: every representable type must still be accepted — over-refusing is as much
// a defect as under-refusing.
TEST_CASE("integration::cpp::test_index_key_type_gate::representable_key_types_are_accepted") {
    auto config = make_test_config(integration_fixture_path("test_index_key_type_gate/accepted"), true);
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE g;")->is_success());
    REQUIRE(exec(d,
                 "CREATE TABLE g.t (id BIGINT, b BOOLEAN, i INT, s SMALLINT, f FLOAT, dd DOUBLE, "
                 "txt TEXT, d DATE, tm TIME, ts TIMESTAMP, tstz TIMESTAMPTZ);")
                ->is_success());

    for (const char* col : {"id", "b", "i", "s", "f", "dd", "txt", "d", "tm", "ts", "tstz"}) {
        const std::string sql = std::string{"CREATE INDEX i_"} + col + " ON g.t (" + col + ");";
        INFO(sql);
        CHECK(exec(d, sql)->is_success());
    }
}

// End-to-end parity: index answers must match an unindexed scan. Bulk INSERT never calls
// convert() (goes through insert_bulk_unchecked), so the abort half is pinned separately in
// services/index/tests/test_index_disk.cpp (convert_temporal_preserves_order, date_keys/timestamp_keys).
TEST_CASE("integration::cpp::test_index_key_type_gate::temporal_indexes_return_the_right_rows") {
    auto config = make_test_config(integration_fixture_path("test_index_key_type_gate/temporal"), true);
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE g;")->is_success());
    // Indexed table and its unindexed twin: the scan path is the oracle for the index path.
    REQUIRE(exec(d, "CREATE TABLE g.ti (id BIGINT, d DATE, tm TIME, ts TIMESTAMP);")->is_success());
    REQUIRE(exec(d, "CREATE TABLE g.tp (id BIGINT, d DATE, tm TIME, ts TIMESTAMP);")->is_success());
    REQUIRE(exec(d, "CREATE INDEX i_d ON g.ti (d);")->is_success());
    REQUIRE(exec(d, "CREATE INDEX i_tm ON g.ti (tm);")->is_success());
    REQUIRE(exec(d, "CREATE INDEX i_ts ON g.ti (ts);")->is_success());

    const char* rows[] = {
        "(1, DATE '2024-01-01', TIME '08:00:00', TIMESTAMP '2024-01-01 00:00:00')",
        "(2, DATE '2024-03-15', TIME '12:30:00', TIMESTAMP '2024-03-15 12:30:00')",
        "(3, DATE '2024-12-31', TIME '23:59:00', TIMESTAMP '2024-12-31 23:59:00')",
    };
    for (const char* r : rows) {
        for (const char* t : {"g.ti", "g.tp"}) {
            const std::string sql = std::string{"INSERT INTO "} + t + " (id, d, tm, ts) VALUES " + r + ";";
            INFO(sql);
            REQUIRE(exec(d, sql)->is_success());
        }
    }

    // Comparing the two isn't enough — a key collapsed to NA would match every predicate on
    // both sides identically. The counts below must also be right in absolute terms.
    auto both = [&](const std::string& pred, size_t expected) {
        auto with_idx = exec(d, "SELECT id FROM g.ti WHERE " + pred + ";");
        auto no_idx = exec(d, "SELECT id FROM g.tp WHERE " + pred + ";");
        INFO("predicate: " << pred);
        REQUIRE(with_idx->is_success());
        REQUIRE(no_idx->is_success());
        CHECK(no_idx->size() == expected);
        CHECK(with_idx->size() == expected);
    };

    both("d = DATE '2024-03-15'", 1);
    both("d < DATE '2024-03-15'", 1);
    both("d > DATE '2024-01-01'", 2);
    both("tm = TIME '12:30:00'", 1);
    both("tm > TIME '08:00:00'", 2);
    both("ts = TIMESTAMP '2024-12-31 23:59:00'", 1);
    both("ts < TIMESTAMP '2024-12-31 23:59:00'", 2);
}

// DECIMAL sits on the seam: physical_value (b+tree) has no DECIMAL representation, so it's
// refused; the hash side round-trips it via append_decimal_payload, proven here with real
// inserts, not just a successful CREATE.
TEST_CASE("integration::cpp::test_index_key_type_gate::decimal_is_hash_only") {
    auto config = make_test_config(integration_fixture_path("test_index_key_type_gate/decimal"), true);
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE g;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE g.t (id BIGINT, n DECIMAL(10,2));")->is_success());
    REQUIRE(exec(d, "CREATE TABLE g.p (id BIGINT, n DECIMAL(10,2));")->is_success());

    // Ordered: refused while the table is still EMPTY, same as the main refusal case.
    refused(d, "CREATE INDEX i_n ON g.t (n);");

    // Hashed: accepted — and it must serve rows, otherwise the acceptance is a lie.
    REQUIRE(exec(d, "CREATE INDEX i_n_h ON g.t USING hash (n);")->is_success());
    for (const char* t : {"g.t", "g.p"}) {
        const std::string sql =
            std::string{"INSERT INTO "} + t + " (id, n) VALUES (1, 1.25), (2, 2.50), (3, 2.50);";
        INFO(sql);
        REQUIRE(exec(d, sql)->is_success());
    }
    auto with_idx = exec(d, "SELECT id FROM g.t WHERE n = 2.50;");
    auto no_idx = exec(d, "SELECT id FROM g.p WHERE n = 2.50;");
    REQUIRE(with_idx->is_success());
    REQUIRE(no_idx->is_success());
    CHECK(no_idx->size() == 2);
    CHECK(with_idx->size() == 2);
}

// physical_value carries no temporal tag: a DATE key is compared as its raw INT32 day count, so
// probes encoded post-restart must land in the same domain as keys written pre-restart. The NULL
// row rides along because a NULL key is legitimately absent from the index.
TEST_CASE("integration::cpp::test_index_key_type_gate::temporal_index_survives_restart") {
    auto config = make_test_config(integration_fixture_path("test_index_key_type_gate/restart"), true);

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        REQUIRE(exec(d, "CREATE DATABASE g;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE g.t (id BIGINT, dt DATE, ts TIMESTAMP);")->is_success());
        REQUIRE(exec(d, "CREATE INDEX i_dt ON g.t (dt);")->is_success());
        REQUIRE(exec(d, "CREATE INDEX i_ts ON g.t (ts);")->is_success());
        REQUIRE(exec(d,
                     "INSERT INTO g.t (id, dt, ts) VALUES "
                     "(1, DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:00'), "
                     "(2, DATE '2024-03-15', TIMESTAMP '2024-03-15 12:30:00'), "
                     "(3, DATE '2024-12-31', TIMESTAMP '2024-12-31 23:59:00'), "
                     "(4, NULL, NULL);")
                    ->is_success());
    }

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto count = [&](const std::string& pred, size_t expected) {
            auto cur = exec(d, "SELECT id FROM g.t WHERE " + pred + ";");
            INFO("post-restart predicate: " << pred);
            REQUIRE(cur->is_success());
            CHECK(cur->size() == expected);
        };
        count("dt = DATE '2024-03-15'", 1);
        count("dt < DATE '2024-12-31'", 2);
        count("ts = TIMESTAMP '2024-03-15 12:30:00'", 1);
        count("ts > TIMESTAMP '2024-01-01 00:00:00'", 2);
        count("dt IS NULL", 1);

        // The rehydrated index must also accept NEW temporal keys, not just answer old ones.
        REQUIRE(exec(d,
                     "INSERT INTO g.t (id, dt, ts) VALUES "
                     "(5, DATE '2024-06-01', TIMESTAMP '2024-06-01 06:00:00');")
                    ->is_success());
        count("dt = DATE '2024-06-01'", 1);
        count("dt > DATE '2024-01-01'", 3);
    }
}
