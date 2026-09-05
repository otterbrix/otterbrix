#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <set>
#include <string>

// CREATE INDEX must refuse a key type that the on-disk key encoders cannot represent.
//
// The encoders sit far below the statement and have no error channel: services::index::convert()
// returns a physical_value by value, and the binary key codec runs inside an actor coroutine whose
// unhandled_exception() is empty. Their only ways to report "I cannot encode this" are an abort or
// a wrong answer. Before the gate, an INTERVAL / TIMETZ / HUGEINT key produced BOTH, depending on
// the build: services::index::convert() had a `default: assert(false); return NA;` arm, so a Debug
// build aborted the process on the first row while an NDEBUG build collapsed every key of that
// column to the same NA value and served wrong rows from the index.
//
// NOTE ON COVERAGE: the NDEBUG-only silent-NA half of that behaviour is NOT observable from this
// suite, which builds Debug+DEV_MODE — there the same arm aborts. It is recorded here rather than
// pretended to be covered. What this suite does pin is the gate: the statement is refused before
// any key ever reaches an encoder, so neither half can be reached from user data at all.
//
// The tables are left EMPTY on purpose. CREATE INDEX on a populated table backfills, and the
// backfill would hit the encoder before the gate's verdict could be observed.

using namespace test_helpers;

namespace {

    // CREATE INDEX must be refused, and refused as index_create_fail specifically — a generic
    // parse/schema error would mean the statement died for the wrong reason.
    void refused(otterbrix::wrapper_dispatcher_t* d, const std::string& sql) {
        auto cur = exec(d, sql);
        INFO(sql);
        REQUIRE(cur);
        REQUIRE(cur->is_error());
        CHECK(cur->get_error().type == core::error_code_t::index_create_fail);
    }

} // namespace

TEST_CASE("integration::cpp::test_index_key_type_gate::unrepresentable_key_types_are_refused") {
    auto config = make_test_config("/tmp/otterbrix/integration/test_index_key_type_gate/refused", true);
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE g;")->is_success());
    // INTERVAL and TIMETZ are physically STRUCT; HUGEINT/UHUGEINT are 16-byte integers the
    // binary codec has no case for. None of the four can be encoded as an index key.
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

// The mirror image: every type the encoders DO carry must still be accepted. A gate that
// over-refuses is as much a defect as one that under-refuses.
TEST_CASE("integration::cpp::test_index_key_type_gate::representable_key_types_are_accepted") {
    auto config = make_test_config("/tmp/otterbrix/integration/test_index_key_type_gate/accepted", true);
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

// DATE / TIME / TIMESTAMP / TIMESTAMP_TZ are physically INT32/INT64 and order correctly the moment
// convert() maps them. This is the end-to-end half of that: rows go in through the index write
// path and come back out through an indexed lookup. NOTE: the batched write path appends through
// the binary codec (insert_bulk_unchecked) and never calls convert(), so on this baseline the
// pre-fix `default:` arm was NOT reached by INSERT — this case pins index/scan answer parity,
// while the abort half of the defect is pinned where convert() actually runs: the
// services::index::index_disk unit suite (convert_temporal_preserves_order red-proofs the abort,
// date_keys / timestamp_keys drive probes and bounds through the tree).
TEST_CASE("integration::cpp::test_index_key_type_gate::temporal_indexes_return_the_right_rows") {
    auto config = make_test_config("/tmp/otterbrix/integration/test_index_key_type_gate/temporal", true);
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

    // Both tables must give the SAME answer, and that answer must be the absolute one below.
    // Equality alone would pass on two identically wrong answers; a key that collapsed to NA
    // would make every predicate match every row, so the counts are what pins the fix.
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

// DECIMAL sits exactly on the seam between the two encoder families, so it gets its own
// case: the ordered (b+tree) side compares stored keys through physical_value, which has
// no DECIMAL representation (width/scale would be lost) — refused; the hashed side
// round-trips DECIMAL through the logical codec (append_decimal_payload) — accepted, and
// the acceptance is proven with rows, not just a successful CREATE: the INSERTs after it
// drive every key through the hash index's maintenance encoder.
TEST_CASE("integration::cpp::test_index_key_type_gate::decimal_is_hash_only") {
    auto config = make_test_config("/tmp/otterbrix/integration/test_index_key_type_gate/decimal", true);
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

// The rehydrate leg: a restart loads the in-memory index back from the on-disk b+tree
// (bootstrap full_scan -> reverse_convert). The b+tree hands keys back as physical_value,
// which has no temporal tag — a DATE key rehydrates as its raw INT32 day count and every
// later DATE probe is cast into that locked domain, so answers must not change across the
// restart. The NULL row rides along to pin the NA leg of reverse_convert (NULL keys are
// legitimately in the tree; collapsing or refusing them on rehydrate would corrupt this).
TEST_CASE("integration::cpp::test_index_key_type_gate::temporal_index_survives_restart") {
    auto config = make_test_config("/tmp/otterbrix/integration/test_index_key_type_gate/restart", true);

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

        // The rehydrated index must also take NEW temporal keys (the in-memory key domain
        // locked by the rehydrate must admit them).
        REQUIRE(exec(d,
                     "INSERT INTO g.t (id, dt, ts) VALUES "
                     "(5, DATE '2024-06-01', TIMESTAMP '2024-06-01 06:00:00');")
                    ->is_success());
        count("dt = DATE '2024-06-01'", 1);
        count("dt > DATE '2024-01-01'", 3);
    }
}
