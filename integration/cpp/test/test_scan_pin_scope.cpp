#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <string>

// A scan pins its block once per segment, not once per row.
//
// Pinning is not free: it takes the block handle's mutex, does atomics, and on release may allocate
// an eviction-queue node from the process-wide pool. Doing it per row turns a sequential read into
// millions of lock round-trips — and the profile of the SSB queries showed exactly that shape, with
// buffer_handle_t::~buffer_handle_t the largest frame of our own code.
//
// The cause is a loop in column_data_t::scan_vector that fetches every row individually and is then
// immediately overwritten by the vectorised scan() over the same range. This test states the
// invariant that loop violates: reading N rows must cost pins proportional to the number of
// SEGMENTS touched, and a segment holds DEFAULT_VECTOR_CAPACITY rows.
//
// Hidden by default ([.]) because it fills enough rows to span many segments. Run it with [scanpin].

namespace {
    void fill(otterbrix::wrapper_dispatcher_t* d, int rows) {
        constexpr int kBatch = 1000;
        for (int base = 0; base < rows; base += kBatch) {
            std::string sql = "INSERT INTO s.t (id, a, b, c) VALUES ";
            for (int i = 0; i < kBatch; ++i) {
                const int v = base + i;
                if (i != 0) {
                    sql += ", ";
                }
                sql += "(" + std::to_string(v) + ", " + std::to_string(v * 2) + ", " + std::to_string(v * 3) +
                       ", " + std::to_string(v * 5) + ")";
            }
            sql += ";";
            auto session = otterbrix::session_id_t();
            REQUIRE(d->execute_sql(session, sql)->is_success());
        }
    }
} // namespace

TEST_CASE("integration::cpp::test_scan_pin_scope::a_scan_pins_per_segment_not_per_row", "[.][scanpin]") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_scan_pin/scope");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = false;
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };

    constexpr int kRows = 200000;

    REQUIRE(exec("CREATE DATABASE s;")->is_success());
    REQUIRE(exec("CREATE TABLE s.t (id bigint, a bigint, b bigint, c bigint);")->is_success());
    fill(d, kRows);

    // A full aggregate over one column: the simplest shape that must read every row exactly once.
    components::table::storage::reset_buffer_pins();
    {
        auto cur = exec("SELECT SUM(a) FROM s.t;");
        REQUIRE(cur->is_success());
    }
    const auto pins = components::table::storage::buffer_pins();

    // Rows per segment is DEFAULT_VECTOR_CAPACITY, so one column of kRows rows spans kRows/1024
    // segments. Allow a generous multiple for the validity child column, the catalog reads the
    // statement performs, and repinning across scan states.
    const uint64_t segments = kRows / components::vector::DEFAULT_VECTOR_CAPACITY;
    const uint64_t bound = segments * 16;

    INFO("pins for a full scan of " << kRows << " rows: " << pins << " (segments " << segments
                                    << ", bound " << bound << ", one-per-row would be " << kRows << ")");
    // Positive control: a counter stuck at zero would satisfy any upper bound.
    REQUIRE(pins > 0);
    CHECK(pins <= bound);
}
