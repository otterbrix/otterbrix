#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/table/column_data.hpp>
#include <string>

// The append state's pin outlives the block it points at.
//
// While a column is being appended to, column_append_state holds a buffer_handle_t pinning the
// current segment's block. When that segment fills, column_data_t::append_data creates the next
// segment and then calls transition_segment_to_disk on the filled one, which ends with
// replace_segment_at_index — dropping the old column_segment_t and with it the block_handle_t the
// append state is still pointing at. The stale buffer_handle_t is only replaced a few lines later by
// initialize_append, and its destructor unpins through the freed pointer.
//
// transition_segment_to_disk releases its OWN pin before the swap for exactly this reason. It
// cannot release the caller's, and nothing else does.
//
// It survives today only because the freed block_handle_t is not immediately reused: luck, not a
// guarantee, and the more eagerly the pool reclaims memory the less of that luck is left.
//
// The counter is checked instead of relying on a sanitizer: the DEV_MODE build is what CI runs, and
// ASAN on macOS is blind inside the pmr pool anyway.
//
// Hidden by default ([.]) because it writes enough rows to fill and transition several segments.
// Run it with [appendpin].

TEST_CASE("integration::cpp::test_append_pin_lifetime::no_transition_happens_under_a_live_pin", "[.][appendpin]") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_append_pin/lifetime");
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

    REQUIRE(exec("CREATE DATABASE p;")->is_success());
    REQUIRE(exec("CREATE TABLE p.t (id bigint, a bigint, b bigint) ;")->is_success());

    components::table::reset_transitions_with_live_pin();

    // Enough rows to fill many segments, so the on-fill transition path runs repeatedly.
    constexpr int kRows = 120000;
    constexpr int kBatch = 1000;
    for (int base = 0; base < kRows; base += kBatch) {
        std::string sql = "INSERT INTO p.t (id, a, b) VALUES ";
        for (int i = 0; i < kBatch; ++i) {
            const int v = base + i;
            if (i != 0) {
                sql += ", ";
            }
            sql += "(" + std::to_string(v) + ", " + std::to_string(v * 2) + ", " + std::to_string(v * 3) + ")";
        }
        sql += ";";
        auto session = otterbrix::session_id_t();
        REQUIRE(d->execute_sql(session, sql)->is_success());
    }

    const auto offending = components::table::transitions_with_live_pin();
    const auto total = components::table::segment_transitions();
    WARN("on-fill transitions: " << total << ", of them under a live append pin: " << offending);
    // Positive control: zero offending transitions out of zero transitions proves nothing.
    REQUIRE(total > 0);
    CHECK(offending == 0);

    // The data must be intact either way — this is a lifetime defect, not a data one, so a green
    // read here does NOT mean the pin was safe. It is here so a fix that breaks the append is caught.
    {
        auto cur = exec("SELECT SUM(a) FROM p.t;");
        REQUIRE(cur->is_success());
    }
}
