#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/table/column_data.hpp>
#include <components/vector/indexing_vector.hpp>
#include <string>

// No segment may be swapped for its disk-backed twin while anyone still pins it; the DEV_MODE
// counter sits at replace_segment_at_index's swap (both the append-fill and row-group-close
// paths funnel through it) and watches for pins other than the swap's own. Checked via counter
// since ASAN on macOS is blind inside the pmr pool; hidden by default ([.]), run with [appendpin].

TEST_CASE("integration::cpp::test_append_pin_lifetime::no_transition_happens_under_a_live_pin", "[.][appendpin]") {
    auto config = test_create_config(integration_fixture_path("test_append_pin/lifetime"));
    test_clear_directory(config);
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
    WARN("transitions to disk: " << total << ", of them under a live pin: " << offending);
    constexpr uint64_t kClosedGroupsFloor = kRows / components::vector::DEFAULT_VECTOR_CAPACITY - 1;
    REQUIRE(total >= kClosedGroupsFloor * 3);
    CHECK(offending == 0);

    {
        auto cur = exec("SELECT SUM(a) FROM p.t;");
        REQUIRE(cur->is_success());
    }
}
