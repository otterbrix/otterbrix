#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/table/row_group.hpp>
#include <string>

// The late-materialisation gather must not leave borrowed string views in a chunk that outlives
// their pin.
//
// When a filter is selective enough (fewer than a fifth of the rows survive), row_group_t stops
// scanning whole vectors and gathers the surviving rows one at a time with fetch_row. That gather
// declares its column_fetch_state INSIDE the branch, so every pin it took is released when the
// branch ends — while the result chunk it just filled is returned to the caller.
//
// For a STRING column that would be a use-after-free: fetch_row's string leg writes a
// std::string_view BORROWED from the pinned block unless result_outlives_pins tells it to copy
// into the result's own heap (fetch_string_owned documents why: an unpinned block may be evicted
// and reloaded at a new address, leaving the borrowed view dangling). Borrowing used to be
// survivable because a block whose pin is gone mostly stayed where it was; the buffer pool can now
// spill a transient block to disk and reload it at a different address.
//
// The test counts rather than trying to catch the dangling read: a use-after-free reproduces only
// when the pool happens to reclaim that block, so a behavioural test would be flaky in the direction
// that matters — passing while the defect is present.
//
// Hidden by default ([.]). Run it with [gatherstr].

TEST_CASE("integration::cpp::test_gather_string_lifetime::gather_leaves_no_borrowed_strings", "[.][gatherstr]") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_gather_string/lifetime");
    test_clear_directory(config);
    config.wal.on = false;
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };

    constexpr int kRows = 50000;
    constexpr int kBatch = 1000;

    REQUIRE(exec("CREATE DATABASE g;")->is_success());
    REQUIRE(exec("CREATE TABLE g.t (id bigint, tag text, v bigint);")->is_success());
    for (int base = 0; base < kRows; base += kBatch) {
        std::string sql = "INSERT INTO g.t (id, tag, v) VALUES ";
        for (int i = 0; i < kBatch; ++i) {
            const int n = base + i;
            if (i != 0) {
                sql += ", ";
            }
            sql += "(" + std::to_string(n) + ", 'tag_value_" + std::to_string(n) + "', " + std::to_string(n) + ")";
        }
        sql += ";";
        auto session = otterbrix::session_id_t();
        REQUIRE(d->execute_sql(session, sql)->is_success());
    }

    // Selective enough to take the gather branch (it needs fewer than a fifth of the rows to
    // survive), and it projects the text column so the gather has a STRING column to fill.
    components::table::reset_gathered_borrowed_strings();
    {
        auto cur = exec("SELECT tag FROM g.t WHERE id > 49900;");
        REQUIRE(cur->is_success());
        // Positive control on the query itself: if the filter matched nothing, the gather never ran
        // and the count below would be zero for the wrong reason.
        REQUIRE(cur->size() > 0);
        // And the values must be right — an owned copy that copies the wrong bytes would satisfy the
        // counter while corrupting the answer.
        CHECK(cur->size() == 99);
    }
    const auto borrowed = components::table::gathered_borrowed_strings();

    INFO("string cells the gather filled with a view borrowed from a pin it then dropped: " << borrowed);
    CHECK(borrowed == 0);
}
