#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

// A0 (plan): content guards for DML row-id addressing past the first 1024-row vector.
//
// STATUS: the predicted DICTIONARY-branch corruption did NOT reproduce through SQL in
// either storage mode — these cases are GREEN on HEAD. Reachability of the branch is under
// a dedicated investigation; until it lands, these tests pin the CONTENT contract (the
// silent-corruption shape would pass any count-based check) and will catch any regression
// the A0 rework could introduce.
//
// A filtered scan slices every column into a DICTIONARY vector whose indexing() holds the
// position INSIDE the 1024-row vector (column_data.cpp filter_scan/select), while the scan
// stamps chunk.row_ids with the ABSOLUTE id = vector_index * 1024 + position
// (row_group.cpp:521-526) — already filter-aligned. operator_delete/operator_update take the
// DICTIONARY branch and use indexing().get_index(i) as the absolute id, i.e. the correct id
// MINUS vector_index * 1024. On any table longer than 1024 rows a predicate matching rows
// past the first vector deletes/updates EXISTING rows 1024*k positions earlier and reports
// a full matched-count.
//
// The checks below assert TABLE CONTENT after the operation, not the count — the count is
// right either way, which is exactly why the corruption is silent.

TEST_CASE("integration::cpp::dml_dictionary_rowids::delete_past_first_vector_kills_the_right_rows") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_dml_dictionary_rowids/del");
    test_clear_directory(config);
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };
    REQUIRE(exec("CREATE DATABASE b;")->is_success());
    REQUIRE(exec("CREATE TABLE b.t (id bigint, v bigint);")->is_success());

    constexpr int64_t kRows = 3000;
    for (int64_t start = 1; start <= kRows; start += 500) {
        std::string sql = "INSERT INTO b.t (id, v) VALUES ";
        for (int64_t i = start; i < start + 500 && i <= kRows; i++) {
            if (i != start) {
                sql += ", ";
            }
            sql += "(" + std::to_string(i) + ", " + std::to_string(i) + ")";
        }
        sql += ";";
        REQUIRE(exec(sql)->is_success());
    }

    // Predicate matches ONLY rows in the third 1024-row vector (positions 2899..2909).
    auto del = exec("DELETE FROM b.t WHERE id >= 2900 AND id <= 2910;");
    REQUIRE(del->is_success());

    // Count is right either way — the silent part.
    auto count = exec("SELECT id FROM b.t;");
    REQUIRE(count->is_success());
    CHECK(count->size() == static_cast<size_t>(kRows - 11));

    // CONTENT: the requested rows must be gone...
    auto requested = exec("SELECT id FROM b.t WHERE id = 2905;");
    REQUIRE(requested->is_success());
    CHECK(requested->size() == 0);

    // ...and the innocent rows ~1024*2 positions earlier must SURVIVE. On HEAD the
    // DICTIONARY branch deletes them instead.
    auto innocent = exec("SELECT id FROM b.t WHERE id = 856;");
    REQUIRE(innocent->is_success());
    CHECK(innocent->size() == 1);
}

TEST_CASE("integration::cpp::dml_dictionary_rowids::update_past_first_vector_hits_the_right_rows") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_dml_dictionary_rowids/upd");
    test_clear_directory(config);
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };
    REQUIRE(exec("CREATE DATABASE b;")->is_success());
    REQUIRE(exec("CREATE TABLE b.t (id bigint, v bigint);")->is_success());

    constexpr int64_t kRows = 3000;
    for (int64_t start = 1; start <= kRows; start += 500) {
        std::string sql = "INSERT INTO b.t (id, v) VALUES ";
        for (int64_t i = start; i < start + 500 && i <= kRows; i++) {
            if (i != start) {
                sql += ", ";
            }
            sql += "(" + std::to_string(i) + ", " + std::to_string(i) + ")";
        }
        sql += ";";
        REQUIRE(exec(sql)->is_success());
    }

    auto upd = exec("UPDATE b.t SET v = 0 WHERE id = 2905;");
    REQUIRE(upd->is_success());

    // The requested row must carry the new value...
    auto requested = exec("SELECT v FROM b.t WHERE id = 2905;");
    REQUIRE(requested->is_success());
    REQUIRE(requested->size() == 1);
    CHECK(requested->value(0, 0).value<int64_t>() == 0);

    // ...and the innocent row far earlier must be untouched. On HEAD the DICTIONARY branch
    // rewrites it instead.
    auto innocent = exec("SELECT v FROM b.t WHERE id = 858;");
    REQUIRE(innocent->is_success());
    REQUIRE(innocent->size() == 1);
    CHECK(innocent->value(0, 0).value<int64_t>() == 858);
}
