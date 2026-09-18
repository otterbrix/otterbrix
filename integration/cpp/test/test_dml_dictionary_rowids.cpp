#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

// A filtered scan's DICTIONARY vector indexing() holds the in-vector position (0..1023), but
// operator_delete/operator_update read it as the absolute row id, off by vector_index * 1024 — so a predicate
// past the first vector hits rows 1024*k earlier while still reporting the correct count. Both cases assert
// CONTENT, not count, and are currently green because the corruption has not reproduced through SQL yet.

TEST_CASE("integration::cpp::dml_dictionary_rowids::delete_past_first_vector_kills_the_right_rows") {
    auto config = test_create_config(integration_fixture_path("test_dml_dictionary_rowids/del"));
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

    // 2900..2910 falls inside the third 1024-row vector.
    auto del = exec("DELETE FROM b.t WHERE id >= 2900 AND id <= 2910;");
    REQUIRE(del->is_success());

    auto count = exec("SELECT id FROM b.t;");
    REQUIRE(count->is_success());
    CHECK(count->size() == static_cast<size_t>(kRows - 11));

    auto requested = exec("SELECT id FROM b.t WHERE id = 2905;");
    REQUIRE(requested->is_success());
    CHECK(requested->size() == 0);

    auto innocent = exec("SELECT id FROM b.t WHERE id = 856;");
    REQUIRE(innocent->is_success());
    CHECK(innocent->size() == 1);
}

TEST_CASE("integration::cpp::dml_dictionary_rowids::update_past_first_vector_hits_the_right_rows") {
    auto config = test_create_config(integration_fixture_path("test_dml_dictionary_rowids/upd"));
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

    auto requested = exec("SELECT v FROM b.t WHERE id = 2905;");
    REQUIRE(requested->is_success());
    REQUIRE(requested->size() == 1);
    CHECK(requested->value(0, 0).value<int64_t>() == 0);

    auto innocent = exec("SELECT v FROM b.t WHERE id = 858;");
    REQUIRE(innocent->is_success());
    REQUIRE(innocent->size() == 1);
    CHECK(innocent->value(0, 0).value<int64_t>() == 858);
}
