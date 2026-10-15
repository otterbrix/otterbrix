#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <optional>
#include <services/collection/executor.hpp>
#include <vector>

// ORDER BY ... NULLS FIRST / NULLS LAST.
//
// The parser has always captured the NULLS placement (SortBy::sortby_nulls) but the transformer
// dropped it, so an explicit NULLS FIRST/LAST was silently ignored and NULLs always landed in the
// SQL-standard DEFAULT position (ASC -> last, DESC -> first). These tests pin both the preserved
// defaults and the now-honored explicit overrides.

namespace {

    otterbrix::wrapper_dispatcher_t* make_space(test_spaces& space) {
        auto* dispatcher = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, sql)->is_success());
        };
        exec("CREATE DATABASE nulls_db;");
        exec("CREATE TABLE nulls_db.t (id INT, v INT);");
        // Rows deliberately inserted out of order; one row has a NULL v.
        exec("INSERT INTO nulls_db.t (id, v) VALUES (2, 20), (4, NULL), (1, 10), (3, 30);");
        return dispatcher;
    }

    // Assert the ordered `v` column equals `expected`, where an empty optional means SQL NULL.
    void check_order(otterbrix::wrapper_dispatcher_t* dispatcher,
                     const std::string& sql,
                     const std::vector<std::optional<int32_t>>& expected) {
        INFO(sql);
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, sql);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == expected.size());
        for (size_t row = 0; row < expected.size(); ++row) {
            if (expected[row].has_value()) {
                auto cell0 = cur->is_null(1, row); // column 1 is v
                REQUIRE_FALSE(cell0.has_error());
                REQUIRE_FALSE(cell0.value());
                auto cell1 = cur->get<int32_t>(1, row);
                REQUIRE_FALSE(cell1.has_error());
                REQUIRE(cell1.value() == *expected[row]);
            } else {
                auto cell2 = cur->is_null(1, row);
                REQUIRE_FALSE(cell2.has_error());
                REQUIRE(cell2.value());
            }
        }
    }

    constexpr std::optional<int32_t> N{}; // NULL slot

} // namespace

TEST_CASE("integration::cpp::order_by_nulls::default_placement") {
    auto config = test_create_config(integration_fixture_path("test_order_by_nulls/default_placement"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = make_space(space);

    // SQL standard defaults: ASC -> NULLs last, DESC -> NULLs first.
    check_order(dispatcher, "SELECT id, v FROM nulls_db.t ORDER BY v ASC;", {10, 20, 30, N});
    check_order(dispatcher, "SELECT id, v FROM nulls_db.t ORDER BY v DESC;", {N, 30, 20, 10});
}

TEST_CASE("integration::cpp::order_by_nulls::explicit_override") {
    auto config = test_create_config(integration_fixture_path("test_order_by_nulls/explicit_override"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = make_space(space);

    // Explicit placement wins over the direction default.
    check_order(dispatcher, "SELECT id, v FROM nulls_db.t ORDER BY v ASC NULLS FIRST;", {N, 10, 20, 30});
    check_order(dispatcher, "SELECT id, v FROM nulls_db.t ORDER BY v DESC NULLS LAST;", {30, 20, 10, N});
    // Explicit placement matching the default is a no-op (still correct).
    check_order(dispatcher, "SELECT id, v FROM nulls_db.t ORDER BY v ASC NULLS LAST;", {10, 20, 30, N});
    check_order(dispatcher, "SELECT id, v FROM nulls_db.t ORDER BY v DESC NULLS FIRST;", {N, 30, 20, 10});
}

TEST_CASE("integration::cpp::order_by_nulls::positional_key") {
    auto config = test_create_config(integration_fixture_path("test_order_by_nulls/positional_key"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = make_space(space);

    // NULLS placement must also thread through the positional (ORDER BY <n>) form.
    check_order(dispatcher, "SELECT id, v FROM nulls_db.t ORDER BY 2 ASC NULLS FIRST;", {N, 10, 20, 30});
}