#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/tests/temp_dir.hpp>
#include <optional>
#include <string>
#include <vector>

// A NULL ARRAY / LIST / STRUCT cell has no interior. Reading an element of it -- v[i], a struct
// field -- must yield NULL, and a predicate over that element must be UNKNOWN (the row excluded),
// never a value read out of a garbage (offset,length)/stride computed from the NULL row.
//
// Before the fix, the deep-path accessor descended into the ARRAY/LIST/STRUCT branch without
// checking the parent cell's validity, so a NULL array cell's subscript returned an arbitrary
// element of the flat child buffer.

namespace {

    using opt = std::optional<int64_t>;

    template<typename D>
    bool ok(D* d, const std::string& sql) {
        auto s = otterbrix::session_id_t();
        auto c = d->execute_sql(s, sql);
        return c && c->is_success();
    }

    template<typename D>
    size_t rows(D* d, const std::string& sql) {
        auto s = otterbrix::session_id_t();
        auto c = d->execute_sql(s, sql);
        REQUIRE(c);
        REQUIRE(c->is_success());
        return c->size();
    }

    template<typename D>
    std::vector<opt> read_col(D* d, const std::string& sql, uint64_t col = 0) {
        auto s = otterbrix::session_id_t();
        auto c = d->execute_sql(s, sql);
        std::vector<opt> out;
        REQUIRE(c);
        REQUIRE(c->is_success());
        for (uint64_t r = 0; r < c->size(); ++r) {
            auto v = c->value(col, r);
            out.push_back(v.is_null() ? opt{} : opt{v.template value<int64_t>()});
        }
        return out;
    }

} // namespace

TEST_CASE("integration::cpp::null_deep::array_subscript_of_null_cell") {
    auto config = test_create_config(test_temp_path("test_null_deep/arr"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE DATABASE dp;"));
    // A declared fixed-size array column. Row 1 and row 3 have arrays; row 2's array cell is
    // NULL as a whole (an explicit NULL), so v[i] has no interior to read.
    REQUIRE(ok(d, "CREATE TABLE dp.t (id bigint, v int[3]);"));
    REQUIRE(ok(d, "INSERT INTO dp.t (id, v) VALUES (1, ARRAY[10, 20, 30]);"));
    REQUIRE(ok(d, "INSERT INTO dp.t (id, v) VALUES (2, NULL);"));
    REQUIRE(ok(d, "INSERT INTO dp.t (id, v) VALUES (3, ARRAY[40, 50, 60]);"));

    // Subscript of the NULL cell (row 2) projects NULL, not an element read from a stride
    // computed off the NULL row. This exercises the in-memory deep-path accessor directly.
    CHECK(read_col(d, "SELECT v[1] FROM dp.t ORDER BY id;") == std::vector<opt>{10, {}, 40});
    CHECK(read_col(d, "SELECT v[2] FROM dp.t ORDER BY id;") == std::vector<opt>{20, {}, 50});
    CHECK(read_col(d, "SELECT v[3] FROM dp.t ORDER BY id;") == std::vector<opt>{30, {}, 60});
    // (A predicate over v[i] against a NULL array cell takes the storage-scan pushdown path, not
    //  this accessor; its own three-valued handling is covered by the pushdown array-element test.)
}
