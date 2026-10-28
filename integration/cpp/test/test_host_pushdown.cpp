// Pushdown: after optimize, every largest part of a plan that reads only one backend's tables is offered to that
// backend (storage_offer); what it takes runs there, and otterbrix runs the rest (docs/embedding-host-api.md,
// "Pushdown").

#include "host_fixture.hpp"

#include <algorithm>
#include <string>
#include <vector>

using host_fixture::host_t;
using host_fixture::strings;
using rows_t = host_actor::rows_t;
using code = core::error_code_t;

namespace {

    otterbrix::session_id_t any() { return otterbrix::session_id_t(); }

    const std::string orders = "m2.shop.public.orders";

    void shop(host_t& host) {
        host.register_orders();
        host.backend().set_rows(orders, {{1, 100}, {2, 200}, {3, 300}});
        host.backend().clear_journal();
    }

    rows_t sorted_rows(const components::cursor::cursor_t_ptr& cursor) {
        rows_t rows;
        for (const auto& chunk : cursor->chunks()) {
            for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                std::vector<int64_t> values;
                for (std::uint64_t column = 0; column < chunk.column_count(); ++column) {
                    values.push_back(chunk.get_value<int64_t>(column, row));
                }
                rows.push_back(std::move(values));
            }
        }
        std::sort(rows.begin(), rows.end());
        return rows;
    }

    strings lines_of(host_t& host, const std::string& sql) {
        auto cursor = host.db.ok(any(), sql);
        strings lines;
        for (const auto& chunk : cursor->chunks()) {
            for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                lines.emplace_back(chunk.get_value<std::string_view>(0, row));
            }
        }
        return lines;
    }

} // namespace

// A SELECT block the backend takes whole is one request: no scan reaches it.
TEST_CASE("integration::cpp::host_pushdown::a_taken_select_is_one_request") {
    host_t host("test_host_pushdown/select");
    shop(host);
    host.backend().set_take_policy({.whole_select = true});
    auto found = host.db.ok(any(), "SELECT * FROM m2.shop.public.orders WHERE id = 2;");
    CHECK(sorted_rows(found) == rows_t{{2, 200}});
    CHECK(host.backend().journal("scan").empty());
    CHECK(host.backend().journal("run") == strings{"run select " + orders + " where column 0"});
}

// A WHERE conjunct the backend leaves is otterbrix's, over the rows the backend reads.
TEST_CASE("integration::cpp::host_pushdown::a_residual_where_stays_with_otterbrix") {
    host_t host("test_host_pushdown/residual");
    shop(host);
    host.backend().set_take_policy({.first_conjunct = true});
    CHECK(sorted_rows(host.db.ok(any(), "SELECT * FROM m2.shop.public.orders WHERE id = 2 AND amount = 200;")) ==
          rows_t{{2, 200}});
    CHECK(sorted_rows(host.db.ok(any(), "SELECT * FROM m2.shop.public.orders WHERE id = 2 AND amount = 999;")).empty());
    CHECK(host.backend().journal("scan").empty());
    CHECK(host.backend().journal("run") ==
          strings{"run scan " + orders + " where column 0", "run scan " + orders + " where column 0"});
}

// EXPLAIN shows what the backend took, under the backend's own lines, and runs nothing.
TEST_CASE("integration::cpp::host_pushdown::explain_shows_the_remote_query") {
    host_t host("test_host_pushdown/explain");
    shop(host);
    host.backend().set_take_policy({.whole_select = true});
    CHECK(lines_of(host, "EXPLAIN SELECT * FROM m2.shop.public.orders WHERE id = 2;") ==
          strings{"Remote Query on m2", "  Request: select " + orders + " where column 0"});
    CHECK(host.backend().journal("run").empty());
    CHECK(host.backend().offers() == 1);
}

// A join of two tables of one backend is offered as one block; a join with otterbrix's own table offers the
// backend's part only, and a statement over otterbrix's tables offers nothing.
TEST_CASE("integration::cpp::host_pushdown::what_is_offered") {
    host_t host("test_host_pushdown/offered", nullptr, {"m2", "pg"});
    shop(host);
    host.db.ok(any(), "CREATE TABLE m2.shop.public.items (id BIGINT, qty BIGINT);");
    host.backend().set_rows("m2.shop.public.items", {{1, 5}, {3, 7}});
    host.register_database("pg", "db");
    host.db.ok(any(), "CREATE TABLE pg.db.public.notes (id BIGINT, note BIGINT);");
    host.backend("pg").set_rows("pg.db.public.notes", {{2, 22}});
    host.db.ok(any(), "CREATE SCHEMA loc;");
    host.db.ok(any(), "CREATE TABLE loc.t (id BIGINT);");
    host.db.ok(any(), "INSERT INTO loc.t (id) VALUES (1), (2);");

    host.db.ok(any(), "SELECT * FROM loc.t;");
    CHECK(host.backend().offers() == 0);

    auto joined = host.db.ok(any(),
                             "SELECT o.amount, i.qty FROM m2.shop.public.orders AS o JOIN m2.shop.public.items AS i "
                             "ON o.id = i.id;");
    CHECK(sorted_rows(joined) == rows_t{{100, 5}, {300, 7}});
    REQUIRE(host.backend().offers() == 1);
    CHECK(host.backend().offered().back().find("$join") != std::string::npos);

    auto two = host.db.ok(any(),
                          "SELECT o.amount, n.note FROM m2.shop.public.orders AS o JOIN pg.db.public.notes AS n "
                          "ON o.id = n.id;");
    CHECK(sorted_rows(two) == rows_t{{200, 22}});
    CHECK(host.backend().offers() == 2);
    CHECK(host.backend("pg").offers() == 1);
    CHECK(host.backend().offered().back().find("$join") == std::string::npos);

    auto mixed = host.db.ok(any(), "SELECT o.amount FROM m2.shop.public.orders AS o JOIN loc.t AS t ON o.id = t.id;");
    CHECK(sorted_rows(mixed) == rows_t{{100}, {200}});
    CHECK(host.backend().offers() == 3);
}

// The block a backend is offered is its own: a copy that points nowhere into otterbrix's plan, with its tables'
// columns, kept by the backend after the statement ended.
TEST_CASE("integration::cpp::host_pushdown::the_offered_block_is_the_backends") {
    host_t host("test_host_pushdown/block");
    shop(host);
    host.db.ok(any(), "SELECT * FROM m2.shop.public.orders WHERE id = 2;");
    host.db.ok(any(), "UPDATE m2.shop.public.orders SET amount = 1 WHERE id = 1;");
    const auto kept = host.backend().kept_blocks();
    REQUIRE(kept.size() == 2);
    for (const auto& block : kept) {
        INFO(block.root);
        CHECK_FALSE(block.foreign_pointers);
        CHECK(block.tables == strings{orders + ": id amount"});
    }
    CHECK(kept[0].root.rfind("$aggregate", 0) == 0);
    CHECK(kept[1].root.rfind("$update", 0) == 0);

    // A scalar sub-query of no aggregate is a join that gives an outer row one inner row at most: the copy says so.
    host.db.ok(any(), "CREATE TABLE m2.shop.public.items (id BIGINT, qty BIGINT);");
    host.db.ok(any(),
               "SELECT o.id, (SELECT i.qty FROM m2.shop.public.items AS i WHERE i.id = o.id) AS q "
               "FROM m2.shop.public.orders AS o;");
    const auto scalar = host.backend().kept_blocks();
    REQUIRE(scalar.size() == 3);
    INFO(scalar[2].root);
    CHECK(scalar[2].single_match_joins == 1);
}

// A page of a taken read always comes with its cursor; a non-empty page without one is refused.
TEST_CASE("integration::cpp::host_pushdown::a_block_page_without_a_cursor_is_refused") {
    host_t host("test_host_pushdown/page_without_cursor");
    shop(host);
    host.backend().set_take_policy({.whole_select = true});
    host.backend().set_pages_without_cursor(true);
    host.db.check_refused(any(),
                          "SELECT * FROM m2.shop.public.orders WHERE id = 2;",
                          code::invalid_parameter,
                          "the backend of connection \"m2\" answered a page without a cursor");
}

// A backend's refusal to plan a block is the statement's error; a take of a node the block does not have is refused.
TEST_CASE("integration::cpp::host_pushdown::a_wrong_answer_to_an_offer") {
    host_t host("test_host_pushdown/wrong");
    shop(host);
    host.backend().set_take_policy({.refuse_offer = true});
    host.db.check_refused(any(),
                          "SELECT * FROM m2.shop.public.orders;",
                          code::connection_closed,
                          "server m2: cannot plan the block");
    host.backend().set_take_policy({.take_a_missing_node = true});
    const auto error = host.db.refused(any(), "SELECT * FROM m2.shop.public.orders;");
    CHECK(error.type == code::invalid_parameter);
    CHECK(std::string{error.what}.starts_with("the backend of connection \"m2\" took a block wrongly"));
    CHECK(host.backend().journal("scan").empty());

    // A residual WHERE of a block that has none.
    host.backend().set_take_policy({.whole_select = true, .residual_without_where = true});
    host.db.check_refused(any(),
                          "SELECT * FROM m2.shop.public.orders;",
                          code::invalid_parameter,
                          "the backend of connection \"m2\" took a block wrongly: a residual WHERE is left of a "
                          "block without one");
}

// The pages of a taken read are checked against the columns otterbrix planned; a page holds at most
// DEFAULT_VECTOR_CAPACITY rows, so the backend answers a read of 2500 rows in three pages of one cursor.
TEST_CASE("integration::cpp::host_pushdown::the_pages_of_a_taken_read") {
    host_t host("test_host_pushdown/pages");
    shop(host);
    rows_t many;
    for (int64_t i = 0; i < 2500; ++i) {
        many.push_back({i, i % 7});
    }
    host.backend().set_rows(orders, many);
    host.backend().set_take_policy({.whole_select = true});
    auto all = host.db.ok(any(), "SELECT * FROM m2.shop.public.orders;");
    CHECK(all->size() == 2500);
    for (const auto& chunk : all->chunks()) {
        CHECK(chunk.size() <= components::vector::DEFAULT_VECTOR_CAPACITY);
    }
    CHECK(host.backend().block_pages() == 3);
    CHECK(host.backend().journal("run") == strings{"run select m2.shop.public.orders"});
    host.backend().set_take_policy({.whole_select = true, .short_block_pages = true});
    host.db.check_refused(any(),
                          "SELECT * FROM m2.shop.public.orders;",
                          code::invalid_parameter,
                          "the backend of connection \"m2\" answered a block with a page of other columns");
}

#ifdef NDEBUG
// A page wider than DEFAULT_VECTOR_CAPACITY rows cannot be built in Debug; release refuses one.
TEST_CASE("integration::cpp::host_pushdown::a_taken_read_page_wider_than_a_chunk_is_refused") {
    host_t host("test_host_pushdown/wide_pages");
    shop(host);
    rows_t many;
    for (int64_t i = 0; i < 2500; ++i) {
        many.push_back({i, i % 7});
    }
    host.backend().set_rows(orders, many);
    host.backend().set_take_policy({.whole_select = true, .wide_block_pages = true});
    host.db.check_refused(any(),
                          "SELECT * FROM m2.shop.public.orders;",
                          code::invalid_parameter,
                          "a storage answered a chunk of 2500 rows, more than the 1024 rows of a chunk");
}
#endif

// A taken read is a part of the transaction: the backend learns of its end; a correlated block runs again with the
// values of every drive.
TEST_CASE("integration::cpp::host_pushdown::a_taken_read_in_a_transaction_and_per_drive") {
    host_t host("test_host_pushdown/drive");
    shop(host);
    host.backend().set_take_policy({.whole_select = true});
    const auto session = otterbrix::session_id_t();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "SELECT * FROM m2.shop.public.orders WHERE id = 1;");
    host.db.ok(session, "COMMIT;");
    CHECK(host.backend().journal("release") == strings{"release"});

    host.db.ok(any(), "CREATE SCHEMA loc;");
    host.db.ok(any(), "CREATE TABLE loc.t (id BIGINT);");
    host.db.ok(any(), "INSERT INTO loc.t (id) VALUES (1), (3), (5);");
    host.backend().clear_journal();
    auto lateral = host.db.ok(any(),
                              "SELECT t.id, o.amount FROM loc.t AS t, "
                              "LATERAL (SELECT * FROM m2.shop.public.orders AS x WHERE x.id = t.id) AS o;");
    CHECK(sorted_rows(lateral) == rows_t{{1, 100}, {3, 300}});
    CHECK(host.backend().journal("run").size() == 3);
    CHECK(host.backend().journal("scan").empty());
}

// The optimizer's rules are the same over every storage: a WHERE over a sub-query of a backend's table reaches the
// table's block, so the backend can take it.
TEST_CASE("integration::cpp::host_pushdown::a_filter_over_a_subquery_reaches_the_block") {
    host_t host("test_host_pushdown/subquery_filter");
    shop(host);
    host.backend().set_take_policy({.whole_select = true});
    auto found = host.db.ok(any(), "SELECT * FROM (SELECT * FROM m2.shop.public.orders) AS s WHERE s.id = 2;");
    CHECK(sorted_rows(found) == rows_t{{2, 200}});
    CHECK(host.backend().journal("run") == strings{"run select " + orders + " where column 0"});
}

// RETURNING reads the columns it names, whether the backend took the DELETE or otterbrix ran it over the backend's
// scan, which reads only the columns the statement needs.
TEST_CASE("integration::cpp::host_pushdown::returning_reads_the_columns_it_names") {
    host_t host("test_host_pushdown/returning_columns");
    shop(host);
    host.db.ok(any(), "CREATE TABLE m2.shop.public.wide (id BIGINT, x BIGINT, amount BIGINT);");
    host.backend().set_rows("m2.shop.public.wide", {{1, 10, 100}, {2, 20, 200}, {3, 30, 300}, {4, 40, 400}});
    host.backend().clear_journal();

    CHECK(sorted_rows(host.db.ok(any(), "DELETE FROM m2.shop.public.wide WHERE id = 1 RETURNING amount + 1;")) ==
          rows_t{{101}});
    CHECK(host.backend().journal("run").empty());

    host.backend().set_take_policy({.simple_modify = true});
    CHECK(sorted_rows(host.db.ok(any(), "DELETE FROM m2.shop.public.wide WHERE id = 2 RETURNING amount + 1;")) ==
          rows_t{{201}});
    CHECK(sorted_rows(host.db.ok(any(), "UPDATE m2.shop.public.wide SET x = 33 WHERE id = 3 RETURNING amount, x;")) ==
          rows_t{{300, 33}});
    host.db.ok(any(), "DELETE FROM m2.shop.public.wide WHERE amount = 400;");
    CHECK(host.backend().journal("run").size() == 3);
    CHECK(host.backend().rows("m2.shop.public.wide") == rows_t{{3, 33, 300}});
}

// An uncorrelated sub-query (an InitPlan) is offered once, as a block of its own.
TEST_CASE("integration::cpp::host_pushdown::an_init_plan_is_offered_once") {
    host_t host("test_host_pushdown/init_plan");
    shop(host);
    host.db.ok(any(), "SELECT (SELECT max(amount) FROM m2.shop.public.orders) AS m;");
    CHECK(host.backend().offered().size() == 1);
    host.db.ok(any(),
               "SELECT id FROM m2.shop.public.orders WHERE amount > (SELECT min(amount) FROM m2.shop.public.orders);");
    const auto offered = host.backend().offered();
    for (const auto& block : offered) {
        UNSCOPED_INFO(block);
    }
    CHECK(offered.size() == 3);
    host.db.ok(any(),
               "SELECT id FROM m2.shop.public.orders WHERE amount IN (SELECT amount FROM m2.shop.public.orders "
               "WHERE id > (SELECT min(id) FROM m2.shop.public.orders));");
    const auto nested = host.backend().offered();
    for (const auto& block : nested) {
        UNSCOPED_INFO(block);
    }
    CHECK(nested.size() == 6);

    host.db.ok(any(), "EXPLAIN SELECT (SELECT max(amount) FROM m2.shop.public.orders) AS m;");
    CHECK(host.backend().offered().size() == 7);
    host.db.ok(any(), "EXPLAIN ANALYZE SELECT (SELECT max(amount) FROM m2.shop.public.orders) AS m;");
    CHECK(host.backend().offered().size() == 8);
}

// Two connections of one actor are two backends: a join of their tables is no block of one backend, each table's
// read is offered on its own, and each connection is a participant of its own.
TEST_CASE("integration::cpp::host_pushdown::two_connections_of_one_actor_are_two_blocks") {
    host_t host("test_host_pushdown/one_actor", nullptr, {"pg_main", "pg_reports=pg_main"});
    host.register_database("pg_main", "db");
    host.register_database("pg_reports", "db");
    host.db.ok(any(), "CREATE TABLE pg_main.db.public.a (id BIGINT);");
    host.db.ok(any(), "CREATE TABLE pg_reports.db.public.b (id BIGINT);");
    host.backend("pg_main").set_rows("pg_main.db.public.a", {{1}, {2}});
    host.backend("pg_main").set_rows("pg_reports.db.public.b", {{2}, {3}});
    host.backend("pg_main").clear_journal();
    const auto offers_before = host.backend("pg_main").offers();
    const auto session = otterbrix::session_id_t();
    host.db.ok(session, "BEGIN;");
    auto joined = host.db.ok(session,
                             "SELECT a.id FROM pg_main.db.public.a AS a JOIN pg_reports.db.public.b AS b "
                             "ON a.id = b.id;");
    CHECK(sorted_rows(joined) == rows_t{{2}});
    host.db.ok(session, "ROLLBACK;");
    CHECK(host.backend("pg_main").offers() - offers_before == 2);
    CHECK(host.backend("pg_main").journal("abort") == strings{"abort 0", "abort 0"});
}
