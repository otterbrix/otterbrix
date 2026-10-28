// A test host whose remote tables are registered in the catalog (connection m2 > database shop > schema public) and
// kept by the backend actor of connection m2. otterbrix runs the SQL over the storage contract's messages: the
// backend's scans number the rows they give out, and its writes change the rows it keeps.

#include "host_fixture.hpp"
#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <algorithm>
#include <string>
#include <vector>

using namespace components;
using host_fixture::host_t;
using host_fixture::strings;
using rows_t = host_actor::rows_t;

namespace {

    const std::string orders = "m2.shop.public.orders";

    components::cursor::cursor_t_ptr
    run(otterbrix::wrapper_dispatcher_t* dispatcher, const otterbrix::session_id_t& session, const std::string& sql) {
        return dispatcher->execute_sql(session, sql);
    }

    components::cursor::cursor_t_ptr run(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        return dispatcher->execute_sql(otterbrix::session_id_t(), sql);
    }

    using session_sql::error_of;

    using column_spec_t = std::pair<std::string, types::logical_type>;

    // m2.shop.public.<name> with these columns (BIGINT or TEXT), created in the backend.
    void register_table(host_t& host, const std::string& name, const std::vector<column_spec_t>& columns) {
        std::string sql = "CREATE TABLE m2.shop.public." + name + " (";
        for (std::size_t i = 0; i < columns.size(); ++i) {
            const auto& [column, type] = columns[i];
            sql += (i == 0 ? "" : ", ") + column + (type == types::logical_type::BIGINT ? " BIGINT" : " TEXT");
        }
        host.db.ok(otterbrix::session_id_t(), sql + ");");
        host.backend().clear_journal();
    }

    // m2.shop.public.orders(id, amount) holding (1, 100), (2, 200), (3, 300).
    void declare_orders(host_t& host) {
        register_table(host, "orders", {{"id", types::logical_type::BIGINT}, {"amount", types::logical_type::BIGINT}});
        host.backend().set_rows(orders, {{1, 100}, {2, 200}, {3, 300}});
    }

    void drop_table(host_t& host, const std::string& name) {
        host.db.ok(otterbrix::session_id_t(), "DROP TABLE m2.shop.public." + name + ";");
    }

    std::uint32_t oid_of(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& relname) {
        auto found = run(dispatcher, "SELECT oid FROM pg_catalog.pg_class WHERE relname = '" + relname + "';");
        REQUIRE(found->size() == 1);
        return found->chunks().front().get_value<std::uint32_t>(0, 0);
    }

    // A statement of a session whose place is m2.shop.public: a column can be qualified by the bare table name.
    components::cursor::cursor_t_ptr run_in_shop(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        REQUIRE(run(dispatcher, session, "SET search_path = 'm2.shop.public';")->is_success());
        return run(dispatcher, session, sql);
    }

    void flush_every_row(configuration::config& config) { config.execution.dml_flush_row_threshold = 1; }

    std::vector<int64_t> int_row(const vector::data_chunk_t& chunk, std::uint64_t row) {
        std::vector<int64_t> values;
        for (std::uint64_t col = 0; col < chunk.column_count(); ++col) {
            values.push_back(chunk.get_value<int64_t>(col, row));
        }
        return values;
    }

    rows_t sorted_int_rows(const components::cursor::cursor_t_ptr& cursor) {
        rows_t rows;
        for (const auto& chunk : cursor->chunks()) {
            for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                rows.push_back(int_row(chunk, row));
            }
        }
        std::sort(rows.begin(), rows.end());
        return rows;
    }

    rows_t sorted(rows_t rows) {
        std::sort(rows.begin(), rows.end());
        return rows;
    }

    std::vector<std::string> explain_lines(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto cursor = run(dispatcher, sql);
        INFO(sql << " -> " << error_of(cursor));
        REQUIRE(cursor->is_success());
        std::vector<std::string> lines;
        for (std::size_t row = 0; row < cursor->size(); ++row) {
            auto cell = cursor->get<std::string_view>(0, row);
            REQUIRE_FALSE(cell.has_error());
            lines.emplace_back(cell.value());
        }
        return lines;
    }

} // namespace

#define HOST_TEST_BOILERPLATE(DIR) HOST_TEST_BOILERPLATE_CAPPED(DIR, nullptr, 0)
#define HOST_TEST_BOILERPLATE_WITH(DIR, CONFIGURE) HOST_TEST_BOILERPLATE_CAPPED(DIR, CONFIGURE, 0)
// CAP: the most rows one write message to m2 carries (0: no limit), set when the engine opens.
#define HOST_TEST_BOILERPLATE_CAPPED(DIR, CONFIGURE, CAP)                                                              \
    host_t host(DIR, CONFIGURE, {"m2"}, {{"m2", CAP}});                                                                \
    auto* dispatcher = host.db.dispatcher();                                                                           \
    host.register_shop();

// The registration decides the name: none — does not exist; registered — the backend's rows; taken away — does not
// exist again.
TEST_CASE("integration::cpp::host_names::the_registration_decides_the_name") {
    HOST_TEST_BOILERPLATE("test_host_names/row_decides")

    auto missing = run(dispatcher, "SELECT * FROM m2.shop.public.orders;");
    REQUIRE(missing->is_error());
    CHECK(std::string{missing->get_error().what}.find("does not exist") != std::string::npos);

    declare_orders(host);
    auto found = run(dispatcher, "SELECT id, amount FROM m2.shop.public.orders;");
    REQUIRE(found->is_success());
    REQUIRE(sorted_int_rows(found) == rows_t{{1, 100}, {2, 200}, {3, 300}});

    drop_table(host, "orders");
    auto gone = run(dispatcher, "SELECT * FROM m2.shop.public.orders;");
    REQUIRE(gone->is_error());
    CHECK(std::string{gone->get_error().what}.find("does not exist") != std::string::npos);
}

TEST_CASE("integration::cpp::host_names::join_a_storage_table_with_a_local_table") {
    HOST_TEST_BOILERPLATE("test_host_names/join_local")
    declare_orders(host);
    REQUIRE(run(dispatcher, "CREATE SCHEMA shopdb;")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE shopdb.customers (id BIGINT, bonus BIGINT);")->is_success());
    REQUIRE(run(dispatcher, "INSERT INTO shopdb.customers (id, bonus) VALUES (1, 7), (3, 9), (5, 11);")->is_success());

    auto joined = run(dispatcher,
                      "SELECT o.id, o.amount, c.bonus FROM m2.shop.public.orders AS o "
                      "JOIN shopdb.customers AS c ON o.id = c.id;");
    REQUIRE(joined->is_success());
    REQUIRE(sorted_int_rows(joined) == rows_t{{1, 100, 7}, {3, 300, 9}});
}

// The registered columns (pg_attribute) drive validation and the types of the result.
TEST_CASE("integration::cpp::host_names::registered_columns_drive_validation_and_types") {
    HOST_TEST_BOILERPLATE("test_host_names/declared")
    register_table(host,
                   "orders",
                   {{"id", types::logical_type::BIGINT}, {"label", types::logical_type::STRING_LITERAL}});
    host.backend().set_rows(orders, {{1, 100}, {2, 200}, {3, 300}});

    REQUIRE(run(dispatcher, "SELECT nope FROM m2.shop.public.orders;")->is_error());

    auto labels = run(dispatcher, "SELECT label FROM m2.shop.public.orders WHERE id = 2;");
    INFO(error_of(labels));
    REQUIRE(labels->is_success());
    REQUIRE(labels->size() == 1);
    REQUIRE(labels->chunks().front().data[0].type().type() == types::logical_type::STRING_LITERAL);
    auto cell0 = labels->get<std::string_view>(0, 0);
    REQUIRE_FALSE(cell0.has_error());
    REQUIRE(cell0.value() == "s200");

    auto sums = run(dispatcher, "SELECT id + 1 AS n FROM m2.shop.public.orders WHERE id = 3;");
    REQUIRE(sums->is_success());
    auto cell1 = sums->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell1.has_error());
    REQUIRE(cell1.value() == 4);
}

TEST_CASE("integration::cpp::host_names::count_star_counts_the_storage_rows") {
    HOST_TEST_BOILERPLATE("test_host_names/count_star")
    declare_orders(host);
    auto counted = run(dispatcher, "SELECT count(*) AS c FROM m2.shop.public.orders;");
    REQUIRE(counted->is_success());
    REQUIRE(counted->size() == 1);
    auto cell2 = counted->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell2.has_error());
    REQUIRE(cell2.value() == 3);
}

// A statement over local tables only sends the backend nothing.
TEST_CASE("integration::cpp::host_names::local_statements_never_reach_the_backend") {
    HOST_TEST_BOILERPLATE("test_host_names/local_only")
    declare_orders(host);
    REQUIRE(run(dispatcher, "CREATE SCHEMA loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE loc.a (k BIGINT, v BIGINT);")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE loc.b (k BIGINT, w BIGINT);")->is_success());
    REQUIRE(run(dispatcher, "INSERT INTO loc.a (k, v) VALUES (1, 10), (2, 20);")->is_success());
    REQUIRE(run(dispatcher, "INSERT INTO loc.b (k, w) VALUES (1, 5);")->is_success());
    REQUIRE(run(dispatcher, "CREATE VIEW loc.av AS SELECT k, v FROM loc.a;")->is_success());
    host.backend().clear_journal();
    const char* statements[] = {
        "SELECT * FROM loc.a;",
        "SELECT count(*) FROM loc.a;",
        "SELECT a.v, b.w FROM loc.a AS a JOIN loc.b AS b ON a.k = b.k;",
        "SELECT a.v FROM loc.a AS a, loc.b AS b WHERE a.k = b.k;",
        "WITH x AS (SELECT k FROM loc.a) SELECT * FROM x;",
        "SELECT v FROM loc.a WHERE k IN (SELECT k FROM loc.b);",
        "SELECT * FROM loc.av;",
        "UPDATE loc.a SET v = 11 WHERE k = 1;",
        "DELETE FROM loc.b WHERE k = 9;",
        "INSERT INTO loc.b (k, w) SELECT k, v FROM loc.a;",
    };
    for (const auto* sql : statements) {
        INFO(sql);
        REQUIRE(run(dispatcher, sql)->is_success());
    }
    CHECK(host.backend().journal().empty());

    REQUIRE(run(dispatcher, "SELECT * FROM m2.shop.public.orders;")->is_success());
    CHECK(host.backend().journal("scan") == strings{"scan " + orders});
}

TEST_CASE("integration::cpp::host_names::dml_on_a_local_table_reads_a_storage_table") {
    HOST_TEST_BOILERPLATE("test_host_names/dml_embedded")
    declare_orders(host);
    REQUIRE(run(dispatcher, "CREATE SCHEMA loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE loc.t (id BIGINT, amount BIGINT);")->is_success());

    SECTION("INSERT ... SELECT copies the storage rows") {
        REQUIRE(run(dispatcher, "INSERT INTO loc.t (id, amount) SELECT id, amount FROM m2.shop.public.orders;")
                    ->is_success());
        auto copied = run(dispatcher, "SELECT id, amount FROM loc.t;");
        REQUIRE(copied->is_success());
        REQUIRE(sorted_int_rows(copied) == rows_t{{1, 100}, {2, 200}, {3, 300}});
    }
    SECTION("UPDATE ... FROM reads the storage rows") {
        REQUIRE(run(dispatcher, "INSERT INTO loc.t (id, amount) VALUES (1, 0), (5, 0);")->is_success());
        auto upd = run(dispatcher,
                       "UPDATE loc.t SET amount = o.amount FROM m2.shop.public.orders AS o WHERE loc.t.id = o.id;");
        INFO(error_of(upd));
        REQUIRE(upd->is_success());
        auto updated = run(dispatcher, "SELECT id, amount FROM loc.t;");
        REQUIRE(sorted_int_rows(updated) == rows_t{{1, 100}, {5, 0}});
    }
    SECTION("DELETE ... USING reads the storage rows") {
        REQUIRE(run(dispatcher, "INSERT INTO loc.t (id, amount) VALUES (2, 0), (7, 0);")->is_success());
        auto del = run(dispatcher, "DELETE FROM loc.t USING m2.shop.public.orders AS o WHERE loc.t.id = o.id;");
        INFO(error_of(del));
        REQUIRE(del->is_success());
        auto left = run(dispatcher, "SELECT id, amount FROM loc.t;");
        REQUIRE(sorted_int_rows(left) == rows_t{{7, 0}});
    }
}

TEST_CASE("integration::cpp::host_names::a_storage_error_reaches_the_cursor") {
    HOST_TEST_BOILERPLATE("test_host_names/operator_error")
    declare_orders(host);
    host.backend().set_unreachable(orders, "server m2: connection refused");
    REQUIRE(run(dispatcher, "CREATE SCHEMA loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE loc.c (id BIGINT);")->is_success());

    for (const char* sql : {"SELECT * FROM m2.shop.public.orders;",
                            "SELECT count(*) FROM m2.shop.public.orders;",
                            "SELECT o.id FROM m2.shop.public.orders AS o JOIN loc.c AS c ON o.id = c.id;",
                            "INSERT INTO loc.c (id) SELECT id FROM m2.shop.public.orders;"}) {
        INFO(sql);
        auto cursor = run(dispatcher, sql);
        REQUIRE(cursor->is_error());
        CHECK(cursor->get_error().type == core::error_code_t::connection_closed);
        CHECK(std::string{cursor->get_error().what} == "server m2: connection refused");
    }
}

// A view body is a query too: it names the foreign table by its catalog oid, as it would a local one, and depends on
// it.
TEST_CASE("integration::cpp::host_names::a_view_over_a_storage_table") {
    HOST_TEST_BOILERPLATE("test_host_names/view_created")
    declare_orders(host);
    REQUIRE(run(dispatcher, "CREATE SCHEMA loc;")->is_success());

    auto created = run(dispatcher, "CREATE VIEW loc.ov AS SELECT id, amount FROM m2.shop.public.orders;");
    INFO("error: " << error_of(created));
    REQUIRE(created->is_success());

    auto read = run(dispatcher, "SELECT id, amount FROM loc.ov;");
    REQUIRE(read->is_success());
    CHECK(sorted_int_rows(read) == rows_t{{1, 100}, {2, 200}, {3, 300}});

    const auto view_oid = std::to_string(oid_of(dispatcher, "ov"));
    const auto orders_oid = oid_of(dispatcher, "orders");
    auto depends =
        run(dispatcher, "SELECT refclassid, refobjid FROM pg_catalog.pg_depend WHERE objid = " + view_oid + ";");
    bool on_orders = false;
    bool on_schema = false;
    for (const auto& chunk : depends->chunks()) {
        for (std::uint64_t row = 0; row < chunk.size(); ++row) {
            const auto refclassid = chunk.get_value<std::uint32_t>(0, row);
            on_schema |= refclassid == components::catalog::well_known_oid::pg_namespace_table;
            on_orders |= refclassid == components::catalog::well_known_oid::pg_class_table &&
                         chunk.get_value<std::uint32_t>(1, row) == orders_oid;
        }
    }
    CHECK(on_schema);
    CHECK(on_orders);
}

// A matview body is a query too: CREATE resolves through the view it names down to the foreign table, REFRESH fills
// it.
TEST_CASE("integration::cpp::host_names::a_matview_over_a_view_over_a_storage_table") {
    HOST_TEST_BOILERPLATE("test_host_names/matview_over_view")
    declare_orders(host);
    REQUIRE(run(dispatcher, "CREATE SCHEMA loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE VIEW loc.ov AS SELECT id, amount FROM m2.shop.public.orders;")->is_success());

    auto created = run(dispatcher, "CREATE MATERIALIZED VIEW loc.mv AS SELECT id, amount FROM loc.ov WITH NO DATA;");
    INFO("error: " << error_of(created));
    REQUIRE(created->is_success());
    auto refreshed = run(dispatcher, "REFRESH MATERIALIZED VIEW loc.mv;");
    INFO("error: " << error_of(refreshed));
    REQUIRE(refreshed->is_success());

    auto read = run(dispatcher, "SELECT id, amount FROM loc.mv;");
    REQUIRE(read->is_success());
    CHECK(sorted_int_rows(read) == rows_t{{1, 100}, {2, 200}, {3, 300}});
}

TEST_CASE("integration::cpp::host_names::rows_without_columns_are_counted") {
    HOST_TEST_BOILERPLATE("test_host_names/no_columns")
    register_table(host, "marks", {});
    host.backend().set_rows("m2.shop.public.marks", {{}, {}, {}});

    SECTION("one batch") {
        auto counted = run(dispatcher, "SELECT count(*) AS c FROM m2.shop.public.marks;");
        REQUIRE(counted->is_success());
        REQUIRE(counted->size() == 1);
        auto cell3 = counted->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell3.has_error());
        CHECK(cell3.value() == 3);
    }
    SECTION("several batches") {
        host.backend().set_batches("m2.shop.public.marks", {2, 1});
        auto counted = run(dispatcher, "SELECT count(*) AS c FROM m2.shop.public.marks;");
        REQUIRE(counted->is_success());
        REQUIRE(counted->size() == 1);
        auto cell4 = counted->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell4.has_error());
        CHECK(cell4.value() == 3);
    }
}

// A backend's pages of any sizes arrive whole; an empty page is the end of the scan, so what a backend would send
// after one is never read.
TEST_CASE("integration::cpp::host_names::the_pages_of_a_scan_arrive_whole") {
    HOST_TEST_BOILERPLATE("test_host_names/pages")
    declare_orders(host);
    host.backend().set_batches(orders, {1, 2});
    auto rows = run(dispatcher, "SELECT id, amount FROM m2.shop.public.orders;");
    REQUIRE(rows->is_success());
    CHECK(sorted_int_rows(rows) == rows_t{{1, 100}, {2, 200}, {3, 300}});

    host.backend().set_batches(orders, {1, 0, 2});
    auto cut = run(dispatcher, "SELECT id, amount FROM m2.shop.public.orders;");
    REQUIRE(cut->is_success());
    CHECK(sorted_int_rows(cut) == rows_t{{1, 100}});

    host.backend().set_batches(orders, {1, 2});
    auto counted = run(dispatcher, "SELECT count(*) AS c FROM m2.shop.public.orders;");
    REQUIRE(counted->is_success());
    auto cell5 = counted->get<int64_t>(0, 0);
    REQUIRE_FALSE(cell5.has_error());
    CHECK(cell5.value() == 3);
}

// A page of a scan always comes with its cursor (as the disk's and PostgreSQL's); a non-empty page without one is
// refused, whichever scan reads it.
TEST_CASE("integration::cpp::host_names::a_page_without_a_cursor_is_refused") {
    HOST_TEST_BOILERPLATE("test_host_names/page_without_cursor")
    declare_orders(host);
    host.backend().set_pages_without_cursor(true);
    for (const char* sql : {"SELECT id, amount FROM m2.shop.public.orders;",
                            "SELECT id FROM m2.shop.public.orders WHERE amount > 150;",
                            "SELECT count(*) AS c FROM m2.shop.public.orders;"}) {
        INFO(sql);
        auto refused = run(dispatcher, sql);
        REQUIRE(refused->is_error());
        CHECK(refused->get_error().type == core::error_code_t::invalid_parameter);
        CHECK(std::string{refused->get_error().what} ==
              "the storage of table \"" + orders + "\" answered a page without a cursor");
    }
}

// A write target is the registered table: every write reaches its backend for that table.
TEST_CASE("integration::cpp::host_names::a_write_target_is_its_registered_table") {
    HOST_TEST_BOILERPLATE("test_host_names/write_schema")
    declare_orders(host);
    struct write_t {
        const char* sql;
        const char* kind;
        std::string line;
    };
    const write_t writes[] = {
        {"INSERT INTO m2.shop.public.orders (id, amount) VALUES (4, 400);",
         "insert",
         "insert " + orders + " id:bigint,amount:bigint"},
        {"UPDATE m2.shop.public.orders SET amount = 1 WHERE id = 1;", "update", "update " + orders + " rows 1"},
        {"DELETE FROM m2.shop.public.orders WHERE id = 2;", "delete", "delete " + orders + " rows 1"},
    };
    for (const auto& write : writes) {
        host.backend().clear_journal();
        auto cursor = run(dispatcher, write.sql);
        INFO(write.sql << " -> " << error_of(cursor));
        REQUIRE(cursor->is_success());
        CHECK(host.backend().journal(write.kind) == strings{write.line});
    }
}

TEST_CASE("integration::cpp::host_names::insert_into_a_storage_table") {
    HOST_TEST_BOILERPLATE("test_host_names/insert")
    declare_orders(host);

    SECTION("the declared columns, cast to their types, reach the storage; the count is the rows written") {
        auto inserted =
            run(dispatcher,
                "INSERT INTO m2.shop.public.orders (id, amount) VALUES (4, CAST(400 AS INTEGER)), (5, 500);");
        INFO(error_of(inserted));
        REQUIRE(inserted->is_success());
        CHECK(inserted->affected_rows() == 2);
        CHECK(host.backend().journal("insert") == strings{"insert " + orders + " id:bigint,amount:bigint"});
        auto read = run(dispatcher, "SELECT id, amount FROM m2.shop.public.orders;");
        REQUIRE(read->is_success());
        CHECK(sorted_int_rows(read) == rows_t{{1, 100}, {2, 200}, {3, 300}, {4, 400}, {5, 500}});
    }
    SECTION("a column list in another order still arrives in the declared order") {
        REQUIRE(run(dispatcher, "INSERT INTO m2.shop.public.orders (amount, id) VALUES (600, 6);")->is_success());
        CHECK(host.backend().rows(orders).back() == std::vector<int64_t>{6, 600});
    }
    SECTION("INSERT ... SELECT from a local table, with and without a column list") {
        REQUIRE(run(dispatcher, "CREATE SCHEMA loc;")->is_success());
        REQUIRE(run(dispatcher, "CREATE TABLE loc.t (id BIGINT, amount BIGINT);")->is_success());
        REQUIRE(run(dispatcher, "INSERT INTO loc.t (id, amount) VALUES (8, 800), (9, 900);")->is_success());
        auto copied = run(dispatcher, "INSERT INTO m2.shop.public.orders (id, amount) SELECT id, amount FROM loc.t;");
        INFO(error_of(copied));
        REQUIRE(copied->is_success());
        CHECK(copied->affected_rows() == 2);
        auto positional = run(dispatcher, "INSERT INTO m2.shop.public.orders SELECT id + 10, amount FROM loc.t;");
        INFO(error_of(positional));
        REQUIRE(positional->is_success());
        CHECK(host.backend().rows(orders).back() == std::vector<int64_t>{19, 900});
        CHECK(host.backend().rows(orders).size() == 7);
    }
    SECTION("a value no assignment cast takes to the declared type is refused before the storage sees it") {
        auto refused = run(dispatcher, "INSERT INTO m2.shop.public.orders (id, amount) VALUES (10, 'ten');");
        REQUIRE(refused->is_error());
        CHECK(host.backend().journal("insert").empty());
    }
}

// The count is what the backend reports after its own work, not the rows handed to it.
TEST_CASE("integration::cpp::host_names::the_count_is_what_the_sink_reports_written") {
    HOST_TEST_BOILERPLATE("test_host_names/sink_count")
    declare_orders(host);
    host.backend().set_short_writing(orders);

    auto inserted =
        run(dispatcher, "INSERT INTO m2.shop.public.orders (id, amount) VALUES (4, 400), (5, 500), (6, 600);");
    INFO(error_of(inserted));
    REQUIRE(inserted->is_success());
    CHECK(inserted->affected_rows() == 2);
    CHECK(host.backend().rows(orders).size() == 5);

    auto updated = run(dispatcher, "UPDATE m2.shop.public.orders SET amount = amount + 1 WHERE id <= 3;");
    INFO(error_of(updated));
    REQUIRE(updated->is_success());
    CHECK(updated->affected_rows() == 2);

    auto deleted = run(dispatcher, "DELETE FROM m2.shop.public.orders WHERE id <= 3;");
    INFO(error_of(deleted));
    REQUIRE(deleted->is_success());
    CHECK(deleted->affected_rows() == 2);
    CHECK(host.backend().rows(orders).size() == 3);
}

// A flush round reports what it wrote: the count is the total once, not a sum of totals.
TEST_CASE("integration::cpp::host_names::the_count_survives_flush_rounds") {
    HOST_TEST_BOILERPLATE_CAPPED("test_host_names/sink_count_rounds", nullptr, 1)
    declare_orders(host);
    host.backend().set_batches(orders, {1, 2});

    auto updated = run(dispatcher, "UPDATE m2.shop.public.orders SET amount = amount + 1;");
    INFO(error_of(updated));
    REQUIRE(updated->is_success());
    CHECK(updated->affected_rows() == 3);
    CHECK(host.backend().rows(orders) == rows_t{{1, 101}, {2, 201}, {3, 301}});

    auto inserted =
        run(dispatcher,
            "INSERT INTO m2.shop.public.orders (id, amount) SELECT id + 10, amount FROM m2.shop.public.orders;");
    INFO(error_of(inserted));
    REQUIRE(inserted->is_success());
    CHECK(inserted->affected_rows() == 3);

    auto deleted = run(dispatcher, "DELETE FROM m2.shop.public.orders WHERE id <= 3;");
    INFO(error_of(deleted));
    REQUIRE(deleted->is_success());
    CHECK(deleted->affected_rows() == 3);
    CHECK(host.backend().rows(orders) == rows_t{{11, 101}, {12, 201}, {13, 301}});
}

// The connection's max_rows_per_write caps every write message; the rest of a flush goes in further messages of the
// same flush, and the count is the total.
TEST_CASE("integration::cpp::host_names::the_storage_caps_each_sink_round") {
    HOST_TEST_BOILERPLATE_CAPPED("test_host_names/flush_rows", nullptr, 2)
    declare_orders(host);
    host.backend().set_rows(orders, {{1, 100}, {2, 200}, {3, 300}, {4, 400}});
    host.backend().set_batches(orders, {1, 1, 1, 1});
    const char* insert_five =
        "INSERT INTO m2.shop.public.orders (id, amount) VALUES (5, 500), (6, 600), (7, 700), (8, 800), "
        "(9, 900)";

    SECTION("INSERT") {
        auto inserted = run(dispatcher, std::string{insert_five} + ";");
        INFO(error_of(inserted));
        REQUIRE(inserted->is_success());
        CHECK(inserted->affected_rows() == 5);
        CHECK(host.backend().write_rounds() == std::vector<std::size_t>{2, 2, 1});
        CHECK(host.backend().rows(orders).size() == 9);
        CHECK(host.backend().rows(orders).back() == std::vector<int64_t>{9, 900});
    }
    SECTION("UPDATE") {
        auto updated = run(dispatcher, "UPDATE m2.shop.public.orders SET amount = amount + 1 WHERE id <= 3;");
        INFO(error_of(updated));
        REQUIRE(updated->is_success());
        CHECK(updated->affected_rows() == 3);
        CHECK(host.backend().write_rounds() == std::vector<std::size_t>{2, 1});
        CHECK(host.backend().rows(orders) == rows_t{{1, 101}, {2, 201}, {3, 301}, {4, 400}});
    }
    SECTION("DELETE") {
        auto deleted = run(dispatcher, "DELETE FROM m2.shop.public.orders WHERE id >= 2;");
        INFO(error_of(deleted));
        REQUIRE(deleted->is_success());
        CHECK(deleted->affected_rows() == 3);
        CHECK(host.backend().write_rounds() == std::vector<std::size_t>{2, 1});
        CHECK(host.backend().rows(orders) == rows_t{{1, 100}});
    }
    SECTION("RETURNING gives every row, in order") {
        auto inserted = run(dispatcher, std::string{insert_five} + " RETURNING id;");
        INFO(error_of(inserted));
        REQUIRE(inserted->is_success());
        CHECK(host.backend().write_rounds() == std::vector<std::size_t>{2, 2, 1});
        rows_t ids;
        for (const auto& chunk : inserted->chunks()) {
            for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                ids.push_back(int_row(chunk, row));
            }
        }
        CHECK(ids == rows_t{{5}, {6}, {7}, {8}, {9}});

        host.backend().clear_journal();
        auto updated =
            run(dispatcher, "UPDATE m2.shop.public.orders SET amount = amount + 1 WHERE id <= 3 RETURNING id, amount;");
        INFO(error_of(updated));
        REQUIRE(updated->is_success());
        CHECK(host.backend().write_rounds() == std::vector<std::size_t>{2, 1});
        CHECK(sorted_int_rows(updated) == rows_t{{1, 101}, {2, 201}, {3, 301}});
    }
    SECTION("a short-writing backend: the count is what every round applied") {
        host.backend().set_short_writing(orders);
        auto inserted = run(dispatcher, std::string{insert_five} + ";");
        INFO(error_of(inserted));
        REQUIRE(inserted->is_success());
        CHECK(host.backend().write_rounds() == std::vector<std::size_t>{2, 2, 1});
        CHECK(inserted->affected_rows() == 2);
    }
}

// A cap above the rows of a flush: one write message.
TEST_CASE("integration::cpp::host_names::a_cap_above_the_rows_is_one_round") {
    HOST_TEST_BOILERPLATE_CAPPED("test_host_names/cap_above", nullptr, 1000)
    declare_orders(host);
    auto inserted = run(dispatcher,
                        "INSERT INTO m2.shop.public.orders (id, amount) VALUES (5, 500), (6, 600), (7, 700), (8, 800), "
                        "(9, 900);");
    INFO(error_of(inserted));
    REQUIRE(inserted->is_success());
    CHECK(inserted->affected_rows() == 5);
    CHECK(host.backend().write_rounds() == std::vector<std::size_t>{5});
}

// A DELETE names its rows in windows of at most DEFAULT_VECTOR_CAPACITY numbers: without a cap the flush is still one
// message, and RETURNING gives every row in the order of the scan.
TEST_CASE("integration::cpp::host_names::a_delete_names_its_rows_in_windows") {
    HOST_TEST_BOILERPLATE("test_host_names/delete_windows")
    declare_orders(host);
    constexpr int64_t n = 1500;
    rows_t many;
    rows_t ids_in_order;
    for (int64_t id = 1; id <= n; ++id) {
        many.push_back({id, id * 10});
        ids_in_order.push_back({id});
    }
    host.backend().set_rows(orders, many);
    host.backend().set_batches(orders, {1000, 500});
    auto deleted = run(dispatcher, "DELETE FROM m2.shop.public.orders WHERE id >= 1 RETURNING id;");
    CHECK(host.backend().write_rounds() == std::vector<std::size_t>{static_cast<std::size_t>(n)});
    const auto windows = host.backend().delete_windows();
    REQUIRE(windows.size() == 1);
    CHECK(windows.front().size() >= 2);
    std::size_t named = 0;
    for (const auto window : windows.front()) {
        CHECK(window <= vector::DEFAULT_VECTOR_CAPACITY);
        named += window;
    }
    CHECK(named == static_cast<std::size_t>(n));
    INFO(error_of(deleted));
    REQUIRE(deleted->is_success());
    rows_t returned;
    for (const auto& chunk : deleted->chunks()) {
        for (std::uint64_t row = 0; row < chunk.size(); ++row) {
            returned.push_back(int_row(chunk, row));
        }
    }
    CHECK(returned == ids_in_order);
    CHECK(host.backend().rows(orders).empty());
}

#ifdef NDEBUG
// A receipt's returned rows come in chunks of at most DEFAULT_VECTOR_CAPACITY rows (in Debug a wider chunk cannot be
// built); a wider one is refused.
TEST_CASE("integration::cpp::host_names::returned_rows_wider_than_a_chunk_are_refused") {
    HOST_TEST_BOILERPLATE("test_host_names/wide_returned")
    declare_orders(host);
    rows_t many;
    for (int64_t id = 1; id <= 2500; ++id) {
        many.push_back({id, id});
    }
    host.backend().set_rows(orders, many);
    host.backend().set_batches(orders, {1000, 1000, 500});
    host.backend().set_wide_returned(true);
    auto deleted = run(dispatcher, "DELETE FROM m2.shop.public.orders RETURNING id;");
    REQUIRE(deleted->is_error());
    CHECK(deleted->get_error().type == core::error_code_t::invalid_parameter);
    CHECK(std::string{deleted->get_error().what} ==
          "a storage answered a chunk of 2500 rows, more than the 1024 rows of a chunk");
}
#endif

// Every connection has its own cap, two connections of one actor included; a table of otterbrix's own flushes on the
// engine's threshold.
TEST_CASE("integration::cpp::host_names::every_connection_has_its_own_cap") {
    host_t host("test_host_names/caps", nullptr, {"m2", "m3=m2"}, {{"m2", 2}, {"m3", 1000}});
    host.register_database("m2", "shop");
    host.register_database("m3", "shop");
    host.db.ok(otterbrix::session_id_t(), "CREATE TABLE m2.shop.public.t (id BIGINT);");
    host.db.ok(otterbrix::session_id_t(), "CREATE TABLE m3.shop.public.t (id BIGINT);");
    host.db.ok(otterbrix::session_id_t(), "CREATE SCHEMA loc;");
    host.db.ok(otterbrix::session_id_t(), "CREATE TABLE loc.t (id BIGINT);");
    const char* five = " (id) VALUES (1), (2), (3), (4), (5);";
    host.backend("m2").clear_journal();
    host.db.ok(otterbrix::session_id_t(), std::string{"INSERT INTO m2.shop.public.t"} + five);
    CHECK(host.backend("m2").write_rounds() == std::vector<std::size_t>{2, 2, 1});
    host.backend("m2").clear_journal();
    host.db.ok(otterbrix::session_id_t(), std::string{"INSERT INTO m3.shop.public.t"} + five);
    CHECK(host.backend("m3").write_rounds() == std::vector<std::size_t>{5});
    host.db.ok(otterbrix::session_id_t(), std::string{"INSERT INTO loc.t"} + five);
    CHECK(host.db.ints(otterbrix::session_id_t(), "SELECT count(*) FROM loc.t;") == std::vector<std::int64_t>{5});
}

// A write message the backend refuses ends the statement with the backend's error; the statement's earlier messages
// are rolled back by abort_transaction, and the next statement runs.
TEST_CASE("integration::cpp::host_names::a_failed_sink_round_is_the_statement_error") {
    HOST_TEST_BOILERPLATE_CAPPED("test_host_names/failed_round", nullptr, 1)
    declare_orders(host);
    host.backend().set_failing_round(2);

    auto failed =
        run(dispatcher, "INSERT INTO m2.shop.public.orders (id, amount) VALUES (4, 400), (5, 500), (6, 600);");
    REQUIRE(failed->is_error());
    CHECK(failed->get_error().type == core::error_code_t::connection_closed);
    CHECK(std::string{failed->get_error().what} == "server m2: round 2 failed");
    CHECK(host.backend().write_rounds() == std::vector<std::size_t>{1, 1});
    CHECK(host.backend().rows(orders) == rows_t{{1, 100}, {2, 200}, {3, 300}});
    CHECK(host.backend().journal("abort") == strings{"abort 0"});
    CHECK(host.backend().open_transactions() == 0);

    auto next = run(dispatcher, "INSERT INTO m2.shop.public.orders (id, amount) VALUES (7, 700);");
    INFO(error_of(next));
    REQUIRE(next->is_success());
    CHECK(next->affected_rows() == 1);
    CHECK(host.backend().rows(orders).back() == std::vector<int64_t>{7, 700});
}

// An INSERT into a backend's table flushes on the connection's max_rows_per_write, not the engine's threshold (as
// PostgreSQL 18 batches a foreign INSERT by the FDW's batch size); an UPDATE / DELETE flushes on the engine's.
TEST_CASE("integration::cpp::host_names::an_insert_flushes_on_the_storage_threshold") {
    HOST_TEST_BOILERPLATE_CAPPED("test_host_names/insert_threshold", &flush_every_row, 1000)
    declare_orders(host);
    host.backend().set_batches(orders, {1, 1, 1});

    auto inserted =
        run(dispatcher,
            "INSERT INTO m2.shop.public.orders (id, amount) SELECT id + 10, amount FROM m2.shop.public.orders;");
    INFO(error_of(inserted));
    REQUIRE(inserted->is_success());
    CHECK(inserted->affected_rows() == 3);
    CHECK(host.backend().write_rounds() == std::vector<std::size_t>{3});

    host.backend().clear_journal();
    auto updated = run(dispatcher, "UPDATE m2.shop.public.orders SET amount = amount + 1 WHERE id <= 3;");
    INFO(error_of(updated));
    REQUIRE(updated->is_success());
    CHECK(updated->affected_rows() == 3);
    CHECK(host.backend().write_rounds() == std::vector<std::size_t>{1, 1, 1});

    host.backend().clear_journal();
    auto deleted = run(dispatcher, "DELETE FROM m2.shop.public.orders WHERE id <= 3;");
    INFO(error_of(deleted));
    REQUIRE(deleted->is_success());
    CHECK(deleted->affected_rows() == 3);
    CHECK(host.backend().write_rounds() == std::vector<std::size_t>{1, 1, 1});
    CHECK(host.backend().rows(orders) == rows_t{{11, 100}, {12, 200}, {13, 300}});
}

// A backend's table declares no defaults: a column the INSERT leaves out is NULL, as for a local table without one.
TEST_CASE("integration::cpp::host_names::an_omitted_column_is_null") {
    HOST_TEST_BOILERPLATE("test_host_names/insert_null")
    declare_orders(host);
    REQUIRE(run(dispatcher, "CREATE SCHEMA loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE loc.t (id BIGINT, amount BIGINT);")->is_success());
    REQUIRE(run(dispatcher, "INSERT INTO loc.t (id, amount) VALUES (8, 800);")->is_success());
    for (const char* sql : {"INSERT INTO m2.shop.public.orders (id) VALUES (4);",
                            "INSERT INTO m2.shop.public.orders VALUES (5);",
                            "INSERT INTO m2.shop.public.orders (amount) SELECT amount FROM loc.t;"}) {
        auto cursor = run(dispatcher, sql);
        INFO(sql << " -> " << error_of(cursor));
        REQUIRE(cursor->is_success());
    }
    CHECK(sorted(host.backend().rows(orders)) == rows_t{{-1, 800}, {1, 100}, {2, 200}, {3, 300}, {4, -1}, {5, -1}});
}

TEST_CASE("integration::cpp::host_names::insert_returning_reads_the_rows_written") {
    HOST_TEST_BOILERPLATE("test_host_names/insert_returning")
    declare_orders(host);
    auto returned =
        run(dispatcher, "INSERT INTO m2.shop.public.orders (id, amount) VALUES (4, 400), (5, 500) RETURNING id;");
    INFO(error_of(returned));
    REQUIRE(returned->is_success());
    CHECK(sorted_int_rows(returned) == rows_t{{4}, {5}});
    CHECK(host.backend().rows(orders).size() == 5);
    CHECK(host.backend().journal("scan").empty());
}

// A RETURNING of constants only is computed before anything is written, as PostgreSQL 18 folds an immutable
// expression while planning: its error is the statement's, and the backend gets no write.
TEST_CASE("integration::cpp::host_names::a_constant_returning_errs_before_the_write") {
    HOST_TEST_BOILERPLATE("test_host_names/constant_returning")
    declare_orders(host);
    host.backend().clear_journal();
    for (const char* sql : {"INSERT INTO m2.shop.public.orders (id, amount) VALUES (4, 400) RETURNING 1 / 0;",
                            "UPDATE m2.shop.public.orders SET amount = 1 WHERE id = 1 RETURNING 1 / 0;",
                            "DELETE FROM m2.shop.public.orders WHERE id = 1 RETURNING 1 / 0;"}) {
        INFO(sql);
        auto refused = run(dispatcher, sql);
        REQUIRE(refused->is_error());
        CHECK(std::string{refused->get_error().what}.find("division by zero") != std::string::npos);
    }
    for (const char* write : {"insert", "update", "delete", "run"}) {
        INFO(write);
        CHECK(host.backend().journal(write).empty());
    }
    CHECK(host.backend().rows(orders) == rows_t{{1, 100}, {2, 200}, {3, 300}});
}

// The backend takes a simple UPDATE / DELETE of its table as one remote statement through storage_offer (as
// postgres_fdw's direct modify does); the statement never scans the table.
TEST_CASE("integration::cpp::host_names::a_simple_update_or_delete_is_one_remote_statement") {
    HOST_TEST_BOILERPLATE("test_host_names/one_remote_statement")
    declare_orders(host);
    host.backend().set_take_policy({false, true});

    auto updated = run(dispatcher, "UPDATE m2.shop.public.orders SET amount = 250 WHERE id = 2;");
    INFO(error_of(updated));
    REQUIRE(updated->is_success());
    CHECK(updated->affected_rows() == 1);

    auto deleted = run(dispatcher, "DELETE FROM m2.shop.public.orders WHERE id = 1;");
    INFO(error_of(deleted));
    REQUIRE(deleted->is_success());
    CHECK(deleted->affected_rows() == 1);

    auto untouched = run(dispatcher, "DELETE FROM m2.shop.public.orders WHERE id = 99;");
    REQUIRE(untouched->is_success());
    CHECK(untouched->affected_rows() == 0);

    CHECK(host.backend().journal("run") == strings{"run update " + orders + " where column 0 set column 1",
                                                   "run delete " + orders + " where column 0",
                                                   "run delete " + orders + " where column 0"});
    CHECK(host.backend().journal("scan").empty());
    CHECK(host.backend().rows(orders) == rows_t{{2, 250}, {3, 300}});

    auto all = run(dispatcher, "DELETE FROM m2.shop.public.orders;");
    REQUIRE(all->is_success());
    CHECK(all->affected_rows() == 2);
    REQUIRE_FALSE(host.backend().journal("run").empty());
    CHECK(host.backend().journal("run").back() == "run delete " + orders);
    CHECK(host.backend().rows(orders).empty());
}

// PostgreSQL 18 postgres_fdw: "Foreign Scan on ..." for a scan otterbrix runs itself; what the backend did not take
// has no remote statement to print.
TEST_CASE("integration::cpp::host_names::explain_prints_the_storage_scan_label") {
    HOST_TEST_BOILERPLATE("test_host_names/explain")
    declare_orders(host);

    SECTION("EXPLAIN") {
        CHECK(explain_lines(dispatcher, "EXPLAIN SELECT * FROM m2.shop.public.orders;") ==
              std::vector<std::string>{"Foreign Scan on " + orders});
    }
    SECTION("EXPLAIN ANALYZE") {
        auto lines = explain_lines(dispatcher, "EXPLAIN ANALYZE SELECT * FROM m2.shop.public.orders;");
        REQUIRE(lines.size() == 1);
        CHECK(lines[0].rfind("Foreign Scan on " + orders + "  (actual time=", 0) == 0);
        CHECK(lines[0].find("rows=3 loops=1)") != std::string::npos);
    }
    SECTION("a node below the root") {
        REQUIRE(run(dispatcher, "CREATE SCHEMA loc;")->is_success());
        REQUIRE(run(dispatcher, "CREATE TABLE loc.c (id BIGINT);")->is_success());
        auto lines =
            explain_lines(dispatcher,
                          "EXPLAIN SELECT o.id FROM m2.shop.public.orders AS o JOIN loc.c AS c ON o.id = c.id;");
        CHECK(lines == std::vector<std::string>{"Project",
                                                "  ->  Hash Join",
                                                "    ->  Foreign Scan on " + orders,
                                                "    ->  Seq Scan on c"});
    }
    SECTION("a write into the backend's table: the foreign write's line") {
        auto inserted =
            explain_lines(dispatcher, "EXPLAIN INSERT INTO m2.shop.public.orders (id, amount) VALUES (4, 400);");
        REQUIRE_FALSE(inserted.empty());
        CHECK(inserted[0] == "Foreign Insert on " + orders);
        struct write_line_t {
            const char* sql;
            std::string label;
        };
        const write_line_t writes[] = {
            {"EXPLAIN UPDATE m2.shop.public.orders SET amount = 1 WHERE id = 1 RETURNING id;",
             "Foreign Update on " + orders},
            {"EXPLAIN DELETE FROM m2.shop.public.orders WHERE id = 1 RETURNING id;", "Foreign Delete on " + orders},
        };
        for (const auto& write : writes) {
            INFO(write.sql);
            auto lines = explain_lines(dispatcher, write.sql);
            REQUIRE(lines.size() >= 2);
            CHECK(lines[0] == write.label);
            CHECK(std::any_of(lines.begin(), lines.end(), [](const std::string& line) {
                return line.find("->  Foreign Scan on " + orders) != std::string::npos;
            }));
        }
        CHECK(host.backend().rows(orders) == rows_t{{1, 100}, {2, 200}, {3, 300}});
    }
}

// What the backend does not take runs as otterbrix's own UPDATE / DELETE: the backend's scan numbers the rows,
// otterbrix does the FROM / USING semi-join, the WHERE and RETURNING, and the numbers come back to the backend's
// update or delete.
TEST_CASE("integration::cpp::host_names::update_and_delete_by_row_number") {
    HOST_TEST_BOILERPLATE("test_host_names/by_row_number")
    declare_orders(host);
    REQUIRE(run(dispatcher, "CREATE SCHEMA loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE loc.t (id BIGINT, amount BIGINT);")->is_success());
    REQUIRE(run(dispatcher, "INSERT INTO loc.t (id, amount) VALUES (1, 111), (3, 333), (9, 999);")->is_success());

    SECTION("UPDATE ... FROM a local table") {
        auto updated =
            run_in_shop(dispatcher, "UPDATE orders SET amount = t.amount FROM main.loc.t AS t WHERE orders.id = t.id;");
        INFO(error_of(updated));
        REQUIRE(updated->is_success());
        CHECK(updated->affected_rows() == 2);
        CHECK(host.backend().journal("update") == strings{"update " + orders + " rows 2"});
        CHECK(host.backend().rows(orders) == rows_t{{1, 111}, {2, 200}, {3, 333}});
    }
    SECTION("DELETE ... USING a local table") {
        auto deleted = run_in_shop(dispatcher, "DELETE FROM orders USING main.loc.t AS t WHERE orders.id = t.id;");
        INFO(error_of(deleted));
        REQUIRE(deleted->is_success());
        CHECK(deleted->affected_rows() == 2);
        CHECK(host.backend().journal("delete") == strings{"delete " + orders + " rows 2"});
        CHECK(host.backend().rows(orders) == rows_t{{2, 200}});
    }
    SECTION("UPDATE ... RETURNING") {
        auto returned =
            run(dispatcher, "UPDATE m2.shop.public.orders SET amount = amount + 1 WHERE id >= 2 RETURNING id, amount;");
        INFO(error_of(returned));
        REQUIRE(returned->is_success());
        CHECK(sorted_int_rows(returned) == rows_t{{2, 201}, {3, 301}});
        CHECK(host.backend().journal("update") == strings{"update " + orders + " rows 2"});
        CHECK(host.backend().rows(orders) == rows_t{{1, 100}, {2, 201}, {3, 301}});
    }
    SECTION("DELETE ... RETURNING, nothing matched: the columns are still typed") {
        auto returned = run(dispatcher, "DELETE FROM m2.shop.public.orders WHERE id = 3 RETURNING amount;");
        INFO(error_of(returned));
        REQUIRE(returned->is_success());
        CHECK(sorted_int_rows(returned) == rows_t{{300}});
        auto none = run(dispatcher, "DELETE FROM m2.shop.public.orders WHERE id = 42 RETURNING amount;");
        INFO(error_of(none));
        REQUIRE(none->is_success());
        CHECK(none->size() == 0);
        CHECK(host.backend().rows(orders) == rows_t{{1, 100}, {2, 200}});
    }
}

// A backend that cannot change a row by its number refuses storage_update / storage_delete_rows: what it did not
// take as one statement is refused with its own error, and no row changes.
TEST_CASE("integration::cpp::host_names::a_storage_without_row_numbers_refuses_the_batch_path") {
    HOST_TEST_BOILERPLATE("test_host_names/no_row_numbers")
    declare_orders(host);
    REQUIRE(run(dispatcher, "CREATE SCHEMA loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE loc.t (id BIGINT, amount BIGINT);")->is_success());
    REQUIRE(run(dispatcher, "INSERT INTO loc.t (id, amount) VALUES (1, 111);")->is_success());
    host.backend().set_no_row_numbers(orders);
    host.backend().set_take_policy({false, true});
    for (const char* sql : {"UPDATE orders SET amount = t.amount FROM main.loc.t AS t WHERE orders.id = t.id;",
                            "DELETE FROM m2.shop.public.orders WHERE amount > 150 RETURNING id;"}) {
        INFO(sql);
        auto refused = run_in_shop(dispatcher, sql);
        REQUIRE(refused->is_error());
        CHECK(refused->get_error().type == core::error_code_t::unimplemented_yet);
        CHECK(std::string{refused->get_error().what} == "storage \"" + orders + "\" cannot change a row by its number");
    }
    CHECK(host.backend().rows(orders) == rows_t{{1, 100}, {2, 200}, {3, 300}});

    host.backend().clear_journal();
    auto simple = run(dispatcher, "UPDATE m2.shop.public.orders SET amount = 1 WHERE id = 1;");
    INFO(error_of(simple));
    REQUIRE(simple->is_success());
    CHECK(host.backend().journal("run") == strings{"run update " + orders + " where column 0 set column 1"});
}

// The backend learns of the transaction's end: its writes of BEGIN ... COMMIT share one transaction, and COMMIT
// prepares it and then releases it; an autocommit write is its own transaction, prepared at once.
TEST_CASE("integration::cpp::host_names::a_storage_learns_of_an_explicit_transaction") {
    HOST_TEST_BOILERPLATE("test_host_names/write_txn")
    declare_orders(host);
    auto session = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());
    REQUIRE(run(dispatcher, session, "INSERT INTO m2.shop.public.orders (id, amount) VALUES (4, 400);")->is_success());
    REQUIRE(run(dispatcher, session, "INSERT INTO m2.shop.public.orders (id, amount) VALUES (5, 500);")->is_success());
    CHECK(host.backend().journal("prepare").empty());
    REQUIRE(run(dispatcher, session, "COMMIT;")->is_success());
    CHECK(host.backend().journal("prepare") == strings{"prepare 1"});
    CHECK(host.backend().journal("release") == strings{"release"});
    const auto inside = host.backend().calls();
    REQUIRE(inside.size() == 2);
    CHECK(inside[0].transaction_id != 0);
    CHECK(inside[0].transaction_id == inside[1].transaction_id);

    host.backend().clear_journal();
    REQUIRE(run(dispatcher, session, "INSERT INTO m2.shop.public.orders (id, amount) VALUES (6, 600);")->is_success());
    CHECK(host.backend().journal() == strings{"insert " + orders + " id:bigint,amount:bigint", "prepare 1", "release"});
    REQUIRE(host.backend().calls().size() == 1);
    CHECK(host.backend().calls().front().transaction_id != inside[0].transaction_id);
    CHECK(host.backend().rows(orders).size() == 6);
    CHECK(host.backend().open_transactions() == 0);
}

// The backend's refusal (a NOT NULL or CHECK it enforces, an unreachable server) reaches the cursor unchanged.
TEST_CASE("integration::cpp::host_names::a_storage_write_error_reaches_the_cursor") {
    HOST_TEST_BOILERPLATE("test_host_names/write_error")
    declare_orders(host);
    host.backend().set_unreachable(orders, "server m2: new row violates check constraint \"amount_positive\"");
    for (const char* sql : {"INSERT INTO m2.shop.public.orders (id, amount) VALUES (4, -1);",
                            "UPDATE m2.shop.public.orders SET amount = -1 WHERE id = 1;",
                            "DELETE FROM m2.shop.public.orders WHERE id = 1;"}) {
        INFO(sql);
        auto cursor = run(dispatcher, sql);
        REQUIRE(cursor->is_error());
        CHECK(cursor->get_error().type == core::error_code_t::connection_closed);
        CHECK(std::string{cursor->get_error().what} ==
              "server m2: new row violates check constraint \"amount_positive\"");
    }
    host.backend().clear_unreachable();
    CHECK(host.backend().rows(orders).size() == 3);
}

// A name has at most four parts (connection.database.schema.table): a fifth is refused by the parser; the four-part
// name is the registered table.
TEST_CASE("integration::cpp::host_names::a_five_part_name_is_refused") {
    HOST_TEST_BOILERPLATE("test_host_names/uid_name")
    declare_orders(host);
    host.backend().set_rows(orders, {{7, 700}});

    auto refused = run(dispatcher, "SELECT id, amount FROM u1.m2.shop.public.orders;");
    REQUIRE(refused->is_error());
    CHECK(refused->get_error().type == core::error_code_t::sql_parse_error);
    CHECK(host.backend().journal().empty());

    auto found = run(dispatcher, "SELECT id, amount FROM m2.shop.public.orders;");
    INFO(error_of(found));
    REQUIRE(found->is_success());
    CHECK(sorted_int_rows(found) == rows_t{{7, 700}});
}

// Every message of a statement carries its own session and transaction; the statements of one BEGIN share it.
TEST_CASE("integration::cpp::host_names::two_open_transactions") {
    HOST_TEST_BOILERPLATE("test_host_names/r5_1")
    declare_orders(host);
    const auto a = otterbrix::session_id_t();
    const auto b = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, a, "BEGIN;")->is_success());
    REQUIRE(run(dispatcher, b, "BEGIN;")->is_success());
    REQUIRE(run(dispatcher, a, "SELECT id FROM m2.shop.public.orders;")->is_success());
    REQUIRE(run(dispatcher, b, "SELECT id FROM m2.shop.public.orders;")->is_success());
    REQUIRE(run(dispatcher, a, "INSERT INTO m2.shop.public.orders (id, amount) VALUES (4, 400);")->is_success());
    REQUIRE(run(dispatcher, b, "INSERT INTO m2.shop.public.orders (id, amount) VALUES (5, 500);")->is_success());
    const auto calls = host.backend().calls();
    REQUIRE(calls.size() == 4);
    const auto& first_a = calls[0];
    const auto& first_b = calls[1];
    CHECK(first_a.session == a.data());
    CHECK(first_b.session == b.data());
    CHECK(first_a.transaction_id != 0);
    CHECK(first_b.transaction_id != 0);
    CHECK(first_a.transaction_id != first_b.transaction_id);
    CHECK(calls[2].session == a.data());
    CHECK(calls[2].transaction_id == first_a.transaction_id);
    CHECK(calls[3].session == b.data());
    CHECK(calls[3].transaction_id == first_b.transaction_id);
    CHECK(host.backend().journal("prepare").empty());
    REQUIRE(run(dispatcher, a, "COMMIT;")->is_success());
    REQUIRE(run(dispatcher, b, "COMMIT;")->is_success());
    CHECK(host.backend().journal("prepare").size() == 2);
    CHECK(host.backend().rows(orders).size() == 5);
}

// Two autocommit statements, two transactions.
TEST_CASE("integration::cpp::host_names::two_autocommit_statements") {
    HOST_TEST_BOILERPLATE("test_host_names/r5_2")
    declare_orders(host);
    REQUIRE(run(dispatcher, "SELECT id FROM m2.shop.public.orders;")->is_success());
    REQUIRE(run(dispatcher, "SELECT id FROM m2.shop.public.orders;")->is_success());
    const auto calls = host.backend().calls();
    REQUIRE(calls.size() == 2);
    CHECK(calls[0].transaction_id != 0);
    CHECK(calls[0].transaction_id != calls[1].transaction_id);
}

// The backend gets the table as the catalog has it, at CREATE: its full name and its columns in attnum order.
TEST_CASE("integration::cpp::host_names::the_backend_gets_the_tables_description") {
    HOST_TEST_BOILERPLATE("test_host_names/description")
    host.db.ok(otterbrix::session_id_t(), "CREATE TABLE m2.shop.public.orders (id BIGINT, amount BIGINT);");
    CHECK(host.backend().journal("create") == strings{"create " + orders + " id amount"});
    CHECK(host.backend().oid_of(orders) == oid_of(dispatcher, "orders"));
    const auto attoids = host.backend().attoids_of(orders);
    REQUIRE(attoids.size() == 2);
    CHECK(attoids[0] != components::catalog::INVALID_OID);
    CHECK(attoids[1] != components::catalog::INVALID_OID);
    CHECK(attoids[0] != attoids[1]);
}

// One table read twice in a statement: each read is its own scan of the backend.
TEST_CASE("integration::cpp::host_names::each_read_of_a_table_is_its_own_scan") {
    HOST_TEST_BOILERPLATE("test_host_names/r5_4")
    declare_orders(host);
    auto joined =
        run(dispatcher, "SELECT a.amount FROM m2.shop.public.orders a JOIN m2.shop.public.orders b USING (id);");
    INFO(error_of(joined));
    REQUIRE(joined->is_success());
    CHECK(sorted_int_rows(joined) == rows_t{{100}, {200}, {300}});
    CHECK(host.backend().journal("scan") == strings{"scan " + orders, "scan " + orders});
    CHECK(host.backend().open_cursors() == 0);
}

// The backend's refusal to describe its table reaches the cursor unchanged.
TEST_CASE("integration::cpp::host_names::a_backend_refusal_reaches_the_cursor") {
    HOST_TEST_BOILERPLATE("test_host_names/r5_5")
    declare_orders(host);
    host.backend().set_unreachable(orders, "server m2: connection refused");
    auto refused = run(dispatcher, "SELECT id FROM m2.shop.public.orders;");
    REQUIRE(refused->is_error());
    CHECK(refused->get_error().type == core::error_code_t::connection_closed);
    CHECK(std::string{refused->get_error().what} == "server m2: connection refused");
}

// A table of a connection the host's map does not hold is refused, and the engine keeps running.
TEST_CASE("integration::cpp::host_names::a_run_without_the_connection_in_the_map") {
    HOST_TEST_BOILERPLATE("test_host_names/no_connector")
    declare_orders(host);
    host.restart_with({});
    dispatcher = host.db.dispatcher();
    auto refused = run(dispatcher, "SELECT id FROM m2.shop.public.orders;");
    REQUIRE(refused->is_error());
    CHECK(refused->get_error().type == core::error_code_t::do_not_exists);
    CHECK(std::string{refused->get_error().what} == "connection \"m2\" does not exist");
    REQUIRE(run(dispatcher, "SELECT 1;")->is_success());
}

// A bare name through a place in the connection.
TEST_CASE("integration::cpp::host_names::a_bare_name_through_the_place") {
    HOST_TEST_BOILERPLATE("test_host_names/r5_9")
    declare_orders(host);
    auto read = run_in_shop(dispatcher, "SELECT id FROM orders;");
    INFO(error_of(read));
    REQUIRE(read->is_success());
    CHECK(sorted_int_rows(read) == rows_t{{1}, {2}, {3}});
    CHECK(host.backend().journal("scan") == strings{"scan " + orders});
}

// Rows of a backend's table without columns reach its delete by their numbers.
TEST_CASE("integration::cpp::host_names::delete_rows_without_columns_by_row_number") {
    HOST_TEST_BOILERPLATE("test_host_names/delete_no_columns")
    register_table(host, "marks", {});
    host.backend().set_rows("m2.shop.public.marks", {{}, {}, {}});

    auto deleted = run(dispatcher, "DELETE FROM m2.shop.public.marks LIMIT 2;");
    INFO(error_of(deleted));
    REQUIRE(deleted->is_success());
    CHECK(deleted->affected_rows() == 2);
    CHECK(host.backend().journal("delete") == strings{"delete m2.shop.public.marks rows 2"});
    CHECK(host.backend().rows("m2.shop.public.marks").size() == 1);
}

// A write after a savepoint reaches the backend; ROLLBACK TO undoes the backend's writes from the savepoint on, and
// COMMIT keeps the rest.
TEST_CASE("integration::cpp::host_names::a_storage_write_after_a_savepoint_is_rolled_back_to_it") {
    HOST_TEST_BOILERPLATE("test_host_names/savepoint_write")
    declare_orders(host);
    auto session = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());
    REQUIRE(run(dispatcher, session, "INSERT INTO m2.shop.public.orders (id, amount) VALUES (4, 400);")->is_success());
    REQUIRE(run(dispatcher, session, "SAVEPOINT s;")->is_success());
    for (const char* sql : {"INSERT INTO m2.shop.public.orders (id, amount) VALUES (5, 500);",
                            "UPDATE m2.shop.public.orders SET amount = 1 WHERE id = 1;",
                            "DELETE FROM m2.shop.public.orders WHERE id = 2;"}) {
        INFO(sql);
        auto written = run(dispatcher, session, sql);
        INFO(error_of(written));
        REQUIRE(written->is_success());
    }
    CHECK(host.backend().rows(orders) == rows_t{{1, 1}, {3, 300}, {4, 400}, {5, 500}});
    REQUIRE(run(dispatcher, session, "ROLLBACK TO SAVEPOINT s;")->is_success());
    const auto aborts = host.backend().journal("abort");
    REQUIRE(aborts.size() == 1);
    CHECK(aborts.front() != "abort 0");
    CHECK(host.backend().rows(orders) == rows_t{{1, 100}, {2, 200}, {3, 300}, {4, 400}});
    REQUIRE(run(dispatcher, session, "COMMIT;")->is_success());
    CHECK(host.backend().rows(orders) == rows_t{{1, 100}, {2, 200}, {3, 300}, {4, 400}});
    CHECK(host.backend().open_transactions() == 0);
}

TEST_CASE("integration::cpp::host_names::a_storage_insert_select_after_a_savepoint_is_written") {
    HOST_TEST_BOILERPLATE("test_host_names/savepoint_insert_select")
    declare_orders(host);
    REQUIRE(run(dispatcher, "CREATE SCHEMA loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE loc.t (id BIGINT, amount BIGINT);")->is_success());
    REQUIRE(run(dispatcher, "INSERT INTO loc.t (id, amount) VALUES (8, 800);")->is_success());
    auto session = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());
    REQUIRE(run(dispatcher, session, "SAVEPOINT s;")->is_success());
    auto copied =
        run(dispatcher, session, "INSERT INTO m2.shop.public.orders (id, amount) SELECT id, amount FROM loc.t;");
    INFO(error_of(copied));
    REQUIRE(copied->is_success());
    REQUIRE(run(dispatcher, session, "ROLLBACK;")->is_success());
    CHECK(host.backend().journal("abort") == strings{"abort 0"});
    CHECK(host.backend().rows(orders).size() == 3);
}

// ROLLBACK TO keeps its savepoint open (PostgreSQL 18 xact.c RollbackToSavepoint): a write after it is undone by
// the next ROLLBACK TO as well.
TEST_CASE("integration::cpp::host_names::a_storage_write_after_rollback_to_is_rolled_back_again") {
    HOST_TEST_BOILERPLATE("test_host_names/savepoint_rollback_to")
    declare_orders(host);
    auto session = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());
    REQUIRE(run(dispatcher, session, "SAVEPOINT s;")->is_success());
    REQUIRE(run(dispatcher, session, "ROLLBACK TO SAVEPOINT s;")->is_success());
    REQUIRE(run(dispatcher, session, "INSERT INTO m2.shop.public.orders (id, amount) VALUES (4, 400);")->is_success());
    CHECK(host.backend().rows(orders).size() == 4);
    REQUIRE(run(dispatcher, session, "ROLLBACK TO SAVEPOINT s;")->is_success());
    CHECK(host.backend().rows(orders).size() == 3);
    REQUIRE(run(dispatcher, session, "COMMIT;")->is_success());
    CHECK(host.backend().rows(orders).size() == 3);
}

TEST_CASE("integration::cpp::host_names::a_storage_write_after_releasing_the_savepoints_commits") {
    HOST_TEST_BOILERPLATE("test_host_names/savepoint_release")
    declare_orders(host);
    auto session = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());
    REQUIRE(run(dispatcher, session, "SAVEPOINT outer_one;")->is_success());
    REQUIRE(run(dispatcher, session, "SAVEPOINT inner_one;")->is_success());

    SECTION("the outermost savepoint released") {
        REQUIRE(run(dispatcher, session, "RELEASE SAVEPOINT outer_one;")->is_success());
        auto inserted = run(dispatcher, session, "INSERT INTO m2.shop.public.orders (id, amount) VALUES (4, 400);");
        INFO(error_of(inserted));
        REQUIRE(inserted->is_success());
        REQUIRE(run(dispatcher, session, "COMMIT;")->is_success());
        CHECK(host.backend().journal("insert") == strings{"insert " + orders + " id:bigint,amount:bigint"});
        CHECK(host.backend().rows(orders).size() == 4);
    }
    SECTION("an inner savepoint released, the outer one rolled back to") {
        REQUIRE(run(dispatcher, session, "RELEASE SAVEPOINT inner_one;")->is_success());
        REQUIRE(
            run(dispatcher, session, "INSERT INTO m2.shop.public.orders (id, amount) VALUES (4, 400);")->is_success());
        REQUIRE(run(dispatcher, session, "ROLLBACK TO SAVEPOINT outer_one;")->is_success());
        REQUIRE(run(dispatcher, session, "COMMIT;")->is_success());
        CHECK(host.backend().rows(orders).size() == 3);
    }
}

// A read inside a transaction makes the backend a participant: it learns of the end, by COMMIT or ROLLBACK.
TEST_CASE("integration::cpp::host_names::a_storage_read_after_a_savepoint_works") {
    HOST_TEST_BOILERPLATE("test_host_names/savepoint_read")
    declare_orders(host);
    auto session = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());
    REQUIRE(run(dispatcher, session, "SAVEPOINT s;")->is_success());
    auto read = run(dispatcher, session, "SELECT id, amount FROM m2.shop.public.orders;");
    INFO(error_of(read));
    REQUIRE(read->is_success());
    CHECK(sorted_int_rows(read) == rows_t{{1, 100}, {2, 200}, {3, 300}});
    CHECK(host.backend().open_transactions() == 1);
    REQUIRE(run(dispatcher, session, "COMMIT;")->is_success());
    CHECK(host.backend().journal("release") == strings{"release"});
    CHECK(host.backend().open_transactions() == 0);
}

// An autocommit read of a backend's table ends its transaction there too.
TEST_CASE("integration::cpp::host_names::an_autocommit_read_ends_at_the_backend") {
    HOST_TEST_BOILERPLATE("test_host_names/autocommit_read")
    declare_orders(host);
    REQUIRE(run(dispatcher, "SELECT id FROM m2.shop.public.orders;")->is_success());
    CHECK(host.backend().journal("release") == strings{"release"});
    CHECK(host.backend().open_transactions() == 0);

    auto session = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());
    REQUIRE(run(dispatcher, session, "SELECT id FROM m2.shop.public.orders;")->is_success());
    REQUIRE(run(dispatcher, session, "ROLLBACK;")->is_success());
    CHECK(host.backend().journal("abort") == strings{"abort 0"});
    CHECK(host.backend().open_transactions() == 0);
}

TEST_CASE("integration::cpp::host_names::a_savepoint_over_local_tables_still_works_in_a_host_topology") {
    HOST_TEST_BOILERPLATE("test_host_names/savepoint_local")
    declare_orders(host);
    REQUIRE(run(dispatcher, "CREATE SCHEMA loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE loc.t (id BIGINT, amount BIGINT);")->is_success());
    REQUIRE(run(dispatcher, "INSERT INTO loc.t (id, amount) VALUES (1, 10);")->is_success());
    auto session = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());
    REQUIRE(run(dispatcher, session, "SAVEPOINT s;")->is_success());
    REQUIRE(run(dispatcher, session, "INSERT INTO loc.t (id, amount) VALUES (2, 20);")->is_success());
    auto copied =
        run(dispatcher, session, "INSERT INTO loc.t (id, amount) SELECT id, amount FROM m2.shop.public.orders;");
    INFO(error_of(copied));
    REQUIRE(copied->is_success());
    auto before = run(dispatcher, session, "SELECT id, amount FROM loc.t;");
    REQUIRE(before->is_success());
    CHECK(sorted_int_rows(before) == rows_t{{1, 10}, {1, 100}, {2, 20}, {2, 200}, {3, 300}});
    REQUIRE(run(dispatcher, session, "ROLLBACK TO SAVEPOINT s;")->is_success());
    auto after = run(dispatcher, session, "SELECT id, amount FROM loc.t;");
    REQUIRE(after->is_success());
    CHECK(sorted_int_rows(after) == rows_t{{1, 10}});
    REQUIRE(run(dispatcher, session, "COMMIT;")->is_success());
    CHECK(sorted_int_rows(run(dispatcher, "SELECT id, amount FROM loc.t;")) == rows_t{{1, 10}});
    CHECK(host.backend().journal("insert").empty());
}

// A LIMIT over a WHERE of a backend's table the backend did not take is otterbrix's, after the filter: the scan
// reads every row.
TEST_CASE("integration::cpp::host_names::a_limit_over_a_where_counts_the_matching_rows") {
    HOST_TEST_BOILERPLATE("test_host_names/limit_over_where")
    declare_orders(host);
    auto found = run(dispatcher, "SELECT id, amount FROM m2.shop.public.orders WHERE amount = 300 LIMIT 1;");
    INFO(error_of(found));
    REQUIRE(found->is_success());
    CHECK(sorted_int_rows(found) == rows_t{{3, 300}});
}

// INSERT ... ON CONFLICT into a backend's table the backend did not take is refused: otterbrix cannot find the
// backend's conflicting row.
TEST_CASE("integration::cpp::host_names::on_conflict_into_a_backends_table_is_refused") {
    HOST_TEST_BOILERPLATE("test_host_names/on_conflict")
    declare_orders(host);
    auto refused =
        run(dispatcher, "INSERT INTO m2.shop.public.orders (id, amount) VALUES (1, 5) ON CONFLICT DO NOTHING;");
    REQUIRE(refused->is_error());
    CHECK(refused->get_error().type == core::error_code_t::unimplemented_yet);
    CHECK(std::string{refused->get_error().what} == "INSERT ... ON CONFLICT into table \"" + orders +
                                                        "\" of connection \"m2\" is not supported unless its "
                                                        "backend takes the statement");
    CHECK(host.backend().journal("insert").empty());
}

// The columns of a scan are the catalog's (a backend has no message that answers them): a LATERAL inner scan read
// once per outer row reads, and an empty table answers the catalog's columns.
TEST_CASE("integration::cpp::host_names::a_scan_takes_its_columns_from_the_catalog") {
    HOST_TEST_BOILERPLATE("test_host_names/columns_from_catalog")
    declare_orders(host);
    REQUIRE(run(dispatcher, "CREATE SCHEMA loc;")->is_success());
    REQUIRE(run(dispatcher, "CREATE TABLE loc.d (id BIGINT);")->is_success());
    REQUIRE(run(dispatcher, "INSERT INTO loc.d (id) VALUES (1), (2), (3);")->is_success());
    auto lateral =
        run(dispatcher,
            "SELECT d.id, s.c FROM loc.d AS d, LATERAL (SELECT count(*) AS c FROM m2.shop.public.orders AS o "
            "WHERE o.id = d.id) AS s;");
    INFO(error_of(lateral));
    REQUIRE(lateral->is_success());
    CHECK(sorted_int_rows(lateral) == rows_t{{1, 1}, {2, 1}, {3, 1}});

    REQUIRE(run(dispatcher, "CREATE TABLE m2.shop.public.empty (id BIGINT, amount BIGINT);")->is_success());
    auto empty = run(dispatcher, "SELECT * FROM m2.shop.public.empty;");
    INFO(error_of(empty));
    REQUIRE(empty->is_success());
    CHECK(empty->size() == 0);
    REQUIRE_FALSE(empty->chunks().empty());
    CHECK(empty->chunks().front().column_count() == 2);
}
