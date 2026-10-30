// A cursor reads its statement's rows as the client asks for them: DECLARE starts the statement and runs nothing,
// FETCH n runs it until it has n rows and keeps what is left for the next one, as PostgreSQL 18's portals do
// (pquery.c PortalRunSelect). A host reads the same portal through a window on execute_plan and fetch_next
// (docs/embedding-host-api.md, "Cursors and windows").

#include "host_fixture.hpp"

#include <components/compute/function.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <core/pmr.hpp>
#include <core/tests/wait_ready.hpp>
#include <services/collection/executor.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/dispatcher/dispatcher.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

using host_fixture::host_t;
using host_fixture::strings;
using code = core::error_code_t;
using components::cursor::cursor_t_ptr;
using components::types::logical_type;

namespace {

    otterbrix::session_id_t any() { return otterbrix::session_id_t(); }

    constexpr std::int64_t kBatches = 20;
    constexpr std::int64_t kRows = kBatches * 1024;

    // The awaits of the sources that read a table: the FETCH's own source, which reads the portal, is not one.
    std::atomic<std::uint64_t> g_source_awaits{0};
    void count_source_await(const components::operators::operator_t* source) {
        if (source->type() != components::operators::operator_type::portal_fetch) {
            g_source_awaits.fetch_add(1);
        }
    }

    struct source_awaits_t {
        source_awaits_t() {
            g_source_awaits.store(0);
            services::collection::executor::dev_set_source_await_hook(&count_source_await);
        }
        ~source_awaits_t() { services::collection::executor::dev_set_source_await_hook(nullptr); }
        std::uint64_t count() const { return g_source_awaits.load(); }
    };

    // s.big (a BIGINT, b TEXT, d DECIMAL(10,2)) of kRows rows: a = row, b NULL on every 7th row, d on every 5th.
    void big(host_t& host) {
        host.db.ok(any(), "CREATE SCHEMA s;");
        host.db.ok(any(), "CREATE TABLE s.big (a BIGINT, b TEXT, d DECIMAL(10,2));");
        for (std::int64_t from = 0; from < kRows; from += 2048) {
            std::string sql = "INSERT INTO s.big (a, b, d) VALUES ";
            for (std::int64_t row = from; row < from + 2048; ++row) {
                sql += "(" + std::to_string(row) + ", ";
                sql += row % 7 == 0 ? std::string{"NULL"} : "'r" + std::to_string(row) + "'";
                sql += ", ";
                sql += row % 5 == 0 ? std::string{"NULL"} : std::to_string(row) + ".25";
                sql += row + 1 == from + 2048 ? ");" : "), ";
            }
            host.db.ok(any(), sql);
        }
    }

    template<typename T>
    T value_of(core::result_wrapper_t<T> read) {
        REQUIRE_FALSE(read.has_error());
        return read.value();
    }

    std::vector<std::int64_t> column_a(const cursor_t_ptr& cursor) {
        INFO(session_sql::error_of(cursor));
        REQUIRE(cursor->is_success());
        std::vector<std::int64_t> out;
        for (std::uint64_t row = 0; row < cursor->size(); ++row) {
            out.push_back(value_of(cursor->get<std::int64_t>(0, row)));
        }
        return out;
    }

    // The first column of every cursor, INTEGER or BIGINT, one after the other.
    std::vector<std::int64_t> numbers(const std::vector<cursor_t_ptr>& cursors) {
        std::vector<std::int64_t> out;
        for (const auto& cursor : cursors) {
            INFO(session_sql::error_of(cursor));
            REQUIRE(cursor->is_success());
            for (std::uint64_t row = 0; row < cursor->size(); ++row) {
                if (cursor->type_data()[0].type() == logical_type::INTEGER) {
                    out.push_back(value_of(cursor->get<std::int32_t>(0, row)));
                } else {
                    out.push_back(value_of(cursor->get<std::int64_t>(0, row)));
                }
            }
        }
        return out;
    }

    std::vector<std::int64_t> range(std::int64_t from, std::int64_t to) {
        std::vector<std::int64_t> out;
        for (auto v = from; v < to; ++v) {
            out.push_back(v);
        }
        return out;
    }

    // One cell as text, by its column's type: what two results must agree on.
    std::string cell(const cursor_t_ptr& cursor, std::uint64_t column, std::uint64_t row) {
        if (value_of(cursor->is_null(column, row))) {
            return "<null>";
        }
        const auto type = cursor->type_data()[column].type();
        switch (type) {
            case logical_type::BIGINT:
                return std::to_string(value_of(cursor->get<std::int64_t>(column, row)));
            case logical_type::STRING_LITERAL:
                return std::string{value_of(cursor->get<std::string_view>(column, row))};
            case logical_type::DECIMAL:
                return std::to_string(value_of(cursor->get<std::int64_t>(column, row)));
            default:
                FAIL("no comparison for type " << static_cast<int>(type));
                return {};
        }
    }

    // Every row of every cursor, one string per row, with the types of the columns first.
    std::vector<std::string> rows_of(const std::vector<cursor_t_ptr>& cursors) {
        std::vector<std::string> out;
        for (const auto& cursor : cursors) {
            INFO(session_sql::error_of(cursor));
            REQUIRE(cursor->is_success());
            if (cursor->size() == 0) {
                continue;
            }
            std::string types;
            for (const auto& type : cursor->type_data()) {
                types += std::to_string(static_cast<int>(type.type())) + ":" + type.alias() + " ";
            }
            for (std::uint64_t row = 0; row < cursor->size(); ++row) {
                std::string line = types + "|";
                for (std::uint64_t column = 0; column < cursor->column_count(); ++column) {
                    line += cell(cursor, column, row) + "|";
                }
                out.push_back(std::move(line));
            }
        }
        return out;
    }

    // The plan execute_sql would send for `sql`, for a host to put a window on.
    components::logical_plan::execution_plan_t plan_of(host_t& host, const std::string& sql) {
        auto* resource = host.resource();
        core::pmr::arena_resource_t arena(resource);
        auto parsed = raw_parser(&arena, sql.c_str());
        REQUIRE_FALSE(parsed.has_error());
        components::sql::transform::transformer transformer(resource, sql.c_str());
        auto finalized =
            transformer.transform(components::sql::transform::pg_cell_to_node_cast(linitial(parsed.value())))
                .finalize();
        REQUIRE_FALSE(finalized.has_error());
        return std::move(finalized.value());
    }

    cursor_t_ptr window(host_t& host, const otterbrix::session_id_t& session, const std::string& sql, uint64_t n) {
        auto plan = plan_of(host, sql);
        plan.fetch_window = n;
        return host.send(session, std::move(plan));
    }

    std::uint64_t watermark(host_t& host) {
        auto [_, future] =
            actor_zeta::otterbrix::send(host.db.space->dispatcher_address(),
                                        &services::dispatcher::manager_dispatcher_t::txn_compact_watermark_msg);
        return actor_zeta::otterbrix::wait_ready(future);
    }

    std::vector<std::int64_t> sorted(std::vector<std::int64_t> values) {
        std::sort(values.begin(), values.end());
        return values;
    }

    // The first column of `query`'s rows, sorted: as execute_sql answers it, and as a cursor's FETCH 2, FETCH 1500 and
    // FETCH ALL answer it.
    std::pair<std::vector<std::int64_t>, std::vector<std::int64_t>> whole_and_fetched(host_t& host,
                                                                                      const std::string& query) {
        auto whole = numbers({host.db.ok(any(), query + ";")});
        const auto session = any();
        host.db.ok(session, "BEGIN;");
        host.db.ok(session, "DECLARE c CURSOR FOR " + query + ";");
        auto fetched = numbers({host.db.ok(session, "FETCH 2 FROM c;"),
                                host.db.ok(session, "FETCH 1500 FROM c;"),
                                host.db.ok(session, "FETCH ALL FROM c;")});
        host.db.ok(session, "COMMIT;");
        return {sorted(std::move(whole)), sorted(std::move(fetched))};
    }

    // The first column of `sql`'s rows through a window of `n` and fetch_next to its end.
    std::vector<std::int64_t> windowed(host_t& host, const std::string& sql, uint64_t n) {
        const auto session = any();
        std::vector<cursor_t_ptr> pages{window(host, session, sql, n)};
        while (pages.back()->is_success() && pages.back()->has_more()) {
            pages.push_back(host.db.dispatcher()->fetch_next(session, pages.front()->portal_id(), n));
        }
        return numbers(pages);
    }

    core::error_t twice_exec(components::compute::kernel_context&,
                             const components::vector::data_chunk_t& in,
                             components::vector::vector_t& out) {
        const auto* source = in.data[0].data<std::int64_t>();
        auto* destination = out.data<std::int64_t>();
        for (std::uint64_t row = 0; row < in.size(); ++row) {
            destination[row] = source[row] * 2;
        }
        return core::error_t::no_error();
    }

    // twice(BIGINT) -> BIGINT.
    core::pmr::polymorphic_unique_ptr<components::compute::vector_function>
    make_twice(std::pmr::memory_resource* resource) {
        using namespace components::compute;
        function_doc doc{resource, "twice", "multiplies by 2", {"arg"}, false};
        auto fn = core::pmr::make_polymorphic_unique<vector_function>(resource,
                                                                      "twice",
                                                                      arity::unary(),
                                                                      doc,
                                                                      std::size_t{1},
                                                                      volatility_t::volatile_);
        kernel_signature_t signature(function_type_t::vector,
                                     {parameter_type::exact(logical_type::BIGINT)},
                                     {output_type::fixed(logical_type::BIGINT)});
        REQUIRE_FALSE(fn->add_kernel(resource, vector_kernel{std::move(signature), twice_exec}).contains_error());
        return fn;
    }

    const std::string orders = "m2.shop.public.orders";

    // m2.shop.public.orders with `count` rows (id, id * 10), answered in pages of 2.
    void orders_of(host_t& host, std::int64_t count) {
        host.register_orders();
        host_actor::rows_t rows;
        for (std::int64_t id = 1; id <= count; ++id) {
            rows.push_back({id, id * 10});
        }
        host.backend().set_rows(orders, rows);
        host.backend().set_batches(orders, std::vector<std::size_t>(static_cast<std::size_t>(count), 2));
        host.backend().clear_journal();
    }

} // namespace

// 1-3: the first FETCH answers after one batch of the scan, a pause runs nothing, and the portal keeps at most the
// rest of one batch beside the next one.
TEST_CASE("integration::cpp::streaming_cursor::fetch_reads_as_far_as_it_answers") {
    host_t host("test_streaming_cursor/batches");
    big(host);
    const auto session = any();
    host.db.ok(session, "BEGIN;");
    source_awaits_t awaits;
    auto declared = host.db.ok(session, "DECLARE c CURSOR FOR SELECT a FROM s.big;");
    CHECK(declared->size() == 0);
    CHECK(awaits.count() == 0);

    auto first = host.db.ok(session, "FETCH 100 FROM c;");
    CHECK(column_a(first) == range(0, 100));
    const auto after_first = awaits.count();
    CHECK(after_first < static_cast<std::uint64_t>(kBatches));

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    CHECK(awaits.count() == after_first);
    CHECK(column_a(host.db.ok(session, "FETCH 100 FROM c;")) == range(100, 200));

    services::collection::executor::dev_reset_portal_carry_peak();
    std::int64_t next = 200;
    for (;;) {
        auto page = host.db.ok(session, "FETCH 1024 FROM c;");
        if (page->size() == 0) {
            break;
        }
        CHECK(column_a(page) == range(next, std::min(next + 1024, kRows)));
        next += static_cast<std::int64_t>(page->size());
    }
    CHECK(next == kRows);
    CHECK(services::collection::executor::dev_portal_carry_peak() <= 2);
    host.db.ok(session, "COMMIT;");
}

// 4: the FETCHes put together are the rows execute_sql answers, cell for cell, with their types.
TEST_CASE("integration::cpp::streaming_cursor::the_fetches_are_the_result") {
    host_t host("test_streaming_cursor/same_rows");
    big(host);
    const auto whole = host.db.ok(any(), "SELECT a, b, d FROM s.big;");
    const auto expected = rows_of({whole});
    REQUIRE(expected.size() == static_cast<std::size_t>(kRows));

    const auto session = any();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "DECLARE c CURSOR FOR SELECT a, b, d FROM s.big;");
    std::vector<cursor_t_ptr> pages;
    for (;;) {
        auto page = host.db.ok(session, "FETCH 1000 FROM c;");
        if (page->size() == 0) {
            break;
        }
        pages.push_back(page);
    }
    CHECK(rows_of(pages) == expected);
    host.db.ok(session, "DECLARE d CURSOR FOR SELECT a, b, d FROM s.big;");
    CHECK(rows_of({host.db.ok(session, "FETCH ALL FROM d;")}) == expected);
    CHECK(host.db.ok(session, "FETCH ALL FROM d;")->size() == 0);
    host.db.ok(session, "COMMIT;");
}

// A root that runs itself (a WITH RECURSIVE whose rows are the statement's, a write and its RETURNING) answers each of
// its rows once, however the fetches cut them.
TEST_CASE("integration::cpp::streaming_cursor::a_root_that_runs_itself_answers_each_row_once") {
    host_t host("test_streaming_cursor/self_running_root");
    const std::string recursive =
        "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3000) SELECT * FROM r";
    const auto session = any();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "DECLARE c CURSOR FOR " + recursive + ";");
    CHECK(numbers({host.db.ok(session, "FETCH 2 FROM c;"),
                   host.db.ok(session, "FETCH 1500 FROM c;"),
                   host.db.ok(session, "FETCH ALL FROM c;")}) == range(1, 3001));
    host.db.ok(session, "COMMIT;");
    CHECK(windowed(host, recursive + ";", 100) == range(1, 3001));

    host.db.ok(any(), "CREATE SCHEMA s;");
    host.db.ok(any(), "CREATE TABLE s.t (a BIGINT);");
    host.db.ok(any(), "CREATE TABLE s.u (b BIGINT);");
    host.db.ok(any(), "CREATE TABLE s.w (id BIGINT);");
    std::string values = "INSERT INTO s.t (a) VALUES ";
    for (std::int64_t a = 1; a <= 3000; ++a) {
        values += "(" + std::to_string(a) + (a == 3000 ? ");" : "), ");
    }
    host.db.ok(any(), values);
    host.db.ok(any(), "INSERT INTO s.u (b) VALUES (1), (2);");
    for (const std::string query :
         {"WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3000) SELECT n FROM r",
          "WITH RECURSIVE r AS (SELECT 1 AS n UNION SELECT n + 1 FROM r WHERE n < 3000) SELECT * FROM r",
          "SELECT a FROM s.t UNION ALL SELECT a FROM s.t",
          "SELECT a FROM s.t UNION SELECT a FROM s.t",
          "SELECT * FROM s.t t, LATERAL (SELECT u.b FROM s.u u) l",
          "WITH x AS (SELECT a FROM s.t) SELECT a FROM x"}) {
        INFO(query);
        const auto [whole, fetched] = whole_and_fetched(host, query);
        CHECK(fetched == whole);
        CHECK(sorted(windowed(host, query + ";", 100)) == whole);
    }
    CHECK(sorted(windowed(host, "INSERT INTO s.w (id) SELECT a FROM s.t RETURNING id;", 100)) == range(1, 3001));
}

// 5: COMMIT, ROLLBACK and close_session close the open cursor of a backend before its transaction ends.
TEST_CASE("integration::cpp::streaming_cursor::the_end_of_the_transaction_closes_the_backend_cursor") {
    host_t host("test_streaming_cursor/backend_end");
    orders_of(host, 10);
    for (const std::string end : {"COMMIT;", "ROLLBACK;", "close"}) {
        INFO(end);
        const auto session = any();
        host.backend().clear_journal();
        const auto closes = host.backend().cursor_closes();
        host.db.ok(session, "BEGIN;");
        host.db.ok(session, "DECLARE c CURSOR FOR SELECT id FROM m2.shop.public.orders;");
        CHECK(column_a(host.db.ok(session, "FETCH 3 FROM c;")) == range(1, 4));
        REQUIRE(host.backend().open_transactions() == 1);
        if (end == "close") {
            host.db.dispatcher()->close_session(session);
        } else {
            host.db.ok(session, end);
        }
        CHECK(host.backend().cursor_closes() == closes + 1);
        CHECK(host.backend().open_cursors() == 0);
        if (end != "COMMIT;") {
            CHECK(host.backend().journal("abort") == strings{"abort 0"});
            CHECK(host.backend().open_cursors_at_abort() == std::vector<std::size_t>{0});
        }
        CHECK(host.backend().open_transactions() == 0);
    }
}

// 6: ROLLBACK TO a savepoint closes the cursors declared after it, as PostgreSQL 18's AtSubAbort_Portals.
TEST_CASE("integration::cpp::streaming_cursor::rollback_to_closes_the_cursors_after_the_savepoint") {
    host_t host("test_streaming_cursor/rollback_to");
    big(host);
    const auto session = any();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "DECLARE c1 CURSOR FOR SELECT a FROM s.big;");
    host.db.ok(session, "SAVEPOINT p;");
    host.db.ok(session, "DECLARE c2 CURSOR FOR SELECT a FROM s.big;");
    host.db.ok(session, "ROLLBACK TO p;");
    host.db.check_refused(session, "FETCH 1 FROM c2;", code::undefined_cursor, "cursor \"c2\" does not exist");
    host.db.ok(session, "ROLLBACK;");
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "DECLARE c1 CURSOR FOR SELECT a FROM s.big;");
    host.db.ok(session, "SAVEPOINT p;");
    host.db.ok(session, "ROLLBACK TO p;");
    CHECK(column_a(host.db.ok(session, "FETCH 2 FROM c1;")) == range(0, 2));
    host.db.ok(session, "COMMIT;");
}

// 7: the refusals PostgreSQL 18 answers for a cursor, and FETCH past the end.
TEST_CASE("integration::cpp::streaming_cursor::what_a_cursor_refuses") {
    host_t host("test_streaming_cursor/refusals");
    big(host);
    const auto session = any();
    host.db.check_refused(session,
                          "DECLARE c CURSOR FOR SELECT a FROM s.big;",
                          code::no_active_sql_transaction,
                          "DECLARE CURSOR can only be used in transaction blocks");
    host.db.ok(session, "BEGIN;");
    host.db.check_refused(session, "FETCH 1 FROM nope;", code::undefined_cursor, "cursor \"nope\" does not exist");
    host.db.ok(session, "ROLLBACK;");

    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "DECLARE c CURSOR FOR SELECT a FROM s.big WHERE a < 3;");
    host.db.check_refused(session,
                          "DECLARE c CURSOR FOR SELECT a FROM s.big;",
                          code::duplicate_cursor,
                          "cursor \"c\" already exists");
    host.db.ok(session, "ROLLBACK;");

    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "DECLARE c CURSOR FOR SELECT a FROM s.big WHERE a < 3;");
    CHECK(column_a(host.db.ok(session, "FETCH 5 FROM c;")) == range(0, 3));
    CHECK(host.db.ok(session, "FETCH 5 FROM c;")->size() == 0);
    host.db.ok(session, "CLOSE c;");
    host.db.check_refused(session, "FETCH 1 FROM c;", code::undefined_cursor, "cursor \"c\" does not exist");
    host.db.ok(session, "ROLLBACK;");

    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "DECLARE c CURSOR FOR SELECT a FROM s.big;");
    host.db.ok(session, "DECLARE d CURSOR FOR SELECT a FROM s.big;");
    host.db.ok(session, "CLOSE ALL;");
    host.db.check_refused(session, "FETCH 1 FROM d;", code::undefined_cursor, "cursor \"d\" does not exist");
    host.db.ok(session, "ROLLBACK;");
    host.db.ok(session, "BEGIN;");
    host.db.check_refused(session, "CLOSE c;", code::undefined_cursor, "cursor \"c\" does not exist");
    host.db.ok(session, "ROLLBACK;");
}

// 8: only forward FETCH and MOVE are run; a cursor that scrolls or outlives its transaction is not.
TEST_CASE("integration::cpp::streaming_cursor::backward_scroll_and_hold_are_not_supported") {
    host_t host("test_streaming_cursor/not_supported");
    big(host);
    const auto session = any();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "DECLARE c CURSOR FOR SELECT a FROM s.big;");
    for (const std::string fetch : {"FETCH PRIOR FROM c;",
                                    "FETCH FIRST FROM c;",
                                    "FETCH LAST FROM c;",
                                    "FETCH ABSOLUTE 3 FROM c;",
                                    "FETCH RELATIVE 1 FROM c;",
                                    "FETCH BACKWARD FROM c;",
                                    "FETCH -1 FROM c;",
                                    "MOVE BACKWARD 2 IN c;"}) {
        INFO(fetch);
        const auto refused = host.db.dispatcher()->execute_sql(session, fetch);
        REQUIRE(refused->is_error());
        CHECK(refused->get_error().type == code::unimplemented_yet);
    }
    host.db.ok(session, "ROLLBACK;");
    for (const std::string declare :
         {"DECLARE c SCROLL CURSOR FOR SELECT a FROM s.big;", "DECLARE c CURSOR WITH HOLD FOR SELECT a FROM s.big;"}) {
        INFO(declare);
        host.db.ok(session, "BEGIN;");
        const auto refused = host.db.dispatcher()->execute_sql(session, declare);
        REQUIRE(refused->is_error());
        CHECK(refused->get_error().type == code::unimplemented_yet);
        host.db.ok(session, "ROLLBACK;");
    }
}

// 9: the options PostgreSQL 18 parses and that change nothing for a cursor that reads forward.
TEST_CASE("integration::cpp::streaming_cursor::the_options_of_a_forward_cursor") {
    host_t host("test_streaming_cursor/options");
    big(host);
    const auto session = any();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "DECLARE c1 NO SCROLL CURSOR FOR SELECT a FROM s.big;");
    host.db.ok(session, "DECLARE c2 BINARY CURSOR FOR SELECT a FROM s.big;");
    host.db.ok(session, "DECLARE c3 INSENSITIVE CURSOR FOR SELECT a FROM s.big;");
    host.db.ok(session, "DECLARE c4 ASENSITIVE CURSOR FOR SELECT a FROM s.big;");
    host.db.ok(session, "DECLARE c5 CURSOR WITHOUT HOLD FOR SELECT a FROM s.big;");
    for (const std::string name : {"c1", "c2", "c3", "c4", "c5"}) {
        CHECK(column_a(host.db.ok(session, "FETCH 2 FROM " + name + ";")) == range(0, 2));
    }
    host.db.ok(session, "COMMIT;");
    for (const std::string declare : {"DECLARE c SENSITIVE CURSOR FOR SELECT a FROM s.big;",
                                      "DECLARE c PARALLEL RETRIEVE CURSOR FOR SELECT a FROM s.big;"}) {
        INFO(declare);
        host.db.ok(session, "BEGIN;");
        const auto refused = host.db.dispatcher()->execute_sql(session, declare);
        REQUIRE(refused->is_error());
        CHECK(refused->get_error().type == code::sql_parse_error);
        host.db.ok(session, "ROLLBACK;");
    }
}

// 10: MOVE answers how many rows it passed; FETCH 0 answers nothing before the first row and after the last, and on a
// row it would read it again, which a cursor that only reads forward refuses (pquery.c DoPortalRunFetch).
TEST_CASE("integration::cpp::streaming_cursor::move_and_fetch_zero") {
    host_t host("test_streaming_cursor/move");
    big(host);
    const auto session = any();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "DECLARE c CURSOR FOR SELECT a FROM s.big WHERE a < 8;");
    CHECK(host.db.ok(session, "FETCH 0 FROM c;")->size() == 0);
    CHECK(host.db.ok(session, "MOVE 0 IN c;")->affected_rows() == 0);
    CHECK(host.db.ok(session, "MOVE FORWARD 5 IN c;")->affected_rows() == 5);
    CHECK(column_a(host.db.ok(session, "FETCH 1 FROM c;")) == range(5, 6));
    CHECK(host.db.ok(session, "MOVE 0 IN c;")->affected_rows() == 1);
    CHECK(host.db.ok(session, "MOVE ALL IN c;")->affected_rows() == 2);
    CHECK(host.db.ok(session, "FETCH 0 FROM c;")->size() == 0);
    CHECK(host.db.ok(session, "MOVE 0 IN c;")->affected_rows() == 0);
    host.db.ok(session, "COMMIT;");

    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "DECLARE c CURSOR FOR SELECT a FROM s.big;");
    host.db.ok(session, "FETCH 1 FROM c;");
    host.db.check_refused(session,
                          "FETCH 0 FROM c;",
                          code::object_not_in_prerequisite_state,
                          "cursor can only scan forward");
    host.db.ok(session, "ROLLBACK;");
}

// MOVE passes the rows without keeping them, as PostgreSQL 18's portal keeps none: at most the rest of one batch beside
// the next one waits in the portal.
TEST_CASE("integration::cpp::streaming_cursor::move_keeps_no_rows") {
    host_t host("test_streaming_cursor/move_keeps_none");
    big(host);
    const auto session = any();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "DECLARE c CURSOR FOR SELECT a FROM s.big;");
    services::collection::executor::dev_reset_portal_carry_peak();
    CHECK(host.db.ok(session, "MOVE FORWARD 5000 IN c;")->affected_rows() == 5000);
    CHECK(column_a(host.db.ok(session, "FETCH 1 FROM c;")) == range(5000, 5001));
    CHECK(host.db.ok(session, "MOVE ALL IN c;")->affected_rows() == kRows - 5001);
    CHECK(services::collection::executor::dev_portal_carry_peak() <= 2);
    CHECK(host.db.ok(session, "FETCH 1 FROM c;")->size() == 0);
    host.db.ok(session, "COMMIT;");
}

// DECLARE answers as PostgreSQL 18 does: what the analysis of its query finds first, then that it runs outside a block
// (PerformCursorOpen's RequireTransactionBlock), then a name taken (CreatePortal, after the plan); outside a block
// nothing of its query runs, its sub-selects included.
TEST_CASE("integration::cpp::streaming_cursor::the_order_of_the_refusals_of_declare") {
    host_t host("test_streaming_cursor/declare_order");
    big(host);
    const auto session = any();
    const auto missing = host.db.refused(session, "SELECT * FROM nope;");
    REQUIRE(missing.type != code::no_active_sql_transaction);
    const auto outside = host.db.refused(session, "DECLARE c CURSOR FOR SELECT * FROM nope;");
    CHECK(outside.type == missing.type);
    CHECK(std::string{outside.what} == std::string{missing.what});
    {
        source_awaits_t awaits;
        host.db.check_refused(session,
                              "DECLARE c CURSOR FOR SELECT a FROM s.big WHERE a > (SELECT min(a) FROM s.big);",
                              code::no_active_sql_transaction,
                              "DECLARE CURSOR can only be used in transaction blocks");
        CHECK(awaits.count() == 0);
    }
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "DECLARE c CURSOR FOR SELECT a FROM s.big;");
    const auto taken = host.db.refused(session, "DECLARE c CURSOR FOR SELECT * FROM nope;");
    CHECK(taken.type == missing.type);
    host.db.ok(session, "ROLLBACK;");
}

// register_udf and unregister_udf are inputs of the session: they close its window first, as any statement does.
TEST_CASE("integration::cpp::streaming_cursor::a_function_registration_closes_the_window") {
    host_t host("test_streaming_cursor/udf_closes_window");
    big(host);
    const auto session = any();
    for (const std::string step : {"register", "unregister"}) {
        INFO(step);
        auto first = window(host, session, "SELECT a FROM s.big;", 100);
        REQUIRE(first->has_more());
        const auto held = watermark(host);
        if (step == "register") {
            REQUIRE_FALSE(host.db.dispatcher()->register_udf(session, make_twice(host.resource())).contains_error());
        } else {
            std::pmr::vector<components::types::complex_logical_type> inputs{host.resource()};
            inputs.emplace_back(logical_type::BIGINT);
            REQUIRE_FALSE(host.db.dispatcher()->unregister_udf(session, "twice", inputs).contains_error());
        }
        const auto gone = host.db.dispatcher()->fetch_next(session, first->portal_id(), 1);
        REQUIRE(gone->is_error());
        CHECK(gone->get_error().type == code::undefined_cursor);
        host.db.ok(any(), "INSERT INTO s.big (a) VALUES (-1);");
        CHECK(watermark(host) > held);
    }
}

namespace {
    // s.t (a BIGINT) of the rows 1..5000.
    void five_thousand(host_t& host) {
        host.db.ok(any(), "CREATE SCHEMA s;");
        host.db.ok(any(), "CREATE TABLE s.t (a BIGINT);");
        std::string values = "INSERT INTO s.t (a) VALUES ";
        for (std::int64_t a = 1; a <= 5000; ++a) {
            values += "(" + std::to_string(a) + (a == 5000 ? ");" : "), ");
        }
        host.db.ok(any(), values);
    }
} // namespace

// A cursor reads its transaction's own writes as they were at its DECLARE, as PostgreSQL 18's portal reads with the
// DECLARE's command id (curcid): what the transaction writes after the DECLARE is not seen, what it wrote before is.
TEST_CASE("integration::cpp::streaming_cursor::a_cursor_does_not_see_the_writes_after_its_declare") {
    host_t host("test_streaming_cursor/own_writes");
    five_thousand(host);
    const auto session = any();

    INFO("a DELETE between two FETCHes");
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "DECLARE c CURSOR FOR SELECT a FROM s.t;");
    CHECK(host.db.ok(session, "FETCH 1 FROM c;")->size() == 1);
    host.db.ok(session, "DELETE FROM s.t WHERE a > 10;");
    CHECK(host.db.ok(session, "FETCH ALL FROM c;")->size() == 4999);
    CHECK(host.db.ints(session, "SELECT count(*) FROM s.t;") == std::vector<std::int64_t>{10});
    host.db.ok(session, "ROLLBACK;");

    INFO("an UPDATE between two FETCHes");
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "DECLARE c CURSOR FOR SELECT a FROM s.t;");
    CHECK(host.db.ok(session, "FETCH 1 FROM c;")->size() == 1);
    host.db.ok(session, "UPDATE s.t SET a = -a WHERE a > 2000;");
    const auto rest = column_a(host.db.ok(session, "FETCH ALL FROM c;"));
    CHECK(rest.size() == 4999);
    CHECK(std::none_of(rest.begin(), rest.end(), [](std::int64_t a) { return a < 0; }));
    host.db.ok(session, "ROLLBACK;");

    INFO("a DELETE before the first FETCH");
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "DECLARE c CURSOR FOR SELECT a FROM s.t;");
    host.db.ok(session, "DELETE FROM s.t WHERE a > 10;");
    CHECK(host.db.ok(session, "FETCH ALL FROM c;")->size() == 5000);
    host.db.ok(session, "ROLLBACK;");

    INFO("a DELETE before the DECLARE");
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "DELETE FROM s.t WHERE a > 2000;");
    host.db.ok(session, "DECLARE c CURSOR FOR SELECT a FROM s.t;");
    CHECK(host.db.ok(session, "FETCH ALL FROM c;")->size() == 2000);
    host.db.ok(session, "ROLLBACK;");

    INFO("another session's DELETE after the reader's FETCH");
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "DECLARE c CURSOR FOR SELECT a FROM s.t;");
    CHECK(host.db.ok(session, "FETCH 1 FROM c;")->size() == 1);
    host.db.ok(any(), "DELETE FROM s.t WHERE a > 10;");
    CHECK(host.db.ok(session, "FETCH ALL FROM c;")->size() == 4999);
    host.db.ok(session, "COMMIT;");
}

// The same through an index: the rows the index finds are checked against the DECLARE's command id.
TEST_CASE("integration::cpp::streaming_cursor::an_index_cursor_does_not_see_the_writes_after_its_declare") {
    host_t host("test_streaming_cursor/own_writes_index");
    five_thousand(host);
    host.db.ok(any(), "CREATE INDEX t_a ON s.t (a);");
    const auto session = any();
    const std::string declare = "DECLARE c CURSOR FOR SELECT a FROM s.t WHERE a = 4000;";
    const auto plan = host.db.ok(any(), "EXPLAIN SELECT a FROM s.t WHERE a = 4000;");
    std::string text;
    for (std::uint64_t row = 0; row < plan->size(); ++row) {
        text += std::string{value_of(plan->get<std::string_view>(0, row))} + "\n";
    }
    INFO(text);
    REQUIRE(text.find("Index") != std::string::npos);
    for (const std::string write : {"INSERT INTO s.t (a) VALUES (4000);", "DELETE FROM s.t WHERE a = 4000;"}) {
        INFO(write);
        host.db.ok(session, "BEGIN;");
        host.db.ok(session, declare);
        host.db.ok(session, write);
        CHECK(column_a(host.db.ok(session, "FETCH ALL FROM c;")) == std::vector<std::int64_t>{4000});
        host.db.ok(session, "ROLLBACK;");
    }
}

// The DDL of a table an open portal of the session reads is refused, as PostgreSQL 18's CheckTableNotInUse refuses ALTER
// TABLE, DROP TABLE and CREATE INDEX; the portal reads on. RENAME is not checked there and is not here.
TEST_CASE("integration::cpp::streaming_cursor::the_ddl_of_a_table_an_open_portal_reads") {
    host_t host("test_streaming_cursor/table_in_use");
    five_thousand(host);
    const auto session = any();
    const std::string in_use = "\" because it is being used by active queries in this session";
    for (const auto& [ddl, statement] : std::vector<std::pair<std::string, std::string>>{
             {"ALTER TABLE s.t ADD COLUMN b BIGINT;", "ALTER TABLE"},
             {"ALTER TABLE s.t DROP COLUMN a;", "ALTER TABLE"},
             {"ALTER TABLE s.t ADD CONSTRAINT t_positive CHECK (a > 0);", "ALTER TABLE"},
             {"DROP TABLE s.t;", "DROP TABLE"},
             {"CREATE INDEX t_a ON s.t (a);", "CREATE INDEX"}}) {
        INFO(ddl);
        host.db.ok(session, "BEGIN;");
        host.db.ok(session, "DECLARE c CURSOR FOR SELECT * FROM s.t;");
        CHECK(host.db.ok(session, "FETCH 1 FROM c;")->size() == 1);
        host.db.ok(session, "SAVEPOINT p;");
        host.db.check_refused(session, ddl, code::object_in_use, "cannot " + statement + " \"t" + in_use);
        host.db.ok(session, "ROLLBACK TO p;");
        CHECK(host.db.ok(session, "FETCH ALL FROM c;")->size() == 4999);
        host.db.ok(session, "CLOSE c;");
        host.db.ok(session, ddl);
        host.db.ok(session, "ROLLBACK;");
    }

    INFO("a host's window in a block");
    host.db.ok(session, "BEGIN;");
    auto first = window(host, session, "SELECT a FROM s.t;", 10);
    REQUIRE(first->has_more());
    host.db.ok(session, "SAVEPOINT p;");
    host.db.check_refused(session, "DROP TABLE s.t;", code::object_in_use, "cannot DROP TABLE \"t" + in_use);
    host.db.ok(session, "ROLLBACK TO p;");
    CHECK(host.db.dispatcher()->fetch_next(session, first->portal_id(), 10000)->size() == 4990);
    host.db.ok(session, "COMMIT;");

    INFO("RENAME COLUMN");
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "DECLARE c CURSOR FOR SELECT a FROM s.t;");
    CHECK(host.db.ok(session, "FETCH 1 FROM c;")->size() == 1);
    host.db.ok(session, "ALTER TABLE s.t RENAME COLUMN a TO z;");
    CHECK(host.db.ok(session, "FETCH ALL FROM c;")->size() == 4999);
    host.db.ok(session, "ROLLBACK;");
}

// 11: in a failed block a cursor's statements are refused as any other, and ROLLBACK closes its backend cursor.
TEST_CASE("integration::cpp::streaming_cursor::a_failed_block_refuses_the_cursor") {
    host_t host("test_streaming_cursor/failed_block");
    orders_of(host, 10);
    const auto session = any();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "DECLARE c CURSOR FOR SELECT id FROM m2.shop.public.orders;");
    host.db.ok(session, "FETCH 1 FROM c;");
    CHECK(host.db.refused(session, "SELECT * FROM nope;").type != code::transaction_finalized);
    CHECK(host.db.refused(session, "FETCH 1 FROM c;").type == code::transaction_finalized);
    CHECK(host.db.refused(session, "CLOSE c;").type == code::transaction_finalized);
    CHECK(host.db.refused(session, "DECLARE d CURSOR FOR SELECT 1;").type == code::transaction_finalized);
    host.db.ok(session, "ROLLBACK;");
    CHECK(host.backend().open_cursors() == 0);
    CHECK(host.backend().journal("abort") == strings{"abort 0"});
    CHECK(host.backend().open_transactions() == 0);
}

// 12: a FETCH sent while another FETCH of the same cursor runs is refused, as PostgreSQL 18's MarkPortalActive
// refuses a portal that is running.
TEST_CASE("integration::cpp::streaming_cursor::a_running_cursor_cannot_be_run") {
    host_t host("test_streaming_cursor/active");
    orders_of(host, 10);
    const auto session = any();
    host.db.ok(session, "BEGIN;");
    host.backend().answer_opens_together(2);
    host.db.ok(session, "DECLARE c CURSOR FOR SELECT id FROM m2.shop.public.orders;");
    source_awaits_t awaits;
    cursor_t_ptr first;
    std::thread running([&] { first = host.db.dispatcher()->execute_sql(session, "FETCH 4 FROM c;"); });
    REQUIRE(test_helpers::wait_until([&] { return awaits.count() > 0; }));
    auto second = host.db.dispatcher()->execute_sql(session, "FETCH 4 FROM c;");
    REQUIRE(second->is_error());
    CHECK(second->get_error().type == code::object_not_in_prerequisite_state);
    CHECK(std::string{second->get_error().what} == "portal \"c\" cannot be run");
    host.db.ok(any(), "SELECT id FROM m2.shop.public.orders;");
    running.join();
    // The refusal failed the block the first FETCH runs in, as any statement's error beside it does.
    REQUIRE(first->is_error());
    CHECK(first->get_error().type == code::transaction_finalized);
    host.db.ok(session, "ROLLBACK;");
    CHECK(host.backend().open_cursors() == 0);
}

// 13: what sorts, groups or joins reads its whole input on the first FETCH, as PostgreSQL 18's Sort and Agg do; the
// rows are still those of the statement.
TEST_CASE("integration::cpp::streaming_cursor::blocking_operators_run_on_the_first_fetch") {
    host_t host("test_streaming_cursor/blocking");
    big(host);
    host.db.ok(any(), "CREATE TABLE s.k (a BIGINT);");
    host.db.ok(any(), "INSERT INTO s.k (a) VALUES (3), (1), (2);");
    const auto session = any();
    for (const std::string select : {"SELECT a FROM s.big ORDER BY a DESC",
                                     "SELECT a FROM s.big GROUP BY a ORDER BY a",
                                     "SELECT s.big.a FROM s.big JOIN s.k ON s.big.a = s.k.a ORDER BY s.big.a"}) {
        INFO(select);
        const auto expected = column_a(host.db.ok(any(), select + ";"));
        REQUIRE(expected.size() > 2);
        host.db.ok(session, "BEGIN;");
        host.db.ok(session, "DECLARE c CURSOR FOR " + select + ";");
        source_awaits_t awaits;
        auto fetched = column_a(host.db.ok(session, "FETCH 2 FROM c;"));
        CHECK(fetched == std::vector<std::int64_t>(expected.begin(), expected.begin() + 2));
        const auto after_first = awaits.count();
        const auto rest = column_a(host.db.ok(session, "FETCH ALL FROM c;"));
        CHECK(awaits.count() == after_first);
        fetched.insert(fetched.end(), rest.begin(), rest.end());
        CHECK(fetched == expected);
        host.db.ok(session, "COMMIT;");
    }
}

// 14: a cursor's statement takes no $n: there is nothing to bind them with.
TEST_CASE("integration::cpp::streaming_cursor::a_cursor_of_a_statement_with_parameters") {
    host_t host("test_streaming_cursor/parameters");
    big(host);
    const auto session = any();
    host.db.ok(session, "BEGIN;");
    const auto refused =
        host.db.dispatcher()->execute_sql(session, "DECLARE c CURSOR FOR SELECT a FROM s.big WHERE a = $1;");
    REQUIRE(refused->is_error());
    CHECK(std::string{refused->get_error().what}.find("Not all parameters were bound") != std::string::npos);
    host.db.ok(session, "ROLLBACK;");
}

// The describe of FETCH answers its cursor's columns, of a cursor the session has not NoData; DECLARE and CLOSE are
// utilities, NoData (PostgreSQL 18 UtilityTupleDescriptor).
TEST_CASE("integration::cpp::streaming_cursor::describe_of_a_cursor_statement") {
    host_t host("test_streaming_cursor/describe");
    big(host);
    const auto session = any();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "DECLARE c CURSOR FOR SELECT a, b FROM s.big;");
    auto* dispatcher = host.db.dispatcher();
    auto fetch = dispatcher->describe_sql(session, "FETCH 10 FROM c;");
    REQUIRE(fetch->is_success());
    REQUIRE(fetch->type_data().size() == 2);
    CHECK(fetch->type_data()[0].alias() == "a");
    CHECK(fetch->type_data()[1].type() == logical_type::STRING_LITERAL);
    CHECK(fetch->size() == 0);
    for (const std::string sql : {"FETCH 10 FROM nope;", "DECLARE d CURSOR FOR SELECT a FROM s.big;", "CLOSE c;"}) {
        INFO(sql);
        auto described = dispatcher->describe_sql(session, sql);
        REQUIRE(described->is_success());
        CHECK(described->type_data().empty());
    }
    CHECK(column_a(host.db.ok(session, "FETCH 2 FROM c;")) == range(0, 2));
    host.db.ok(session, "COMMIT;");
}

// 15: a host's window: the first n rows, then fetch_next until the portal has no more, then no transaction is open.
TEST_CASE("integration::cpp::streaming_cursor::a_window_of_a_host") {
    host_t host("test_streaming_cursor/window");
    big(host);
    const auto expected = rows_of({host.db.ok(any(), "SELECT a, b, d FROM s.big;")});
    const auto session = any();
    auto first = window(host, session, "SELECT a, b, d FROM s.big;", 100);
    REQUIRE(first->is_success());
    CHECK(first->size() == 100);
    CHECK(first->has_more());
    REQUIRE(first->portal_id() != 0);
    CHECK(first->transaction_status() == components::session::transaction_status_t::idle);
    std::vector<cursor_t_ptr> pages{first};
    auto last = first;
    while (last->has_more()) {
        last = host.db.dispatcher()->fetch_next(session, first->portal_id(), 700);
        REQUIRE(last->is_success());
        pages.push_back(last);
    }
    CHECK(rows_of(pages) == expected);
    CHECK(last->portal_id() == 0);
    const auto gone = host.db.dispatcher()->fetch_next(session, first->portal_id(), 1);
    REQUIRE(gone->is_error());
    CHECK(gone->get_error().type == code::undefined_cursor);

    auto all = window(host, session, "SELECT a FROM s.big WHERE a < 5;", 100);
    CHECK(column_a(all) == range(0, 5));
    CHECK_FALSE(all->has_more());
    CHECK(all->portal_id() == 0);
}

// 16-17: the next statement of the session closes its window first; a describe runs in the window's transaction.
TEST_CASE("integration::cpp::streaming_cursor::the_next_statement_closes_the_window") {
    host_t host("test_streaming_cursor/abandoned");
    big(host);
    const auto session = any();
    auto first = window(host, session, "SELECT a FROM s.big;", 100);
    REQUIRE(first->has_more());
    const auto held = watermark(host);
    host.db.ok(any(), "INSERT INTO s.big (a) VALUES (-1);");
    auto described = host.db.dispatcher()->describe_sql(session, "SELECT a, b FROM s.big;");
    REQUIRE(described->is_success());
    CHECK(described->type_data().size() == 2);
    host.db.ok(any(), "INSERT INTO s.big (a) VALUES (-2);");
    CHECK(watermark(host) == held);
    CHECK(column_a(host.db.dispatcher()->fetch_next(session, first->portal_id(), 3)) == range(100, 103));

    CHECK(host.db.ints(session, "SELECT 1;") == std::vector<std::int64_t>{1});
    const auto gone = host.db.dispatcher()->fetch_next(session, first->portal_id(), 1);
    REQUIRE(gone->is_error());
    CHECK(gone->get_error().type == code::undefined_cursor);
    host.db.ok(any(), "INSERT INTO s.big (a) VALUES (-3);");
    CHECK(watermark(host) > held);
}

// Inside a transaction block a window lives as a cursor does: the block's other statements leave it open, its end
// closes it.
TEST_CASE("integration::cpp::streaming_cursor::a_window_in_a_block") {
    host_t host("test_streaming_cursor/window_in_block");
    big(host);
    const auto session = any();
    host.db.ok(session, "BEGIN;");
    auto first = window(host, session, "SELECT a FROM s.big;", 10);
    REQUIRE(first->has_more());
    CHECK(first->transaction_status() == components::session::transaction_status_t::in_transaction);
    CHECK(host.db.ints(session, "SELECT 1;") == std::vector<std::int64_t>{1});
    CHECK(column_a(host.db.dispatcher()->fetch_next(session, first->portal_id(), 5)) == range(10, 15));
    host.db.ok(session, "COMMIT;");
    const auto gone = host.db.dispatcher()->fetch_next(session, first->portal_id(), 1);
    REQUIRE(gone->is_error());
    CHECK(gone->get_error().type == code::undefined_cursor);
}

// 18: a write with a window runs whole and commits at once; its RETURNING rows come from the portal's buffer.
TEST_CASE("integration::cpp::streaming_cursor::a_write_with_a_window") {
    host_t host("test_streaming_cursor/write");
    host.db.ok(any(), "CREATE SCHEMA s;");
    host.db.ok(any(), "CREATE TABLE s.w (id BIGINT);");
    host.db.ok(any(), "ALTER TABLE s.w ADD PRIMARY KEY (id);");
    const auto session = any();
    auto first = window(host, session, "INSERT INTO s.w (id) VALUES (1), (2), (3), (4), (5) RETURNING id;", 2);
    REQUIRE(first->is_success());
    CHECK(column_a(first) == range(1, 3));
    CHECK(first->has_more());
    CHECK(host.db.ints(any(), "SELECT count(*) FROM s.w;") == std::vector<std::int64_t>{5});
    auto second = host.db.dispatcher()->fetch_next(session, first->portal_id(), 2);
    CHECK(column_a(second) == range(3, 5));
    CHECK(second->has_more());
    auto third = host.db.dispatcher()->fetch_next(session, first->portal_id(), 2);
    CHECK(column_a(third) == range(5, 6));
    CHECK_FALSE(third->has_more());

    auto conflict = window(host, session, "INSERT INTO s.w (id) VALUES (6), (1) RETURNING id;", 1);
    REQUIRE(conflict->is_error());
    CHECK(conflict->portal_id() == 0);
    CHECK(host.db.ints(any(), "SELECT count(*) FROM s.w;") == std::vector<std::int64_t>{5});

    auto open = window(host, session, "INSERT INTO s.w (id) VALUES (7), (8), (9) RETURNING id;", 1);
    REQUIRE(open->has_more());
    host.db.dispatcher()->close_session(session);
    CHECK(host.db.ints(any(), "SELECT count(*) FROM s.w;") == std::vector<std::int64_t>{8});
    const auto gone = host.db.dispatcher()->fetch_next(session, open->portal_id(), 1);
    REQUIRE(gone->is_error());
    CHECK(gone->get_error().type == code::undefined_cursor);
}

// 19: a statement that is no query answers whole, whatever the window.
TEST_CASE("integration::cpp::streaming_cursor::a_window_on_a_statement_that_is_no_query") {
    host_t host("test_streaming_cursor/utility");
    const auto session = any();
    for (const std::string sql : {"CREATE SCHEMA s;",
                                  "CREATE TABLE s.t (id BIGINT);",
                                  "SET timezone = 'UTC';",
                                  "ALTER SYSTEM SET timezone = 'UTC';",
                                  "EXPLAIN ANALYZE INSERT INTO s.t (id) VALUES (1), (2), (3);"}) {
        INFO(sql);
        auto answered = window(host, session, sql, 1);
        INFO(session_sql::error_of(answered));
        REQUIRE(answered->is_success());
        CHECK_FALSE(answered->has_more());
        CHECK(answered->portal_id() == 0);
    }
    CHECK(host.db.ints(any(), "SELECT count(*) FROM s.t;") == std::vector<std::int64_t>{3});
}

// 22: EXECUTE of a prepared query with a window streams as the query does: the window is its body's.
TEST_CASE("integration::cpp::streaming_cursor::a_window_on_execute") {
    host_t host("test_streaming_cursor/execute_window");
    big(host);
    const auto session = any();
    host.db.ok(session, "PREPARE q AS SELECT a FROM s.big WHERE a < $1;");
    source_awaits_t awaits;
    auto first = window(host, session, "EXECUTE q(5000);", 100);
    INFO(session_sql::error_of(first));
    REQUIRE(first->is_success());
    CHECK(first->size() == 100);
    REQUIRE(first->has_more());
    CHECK(awaits.count() < static_cast<std::uint64_t>(kBatches));
    std::vector<cursor_t_ptr> pages{first};
    while (pages.back()->has_more()) {
        pages.push_back(host.db.dispatcher()->fetch_next(session, first->portal_id(), 1000));
        REQUIRE(pages.back()->is_success());
    }
    CHECK(numbers(pages) == range(0, 5000));
}

// 20: a window whose backend fails on a later page: that fetch is the error, the portal is gone, the session goes on.
TEST_CASE("integration::cpp::streaming_cursor::a_page_that_fails") {
    host_t host("test_streaming_cursor/failing_page");
    orders_of(host, 10);
    const auto session = any();
    auto first = window(host, session, "SELECT id FROM m2.shop.public.orders;", 2);
    REQUIRE(first->has_more());
    host.backend().set_unreachable(orders, "server m2 went away");
    auto failed = host.db.dispatcher()->fetch_next(session, first->portal_id(), 4);
    REQUIRE(failed->is_error());
    CHECK(failed->portal_id() == 0);
    host.backend().clear_unreachable();
    CHECK(host.db.ints(session, "SELECT 1;") == std::vector<std::int64_t>{1});
    CHECK(host.backend().open_transactions() == 0);
    const auto gone = host.db.dispatcher()->fetch_next(session, first->portal_id(), 1);
    CHECK(gone->get_error().type == code::undefined_cursor);
}

// 21: the shutdown closes every portal before the engine stops; a FETCH after it is refused, a CLOSE is not.
TEST_CASE("integration::cpp::streaming_cursor::shutdown_closes_the_portals") {
    host_t host("test_streaming_cursor/shutdown");
    orders_of(host, 10);
    const auto session = any();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "DECLARE c CURSOR FOR SELECT id FROM m2.shop.public.orders;");
    host.db.ok(session, "FETCH 1 FROM c;");
    auto [_, shut] = actor_zeta::otterbrix::send(host.db.space->dispatcher_address(),
                                                 &services::dispatcher::manager_dispatcher_t::begin_shutdown);
    actor_zeta::otterbrix::wait_ready(shut);
    CHECK(host.backend().open_cursors() == 0);
    CHECK(host.db.refused(session, "FETCH 1 FROM c;").type == code::connection_closed);
    host.db.ok(session, "CLOSE ALL;");
}

// A DECLARE admitted before the shutdown that ends after it leaves no portal open: the shutdown closed those it found,
// the DECLARE closes its own.
TEST_CASE("integration::cpp::streaming_cursor::a_declare_that_ends_after_the_shutdown") {
    host_t host("test_streaming_cursor/shutdown_race");
    orders_of(host, 10);
    const auto session = any();
    host.db.ok(session, "BEGIN;");
    // The DECLARE's sub-select waits at its open until the shutdown has run.
    host.backend().answer_opens_together(2);
    cursor_t_ptr declared;
    std::thread declaring([&] {
        declared = host.db.dispatcher()->execute_sql(session,
                                                     "DECLARE c CURSOR FOR SELECT id FROM m2.shop.public.orders WHERE "
                                                     "id > (SELECT min(id) FROM m2.shop.public.orders);");
    });
    REQUIRE(test_helpers::wait_until([&] { return host.backend().peak_waiting_opens() > 0; }));
    auto [_, shut] = actor_zeta::otterbrix::send(host.db.space->dispatcher_address(),
                                                 &services::dispatcher::manager_dispatcher_t::begin_shutdown);
    actor_zeta::otterbrix::wait_ready(shut);
    host.backend().answer_opens_together(1);
    declaring.join();
    INFO(session_sql::error_of(declared));
    REQUIRE(declared->is_success());
    CHECK(host.backend().open_cursors() == 0);
    host.db.check_refused(session, "CLOSE c;", code::undefined_cursor, "cursor \"c\" does not exist");
}

// A cursor keeps what the storages took until it closes (CLOSE, the end of its transaction or of its savepoint, the
// end of the session), a window until the next statement; each take is released once (storage_release_block).
TEST_CASE("integration::cpp::streaming_cursor::a_cursor_holds_its_takes_until_close") {
    host_t host("test_streaming_cursor/takes");
    big(host);
    orders_of(host, 10);
    host.backend().set_take_policy({.whole_select = true});
    const auto none_held = [] {
        return test_helpers::wait_until([] { return services::disk::disk_held_takes() == 0; });
    };
    REQUIRE(none_held());
    for (const std::string end : {"CLOSE c;", "COMMIT;", "ROLLBACK;", "close"}) {
        INFO(end);
        const auto session = any();
        host.db.ok(session, "BEGIN;");
        host.db.ok(session, "DECLARE c CURSOR FOR SELECT a FROM s.big WHERE a >= 10;");
        CHECK(column_a(host.db.ok(session, "FETCH 1 FROM c;")) == range(10, 11));
        CHECK(services::disk::disk_held_takes() == 1);
        if (end == "close") {
            host.db.dispatcher()->close_session(session);
        } else {
            host.db.ok(session, end);
        }
        CHECK(none_held());
        if (end == "CLOSE c;") {
            host.db.ok(session, "COMMIT;");
        }
    }
    INFO("ROLLBACK TO the savepoint before the cursor");
    {
        const auto session = any();
        host.db.ok(session, "BEGIN;");
        host.db.ok(session, "SAVEPOINT p;");
        host.db.ok(session, "DECLARE c CURSOR FOR SELECT a FROM s.big WHERE a >= 10;");
        CHECK(column_a(host.db.ok(session, "FETCH 1 FROM c;")) == range(10, 11));
        CHECK(services::disk::disk_held_takes() == 1);
        host.db.ok(session, "ROLLBACK TO p;");
        CHECK(none_held());
        host.db.ok(session, "ROLLBACK;");
    }
    INFO("a window the next statement closes");
    {
        const auto session = any();
        auto first = window(host, session, "SELECT a FROM s.big WHERE a >= 0;", 100);
        REQUIRE(first->is_success());
        CHECK(first->has_more());
        CHECK(services::disk::disk_held_takes() == 1);
        host.db.ok(session, "SELECT 1;");
        CHECK(none_held());
    }
    INFO("a backend's take: released once, at CLOSE");
    {
        const auto session = any();
        const auto taken_before = host.backend().taken_requests().size();
        host.db.ok(session, "BEGIN;");
        host.db.ok(session, "DECLARE c CURSOR FOR SELECT * FROM m2.shop.public.orders WHERE id = 2;");
        CHECK(host.db.ok(session, "FETCH 1 FROM c;")->size() == 1);
        REQUIRE(host.backend().taken_requests().size() == taken_before + 1);
        CHECK(host.backend().released_requests().size() == taken_before);
        host.db.ok(session, "CLOSE c;");
        host.db.ok(session, "COMMIT;");
        CHECK(test_helpers::wait_until(
            [&] { return host.backend().released_requests().size() == host.backend().taken_requests().size(); }));
        CHECK(host.backend().released_requests() == host.backend().taken_requests());
    }
}
