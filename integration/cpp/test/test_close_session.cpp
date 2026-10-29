// close_session ends a session as PostgreSQL 18 ends a backend's: the statements sent before it run, then its open
// transaction rolls back at every backend it reached, then its own settings go (docs/embedding-host-api.md,
// "Sessions").

#include "host_fixture.hpp"

#include <components/casts/cast_registry.hpp>
#include <components/compute/function.hpp>
#include <core/tests/wait_ready.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/index/manager_index.hpp>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

using host_fixture::host_t;
using host_fixture::strings;
using rows_t = host_actor::rows_t;
using code = core::error_code_t;
using components::session::transaction_status_t;

namespace {

    otterbrix::session_id_t any() { return otterbrix::session_id_t(); }

    const std::string orders = "m2.shop.public.orders";
    const rows_t three_orders{{1, 100}, {2, 200}, {3, 300}};

    // m2.shop.public.orders (1, 100), (2, 200), (3, 300) and the local table loc.t.
    void shop(host_t& host) {
        host.register_orders();
        host.backend().set_rows(orders, three_orders);
        host.db.ok(any(), "CREATE SCHEMA loc;");
        host.db.ok(any(), "CREATE TABLE loc.t (id BIGINT);");
        host.backend().clear_journal();
    }

    std::int64_t local_rows(host_t& host) { return host.db.ints(any(), "SELECT count(*) FROM loc.t;").front(); }

    void close_session(host_t& host, const otterbrix::session_id_t& session) {
        host.db.dispatcher()->close_session(session);
    }

    transaction_status_t status_after_select(host_t& host, const otterbrix::session_id_t& session) {
        return host.db.ok(session, "SELECT 1;")->transaction_status();
    }

    core::error_t noop_cast(const components::vector::vector_t&,
                            components::vector::vector_t*,
                            const components::graph_execution_context&,
                            uint64_t) noexcept {
        return core::error_t::no_error();
    }

    components::casts::cast_entry noop_cast_entry() {
        return components::casts::cast_entry{components::casts::cast_function_t{noop_cast, nullptr},
                                             components::casts::cast_cost{.precision_loss = 0, .footprint = 8},
                                             false};
    }

    core::error_t twice(components::compute::kernel_context&,
                        const components::vector::data_chunk_t& in,
                        components::vector::vector_t& out) {
        for (std::uint64_t row = 0; row < in.size(); ++row) {
            out.data<std::int64_t>()[row] = in.data[0].data<std::int64_t>()[row] * 2;
        }
        return core::error_t::no_error();
    }

    components::compute::function_ptr twice_function(std::pmr::memory_resource* resource) {
        using namespace components::compute;
        function_doc doc{resource, "twice", "twice", {"value"}, false};
        auto function = core::pmr::make_polymorphic_unique<vector_function>(resource,
                                                                            "twice",
                                                                            arity::unary(),
                                                                            doc,
                                                                            std::size_t{1},
                                                                            volatility_t::immutable);
        kernel_signature_t signature(function_type_t::vector,
                                     {parameter_type::exact(components::types::logical_type::BIGINT)},
                                     {output_type::fixed(components::types::logical_type::BIGINT)});
        REQUIRE_FALSE(function->add_kernel(resource, vector_kernel{std::move(signature), twice}).contains_error());
        return function;
    }

    bool waited_beyond(std::uint64_t count) {
        return test_helpers::wait_until([count] { return services::dispatcher::dev_statements_waited_turn() > count; });
    }

} // namespace

TEST_CASE("integration::cpp::close_session::the_open_transaction_rolls_back_at_every_backend") {
    host_t host("test_close_session/rollback");
    shop(host);
    const auto session = any();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "INSERT INTO loc.t (id) VALUES (1);");
    host.db.ok(session, "INSERT INTO m2.shop.public.orders (id, amount) VALUES (4, 400);");
    close_session(host, session);
    CHECK(local_rows(host) == 0);
    CHECK(host.backend().journal("abort") == strings{"abort 0"});
    CHECK(host.backend().journal("prepare").empty());
    CHECK(host.backend().rows(orders) == three_orders);
    CHECK(host.backend().open_transactions() == 0);
}

TEST_CASE("integration::cpp::close_session::a_backend_the_transaction_only_read_gets_its_end") {
    host_t host("test_close_session/reader");
    shop(host);
    const auto session = any();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "SELECT id FROM m2.shop.public.orders;");
    REQUIRE(host.backend().open_transactions() == 1);
    close_session(host, session);
    CHECK(host.backend().journal("abort") == strings{"abort 0"});
    CHECK(host.backend().journal("prepare").empty());
    CHECK(host.backend().open_transactions() == 0);
}

TEST_CASE("integration::cpp::close_session::a_failed_block_rolls_back_and_the_session_starts_idle") {
    host_t host("test_close_session/failed", nullptr, {"m2"}, {{"m2", 1}});
    shop(host);
    const auto session = any();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "INSERT INTO m2.shop.public.orders (id, amount) VALUES (4, 400);");
    host.backend().set_failing_round(3);
    host.db.check_refused(session,
                          "INSERT INTO m2.shop.public.orders (id, amount) VALUES (5, 500), (6, 600);",
                          code::connection_closed,
                          "server m2: round 3 failed");
    REQUIRE(host.backend().journal("abort").empty());
    close_session(host, session);
    CHECK(host.backend().journal("abort") == strings{"abort 0"});
    CHECK(host.backend().rows(orders) == three_orders);
    CHECK(host.backend().open_transactions() == 0);
    CHECK(status_after_select(host, session) == transaction_status_t::idle);
}

// SET outside a transaction block is not undone by ROLLBACK; the session's id after close is a session of its own.
TEST_CASE("integration::cpp::close_session::an_idle_sessions_own_settings_go") {
    host_t host("test_close_session/settings");
    host.db.ok(any(), "CREATE SCHEMA s;");
    const auto session = any();
    host.db.ok(session, "SET search_path = 's';");
    host.db.ok(session, "SET autocommit = off;");
    REQUIRE(host.db.strings(session, "SELECT current_schema();") == strings{"s"});
    host.db.ok(session, "ROLLBACK;");
    close_session(host, session);
    CHECK(host.db.strings(session, "SELECT current_schema();") == strings{"public"});
    CHECK(status_after_select(host, session) == transaction_status_t::idle);
}

TEST_CASE("integration::cpp::close_session::an_unknown_or_closed_session_is_a_no_op") {
    host_t host("test_close_session/no_op");
    shop(host);
    close_session(host, any());
    CHECK(host.backend().journal().empty());

    const auto session = any();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "INSERT INTO m2.shop.public.orders (id, amount) VALUES (4, 400);");
    close_session(host, session);
    close_session(host, session);
    CHECK(host.backend().journal("abort") == strings{"abort 0"});
    CHECK(host.backend().rows(orders) == three_orders);
    CHECK(status_after_select(host, session) == transaction_status_t::idle);
}

// The index sweep of a committed DELETE waits for every transaction that may still see the row.
TEST_CASE("integration::cpp::close_session::the_horizon_moves_past_a_closed_session") {
    host_t host("test_close_session/horizon");
    host.db.ok(any(), "CREATE SCHEMA pin;");
    host.db.ok(any(), "CREATE TABLE pin.t (id BIGINT);");
    host.db.ok(any(), "INSERT INTO pin.t (id) VALUES (1), (2), (3);");
    host.db.ok(any(), "CREATE INDEX pin_t_id ON pin.t (id);");
    host.db.ok(any(), "DELETE FROM pin.t WHERE id = 1;");
    const auto drained = [] { return services::index::index_deferred_deletes() == 0; };
    REQUIRE(test_helpers::wait_until(drained));

    const auto session = any();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "SELECT id FROM pin.t;");
    host.db.ok(any(), "DELETE FROM pin.t WHERE id = 2;");
    CHECK_FALSE(test_helpers::wait_until(drained, std::chrono::milliseconds(200)));
    close_session(host, session);
    CHECK(test_helpers::wait_until(drained));
}

// A describe sent after close is refused as a statement is, a describe of a utility too.
TEST_CASE("integration::cpp::close_session::a_describe_sent_after_close_is_refused") {
    host_t host("test_close_session/describe");
    shop(host);
    const auto session = any();
    host.db.ok(session, "BEGIN;");
    host.backend().answer_opens_together(2);

    components::cursor::cursor_t_ptr running;
    std::thread reader([&] { running = host.db.run(session, "SELECT id FROM m2.shop.public.orders;"); });
    REQUIRE(test_helpers::wait_until([&] { return host.backend().opens() == 1; }));

    const auto waited = services::dispatcher::dev_statements_waited_turn();
    std::thread closer([&] { close_session(host, session); });
    REQUIRE(waited_beyond(waited));

    components::cursor::cursor_t_ptr query;
    std::thread describer([&] {
        query = host.db.dispatcher()->describe_sql(session, "SELECT id FROM m2.shop.public.orders WHERE id = $1;");
    });
    REQUIRE(waited_beyond(waited + 1));
    components::cursor::cursor_t_ptr utility;
    std::thread utility_describer([&] { utility = host.db.dispatcher()->describe_sql(session, "BEGIN;"); });
    REQUIRE(waited_beyond(waited + 2));

    host.db.ok(any(), "SELECT id FROM m2.shop.public.orders;");
    reader.join();
    closer.join();
    describer.join();
    utility_describer.join();

    INFO(session_sql::error_of(running));
    CHECK(running->is_success());
    for (const auto* described : {&query, &utility}) {
        REQUIRE((*described)->is_error());
        CHECK((*described)->get_error().type == code::connection_closed);
        CHECK(std::string{(*described)->get_error().what} == "the session is closed");
    }
}

// The statement running when close arrives and the one waiting before it end first; a statement sent after close is
// refused, as PostgreSQL 18 reads nothing after Terminate.
TEST_CASE("integration::cpp::close_session::a_close_waits_for_the_statements_sent_before_it") {
    host_t host("test_close_session/race");
    shop(host);
    const auto session = any();
    host.db.ok(session, "BEGIN;");
    host.backend().answer_opens_together(2);

    components::cursor::cursor_t_ptr running;
    std::thread reader([&] { running = host.db.run(session, "SELECT id FROM m2.shop.public.orders;"); });
    REQUIRE(test_helpers::wait_until([&] { return host.backend().opens() == 1; }));

    const auto waited = services::dispatcher::dev_statements_waited_turn();
    components::cursor::cursor_t_ptr before_close;
    std::thread savepoint([&] { before_close = host.db.run(session, "SAVEPOINT a;"); });
    REQUIRE(waited_beyond(waited));

    std::atomic<bool> closed{false};
    std::thread closer([&] {
        close_session(host, session);
        closed.store(true);
    });
    REQUIRE(waited_beyond(waited + 1));

    components::cursor::cursor_t_ptr after_close;
    std::thread writer(
        [&] { after_close = host.db.run(session, "INSERT INTO m2.shop.public.orders (id, amount) VALUES (9, 900);"); });
    REQUIRE(waited_beyond(waited + 2));

    CHECK_FALSE(closed.load());
    CHECK(host.backend().journal("abort").empty());
    host.db.ok(any(), "SELECT id FROM m2.shop.public.orders;");
    reader.join();
    savepoint.join();
    closer.join();
    writer.join();

    INFO(session_sql::error_of(running));
    CHECK(running->is_success());
    CHECK(running->size() == 3);
    INFO(session_sql::error_of(before_close));
    CHECK(before_close->is_success());
    REQUIRE(after_close->is_error());
    CHECK(after_close->get_error().type == code::connection_closed);
    CHECK(std::string{after_close->get_error().what} == "the session is closed");
    CHECK(after_close->transaction_status() == transaction_status_t::idle);
    CHECK(host.backend().journal("abort") == strings{"abort 0"});
    CHECK(host.backend().rows(orders) == three_orders);
    CHECK(host.backend().open_transactions() == 0);
}

// BOOLEAN -> DATE has no cast of its own.
TEST_CASE("integration::cpp::close_session::a_cast_sent_after_close_is_refused") {
    using components::types::complex_logical_type;
    using components::types::logical_type;
    const complex_logical_type source{logical_type::BOOLEAN};
    const complex_logical_type target{logical_type::DATE};
    host_t host("test_close_session/cast");
    shop(host);
    const auto session = any();
    host.db.ok(session, "BEGIN;");
    host.backend().answer_opens_together(2);

    components::cursor::cursor_t_ptr running;
    std::thread reader([&] { running = host.db.run(session, "SELECT id FROM m2.shop.public.orders;"); });
    REQUIRE(test_helpers::wait_until([&] { return host.backend().opens() == 1; }));

    const auto waited = services::dispatcher::dev_statements_waited_turn();
    std::thread closer([&] { close_session(host, session); });
    REQUIRE(waited_beyond(waited));

    core::error_t registered = core::error_t::no_error();
    std::thread registering(
        [&] { registered = host.db.dispatcher()->register_cast(session, source, target, noop_cast_entry()); });
    REQUIRE(waited_beyond(waited + 1));

    core::error_t unregistered = core::error_t::no_error();
    std::thread unregistering([&] { unregistered = host.db.dispatcher()->unregister_cast(session, source, target); });
    REQUIRE(waited_beyond(waited + 2));

    host.db.ok(any(), "SELECT id FROM m2.shop.public.orders;");
    reader.join();
    closer.join();
    registering.join();
    unregistering.join();

    CHECK(running->is_success());
    CHECK(registered.type == code::connection_closed);
    CHECK(std::string{registered.what} == "the session is closed");
    CHECK(unregistered.type == code::connection_closed);
    CHECK(std::string{unregistered.what} == "the session is closed");
}

// A function's registration takes no turn: it is answered while the statement before close still runs.
TEST_CASE("integration::cpp::close_session::a_function_sent_after_close_is_refused") {
    host_t host("test_close_session/udf");
    shop(host);
    const auto session = any();
    host.db.ok(session, "BEGIN;");
    host.backend().answer_opens_together(2);

    components::cursor::cursor_t_ptr running;
    std::thread reader([&] { running = host.db.run(session, "SELECT id FROM m2.shop.public.orders;"); });
    REQUIRE(test_helpers::wait_until([&] { return host.backend().opens() == 1; }));

    const auto waited = services::dispatcher::dev_statements_waited_turn();
    std::thread closer([&] { close_session(host, session); });
    REQUIRE(waited_beyond(waited));

    core::error_t registered = core::error_t::no_error();
    core::error_t unregistered = core::error_t::no_error();
    std::atomic<bool> answered{false};
    std::thread registering([&] {
        registered = host.db.dispatcher()->register_udf(session, twice_function(host.resource()));
        std::pmr::vector<components::types::complex_logical_type> inputs(host.resource());
        inputs.emplace_back(components::types::logical_type::BIGINT);
        unregistered = host.db.dispatcher()->unregister_udf(session, "twice", inputs);
        answered.store(true);
    });
    CHECK(test_helpers::wait_until([&] { return answered.load(); }, std::chrono::seconds(30)));

    host.db.ok(any(), "SELECT id FROM m2.shop.public.orders;");
    reader.join();
    closer.join();
    registering.join();

    CHECK(running->is_success());
    CHECK(registered.type == code::connection_closed);
    CHECK(std::string{registered.what} == "the session is closed");
    CHECK(unregistered.type == code::connection_closed);
    CHECK(std::string{unregistered.what} == "the session is closed");
}

TEST_CASE("integration::cpp::close_session::a_close_during_shutdown_reaches_the_backend") {
    host_t host("test_close_session/shutdown");
    shop(host);
    const auto session = any();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "INSERT INTO m2.shop.public.orders (id, amount) VALUES (4, 400);");
    auto [_, shutting] = actor_zeta::otterbrix::send(host.db.space->dispatcher_address(),
                                                     &services::dispatcher::manager_dispatcher_t::begin_shutdown);
    REQUIRE(test_helpers::wait_ready(shutting));
    REQUIRE(host.db.refused(any(), "SELECT 1;").type == code::connection_closed);
    close_session(host, session);
    CHECK(host.backend().journal("abort") == strings{"abort 0"});
    CHECK(host.backend().rows(orders) == three_orders);
    CHECK(host.backend().open_transactions() == 0);
}
