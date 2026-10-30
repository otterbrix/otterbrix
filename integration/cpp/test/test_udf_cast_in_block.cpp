// A function's and a cast's registration is a statement of its session: it waits for the session's turn and writes
// pg_proc / pg_cast in the session's transaction, as PostgreSQL 18 CreateFunction and CreateCast do
// (functioncmds.c:1026, :1539, no PreventInTransactionBlock).

#include "host_fixture.hpp"

#include <components/casts/cast_registry.hpp>
#include <components/compute/function.hpp>
#include <components/physical_plan/operators/operator_commit_transaction.hpp>
#include <core/tests/wait_ready.hpp>
#include <services/collection/executor.hpp>
#include <services/dispatcher/dispatcher.hpp>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

using host_fixture::host_t;
using rows_t = host_actor::rows_t;

namespace {

    otterbrix::session_id_t any() { return otterbrix::session_id_t(); }

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

    std::pmr::vector<components::types::complex_logical_type> bigint_input(std::pmr::memory_resource* resource) {
        std::pmr::vector<components::types::complex_logical_type> inputs(resource);
        inputs.emplace_back(components::types::logical_type::BIGINT);
        return inputs;
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

    // BOOLEAN -> DATE has no cast of its own.
    const components::types::complex_logical_type cast_source{components::types::logical_type::BOOLEAN};
    const components::types::complex_logical_type cast_target{components::types::logical_type::DATE};

    std::int64_t twice_rows(host_t& host) {
        return host.db.ints(any(), "SELECT count(*) FROM pg_catalog.pg_proc WHERE proname = 'twice';").front();
    }

    std::int64_t cast_rows(host_t& host) {
        return host.db.ints(any(), "SELECT count(*) FROM pg_catalog.pg_cast;").front();
    }

    bool waited_beyond(std::uint64_t count) {
        return test_helpers::wait_until([count] { return services::dispatcher::dev_statements_waited_turn() > count; });
    }

    bool mentions(const core::error_t& error, const std::string& text) {
        return std::string{error.what}.find(text) != std::string::npos;
    }

    std::int64_t first_int(const components::cursor::cursor_t_ptr& cursor) {
        REQUIRE(cursor->is_success());
        REQUIRE(cursor->size() == 1);
        return cursor->chunks().front().get_value<std::int64_t>(0, 0);
    }

    // The next commit stops right after it is published, until released (five seconds at most).
    std::atomic<bool> g_published{false};
    std::atomic<bool> g_released{false};
    void hold_after_publish() {
        g_published.store(true);
        const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!g_released.load() && std::chrono::steady_clock::now() < until) {
            std::this_thread::yield();
        }
    }

    struct seen_t {
        components::cursor::cursor_t_ptr rows;
        components::cursor::cursor_t_ptr call;
    };

    // What another session reads of `twice` while the COMMIT of `session` stands right after its publish.
    seen_t seen_while_committing(host_t& host, const otterbrix::session_id_t& session) {
        g_published.store(false);
        g_released.store(false);
        components::operators::dev_set_after_publish_hook(&hold_after_publish);
        components::cursor::cursor_t_ptr committed;
        std::thread committer([&] { committed = host.db.run(session, "COMMIT;"); });
        REQUIRE(test_helpers::wait_until([] { return g_published.load(); }));
        seen_t seen;
        std::atomic<bool> read{false};
        std::thread reader([&] {
            seen.rows = host.db.run(any(), "SELECT count(*) FROM pg_catalog.pg_proc WHERE proname = 'twice';");
            seen.call = host.db.run(any(), "SELECT twice(id) FROM t;");
            read.store(true);
        });
        // The reader may stand behind the held commit on its executor.
        const bool read_while_held = test_helpers::wait_until([&] { return read.load(); }, std::chrono::seconds(2));
        INFO("read while the commit was held: " << read_while_held);
        g_released.store(true);
        committer.join();
        reader.join();
        INFO(session_sql::error_of(committed));
        REQUIRE(committed->is_success());
        return seen;
    }

} // namespace

TEST_CASE("integration::cpp::udf_cast_in_block::a_rolled_back_registration_leaves_no_function") {
    host_t host("test_udf_cast_in_block/udf_rollback");
    const auto session = any();
    host.db.ok(any(), "CREATE TABLE t (id BIGINT);");
    host.db.ok(any(), "INSERT INTO t (id) VALUES (1);");
    host.db.ok(session, "BEGIN;");
    const auto registered = host.db.dispatcher()->register_udf(session, twice_function(host.resource()));
    INFO(registered.what);
    REQUIRE_FALSE(registered.contains_error());
    CHECK(host.db.ints(session, "SELECT twice(id) FROM t;") == std::vector<std::int64_t>{2});
    host.db.ok(session, "ROLLBACK;");
    CHECK(twice_rows(host) == 0);
    CHECK(host.db.run(session, "SELECT twice(id) FROM t;")->is_error());
}

TEST_CASE("integration::cpp::udf_cast_in_block::a_function_goes_into_a_schema_the_block_created") {
    host_t host("test_udf_cast_in_block/udf_schema");
    const auto session = any();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "CREATE SCHEMA s;");
    host.db.ok(session, "SET search_path = 's';");
    const auto registered = host.db.dispatcher()->register_udf(session, twice_function(host.resource()));
    INFO(registered.what);
    REQUIRE_FALSE(registered.contains_error());
    CHECK(host.db.strings(session,
                          "SELECT n.nspname FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n "
                          "ON p.pronamespace = n.oid WHERE p.proname = 'twice';") == host_fixture::strings{"s"});
    host.db.ok(session, "ROLLBACK;");
    CHECK(twice_rows(host) == 0);
}

TEST_CASE("integration::cpp::udf_cast_in_block::a_rolled_back_unregistration_keeps_the_function") {
    host_t host("test_udf_cast_in_block/udf_unregister_rollback");
    const auto session = any();
    host.db.ok(any(), "CREATE TABLE t (id BIGINT);");
    host.db.ok(any(), "INSERT INTO t (id) VALUES (3);");
    REQUIRE_FALSE(host.db.dispatcher()->register_udf(any(), twice_function(host.resource())).contains_error());
    host.db.ok(session, "BEGIN;");
    const auto unregistered = host.db.dispatcher()->unregister_udf(session, "twice", bigint_input(host.resource()));
    INFO(unregistered.what);
    REQUIRE_FALSE(unregistered.contains_error());
    host.db.ok(session, "ROLLBACK;");
    CHECK(twice_rows(host) == 1);
    CHECK(host.db.ints(any(), "SELECT twice(id) FROM t;") == std::vector<std::int64_t>{6});
}

// The registration waits for the statement its session runs, as a statement of autocommit does.
TEST_CASE("integration::cpp::udf_cast_in_block::a_registration_waits_for_its_sessions_turn") {
    host_t host("test_udf_cast_in_block/udf_turn");
    host.register_orders();
    host.backend().set_rows("m2.shop.public.orders", rows_t{{1, 100}, {2, 200}, {3, 300}});
    const auto session = any();
    host.backend().answer_opens_together(2);

    components::cursor::cursor_t_ptr running;
    std::thread reader([&] { running = host.db.run(session, "SELECT id FROM m2.shop.public.orders;"); });
    REQUIRE(test_helpers::wait_until([&] { return host.backend().opens() == 1; }));

    const auto waited = services::dispatcher::dev_statements_waited_turn();
    core::error_t registered = core::error_t::no_error();
    std::thread registering(
        [&] { registered = host.db.dispatcher()->register_udf(session, twice_function(host.resource())); });
    CHECK(waited_beyond(waited));

    host.db.ok(any(), "SELECT id FROM m2.shop.public.orders;");
    reader.join();
    registering.join();
    INFO(session_sql::error_of(running));
    CHECK(running->is_success());
    INFO(registered.what);
    CHECK_FALSE(registered.contains_error());
    CHECK(twice_rows(host) == 1);
}

TEST_CASE("integration::cpp::udf_cast_in_block::a_rolled_back_cast_leaves_pg_cast_unchanged") {
    host_t host("test_udf_cast_in_block/cast_rollback");
    const auto session = any();
    REQUIRE(cast_rows(host) == 0);
    host.db.ok(session, "BEGIN;");
    const auto registered = host.db.dispatcher()->register_cast(session, cast_source, cast_target, noop_cast_entry());
    INFO(registered.what);
    REQUIRE_FALSE(registered.contains_error());
    host.db.ok(session, "ROLLBACK;");
    CHECK(cast_rows(host) == 0);
    CHECK_FALSE(
        host.db.dispatcher()->register_cast(any(), cast_source, cast_target, noop_cast_entry()).contains_error());
}

TEST_CASE("integration::cpp::udf_cast_in_block::a_rolled_back_cast_drop_keeps_the_pg_cast_row") {
    host_t host("test_udf_cast_in_block/cast_unregister_rollback");
    const auto session = any();
    REQUIRE_FALSE(
        host.db.dispatcher()->register_cast(any(), cast_source, cast_target, noop_cast_entry()).contains_error());
    REQUIRE(cast_rows(host) == 1);
    host.db.ok(session, "BEGIN;");
    const auto unregistered = host.db.dispatcher()->unregister_cast(session, cast_source, cast_target);
    INFO(unregistered.what);
    REQUIRE_FALSE(unregistered.contains_error());
    host.db.ok(session, "ROLLBACK;");
    CHECK(cast_rows(host) == 1);
    CHECK(host.db.dispatcher()->register_cast(any(), cast_source, cast_target, noop_cast_entry()).contains_error());
}

// Another session sees the function only once the block commits (PostgreSQL 18 inval.c:1199 AtEOXact_Inval).
TEST_CASE("integration::cpp::udf_cast_in_block::another_session_sees_the_function_after_commit") {
    host_t host("test_udf_cast_in_block/udf_other_session");
    const auto session = any();
    const auto other = any();
    host.db.ok(any(), "CREATE TABLE t (id BIGINT);");
    host.db.ok(any(), "INSERT INTO t (id) VALUES (4);");
    host.db.ok(session, "BEGIN;");
    REQUIRE_FALSE(host.db.dispatcher()->register_udf(session, twice_function(host.resource())).contains_error());
    CHECK(host.db.ints(session, "SELECT twice(id) FROM t;") == std::vector<std::int64_t>{8});
    CHECK(host.db.run(other, "SELECT twice(id) FROM t;")->is_error());
    host.db.ok(session, "COMMIT;");
    CHECK(host.db.ints(other, "SELECT twice(id) FROM t;") == std::vector<std::int64_t>{8});
    CHECK(twice_rows(host) == 1);
}

TEST_CASE("integration::cpp::udf_cast_in_block::another_session_sees_the_cast_after_commit") {
    host_t host("test_udf_cast_in_block/cast_other_session");
    const auto session = any();
    const auto other = any();
    const std::string cast = "SELECT CAST(TRUE AS DATE);";
    REQUIRE(host.db.run(other, cast)->is_error());
    host.db.ok(session, "BEGIN;");
    REQUIRE_FALSE(
        host.db.dispatcher()->register_cast(session, cast_source, cast_target, noop_cast_entry()).contains_error());
    CHECK(host.db.run(session, cast)->is_success());
    CHECK(host.db.run(other, cast)->is_error());
    host.db.ok(session, "COMMIT;");
    CHECK(host.db.run(other, cast)->is_success());
}

// The signature an open block registered is refused to another session until the block ends, as the catalog refuses
// a relation name another transaction holds; a rollback frees it.
TEST_CASE("integration::cpp::udf_cast_in_block::a_signature_an_open_block_holds_is_refused_to_another") {
    host_t host("test_udf_cast_in_block/udf_contested");
    const auto session = any();
    const auto other = any();
    host.db.ok(session, "BEGIN;");
    REQUIRE_FALSE(host.db.dispatcher()->register_udf(session, twice_function(host.resource())).contains_error());
    const auto refused = host.db.dispatcher()->register_udf(other, twice_function(host.resource()));
    CHECK(refused.type == core::error_code_t::duplicate_function);
    CHECK(mentions(refused, "function 'twice' already exists"));
    CHECK(mentions(refused, "pending in another transaction"));
    host.db.ok(session, "ROLLBACK;");
    const auto registered = host.db.dispatcher()->register_udf(other, twice_function(host.resource()));
    INFO(registered.what);
    CHECK_FALSE(registered.contains_error());
    CHECK(twice_rows(host) == 1);
}

TEST_CASE("integration::cpp::udf_cast_in_block::a_cast_an_open_block_holds_is_refused_to_another") {
    host_t host("test_udf_cast_in_block/cast_contested");
    const auto session = any();
    const auto other = any();
    host.db.ok(session, "BEGIN;");
    REQUIRE_FALSE(
        host.db.dispatcher()->register_cast(session, cast_source, cast_target, noop_cast_entry()).contains_error());
    const auto refused = host.db.dispatcher()->register_cast(other, cast_source, cast_target, noop_cast_entry());
    CHECK(refused.type == core::error_code_t::duplicate_object);
    INFO(refused.what);
    CHECK(mentions(refused, "cast '20' -> '36' already exists"));
    CHECK(mentions(refused, "pending in another transaction"));
    host.db.ok(session, "ROLLBACK;");
    const auto registered = host.db.dispatcher()->register_cast(other, cast_source, cast_target, noop_cast_entry());
    INFO(registered.what);
    CHECK_FALSE(registered.contains_error());
    CHECK(cast_rows(host) == 1);
}

// A failed block admits only its end (PostgreSQL 18 25P02).
TEST_CASE("integration::cpp::udf_cast_in_block::a_failed_block_refuses_a_registration") {
    host_t host("test_udf_cast_in_block/udf_failed_block");
    const auto session = any();
    host.db.ok(session, "BEGIN;");
    REQUIRE(host.db.run(session, "SELECT * FROM nope;")->is_error());
    const auto registered = host.db.dispatcher()->register_udf(session, twice_function(host.resource()));
    CHECK(registered.type == core::error_code_t::transaction_finalized);
    const auto cast = host.db.dispatcher()->register_cast(session, cast_source, cast_target, noop_cast_entry());
    CHECK(cast.type == core::error_code_t::transaction_finalized);
    host.db.ok(session, "ROLLBACK;");
    CHECK(twice_rows(host) == 0);
    CHECK(cast_rows(host) == 0);
}

// ROLLBACK TO drops what was registered after the savepoint and keeps what was registered before it.
TEST_CASE("integration::cpp::udf_cast_in_block::rollback_to_a_savepoint_drops_the_later_registrations") {
    host_t host("test_udf_cast_in_block/savepoint");
    const auto session = any();
    const auto other = any();
    host.db.ok(any(), "CREATE TABLE t (id BIGINT);");
    host.db.ok(any(), "INSERT INTO t (id) VALUES (5);");
    const std::string cast = "SELECT CAST(TRUE AS DATE);";
    host.db.ok(session, "BEGIN;");
    REQUIRE_FALSE(host.db.dispatcher()->register_udf(session, twice_function(host.resource())).contains_error());
    host.db.ok(session, "SAVEPOINT a;");
    REQUIRE_FALSE(
        host.db.dispatcher()->register_cast(session, cast_source, cast_target, noop_cast_entry()).contains_error());
    REQUIRE_FALSE(
        host.db.dispatcher()->unregister_udf(session, "twice", bigint_input(host.resource())).contains_error());
    host.db.ok(session, "ROLLBACK TO a;");
    CHECK(host.db.ints(session, "SELECT twice(id) FROM t;") == std::vector<std::int64_t>{10});
    host.db.ok(session, "COMMIT;");
    CHECK(host.db.ints(other, "SELECT twice(id) FROM t;") == std::vector<std::int64_t>{10});
    CHECK(host.db.run(other, cast)->is_error());
    CHECK(twice_rows(host) == 1);
    CHECK(cast_rows(host) == 0);
}

// close_session rolls the block back: its registrations go with it.
TEST_CASE("integration::cpp::udf_cast_in_block::a_closed_session_leaves_no_function") {
    host_t host("test_udf_cast_in_block/close");
    const auto session = any();
    host.db.ok(any(), "CREATE TABLE t (id BIGINT);");
    host.db.ok(session, "BEGIN;");
    REQUIRE_FALSE(host.db.dispatcher()->register_udf(session, twice_function(host.resource())).contains_error());
    host.db.dispatcher()->close_session(session);
    CHECK(host.db.run(any(), "SELECT twice(id) FROM t;")->is_error());
    CHECK(twice_rows(host) == 0);
    const auto registered = host.db.dispatcher()->register_udf(any(), twice_function(host.resource()));
    INFO(registered.what);
    CHECK_FALSE(registered.contains_error());
}

// The registries take a committed function before the commit is published: a statement that sees the pg_proc row sees
// the function (PostgreSQL 18 AcceptInvalidationMessages at the start of a command).
TEST_CASE("integration::cpp::udf_cast_in_block::a_committed_function_is_seen_with_its_row") {
    host_t host("test_udf_cast_in_block/commit_window_register");
    const auto session = any();
    host.db.ok(any(), "CREATE TABLE t (id BIGINT);");
    host.db.ok(any(), "INSERT INTO t (id) VALUES (6);");
    host.db.ok(session, "BEGIN;");
    REQUIRE_FALSE(host.db.dispatcher()->register_udf(session, twice_function(host.resource())).contains_error());
    const auto seen = seen_while_committing(host, session);
    CHECK(first_int(seen.rows) == 1);
    INFO(session_sql::error_of(seen.call));
    CHECK(seen.call->is_success());
}

TEST_CASE("integration::cpp::udf_cast_in_block::a_dropped_function_goes_with_its_row") {
    host_t host("test_udf_cast_in_block/commit_window_unregister");
    const auto session = any();
    host.db.ok(any(), "CREATE TABLE t (id BIGINT);");
    host.db.ok(any(), "INSERT INTO t (id) VALUES (6);");
    REQUIRE_FALSE(host.db.dispatcher()->register_udf(any(), twice_function(host.resource())).contains_error());
    host.db.ok(session, "BEGIN;");
    REQUIRE_FALSE(
        host.db.dispatcher()->unregister_udf(session, "twice", bigint_input(host.resource())).contains_error());
    const auto seen = seen_while_committing(host, session);
    CHECK(first_int(seen.rows) == 0);
    CHECK(seen.call->is_error());
}

// A cascade that drops a function's pg_proc rows drops the function from the registries too.
TEST_CASE("integration::cpp::udf_cast_in_block::drop_schema_cascade_drops_its_functions") {
    host_t host("test_udf_cast_in_block/cascade_autocommit");
    const auto session = any();
    host.db.ok(session, "CREATE SCHEMA s;");
    host.db.ok(session, "SET search_path = 's';");
    const auto before = services::collection::executor::dev_user_functions_held();
    REQUIRE_FALSE(host.db.dispatcher()->register_udf(session, twice_function(host.resource())).contains_error());
    REQUIRE(services::collection::executor::dev_user_functions_held() > before);
    host.db.ok(any(), "DROP SCHEMA s CASCADE;");
    CHECK(services::collection::executor::dev_user_functions_held() == before);
    CHECK(twice_rows(host) == 0);
}

TEST_CASE("integration::cpp::udf_cast_in_block::drop_schema_cascade_in_a_block_drops_its_functions_at_commit") {
    host_t host("test_udf_cast_in_block/cascade_block");
    const auto session = any();
    host.db.ok(session, "CREATE SCHEMA s;");
    const auto before = services::collection::executor::dev_user_functions_held();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "SET search_path = 's';");
    REQUIRE_FALSE(host.db.dispatcher()->register_udf(session, twice_function(host.resource())).contains_error());
    host.db.ok(session, "DROP SCHEMA s CASCADE;");
    host.db.ok(session, "COMMIT;");
    CHECK(services::collection::executor::dev_user_functions_held() == before);
    CHECK(twice_rows(host) == 0);
}

// A registration in a block runs alone: it waits for the statement its session runs, and the next one waits for it.
TEST_CASE("integration::cpp::udf_cast_in_block::a_registration_in_a_block_runs_alone") {
    host_t host("test_udf_cast_in_block/exclusive");
    host.register_orders();
    host.backend().set_rows("m2.shop.public.orders", rows_t{{1, 100}, {2, 200}, {3, 300}});
    const auto session = any();
    host.db.ok(session, "BEGIN;");
    host.backend().answer_opens_together(2);

    components::cursor::cursor_t_ptr running;
    std::thread reader([&] { running = host.db.run(session, "SELECT id FROM m2.shop.public.orders;"); });
    REQUIRE(test_helpers::wait_until([&] { return host.backend().opens() == 1; }));

    const auto waited = services::dispatcher::dev_statements_waited_turn();
    core::error_t registered = core::error_t::no_error();
    std::thread registering(
        [&] { registered = host.db.dispatcher()->register_udf(session, twice_function(host.resource())); });
    CHECK(waited_beyond(waited));
    components::cursor::cursor_t_ptr called;
    std::thread caller([&] { called = host.db.run(session, "SELECT twice(1);"); });
    CHECK(waited_beyond(waited + 1));

    host.db.ok(any(), "SELECT id FROM m2.shop.public.orders;");
    reader.join();
    registering.join();
    caller.join();
    INFO(session_sql::error_of(running));
    CHECK(running->is_success());
    INFO(registered.what);
    CHECK_FALSE(registered.contains_error());
    INFO(session_sql::error_of(called));
    REQUIRE(called->is_success());
    CHECK(first_int(called) == 2);
    host.db.ok(session, "ROLLBACK;");
}

// A view a block creates over the function it registers is read by another session once the block commits.
TEST_CASE("integration::cpp::udf_cast_in_block::a_view_over_a_function_of_the_block") {
    host_t host("test_udf_cast_in_block/view");
    const auto session = any();
    host.db.ok(any(), "CREATE TABLE t (id BIGINT);");
    host.db.ok(any(), "INSERT INTO t (id) VALUES (7);");
    host.db.ok(session, "BEGIN;");
    REQUIRE_FALSE(host.db.dispatcher()->register_udf(session, twice_function(host.resource())).contains_error());
    host.db.ok(session, "CREATE VIEW v AS SELECT twice(id) AS x FROM t;");
    host.db.ok(session, "COMMIT;");
    CHECK(host.db.ints(any(), "SELECT x FROM v;") == std::vector<std::int64_t>{14});
}

// A COMMIT a backend refuses changes no registry: the same registration passes again.
TEST_CASE("integration::cpp::udf_cast_in_block::a_refused_commit_leaves_the_registries") {
    host_t host("test_udf_cast_in_block/refused_commit");
    host.register_orders();
    host.backend().set_rows("m2.shop.public.orders", rows_t{{1, 100}});
    host.db.ok(any(), "CREATE TABLE t (id BIGINT);");
    host.db.ok(any(), "INSERT INTO t (id) VALUES (1);");
    const auto session = any();
    const auto before = services::collection::executor::dev_user_functions_held();
    host.db.ok(session, "BEGIN;");
    REQUIRE_FALSE(host.db.dispatcher()->register_udf(session, twice_function(host.resource())).contains_error());
    host.db.ok(session, "INSERT INTO m2.shop.public.orders (id, amount) VALUES (4, 400);");
    host.backend().set_prepare_refusal("server m2: serialization failure");
    host.db.check_refused(session,
                          "COMMIT;",
                          core::error_code_t::connection_closed,
                          "server m2: serialization failure");
    CHECK(services::collection::executor::dev_user_functions_held() == before);
    CHECK(host.db.run(any(), "SELECT twice(id) FROM t;")->is_error());
    CHECK(twice_rows(host) == 0);
    const auto registered = host.db.dispatcher()->register_udf(any(), twice_function(host.resource()));
    INFO(registered.what);
    CHECK_FALSE(registered.contains_error());
}

// A schema of a connection holds tables only: a function there is refused as a view or a type is.
TEST_CASE("integration::cpp::udf_cast_in_block::a_function_in_a_schema_of_a_connection_is_refused") {
    host_t host("test_udf_cast_in_block/udf_connection");
    host.register_shop();
    const auto session = any();
    host.db.ok(session, "SET search_path = 'm2.shop.public';");
    const auto refused = host.db.dispatcher()->register_udf(session, twice_function(host.resource()));
    CHECK(refused.type == core::error_code_t::unimplemented_yet);
    CHECK(std::string{refused.what} == "only tables can be created in a schema of connection \"m2\"");
    CHECK(twice_rows(host) == 0);
}
