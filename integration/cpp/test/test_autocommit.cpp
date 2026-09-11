#include "test_config.hpp"
#include "integration_fixture_path.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <services/disk/agent_disk.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace components;
using namespace components::cursor;

namespace {
    cursor_t_ptr run(otterbrix::wrapper_dispatcher_t* dispatcher,
                     const otterbrix::session_id_t& session,
                     const std::string& sql) {
        auto cursor = dispatcher->execute_sql(session, sql);
        INFO("statement: " << sql);
        return cursor;
    }

    std::size_t committed_rows(otterbrix::wrapper_dispatcher_t* dispatcher) {
        auto session = otterbrix::session_id_t();
        auto cursor = dispatcher->execute_sql(session, "SELECT id FROM TestDatabase.marks;");
        REQUIRE(cursor->is_success());
        return cursor->size();
    }

    void create_table(otterbrix::wrapper_dispatcher_t* dispatcher) {
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE TestDatabase;")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.marks (id bigint);")->is_success());
        }
    }

    // Not multiple of default column segment size
    constexpr std::size_t SOURCE_ROWS =
        static_cast<std::size_t>(static_cast<double>(vector::DEFAULT_VECTOR_CAPACITY) * 3.4);
    constexpr std::size_t SOURCE_INSERT_BATCH = 512;

    void create_source_table(otterbrix::wrapper_dispatcher_t* dispatcher) {
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE TestDatabase.source (id bigint);")->is_success());
        }
        std::size_t done = 0;
        while (done < SOURCE_ROWS) {
            const std::size_t batch_end = std::min(done + SOURCE_INSERT_BATCH, SOURCE_ROWS);
            std::stringstream query;
            query << "INSERT INTO TestDatabase.source (id) VALUES ";
            for (std::size_t row = done; row < batch_end; ++row) {
                query << "(" << row << ")" << (row + 1 == batch_end ? ";" : ", ");
            }
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, query.str())->is_success());
            done = batch_end;
        }
    }

    struct scan_pause_t final : services::disk::scan_advance_gate_t {
        std::atomic<bool> reached{false};
        std::atomic<bool> released{false};

        bool hold(catalog::oid_t table_oid, uint64_t /*cursor_id*/) override {
            if (static_cast<uint32_t>(table_oid) < static_cast<uint32_t>(catalog::FIRST_USER_OID)) {
                return false;
            }
            reached.store(true, std::memory_order_release);
            return !released.load(std::memory_order_acquire);
        }
    };

    struct scan_pause_guard_t {
        scan_pause_t gate;
        scan_pause_guard_t() { services::disk::dev_set_scan_advance_gate(&gate); }
        ~scan_pause_guard_t() { services::disk::dev_set_scan_advance_gate(nullptr); }
        scan_pause_guard_t(const scan_pause_guard_t&) = delete;
        scan_pause_guard_t& operator=(const scan_pause_guard_t&) = delete;
    };

    bool wait_until_reached(const std::atomic<bool>& flag) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (!flag.load(std::memory_order_acquire)) {
            if (std::chrono::steady_clock::now() > deadline) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return true;
    }

    struct overlap_result_t {
        cursor_t_ptr held;
        std::vector<cursor_t_ptr> while_held;
        bool reached{false};
    };

    overlap_result_t run_overlapping(otterbrix::wrapper_dispatcher_t* dispatcher,
                                     const otterbrix::session_id_t& session,
                                     const std::string& held_sql,
                                     const std::vector<std::string>& while_held_sql) {
        overlap_result_t out;
        scan_pause_guard_t guard;
        std::thread held([&] { out.held = dispatcher->execute_sql(session, held_sql); });
        out.reached = wait_until_reached(guard.gate.reached);
        if (out.reached) {
            for (const auto& sql : while_held_sql) {
                out.while_held.push_back(dispatcher->execute_sql(session, sql));
            }
        }
        guard.gate.released.store(true, std::memory_order_release);
        held.join();
        return out;
    }

    struct queued_result_t {
        cursor_t_ptr held;
        cursor_t_ptr queued;
        bool reached{false};
        bool finished_while_held{false};
    };

    queued_result_t run_queued_behind(otterbrix::wrapper_dispatcher_t* dispatcher,
                                      const otterbrix::session_id_t& session,
                                      const std::string& held_sql,
                                      const std::string& queued_sql) {
        queued_result_t out;
        scan_pause_guard_t guard;
        std::thread held([&] { out.held = dispatcher->execute_sql(session, held_sql); });
        out.reached = wait_until_reached(guard.gate.reached);
        std::atomic<bool> queued_finished{false};
        std::thread queued([&] {
            out.queued = dispatcher->execute_sql(session, queued_sql);
            queued_finished.store(true, std::memory_order_release);
        });
        if (out.reached) {
            // Ample for a statement that is not held back to run to completion.
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            out.finished_while_held = queued_finished.load(std::memory_order_acquire);
        }
        guard.gate.released.store(true, std::memory_order_release);
        held.join();
        queued.join();
        return out;
    }
} // namespace

TEST_CASE("integration::cpp::autocommit::on_commits_each_statement") {
    auto config = test_create_config(integration_fixture_path("test_autocommit/on_commits_each_statement"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_table(dispatcher);

    // The default: each statement is committed before the next one runs, on one shared session.
    auto writer = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.marks (id) VALUES (1);")->is_success());
    REQUIRE(committed_rows(dispatcher) == 1);
    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.marks (id) VALUES (2);")->is_success());
    REQUIRE(committed_rows(dispatcher) == 2);
}

TEST_CASE("integration::cpp::autocommit::off_defers_until_commit") {
    auto config = test_create_config(integration_fixture_path("test_autocommit/off_defers_until_commit"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_table(dispatcher);

    auto writer = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, writer, "SET autocommit = off;")->is_success());

    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.marks (id) VALUES (1);")->is_success());
    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.marks (id) VALUES (2);")->is_success());
    // Nothing committed yet, though both statements reported success.
    REQUIRE(committed_rows(dispatcher) == 0);

    REQUIRE(run(dispatcher, writer, "COMMIT;")->is_success());
    REQUIRE(committed_rows(dispatcher) == 2);
}

TEST_CASE("integration::cpp::autocommit::off_rolls_back_everything_uncommitted") {
    auto config = test_create_config(integration_fixture_path("test_autocommit/off_rolls_back"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_table(dispatcher);

    auto writer = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, writer, "SET autocommit = off;")->is_success());
    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.marks (id) VALUES (1);")->is_success());
    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.marks (id) VALUES (2);")->is_success());
    REQUIRE(run(dispatcher, writer, "ROLLBACK;")->is_success());

    REQUIRE(committed_rows(dispatcher) == 0);
}

TEST_CASE("integration::cpp::autocommit::commit_starts_next_transaction") {
    auto config = test_create_config(integration_fixture_path("test_autocommit/commit_starts_fresh"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_table(dispatcher);

    auto writer = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, writer, "SET autocommit = off;")->is_success());

    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.marks (id) VALUES (1);")->is_success());
    REQUIRE(run(dispatcher, writer, "COMMIT;")->is_success());
    REQUIRE(committed_rows(dispatcher) == 1);

    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.marks (id) VALUES (2);")->is_success());
    REQUIRE(committed_rows(dispatcher) == 1);
    REQUIRE(run(dispatcher, writer, "ROLLBACK;")->is_success());
    REQUIRE(committed_rows(dispatcher) == 1);
}

TEST_CASE("integration::cpp::autocommit::the_setting_survives_restart") {
    auto config = test_create_config(integration_fixture_path("test_autocommit/survives_reopen"));
    test_clear_directory(config);
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        create_table(dispatcher);
        auto session = otterbrix::session_id_t();
        REQUIRE(run(dispatcher, session, "SET autocommit = off;")->is_success());
        REQUIRE(run(dispatcher, session, "INSERT INTO TestDatabase.marks (id) VALUES (1);")->is_success());
        REQUIRE(run(dispatcher, session, "COMMIT;")->is_success());
    }
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        auto session = otterbrix::session_id_t();
        REQUIRE(run(dispatcher, session, "INSERT INTO TestDatabase.marks (id) VALUES (2);")->is_success());
        REQUIRE(committed_rows(dispatcher) == 1);
        REQUIRE(run(dispatcher, session, "COMMIT;")->is_success());
        REQUIRE(committed_rows(dispatcher) == 2);
    }
}

TEST_CASE("integration::cpp::autocommit::a_failed_statement_leaves_the_transaction_failed_until_rollback") {
    auto config = test_create_config(integration_fixture_path("test_autocommit/failed_until_rollback"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_table(dispatcher);

    auto writer = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, writer, "BEGIN;")->is_success());
    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.marks (id) VALUES (1);")->is_success());
    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.missing (id) VALUES (2);")->is_error());

    // Refused, rather than run in a transaction of its own and committed.
    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.marks (id) VALUES (3);")->is_error());
    REQUIRE(run(dispatcher, writer, "BEGIN;")->is_error());
    REQUIRE(committed_rows(dispatcher) == 0);

    REQUIRE(run(dispatcher, writer, "ROLLBACK;")->is_success());
    REQUIRE(committed_rows(dispatcher) == 0);

    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.marks (id) VALUES (4);")->is_success());
    REQUIRE(committed_rows(dispatcher) == 1);
}

TEST_CASE("integration::cpp::autocommit::commit_ends_a_failed_transaction_and_reports_it") {
    auto config = test_create_config(integration_fixture_path("test_autocommit/commit_after_failure"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_table(dispatcher);

    auto writer = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, writer, "BEGIN;")->is_success());
    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.marks (id) VALUES (1);")->is_success());
    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.missing (id) VALUES (2);")->is_error());

    REQUIRE(run(dispatcher, writer, "COMMIT;")->is_error());
    REQUIRE(committed_rows(dispatcher) == 0);

    // The COMMIT ended the transaction, so the session needs no ROLLBACK to be usable again.
    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.marks (id) VALUES (4);")->is_success());
    REQUIRE(committed_rows(dispatcher) == 1);
}

TEST_CASE("integration::cpp::autocommit::a_statement_failing_inside_begin_undoes_the_earlier_ones") {
    auto config = test_create_config(integration_fixture_path("test_autocommit/failure_undoes_earlier"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_table(dispatcher);
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher
                    ->execute_sql(session, "ALTER TABLE TestDatabase.marks ADD CONSTRAINT positive_id CHECK (id > 0);")
                    ->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO TestDatabase.marks (id) VALUES (1), (2);")->is_success());
    }

    auto writer = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, writer, "BEGIN;")->is_success());
    REQUIRE(run(dispatcher, writer, "DELETE FROM TestDatabase.marks WHERE id = 1;")->is_success());
    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.marks (id) VALUES (5);")->is_success());
    // Fails while executing, after its row was appended.
    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.marks (id) VALUES (-1);")->is_error());
    REQUIRE(run(dispatcher, writer, "ROLLBACK;")->is_success());
    REQUIRE(committed_rows(dispatcher) == 2);

    // A delete stamp the failed transaction left on row 1 would make this DELETE skip it.
    auto cleaner = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, cleaner, "DELETE FROM TestDatabase.marks WHERE id = 1;")->is_success());
    REQUIRE(committed_rows(dispatcher) == 1);
}

TEST_CASE("integration::cpp::autocommit::off_a_failed_statement_refuses_until_rollback") {
    auto config = test_create_config(integration_fixture_path("test_autocommit/off_failed_until_rollback"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_table(dispatcher);

    auto writer = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, writer, "SET autocommit = off;")->is_success());
    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.marks (id) VALUES (1);")->is_success());
    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.missing (id) VALUES (2);")->is_error());
    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.marks (id) VALUES (3);")->is_error());
    REQUIRE(run(dispatcher, writer, "ROLLBACK;")->is_success());
    REQUIRE(committed_rows(dispatcher) == 0);

    REQUIRE(run(dispatcher, writer, "INSERT INTO TestDatabase.marks (id) VALUES (4);")->is_success());
    REQUIRE(run(dispatcher, writer, "COMMIT;")->is_success());
    REQUIRE(committed_rows(dispatcher) == 1);
}

TEST_CASE("integration::cpp::autocommit::a_second_transaction_on_a_busy_session_waits_its_turn") {
    auto config = test_create_config(integration_fixture_path("test_autocommit/busy_session"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_table(dispatcher);
    create_source_table(dispatcher);

    auto session = otterbrix::session_id_t();
    auto result = run_queued_behind(dispatcher,
                                    session,
                                    "SELECT id FROM TestDatabase.source;",
                                    "INSERT INTO TestDatabase.marks (id) VALUES (1);");
    REQUIRE(result.reached);
    // Neither refused nor run beside the transaction ahead of it: it waited, then ran.
    REQUIRE_FALSE(result.finished_while_held);
    REQUIRE(result.queued->is_success());
    REQUIRE(result.held->is_success());
    REQUIRE(result.held->size() == SOURCE_ROWS);
    REQUIRE(committed_rows(dispatcher) == 1);
}

TEST_CASE("integration::cpp::autocommit::statements_of_one_transaction_may_overlap") {
    auto config = test_create_config(integration_fixture_path("test_autocommit/overlap_in_transaction"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_table(dispatcher);
    create_source_table(dispatcher);

    auto session = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());
    auto result = run_overlapping(dispatcher,
                                  session,
                                  "SELECT id FROM TestDatabase.source;",
                                  {"INSERT INTO TestDatabase.marks (id) VALUES (1);"});
    REQUIRE(result.reached);
    REQUIRE(result.while_held.front()->is_success());
    REQUIRE(result.held->is_success());
    REQUIRE(committed_rows(dispatcher) == 0);

    REQUIRE(run(dispatcher, session, "COMMIT;")->is_success());
    REQUIRE(committed_rows(dispatcher) == 1);
}

TEST_CASE("integration::cpp::autocommit::a_statement_outliving_its_failed_transaction_reports_an_error") {
    auto config = test_create_config(integration_fixture_path("test_autocommit/outlives_failed"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_table(dispatcher);
    create_source_table(dispatcher);

    auto session = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());
    auto result = run_overlapping(dispatcher,
                                  session,
                                  "INSERT INTO TestDatabase.marks (id) SELECT id FROM TestDatabase.source;",
                                  {"INSERT INTO TestDatabase.missing (id) VALUES (1);"});
    REQUIRE(result.reached);
    REQUIRE(result.while_held.front()->is_error());
    // Its transaction failed while it ran, so its rows are not parked in it as if nothing happened.
    REQUIRE(result.held->is_error());

    REQUIRE(run(dispatcher, session, "ROLLBACK;")->is_success());
    REQUIRE(committed_rows(dispatcher) == 0);
}

TEST_CASE("integration::cpp::autocommit::commit_waits_for_its_transactions_running_statements") {
    auto config = test_create_config(integration_fixture_path("test_autocommit/commit_waits"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_table(dispatcher);
    create_source_table(dispatcher);

    auto session = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());
    auto result = run_queued_behind(dispatcher,
                                    session,
                                    "INSERT INTO TestDatabase.marks (id) SELECT id FROM TestDatabase.source;",
                                    "COMMIT;");
    REQUIRE(result.reached);
    // A COMMIT that overtook the INSERT would publish the transaction without it.
    REQUIRE_FALSE(result.finished_while_held);
    REQUIRE(result.held->is_success());
    REQUIRE(result.queued->is_success());
    REQUIRE(committed_rows(dispatcher) == SOURCE_ROWS);
}

TEST_CASE("integration::cpp::autocommit::begin_with_transaction_modes_is_refused") {
    auto config = test_create_config(integration_fixture_path("test_autocommit/begin_modes"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_table(dispatcher);

    auto session = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, session, "BEGIN READ ONLY;")->is_error());
    REQUIRE(run(dispatcher, session, "BEGIN ISOLATION LEVEL SERIALIZABLE;")->is_error());
    REQUIRE(run(dispatcher, session, "START TRANSACTION ISOLATION LEVEL READ COMMITTED;")->is_error());

    // No transaction was opened behind the refusals: this commits on its own.
    REQUIRE(run(dispatcher, session, "INSERT INTO TestDatabase.marks (id) VALUES (1);")->is_success());
    REQUIRE(committed_rows(dispatcher) == 1);

    REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());
    REQUIRE(run(dispatcher, session, "INSERT INTO TestDatabase.marks (id) VALUES (2);")->is_success());
    REQUIRE(committed_rows(dispatcher) == 1);
    REQUIRE(run(dispatcher, session, "COMMIT;")->is_success());
    REQUIRE(committed_rows(dispatcher) == 2);
}
