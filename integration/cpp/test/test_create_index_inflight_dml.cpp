// Every row a reader can see through the table must be reachable through the index: the index
// tolerates a superset of ids (the table's visibility check drops extras), but a missing id is a
// defect nothing downstream can repair.

#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>

// DEV_MODE seams defined in services/collection/executor.cpp; declared here rather than in a
// header so the seam stays out of every production include path (drift = loud link error).
namespace services::collection::executor {
    void dev_set_dml_pre_drive_hook(void (*hook)(uint64_t session_data)) noexcept;
    uint64_t index_reconcile_staged_ranges() noexcept;
} // namespace services::collection::executor

using namespace test_helpers;

namespace {

    // One-shot pre-drive latch for exactly one session: holds that session's statement between
    // its (already finished) planning and its first append, on the executor's own thread.
    // Deadline-bounded so a wiring mistake reports instead of hanging the suite.
    std::atomic<uint64_t> g_pause_session{0};
    std::atomic<bool> g_paused{false};
    std::atomic<bool> g_release{false};
    std::atomic<bool> g_pause_timed_out{false};

    void pre_drive_pause(uint64_t session_data) {
        if (session_data != g_pause_session.load() || g_release.load()) {
            return;
        }
        g_paused.store(true);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
        while (!g_release.load()) {
            if (std::chrono::steady_clock::now() > deadline) {
                g_pause_timed_out.store(true);
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

} // namespace

TEST_CASE("integration::cpp::create_index_inflight_dml::uncommitted_insert_lands_in_the_index") {
    auto config = make_test_config(integration_fixture_path("test_create_index_inflight_dml/uncommitted"),
                                   /*wal_on=*/true);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE db;")->is_success());

    REQUIRE(exec(d, "CREATE TABLE db.ctrl (id bigint, v bigint);")->is_success());
    REQUIRE(seed_rows(d, "db.ctrl", "id, v", 2000, [](unsigned i) {
                return "(" + std::to_string(i) + ", " + std::to_string(i) + ")";
            })->is_success());
    REQUIRE(exec(d, "CREATE INDEX ctrl_id ON db.ctrl (id);")->is_success());
    {
        auto a = otterbrix::session_id_t();
        REQUIRE(d->execute_sql(a, "BEGIN;")->is_success());
        REQUIRE(d->execute_sql(a, "INSERT INTO db.ctrl (id, v) VALUES (111111, 1);")->is_success());
        REQUIRE(d->execute_sql(a, "COMMIT;")->is_success());
    }
    {
        auto cur = exec(d, "SELECT v FROM db.ctrl WHERE id = 111111;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    // The build's snapshot scan cannot see a foreign transaction's uncommitted insert; since the
    // row's DML predates the build, no mirror or later feed carries it once it commits.
    REQUIRE(exec(d, "CREATE TABLE db.probe (id bigint, v bigint);")->is_success());
    REQUIRE(seed_rows(d, "db.probe", "id, v", 2000, [](unsigned i) {
                return "(" + std::to_string(i) + ", " + std::to_string(i) + ")";
            })->is_success());
    {
        auto a = otterbrix::session_id_t();
        REQUIRE(d->execute_sql(a, "BEGIN;")->is_success());
        REQUIRE(d->execute_sql(a, "INSERT INTO db.probe (id, v) VALUES (222222, 2);")->is_success());
        REQUIRE(exec(d, "CREATE INDEX probe_id ON db.probe (id);")->is_success());
        REQUIRE(d->execute_sql(a, "COMMIT;")->is_success());
    }

    {
        auto heap = exec(d, "SELECT v FROM db.probe WHERE id + 0 = 222222;");
        REQUIRE(heap->is_success());
        REQUIRE(heap->size() == 1);
    }
    {
        auto total = exec(d, "SELECT id FROM db.probe;");
        REQUIRE(total->is_success());
        REQUIRE(total->size() == 2001);
    }
    {
        auto eq = exec(d, "SELECT v FROM db.probe WHERE id = 222222;");
        REQUIRE(eq->is_success());
        INFO("equality through the index answered " << eq->size() << " row(s), expected 1");
        REQUIRE(eq->size() == 1);
    }
    {
        auto rng = exec(d, "SELECT v FROM db.probe WHERE id > 222221;");
        REQUIRE(rng->is_success());
        INFO("range through the index answered " << rng->size() << " row(s), expected 1");
        REQUIRE(rng->size() == 1);
    }
}

// A DELETE held uncommitted across the build, then rolled back: the surviving row must still be
// reachable through the index, since the build may not act on an undecided delete.
TEST_CASE("integration::cpp::create_index_inflight_dml::rolled_back_delete_keeps_the_row_indexed") {
    auto config = make_test_config(integration_fixture_path("test_create_index_inflight_dml/rolled_back_delete"),
                                   /*wal_on=*/true);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE db;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE db.t (id bigint, v bigint);")->is_success());
    REQUIRE(seed_rows(d, "db.t", "id, v", 2000, [](unsigned i) {
                return "(" + std::to_string(i) + ", " + std::to_string(i) + ")";
            })->is_success());

    {
        auto a = otterbrix::session_id_t();
        REQUIRE(d->execute_sql(a, "BEGIN;")->is_success());
        REQUIRE(d->execute_sql(a, "DELETE FROM db.t WHERE id = 777;")->is_success());
        REQUIRE(exec(d, "CREATE INDEX t_id ON db.t (id);")->is_success());
        REQUIRE(d->execute_sql(a, "ROLLBACK;")->is_success());
    }

    {
        auto heap = exec(d, "SELECT v FROM db.t WHERE id + 0 = 777;");
        REQUIRE(heap->is_success());
        REQUIRE(heap->size() == 1);
    }
    {
        auto eq = exec(d, "SELECT v FROM db.t WHERE id = 777;");
        REQUIRE(eq->is_success());
        INFO("equality through the index answered " << eq->size() << " row(s), expected 1");
        REQUIRE(eq->size() == 1);
    }
}

// The INSERT is planned while the table has no index (stamped "mirror nothing"), then frozen at
// the pre-drive seam while CREATE INDEX finishes elsewhere; only the executor's post-append
// reconciliation with manager_index can carry its rows into the index.
TEST_CASE("integration::cpp::create_index_inflight_dml::stale_planned_insert_reaches_the_built_index") {
    auto config = make_test_config(integration_fixture_path("test_create_index_inflight_dml/stale_plan"),
                                   /*wal_on=*/true);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE db;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE db.late (id bigint, v bigint);")->is_success());
    REQUIRE(seed_rows(d, "db.late", "id, v", 100, [](unsigned i) {
                return "(" + std::to_string(i) + ", " + std::to_string(i) + ")";
            })->is_success());

    auto ins_session = otterbrix::session_id_t();
    auto ddl_session = otterbrix::session_id_t();
    while (std::hash<components::session::session_id_t>{}(ins_session) % 4 ==
           std::hash<components::session::session_id_t>{}(ddl_session) % 4) {
        ddl_session = otterbrix::session_id_t();
    }

    g_paused.store(false);
    g_release.store(false);
    g_pause_timed_out.store(false);
    g_pause_session.store(ins_session.data());
    services::collection::executor::dev_set_dml_pre_drive_hook(&pre_drive_pause);

    const auto staged_before = services::collection::executor::index_reconcile_staged_ranges();

    components::cursor::cursor_t_ptr ins_cur;
    std::thread ins([&] { ins_cur = d->execute_sql(ins_session, "INSERT INTO db.late (id, v) VALUES (424242, 7);"); });

    const auto pause_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!g_paused.load() && std::chrono::steady_clock::now() < pause_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    REQUIRE(g_paused.load());

    REQUIRE(d->execute_sql(ddl_session, "CREATE INDEX late_id ON db.late (id);")->is_success());

    g_release.store(true);
    ins.join();
    services::collection::executor::dev_set_dml_pre_drive_hook(nullptr);
    REQUIRE_FALSE(g_pause_timed_out.load());
    REQUIRE(ins_cur != nullptr);
    REQUIRE(ins_cur->is_success());

    {
        auto heap = exec(d, "SELECT v FROM db.late WHERE id + 0 = 424242;");
        REQUIRE(heap->is_success());
        REQUIRE(heap->size() == 1);
    }
    {
        auto eq = exec(d, "SELECT v FROM db.late WHERE id = 424242;");
        REQUIRE(eq->is_success());
        INFO("equality through the index answered " << eq->size() << " row(s), expected 1");
        REQUIRE(eq->size() == 1);
    }
    {
        const auto staged_after = services::collection::executor::index_reconcile_staged_ranges();
        INFO("reconcile-staged ranges during the scenario: " << (staged_after - staged_before));
        REQUIRE(staged_after > staged_before);
    }
}
