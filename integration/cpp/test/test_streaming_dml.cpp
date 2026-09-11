// DML folds one batch at a time via push() into the streaming executor instead of
// materializing the whole scan first; atomicity is held by the MVCC transaction, not
// the operator. The streaming sink shares the legacy on_execute path's append core
// (R6), so tests check both that results land and that streaming_pipeline_runs()
// bumps (stubbing role() back to none makes that assertion RED).

#include "test_config.hpp"
#include "integration_fixture_path.hpp"
#include <catch2/catch_test_macros.hpp>
#include <services/collection/executor.hpp>

using namespace components;
using namespace components::cursor;

namespace {
    // >> DEFAULT_VECTOR_CAPACITY (1024) so the scan spans multiple batches.
    constexpr unsigned kRowCount = 5000;

    cursor_t_ptr exec(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    }
} // namespace

TEST_CASE("integration::cpp::streaming_dml::insert_select_streams_and_lands") {
    auto config = test_create_config(integration_fixture_path("test_streaming_dml_insert"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE StreamDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.src (id bigint, grp int, val bigint);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.dst (id bigint, grp int, val bigint);")->is_success());

    {
        std::stringstream q;
        q << "INSERT INTO StreamDb.src (id, grp, val) VALUES ";
        for (unsigned i = 0; i < kRowCount; ++i) {
            q << "(" << i << ", " << (i % 8) << ", " << (i * 2) << ")" << (i + 1 == kRowCount ? ";" : ", ");
        }
        auto cur = exec(dispatcher, q.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRowCount);
    }

    const auto runs_before = services::collection::executor::streaming_pipeline_runs();
    {
        auto cur = exec(dispatcher, "INSERT INTO StreamDb.dst (id, grp, val) SELECT id, grp, val FROM StreamDb.src;");
        INFO("INSERT...SELECT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRowCount);
    }
    const auto runs_after = services::collection::executor::streaming_pipeline_runs();

    REQUIRE(runs_after > runs_before);

    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM StreamDb.dst;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<uint64_t>() == static_cast<uint64_t>(kRowCount));
    }
    {
        int64_t expected_sum = 0;
        for (unsigned i = 0; i < kRowCount; ++i) {
            expected_sum += static_cast<int64_t>(i) * 2;
        }
        auto cur = exec(dispatcher, "SELECT SUM(val) AS s FROM StreamDb.dst;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == expected_sum);
    }
}

TEST_CASE("integration::cpp::streaming_dml::insert_values_streams") {
    auto config = test_create_config(integration_fixture_path("test_streaming_dml_values"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE StreamDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.t (id bigint, val bigint);")->is_success());

    const auto runs_before = services::collection::executor::streaming_pipeline_runs();
    {
        auto cur = exec(dispatcher, "INSERT INTO StreamDb.t (id, val) VALUES (1, 10), (2, 20), (3, 30);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }
    const auto runs_after = services::collection::executor::streaming_pipeline_runs();
    REQUIRE(runs_after > runs_before);

    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM StreamDb.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<uint64_t>() == 3);
    }
    {
        auto cur = exec(dispatcher, "SELECT val FROM StreamDb.t WHERE id = 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 20);
    }
}

TEST_CASE("integration::cpp::streaming_dml::insert_values_returning_streams") {
    // RETURNING re-reads the appended segment after push()+await_async_and_resume commits it.
    auto config = test_create_config(integration_fixture_path("test_streaming_dml_values_returning"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE StreamDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.t (id bigint, val bigint);")->is_success());

    const auto runs_before = services::collection::executor::streaming_pipeline_runs();
    {
        auto cur =
            exec(dispatcher, "INSERT INTO StreamDb.t (id, val) VALUES (1, 10), (2, 20) RETURNING id, val * 2 AS d;");
        INFO("INSERT VALUES RETURNING error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
    const auto runs_after = services::collection::executor::streaming_pipeline_runs();
    REQUIRE(runs_after > runs_before);

    // RETURNING rows aren't order-guaranteed; spot-check via SELECT.
    {
        auto cur = exec(dispatcher, "SELECT val FROM StreamDb.t WHERE id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 10);
    }
}

TEST_CASE("integration::cpp::streaming_dml::delete_predicate_streams_and_lands") {
    auto config = test_create_config(integration_fixture_path("test_streaming_dml_delete"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE StreamDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.t (id bigint, val bigint);")->is_success());
    // Proves the DELETE's index mirror ran: a deleted key must no longer be found via the index.
    REQUIRE(exec(dispatcher, "CREATE INDEX idx_id ON StreamDb.t (id);")->is_success());

    {
        std::stringstream q;
        q << "INSERT INTO StreamDb.t (id, val) VALUES ";
        for (unsigned i = 0; i < kRowCount; ++i) {
            q << "(" << i << ", " << i << ")" << (i + 1 == kRowCount ? ";" : ", ");
        }
        auto cur = exec(dispatcher, q.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRowCount);
    }

    constexpr unsigned kThreshold = 3000;
    const auto runs_before = services::collection::executor::streaming_pipeline_runs();
    {
        auto cur = exec(dispatcher,
                        "DELETE FROM StreamDb.t WHERE val < " + std::to_string(kThreshold) + " RETURNING id, val;");
        INFO("DELETE error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kThreshold);
    }
    const auto runs_after = services::collection::executor::streaming_pipeline_runs();
    REQUIRE(runs_after > runs_before);

    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM StreamDb.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<uint64_t>() == static_cast<uint64_t>(kRowCount - kThreshold));
    }
    {
        auto cur = exec(dispatcher, "SELECT id FROM StreamDb.t WHERE id = 5;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
    {
        auto cur = exec(dispatcher, "SELECT id FROM StreamDb.t WHERE id = 4000;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 4000);
    }
}

TEST_CASE("integration::cpp::streaming_dml::update_predicate_streams_and_lands") {
    auto config = test_create_config(integration_fixture_path("test_streaming_dml_update"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE StreamDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.t (id bigint, val bigint);")->is_success());
    // Proves the UPDATE's index mirror (old delete + new insert) ran for the streamed batches.
    REQUIRE(exec(dispatcher, "CREATE INDEX idx_val ON StreamDb.t (val);")->is_success());

    {
        std::stringstream q;
        q << "INSERT INTO StreamDb.t (id, val) VALUES ";
        for (unsigned i = 0; i < kRowCount; ++i) {
            q << "(" << i << ", " << i << ")" << (i + 1 == kRowCount ? ";" : ", ");
        }
        auto cur = exec(dispatcher, q.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRowCount);
    }

    constexpr unsigned kThreshold = 2500;
    constexpr int64_t kBump = 1000000; // pushes updated val out of the [0, kRowCount) range
    const auto runs_before = services::collection::executor::streaming_pipeline_runs();
    {
        auto cur = exec(dispatcher,
                        "UPDATE StreamDb.t SET val = val + " + std::to_string(kBump) + " WHERE id < " +
                            std::to_string(kThreshold) + " RETURNING id, val;");
        INFO("UPDATE error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kThreshold);
    }
    const auto runs_after = services::collection::executor::streaming_pipeline_runs();
    REQUIRE(runs_after > runs_before);

    {
        auto cur = exec(dispatcher, "SELECT val FROM StreamDb.t WHERE id = 5;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 5 + kBump);
    }
    {
        auto cur = exec(dispatcher, "SELECT val FROM StreamDb.t WHERE id = 3000;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 3000);
    }
    {
        auto cur = exec(dispatcher, "SELECT id FROM StreamDb.t WHERE val = 5;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
    {
        auto cur = exec(dispatcher, "SELECT id FROM StreamDb.t WHERE val = " + std::to_string(5 + kBump) + ";");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 5);
    }
}

// fk_check / fk_cascade / check_constraint are role()==sink + needs_async_finalize,
// parented over the DML sink, so the whole constraint->DML->scan chain streams. The
// executor commits the DML first, snapshotting written rows into constraint_input_,
// before the constraint validates/cascades — hence a multi-batch INSERT...SELECT, so
// the constraint reads that snapshot rather than the streaming scan's empty output_.

TEST_CASE("integration::cpp::streaming_dml::fk_check_streams_insert_select") {
    auto config = test_create_config(integration_fixture_path("test_streaming_dml_fk_check"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE StreamDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.parent (id bigint, name text);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.child (id bigint, parent_id bigint);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.src_ok (id bigint, parent_id bigint);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.src_bad (id bigint, parent_id bigint);")->is_success());
    REQUIRE(exec(dispatcher,
                 "ALTER TABLE StreamDb.child ADD CONSTRAINT fk_p "
                 "FOREIGN KEY (parent_id) REFERENCES StreamDb.parent (id);")
                ->is_success());

    REQUIRE(exec(dispatcher, "INSERT INTO StreamDb.parent (id, name) VALUES (1, 'p1');")->is_success());
    {
        std::stringstream q;
        q << "INSERT INTO StreamDb.src_ok (id, parent_id) VALUES ";
        for (unsigned i = 0; i < kRowCount; ++i) {
            q << "(" << i << ", 1)" << (i + 1 == kRowCount ? ";" : ", ");
        }
        REQUIRE(exec(dispatcher, q.str())->is_success());
    }
    REQUIRE(exec(dispatcher, "INSERT INTO StreamDb.src_bad (id, parent_id) VALUES (1, 99);")->is_success());

    const auto runs_before = services::collection::executor::streaming_pipeline_runs();
    {
        auto cur =
            exec(dispatcher, "INSERT INTO StreamDb.child (id, parent_id) SELECT id, parent_id FROM StreamDb.src_ok;");
        INFO("constrained INSERT...SELECT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    const auto runs_after = services::collection::executor::streaming_pipeline_runs();
    REQUIRE(runs_after > runs_before);

    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM StreamDb.child;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<uint64_t>() == static_cast<uint64_t>(kRowCount));
    }

    {
        auto cur =
            exec(dispatcher, "INSERT INTO StreamDb.child (id, parent_id) SELECT id, parent_id FROM StreamDb.src_bad;");
        REQUIRE(cur->is_error());
    }
}

TEST_CASE("integration::cpp::streaming_dml::check_constraint_streams_insert_select") {
    auto config = test_create_config(integration_fixture_path("test_streaming_dml_check"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE StreamDb;")->is_success());
    // age is bigint so the CHECK constant compares same-type (an int32 column would
    // hit an unrelated logical_value_t coercion gap, not the path under test here).
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.items (id bigint, age bigint);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.src_ok (id bigint, age bigint);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.src_bad (id bigint, age bigint);")->is_success());
    REQUIRE(exec(dispatcher, "ALTER TABLE StreamDb.items ADD CONSTRAINT chk_age CHECK (age > 0);")->is_success());

    {
        std::stringstream q;
        q << "INSERT INTO StreamDb.src_ok (id, age) VALUES ";
        for (unsigned i = 0; i < kRowCount; ++i) {
            q << "(" << i << ", " << (i + 1) << ")" << (i + 1 == kRowCount ? ";" : ", ");
        }
        REQUIRE(exec(dispatcher, q.str())->is_success());
    }
    REQUIRE(exec(dispatcher, "INSERT INTO StreamDb.src_bad (id, age) VALUES (1, -5);")->is_success());

    const auto runs_before = services::collection::executor::streaming_pipeline_runs();
    {
        auto cur = exec(dispatcher, "INSERT INTO StreamDb.items (id, age) SELECT id, age FROM StreamDb.src_ok;");
        INFO("CHECK INSERT...SELECT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    const auto runs_after = services::collection::executor::streaming_pipeline_runs();
    REQUIRE(runs_after > runs_before);
    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM StreamDb.items;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<uint64_t>() == static_cast<uint64_t>(kRowCount));
    }

    {
        auto cur = exec(dispatcher, "INSERT INTO StreamDb.items (id, age) SELECT id, age FROM StreamDb.src_bad;");
        REQUIRE(cur->is_error());
    }
}

TEST_CASE("integration::cpp::streaming_dml::fk_cascade_streams_delete") {
    auto config = test_create_config(integration_fixture_path("test_streaming_dml_cascade"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE StreamDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.parent (id bigint, val text);")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE StreamDb.child (id bigint, parent_id bigint);")->is_success());
    REQUIRE(exec(dispatcher,
                 "ALTER TABLE StreamDb.child ADD CONSTRAINT fk_c "
                 "FOREIGN KEY (parent_id) REFERENCES StreamDb.parent (id) ON DELETE CASCADE;")
                ->is_success());

    REQUIRE(exec(dispatcher, "INSERT INTO StreamDb.parent (id, val) VALUES (1, 'p1'), (2, 'p2');")->is_success());
    {
        std::stringstream q;
        q << "INSERT INTO StreamDb.child (id, parent_id) VALUES ";
        for (unsigned i = 0; i < kRowCount; ++i) {
            q << "(" << i << ", " << ((i % 2) + 1) << ")" << (i + 1 == kRowCount ? ";" : ", ");
        }
        REQUIRE(exec(dispatcher, q.str())->is_success());
    }

    const auto runs_before = services::collection::executor::streaming_pipeline_runs();
    {
        auto cur = exec(dispatcher, "DELETE FROM StreamDb.parent WHERE id = 1;");
        INFO("cascade delete error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    const auto runs_after = services::collection::executor::streaming_pipeline_runs();
    REQUIRE(runs_after > runs_before);

    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM StreamDb.child WHERE parent_id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<uint64_t>() == 0);
    }
    {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM StreamDb.child WHERE parent_id = 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<uint64_t>() == static_cast<uint64_t>(kRowCount / 2));
    }
}

TEST_CASE("integration::cpp::streaming_dml::dml_limit_bounds_affected_rows") {
    // LIMIT n bounds affected/matched rows (MySQL/SQLite semantics): the cap sits on the
    // disk scan if pushable, on operator_match otherwise, or on the semi-join's own bound.
    auto config = test_create_config(integration_fixture_path("test_streaming_dml_limit"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE LimDb;")->is_success());

    auto seed = [&](const std::string& tbl) {
        REQUIRE(exec(dispatcher, "CREATE TABLE LimDb." + tbl + " (id bigint, a bigint, b bigint);")->is_success());
        std::stringstream q;
        q << "INSERT INTO LimDb." << tbl << " (id, a, b) VALUES ";
        for (int i = 0; i < 10; ++i) {
            q << "(" << i << ", " << (i % 5) << ", 100)" << (i + 1 == 10 ? ";" : ", ");
        }
        REQUIRE(exec(dispatcher, q.str())->is_success());
    };
    auto count = [&](const std::string& tbl) -> int64_t {
        auto cur = exec(dispatcher, "SELECT COUNT(id) AS c FROM LimDb." + tbl + ";");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        return static_cast<int64_t>(cur->value(0, 0).value<uint64_t>());
    };

    // Column-vs-column predicate (a < b) is non-pushable, so this exercises operator_match.
    seed("t_np");
    {
        auto cur = exec(dispatcher, "DELETE FROM LimDb.t_np WHERE a < b LIMIT 2 RETURNING id;");
        INFO("nonpushable delete limit: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(count("t_np") == 8);
    }

    // Pushable predicate (a = 2) exercises the disk scan's post-filter cap.
    seed("t_p");
    {
        auto cur = exec(dispatcher, "DELETE FROM LimDb.t_p WHERE a = 2 LIMIT 1 RETURNING id;");
        INFO("pushable delete limit: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(count("t_p") == 9);
    }

    seed("t_bare");
    {
        auto cur = exec(dispatcher, "DELETE FROM LimDb.t_bare LIMIT 3 RETURNING id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(count("t_bare") == 7);
    }

    seed("t_upd");
    {
        auto cur = exec(dispatcher, "UPDATE LimDb.t_upd SET b = b + 1 WHERE a < b LIMIT 2 RETURNING id;");
        INFO("update limit: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        auto cur2 = exec(dispatcher, "SELECT COUNT(id) AS c FROM LimDb.t_upd WHERE b = 101;");
        REQUIRE(cur2->is_success());
        REQUIRE(cur2->value(0, 0).value<uint64_t>() == 2u);
    }

    // DELETE ... USING ... LIMIT n bounds the semi-join's matched-row count directly.
    seed("t_src");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE LimDb.s (k bigint);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO LimDb.s (k) VALUES (0), (1), (2), (3), (4);")->is_success());
        auto cur =
            exec(dispatcher, "DELETE FROM LimDb.t_src USING LimDb.s WHERE t_src.a = s.k LIMIT 2 RETURNING t_src.id;");
        INFO("source delete limit: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(count("t_src") == 8);
    }

    seed("t_off");
    {
        auto cur = exec(dispatcher, "DELETE FROM LimDb.t_off LIMIT 2 OFFSET 1;");
        REQUIRE(cur->is_error());
    }
}
