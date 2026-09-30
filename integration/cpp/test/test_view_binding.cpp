// Views bind early (PostgreSQL 18 view.c DefineView): CREATE VIEW runs the body through the canonical path and
// refuses a broken body, records its output columns, what every body name was bound to (pg_rewrite_ref) and its
// dependencies (pg_depend); a read takes the relations by the recorded oids and never looks a body name up again.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/compute/function.hpp>
#include <services/disk/manager_disk.hpp>

#include <limits>
#include <set>
#include <string>
#include <thread>

using namespace test_helpers;
using components::cursor::cursor_t_ptr;

namespace {

    namespace catalog = components::catalog;

    class view_space_t final : public otterbrix::base_otterbrix_t {
    public:
        explicit view_space_t(const configuration::config& config)
            : otterbrix::base_otterbrix_t(test_open_engine(config)) {}

        actor_zeta::address_t disk_address() const noexcept { return engine().disk_address(); }
    };

    configuration::config config_for(const char* leaf) {
        auto config = make_test_config(integration_fixture_path(std::string("test_view_binding/") + leaf));
        config.log.level = log_t::level::off;
        return config;
    }

    std::string error_text(const cursor_t_ptr& cur) {
        return cur->is_error() ? std::string{cur->get_error().what} : std::string{};
    }

    cursor_t_ptr run_ok(otterbrix::wrapper_dispatcher_t* d, const std::string& sql) {
        auto cur = exec(d, sql);
        INFO("statement: " << sql);
        INFO("error: " << error_text(cur));
        REQUIRE(cur->is_success());
        return cur;
    }

    catalog::oid_t oid_of(otterbrix::wrapper_dispatcher_t* d, const std::string& relname) {
        auto cur = run_ok(d, "SELECT oid FROM pg_catalog.pg_class WHERE relname = '" + relname + "';");
        REQUIRE(cur->size() == 1);
        return static_cast<catalog::oid_t>(cur->chunks().front().get_value<std::uint32_t>(0, 0));
    }

    std::set<std::int64_t> bigints(const cursor_t_ptr& cur) {
        std::set<std::int64_t> out;
        for (const auto& chunk : cur->chunks()) {
            for (std::uint64_t r = 0; r < chunk.size(); ++r) {
                out.insert(chunk.get_value<std::int64_t>(0, r));
            }
        }
        return out;
    }

    std::set<std::string> strings(const cursor_t_ptr& cur) {
        std::set<std::string> out;
        for (const auto& chunk : cur->chunks()) {
            for (std::uint64_t r = 0; r < chunk.size(); ++r) {
                out.insert(std::string{chunk.get_value<std::string_view>(0, r)});
            }
        }
        return out;
    }

    std::set<std::string> column_names(const cursor_t_ptr& cur) {
        std::set<std::string> out;
        if (!cur->chunks().empty()) {
            for (const auto& column : cur->chunks().front().data) {
                out.insert(column.type().alias());
            }
        }
        return out;
    }

    template<typename Future>
    void spin_until_ready(Future& fut) {
        for (int i = 0; i < 2000000 && !fut.is_ready(); ++i) {
            std::this_thread::yield();
        }
        REQUIRE(fut.is_ready());
    }

    // No statement can take a view's dependencies away; this does, so the relation under it can go.
    void forget_dependencies(view_space_t& space, catalog::oid_t view_oid) {
        auto* resource = space.dispatcher()->resource();
        components::table::transaction_data td{0, 0};
        td.snapshot_horizon = std::numeric_limits<uint64_t>::max();
        components::execution_context_t ctx{otterbrix::session_id_t{}, td, {}};
        std::pmr::vector<services::disk::pg_catalog_delete_spec_t> specs{resource};
        specs.push_back({catalog::well_known_oid::pg_depend_table, std::int64_t{1}, view_oid});
        auto [_, fut] = actor_zeta::otterbrix::send(space.disk_address(),
                                                    &services::disk::manager_disk_t::delete_pg_catalog_rows_many,
                                                    ctx,
                                                    std::move(specs));
        spin_until_ready(fut);
        auto deleted = std::move(fut).take_ready();
        REQUIRE_FALSE(deleted.has_error());
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

    std::unique_ptr<components::compute::vector_function> make_twice(std::pmr::memory_resource* resource) {
        using namespace components::compute;
        function_doc doc{"twice", "twice", {"arg"}, false};
        auto fn = std::make_unique<vector_function>("twice", arity::unary(), doc, 1);
        kernel_signature_t sig(function_type_t::vector,
                               {parameter_type::exact(components::types::logical_type::BIGINT)},
                               {output_type::fixed(components::types::logical_type::BIGINT)});
        vector_kernel k{std::move(sig), twice_exec};
        REQUIRE_FALSE(fn->add_kernel(resource, std::move(k)).contains_error());
        return fn;
    }

    void seed(otterbrix::wrapper_dispatcher_t* d) {
        run_ok(d, "CREATE DATABASE vb;");
        run_ok(d, "CREATE TABLE vb.t (a BIGINT, b STRING);");
        run_ok(d, "INSERT INTO vb.t (a, b) VALUES (1, 'x'), (2, 'y');");
    }

} // namespace

TEST_CASE("integration::cpp::view_binding::create_view_refuses_a_missing_relation") {
    test_spaces space(config_for("missing_relation"));
    auto* d = space.dispatcher();
    seed(d);

    auto refused = exec(d, "CREATE VIEW vb.v AS SELECT * FROM vb.missing;");
    INFO("error: " << error_text(refused));
    CHECK_FALSE(refused->is_success());
    CHECK(run_ok(d, "SELECT relname FROM pg_catalog.pg_class WHERE relname = 'v';")->size() == 0);
}

TEST_CASE("integration::cpp::view_binding::create_view_refuses_a_missing_column") {
    test_spaces space(config_for("missing_column"));
    auto* d = space.dispatcher();
    seed(d);

    auto refused = exec(d, "CREATE VIEW vb.v AS SELECT nosuch FROM vb.t;");
    INFO("error: " << error_text(refused));
    CHECK_FALSE(refused->is_success());
    CHECK(run_ok(d, "SELECT relname FROM pg_catalog.pg_class WHERE relname = 'v';")->size() == 0);
}

TEST_CASE("integration::cpp::view_binding::create_view_refuses_a_duplicate_column_name") {
    test_spaces space(config_for("duplicate_column"));
    auto* d = space.dispatcher();
    seed(d);

    auto refused = exec(d, "CREATE VIEW vb.v AS SELECT x.a, y.a FROM vb.t x JOIN vb.t y ON x.a = y.a;");
    INFO("error: " << error_text(refused));
    CHECK(error_text(refused).find("column \"a\" specified more than once") != std::string::npos);
}

TEST_CASE("integration::cpp::view_binding::the_output_columns_are_the_views_attributes") {
    test_spaces space(config_for("attributes"));
    auto* d = space.dispatcher();
    seed(d);
    run_ok(d, "CREATE VIEW vb.v AS SELECT b, a FROM vb.t;");

    const auto view_oid = oid_of(d, "v");
    auto names =
        run_ok(d, "SELECT attname FROM pg_catalog.pg_attribute WHERE attrelid = " + std::to_string(view_oid) + ";");
    CHECK(strings(names) == std::set<std::string>{"a", "b"});
}

TEST_CASE("integration::cpp::view_binding::an_unqualified_body_name_stays_on_the_relation_it_was_bound_to") {
    test_spaces space(config_for("unqualified"));
    auto* d = space.dispatcher();
    run_ok(d, "CREATE DATABASE db1;");
    run_ok(d, "CREATE TABLE db1.t (a BIGINT);");
    run_ok(d, "INSERT INTO db1.t (a) VALUES (1);");
    run_ok(d, "CREATE VIEW db1.v AS SELECT a FROM t;");
    run_ok(d, "CREATE DATABASE db2;");
    run_ok(d, "CREATE TABLE db2.t (a BIGINT);");
    run_ok(d, "INSERT INTO db2.t (a) VALUES (2);");

    auto read = run_ok(d, "SELECT a FROM db1.v;");
    CHECK(bigints(read) == std::set<std::int64_t>{1});
}

TEST_CASE("integration::cpp::view_binding::a_star_view_keeps_its_columns_after_add_column") {
    test_spaces space(config_for("star_add_column"));
    auto* d = space.dispatcher();
    seed(d);
    run_ok(d, "CREATE VIEW vb.v AS SELECT * FROM vb.t;");
    run_ok(d, "ALTER TABLE vb.t ADD COLUMN c BIGINT;");

    auto read = run_ok(d, "SELECT * FROM vb.v;");
    CHECK(read->size() == 2);
    CHECK(column_names(read) == std::set<std::string>{"a", "b"});
}

TEST_CASE("integration::cpp::view_binding::the_view_depends_on_its_relation_and_the_columns_it_reads") {
    test_spaces space(config_for("dependencies"));
    auto* d = space.dispatcher();
    seed(d);
    run_ok(d, "ALTER TABLE vb.t ADD COLUMN c BIGINT;");
    run_ok(d, "CREATE VIEW vb.v AS SELECT a FROM vb.t WHERE b = 'x';");

    const auto view_oid = std::to_string(oid_of(d, "v"));
    const auto table_oid = std::to_string(oid_of(d, "t"));
    auto on_table = run_ok(d,
                           "SELECT deptype FROM pg_catalog.pg_depend WHERE objid = " + view_oid +
                               " AND refobjid = " + table_oid + ";");
    CHECK(strings(on_table) == std::set<std::string>{"n"});
    std::set<std::string> read_columns;
    for (const auto* column : {"a", "b", "c"}) {
        auto attoid = run_ok(d,
                             "SELECT attoid FROM pg_catalog.pg_attribute WHERE attrelid = " + table_oid +
                                 " AND attname = '" + column + "';");
        REQUIRE(attoid->size() == 1);
        const auto oid = std::to_string(attoid->chunks().front().get_value<std::uint32_t>(0, 0));
        if (run_ok(d,
                   "SELECT deptype FROM pg_catalog.pg_depend WHERE objid = " + view_oid + " AND refobjid = " + oid +
                       ";")
                ->size() == 1) {
            read_columns.insert(column);
        }
    }
    CHECK(read_columns == std::set<std::string>{"a", "b"});
}

TEST_CASE("integration::cpp::view_binding::drop_table_under_a_view_is_refused") {
    test_spaces space(config_for("drop_table_restrict"));
    auto* d = space.dispatcher();
    seed(d);
    run_ok(d, "CREATE VIEW vb.v AS SELECT a FROM vb.t;");

    auto refused = exec(d, "DROP TABLE vb.t;");
    INFO("error: " << error_text(refused));
    CHECK(error_text(refused).find("cannot drop table vb.t because other objects depend on it") != std::string::npos);
    CHECK(error_text(refused).find(std::to_string(oid_of(d, "v"))) != std::string::npos);
    CHECK(run_ok(d, "SELECT a FROM vb.v;")->size() == 2);
}

TEST_CASE("integration::cpp::view_binding::drop_table_cascade_drops_the_views_over_it") {
    test_spaces space(config_for("drop_table_cascade"));
    auto* d = space.dispatcher();
    seed(d);
    run_ok(d, "CREATE VIEW vb.v AS SELECT a FROM vb.t;");
    run_ok(d, "CREATE VIEW vb.w AS SELECT a FROM vb.v;");
    const auto v_oid = std::to_string(oid_of(d, "v"));
    const auto w_oid = std::to_string(oid_of(d, "w"));

    run_ok(d, "DROP TABLE vb.t CASCADE;");

    CHECK_FALSE(exec(d, "SELECT a FROM vb.v;")->is_success());
    CHECK_FALSE(exec(d, "SELECT a FROM vb.w;")->is_success());
    for (const auto& oid : {v_oid, w_oid}) {
        CHECK(run_ok(d, "SELECT oid FROM pg_catalog.pg_rewrite WHERE ev_class = " + oid + ";")->size() == 0);
        CHECK(run_ok(d, "SELECT relname FROM pg_catalog.pg_rewrite_ref WHERE ev_class = " + oid + ";")->size() == 0);
        CHECK(run_ok(d, "SELECT attname FROM pg_catalog.pg_attribute WHERE attrelid = " + oid + ";")->size() == 0);
    }
}

TEST_CASE("integration::cpp::view_binding::drop_view_under_a_view_is_refused") {
    test_spaces space(config_for("drop_view_restrict"));
    auto* d = space.dispatcher();
    seed(d);
    run_ok(d, "CREATE VIEW vb.v AS SELECT a FROM vb.t;");
    run_ok(d, "CREATE VIEW vb.w AS SELECT a FROM vb.v;");

    auto refused = exec(d, "DROP VIEW vb.v;");
    INFO("error: " << error_text(refused));
    CHECK(error_text(refused).find("cannot drop view vb.v because other objects depend on it") != std::string::npos);
    CHECK(run_ok(d, "SELECT a FROM vb.w;")->size() == 2);
}

TEST_CASE("integration::cpp::view_binding::drop_column_the_view_reads_is_refused") {
    test_spaces space(config_for("drop_column_restrict"));
    auto* d = space.dispatcher();
    seed(d);
    run_ok(d, "CREATE VIEW vb.v AS SELECT a FROM vb.t;");

    auto refused = exec(d, "ALTER TABLE vb.t DROP COLUMN a;");
    INFO("error: " << error_text(refused));
    CHECK_FALSE(refused->is_success());
    CHECK(run_ok(d, "SELECT a FROM vb.v;")->size() == 2);
}

TEST_CASE("integration::cpp::view_binding::drop_column_the_view_does_not_read_is_allowed") {
    test_spaces space(config_for("drop_column_unused"));
    auto* d = space.dispatcher();
    seed(d);
    run_ok(d, "CREATE VIEW vb.v AS SELECT a FROM vb.t;");

    run_ok(d, "ALTER TABLE vb.t DROP COLUMN b;");
    CHECK(bigints(run_ok(d, "SELECT a FROM vb.v;")) == std::set<std::int64_t>{1, 2});
}

TEST_CASE("integration::cpp::view_binding::drop_type_the_view_uses_is_refused") {
    test_spaces space(config_for("drop_type"));
    auto* d = space.dispatcher();
    seed(d);
    run_ok(d, "CREATE TYPE mood AS ENUM ('x', 'y');");
    run_ok(d, "CREATE VIEW vb.v AS SELECT a, CAST('x' AS mood) AS m FROM vb.t;");

    auto refused = exec(d, "DROP TYPE mood;");
    INFO("error: " << error_text(refused));
    CHECK(error_text(refused).find("cannot drop type mood because other objects depend on it") != std::string::npos);
    CHECK(run_ok(d, "SELECT a FROM vb.v;")->size() == 2);
}

TEST_CASE("integration::cpp::view_binding::unregister_udf_the_view_calls_is_refused") {
    test_spaces space(config_for("unregister_udf"));
    auto* d = space.dispatcher();
    seed(d);
    REQUIRE_FALSE(d->register_udf(otterbrix::session_id_t(), make_twice(d->resource())).contains_error());
    run_ok(d, "CREATE VIEW vb.v AS SELECT twice(a) AS t2 FROM vb.t;");

    auto refused =
        d->unregister_udf(otterbrix::session_id_t(), "twice", {components::types::logical_type::BIGINT});
    INFO("error: " << refused.what);
    CHECK(refused.contains_error());
    CHECK(bigints(run_ok(d, "SELECT t2 FROM vb.v;")) == std::set<std::int64_t>{2, 4});
}

TEST_CASE("integration::cpp::view_binding::unregister_udf_cascade_drops_the_view_that_calls_it") {
    test_spaces space(config_for("unregister_udf_cascade"));
    auto* d = space.dispatcher();
    seed(d);
    REQUIRE_FALSE(d->register_udf(otterbrix::session_id_t(), make_twice(d->resource())).contains_error());
    run_ok(d, "CREATE VIEW vb.v AS SELECT twice(a) AS t2 FROM vb.t;");

    auto dropped = d->unregister_udf(otterbrix::session_id_t(),
                                     "twice",
                                     {components::types::logical_type::BIGINT},
                                     components::catalog::drop_behavior_t::cascade_);
    INFO("error: " << dropped.what);
    CHECK_FALSE(dropped.contains_error());
    CHECK(run_ok(d, "SELECT relname FROM pg_catalog.pg_class WHERE relname = 'v';")->size() == 0);
    CHECK(run_ok(d, "SELECT a FROM vb.t;")->size() == 2);
}

// The relation a view was bound to is gone and a new one took its name: the view does not read the new one.
TEST_CASE("integration::cpp::view_binding::a_view_whose_relation_is_gone_is_stale") {
    view_space_t space(config_for("stale_relation"));
    auto* d = space.dispatcher();
    seed(d);
    run_ok(d, "CREATE VIEW vb.v AS SELECT a FROM vb.t;");
    forget_dependencies(space, oid_of(d, "v"));
    run_ok(d, "DROP TABLE vb.t;");
    run_ok(d, "CREATE TABLE vb.t (a BIGINT, b STRING);");
    run_ok(d, "INSERT INTO vb.t (a, b) VALUES (7, 'z');");

    auto stale = exec(d, "SELECT a FROM vb.v;");
    INFO("error: " << error_text(stale));
    CHECK(error_text(stale).find("view \"v\" is stale") != std::string::npos);
}

// The body text names the column, so a rename would leave the view reading a name that is gone (until #668).
TEST_CASE("integration::cpp::view_binding::rename_of_a_column_the_view_reads_is_refused") {
    test_spaces space(config_for("rename_used"));
    auto* d = space.dispatcher();
    seed(d);
    run_ok(d, "CREATE VIEW vb.v AS SELECT a FROM vb.t;");

    auto refused = exec(d, "ALTER TABLE vb.t RENAME COLUMN a TO z;");
    INFO("error: " << error_text(refused));
    CHECK(error_text(refused).find("cannot rename column \"a\"") != std::string::npos);
    CHECK(error_text(refused).find(std::to_string(oid_of(d, "v"))) != std::string::npos);
    CHECK(bigints(run_ok(d, "SELECT a FROM vb.v;")) == std::set<std::int64_t>{1, 2});
}

TEST_CASE("integration::cpp::view_binding::rename_of_a_column_the_view_does_not_read_is_allowed") {
    test_spaces space(config_for("rename_unused"));
    auto* d = space.dispatcher();
    seed(d);
    run_ok(d, "CREATE VIEW vb.v AS SELECT a FROM vb.t;");

    run_ok(d, "ALTER TABLE vb.t RENAME COLUMN b TO z;");
    CHECK(bigints(run_ok(d, "SELECT a FROM vb.v;")) == std::set<std::int64_t>{1, 2});
}

TEST_CASE("integration::cpp::view_binding::a_column_alias_names_the_view_column") {
    test_spaces space(config_for("column_alias"));
    auto* d = space.dispatcher();
    seed(d);
    run_ok(d, "CREATE VIEW vb.v AS SELECT a AS z FROM vb.t;");

    const auto view_oid = std::to_string(oid_of(d, "v"));
    CHECK(strings(run_ok(d, "SELECT attname FROM pg_catalog.pg_attribute WHERE attrelid = " + view_oid + ";")) ==
          std::set<std::string>{"z"});
    CHECK(bigints(run_ok(d, "SELECT z FROM vb.v;")) == std::set<std::int64_t>{1, 2});
}

// CREATE OR REPLACE VIEW, PostgreSQL 18 view.c: the oid stays, so views over it keep working; columns may only be
// appended (checkViewColumns).
TEST_CASE("integration::cpp::view_binding::or_replace_keeps_the_oid_and_the_views_over_it") {
    test_spaces space(config_for("replace_keeps_oid"));
    auto* d = space.dispatcher();
    seed(d);
    run_ok(d, "CREATE VIEW vb.v AS SELECT a FROM vb.t WHERE a = 1;");
    run_ok(d, "CREATE VIEW vb.w AS SELECT a FROM vb.v;");
    const auto before = oid_of(d, "v");

    run_ok(d, "CREATE OR REPLACE VIEW vb.v AS SELECT a, b FROM vb.t;");

    CHECK(oid_of(d, "v") == before);
    CHECK(bigints(run_ok(d, "SELECT a FROM vb.w;")) == std::set<std::int64_t>{1, 2});
    CHECK(strings(run_ok(d, "SELECT b FROM vb.v;")) == std::set<std::string>{"x", "y"});
}

TEST_CASE("integration::cpp::view_binding::or_replace_creates_a_missing_view") {
    test_spaces space(config_for("replace_creates"));
    auto* d = space.dispatcher();
    seed(d);

    run_ok(d, "CREATE OR REPLACE VIEW vb.v AS SELECT a FROM vb.t;");
    CHECK(bigints(run_ok(d, "SELECT a FROM vb.v;")) == std::set<std::int64_t>{1, 2});
}

TEST_CASE("integration::cpp::view_binding::or_replace_refuses_to_drop_a_column") {
    test_spaces space(config_for("replace_drop_column"));
    auto* d = space.dispatcher();
    seed(d);
    run_ok(d, "CREATE VIEW vb.v AS SELECT a, b FROM vb.t;");

    auto refused = exec(d, "CREATE OR REPLACE VIEW vb.v AS SELECT a FROM vb.t;");
    INFO("error: " << error_text(refused));
    CHECK(error_text(refused).find("cannot drop columns from view") != std::string::npos);
    CHECK(column_names(run_ok(d, "SELECT * FROM vb.v;")) == std::set<std::string>{"a", "b"});
}

TEST_CASE("integration::cpp::view_binding::or_replace_refuses_to_rename_a_column") {
    test_spaces space(config_for("replace_rename_column"));
    auto* d = space.dispatcher();
    seed(d);
    run_ok(d, "CREATE VIEW vb.v AS SELECT a FROM vb.t;");

    auto refused = exec(d, "CREATE OR REPLACE VIEW vb.v AS SELECT a AS z FROM vb.t;");
    INFO("error: " << error_text(refused));
    CHECK(error_text(refused).find("cannot change name of view column \"a\" to \"z\"") != std::string::npos);
}

TEST_CASE("integration::cpp::view_binding::or_replace_refuses_to_change_a_column_type") {
    test_spaces space(config_for("replace_retype_column"));
    auto* d = space.dispatcher();
    seed(d);
    run_ok(d, "CREATE VIEW vb.v AS SELECT a FROM vb.t;");

    auto refused = exec(d, "CREATE OR REPLACE VIEW vb.v AS SELECT b AS a FROM vb.t;");
    INFO("error: " << error_text(refused));
    CHECK(error_text(refused).find("cannot change data type of view column \"a\"") != std::string::npos);
}

TEST_CASE("integration::cpp::view_binding::or_replace_of_a_table_is_refused") {
    test_spaces space(config_for("replace_table"));
    auto* d = space.dispatcher();
    seed(d);

    auto refused = exec(d, "CREATE OR REPLACE VIEW vb.t AS SELECT 1 AS one;");
    INFO("error: " << error_text(refused));
    CHECK(error_text(refused).find("\"t\" is not a view") != std::string::npos);
    CHECK(run_ok(d, "SELECT a FROM vb.t;")->size() == 2);
}

TEST_CASE("integration::cpp::view_binding::or_replace_moves_the_dependencies_to_the_new_body") {
    test_spaces space(config_for("replace_dependencies"));
    auto* d = space.dispatcher();
    seed(d);
    run_ok(d, "CREATE TABLE vb.u (a BIGINT);");
    run_ok(d, "CREATE VIEW vb.v AS SELECT a FROM vb.t;");

    run_ok(d, "CREATE OR REPLACE VIEW vb.v AS SELECT a FROM vb.u;");

    run_ok(d, "DROP TABLE vb.t;");
    auto refused = exec(d, "DROP TABLE vb.u;");
    INFO("error: " << error_text(refused));
    CHECK(error_text(refused).find("cannot drop table vb.u because other objects depend on it") != std::string::npos);
}

TEST_CASE("integration::cpp::view_binding::or_replace_that_would_read_itself_is_refused") {
    test_spaces space(config_for("replace_self"));
    auto* d = space.dispatcher();
    seed(d);
    run_ok(d, "CREATE VIEW vb.v AS SELECT a FROM vb.t;");
    run_ok(d, "CREATE VIEW vb.w AS SELECT a FROM vb.v;");

    auto refused = exec(d, "CREATE OR REPLACE VIEW vb.v AS SELECT a FROM vb.w;");
    INFO("error: " << error_text(refused));
    CHECK(error_text(refused).find("view \"v\" would read itself") != std::string::npos);
    CHECK(bigints(run_ok(d, "SELECT a FROM vb.w;")) == std::set<std::int64_t>{1, 2});
}
