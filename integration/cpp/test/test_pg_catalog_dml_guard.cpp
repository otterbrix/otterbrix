#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_sequence.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/transformer/utils.hpp>

#include <filesystem>
#include <string>
#include <utility>
#include <vector>

// SQL DML must never reach a pg_catalog table: pg_class IS the list of relations, so a landed
// `DELETE FROM pg_class` leaves the storage file on disk while the row naming it is gone.
//
// These cases pin the OUTCOME (an error cursor), not today's refusal message, which is only a
// side effect of pg_class being unaddressable and would go stale once a read fix lands.

using namespace components;

namespace {

    constexpr std::string_view kUserDb = "guarddb";
    constexpr std::string_view kUserTable = "guarddb.alpha";

    void seed_user_table(otterbrix::wrapper_dispatcher_t* dispatcher) {
        REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE guarddb;")->is_success());
        REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE guarddb.alpha (id BIGINT, name STRING);")->is_success());
        REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO guarddb.alpha (id, name) VALUES (1, 'one'), (2, 'two');")
                    ->is_success());
    }

    // Checked through SQL, not the filesystem: a scrubbed pg_class row leaves the .otbx untouched.
    void require_user_table_intact(otterbrix::wrapper_dispatcher_t* dispatcher) {
        auto cursor = test_helpers::exec(dispatcher, "SELECT * FROM guarddb.alpha;");
        REQUIRE(cursor);
        INFO("the user table must survive every refused catalog DML: "
             << (cursor->is_error() ? cursor->get_error().what.c_str() : "<no error>"));
        REQUIRE(cursor->is_success());
        REQUIRE(cursor->size() == 2);
    }

    void require_refused(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto cursor = test_helpers::exec(dispatcher, sql);
        REQUIRE(cursor);
        INFO("[" << sql << "] must be refused, it reached a pg_catalog table");
        REQUIRE(cursor->is_error());
    }

} // namespace

TEST_CASE("integration::cpp::pg_catalog_dml_guard::delete_from_pg_class_cannot_erase_user_tables") {
    auto config = test_helpers::make_test_config(integration_fixture_path("pg_catalog_dml_guard/delete_pg_class"),
                                                 /*wal_on=*/true);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_user_table(dispatcher);

    require_refused(dispatcher, "DELETE FROM pg_catalog.pg_class WHERE relname = 'alpha';");
    require_user_table_intact(dispatcher);

    require_refused(dispatcher, "DELETE FROM pg_class;");
    require_user_table_intact(dispatcher);
}

// pg_attribute is the column list; scrubbing it loses the schema rather than the relation, so
// it needs its own case -- a guard that only covered pg_class would pass the one above.
TEST_CASE("integration::cpp::pg_catalog_dml_guard::delete_from_pg_attribute_cannot_erase_columns") {
    auto config = test_helpers::make_test_config(integration_fixture_path("pg_catalog_dml_guard/delete_pg_attribute"),
                                                 /*wal_on=*/true);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_user_table(dispatcher);

    require_refused(dispatcher, "DELETE FROM pg_catalog.pg_attribute;");
    require_user_table_intact(dispatcher);
}

// UPDATE, not DELETE: renaming a pg_class row leaves the count intact and still detaches the
// table from its name, so a guard written against row counts would miss it.
TEST_CASE("integration::cpp::pg_catalog_dml_guard::update_of_pg_class_cannot_rename_a_user_table") {
    auto config = test_helpers::make_test_config(integration_fixture_path("pg_catalog_dml_guard/update_pg_class"),
                                                 /*wal_on=*/true);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_user_table(dispatcher);

    require_refused(dispatcher, "UPDATE pg_catalog.pg_class SET relname = 'renamed_by_dml';");
    require_user_table_intact(dispatcher);

    require_refused(dispatcher, "UPDATE pg_class SET relname = 'renamed_by_dml';");
    require_user_table_intact(dispatcher);
}

// INSERT mints a relation the engine never created: a pg_class row with no storage behind it,
// and an oid the allocator will hand out again after the next restart reseeds from max+1.
TEST_CASE("integration::cpp::pg_catalog_dml_guard::insert_into_pg_class_cannot_mint_a_relation") {
    auto config = test_helpers::make_test_config(integration_fixture_path("pg_catalog_dml_guard/insert_pg_class"),
                                                 /*wal_on=*/true);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_user_table(dispatcher);

    require_refused(dispatcher,
                    "INSERT INTO pg_catalog.pg_class (oid, relname, relnamespace, relkind, relstoragemode) "
                    "VALUES (999999, 'phantom', 1, 'r', 'd');");
    require_user_table_intact(dispatcher);
}

TEST_CASE("integration::cpp::pg_catalog_dml_guard::ddl_cannot_drop_or_alter_the_catalog") {
    auto config = test_helpers::make_test_config(integration_fixture_path("pg_catalog_dml_guard/ddl_pg_class"),
                                                 /*wal_on=*/true);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_user_table(dispatcher);

    require_refused(dispatcher, "DROP TABLE pg_catalog.pg_class;");
    require_user_table_intact(dispatcher);

    require_refused(dispatcher, "ALTER TABLE pg_catalog.pg_class ADD COLUMN smuggled BIGINT;");
    require_user_table_intact(dispatcher);

    // PostgreSQL refuses to drop pg_catalog because the database system requires it; so does otterbrix.
    require_refused(dispatcher, "DROP DATABASE pg_catalog;");
    require_user_table_intact(dispatcher);
}

// CREATE INDEX neither drops nor alters, so the guards above don't cover it. PostgreSQL refuses
// this unless allow_system_table_mods is set; otterbrix has no such escape hatch.
TEST_CASE("integration::cpp::pg_catalog_dml_guard::create_index_cannot_target_the_catalog") {
    auto config = test_helpers::make_test_config(integration_fixture_path("pg_catalog_dml_guard/index_pg_class"),
                                                 /*wal_on=*/true);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_user_table(dispatcher);

    require_refused(dispatcher, "CREATE INDEX smuggled_idx ON pg_catalog.pg_class (relname);");
    require_user_table_intact(dispatcher);

    require_refused(dispatcher, "CREATE INDEX smuggled_attr_idx ON pg_catalog.pg_attribute (attname);");
    require_user_table_intact(dispatcher);

    // Positive control: the same statement still succeeds against the user table.
    REQUIRE(test_helpers::exec(dispatcher, "CREATE INDEX smuggled_idx ON guarddb.alpha (name);")->is_success());
    require_user_table_intact(dispatcher);
}

// The guard above only fires for a create_index ROOT: only the root's create-index arm checks the
// catalog. A raw plan (built here like test_constraint_entry_lost_target) can nest create_index
// under a sequence_t, whose child arm has no catalog check; this case walks that arm.
TEST_CASE("integration::cpp::pg_catalog_dml_guard::sequence_wrapped_create_index_cannot_reach_the_catalog") {
    auto config = test_helpers::make_test_config(integration_fixture_path("pg_catalog_dml_guard/index_pg_class_seq"),
                                                 /*wal_on=*/true);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_user_table(dispatcher);

    auto* resource = dispatcher->resource();
    auto index_node = logical_plan::make_node_create_index(resource,
                                                           core::indexname_t{std::string{"smuggled_seq_idx"}},
                                                           logical_plan::index_type::single);
    index_node->keys().emplace_back(resource, "relname");
    index_node->set_dbname("pg_catalog");
    index_node->set_relname("pg_class");
    auto sequence =
        boost::intrusive_ptr<logical_plan::node_t>(new logical_plan::node_sequence_t(resource));
    sequence->append_child(index_node);

    logical_plan::execution_plan_t plan{resource, sequence, logical_plan::make_parameter_node(resource)};
    std::vector<std::pair<std::string, std::string>> targets;
    targets.emplace_back("pg_catalog", "pg_class");
    targets.emplace_back("pg_catalog", "smuggled_seq_idx");
    components::sql::transform::register_catalog_resolve_tables(resource, &plan.catalog_resolves, targets);

    auto cursor = dispatcher->execute_plan(otterbrix::session_id_t(), std::move(plan));
    REQUIRE(cursor);
    INFO("a create_index smuggled under a sequence_t must be refused, it targets pg_class: "
         << (cursor->is_error() ? cursor->get_error().what.c_str() : "<accepted>"));
    REQUIRE(cursor->is_error());
    require_user_table_intact(dispatcher);
}
