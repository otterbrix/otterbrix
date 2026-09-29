// Foreign servers and the cache of remote table descriptions (relkind 'f'): catalog rows only — no .otbx,
// no WAL data, no indexes. A name whose first part is a server is remote; every other name resolves as before.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <services/collection/context_storage.hpp>

#include <cstdlib>
#include <filesystem>
#include <map>
#include <set>

using namespace components;

namespace {

    components::cursor::cursor_t_ptr
    run(otterbrix::wrapper_dispatcher_t* dispatcher, const otterbrix::session_id_t& session, const std::string& sql) {
        return dispatcher->execute_sql(session, sql);
    }

    components::cursor::cursor_t_ptr exec(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        return run(dispatcher, otterbrix::session_id_t(), sql);
    }

    std::string error_of(const components::cursor::cursor_t_ptr& cur) {
        return cur->is_error() ? std::string(cur->get_error().what) : std::string{"<ok>"};
    }

    std::size_t rows_of(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto cur = exec(dispatcher, sql);
        INFO(sql << ": " << error_of(cur));
        REQUIRE(cur->is_success());
        return cur->size();
    }

    std::string relkind_of(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& relname) {
        auto cur = exec(dispatcher, "SELECT relkind FROM pg_catalog.pg_class WHERE relname = '" + relname + "';");
        REQUIRE(cur->is_success());
        if (cur->size() == 0) {
            return {};
        }
        auto cell = cur->value(0, 0);
        return std::string(cell.value<std::string_view>());
    }

    std::uint32_t oid_of_server(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& name) {
        auto cur = exec(dispatcher, "SELECT oid FROM pg_catalog.pg_foreign_server WHERE srvname = '" + name + "';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        return cur->value(0, 0).value<std::uint32_t>();
    }

    std::uint32_t oid_of_relation(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& relname) {
        auto cur = exec(dispatcher, "SELECT oid FROM pg_catalog.pg_class WHERE relname = '" + relname + "';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        return cur->value(0, 0).value<std::uint32_t>();
    }

    std::map<std::string, std::string> options_of(otterbrix::wrapper_dispatcher_t* dispatcher,
                                                  std::uint32_t owner_oid) {
        auto cur = exec(dispatcher,
                        "SELECT key, value FROM pg_catalog.pg_foreign_option WHERE owner_oid = " +
                            std::to_string(owner_oid) + ";");
        INFO(error_of(cur));
        REQUIRE(cur->is_success());
        std::map<std::string, std::string> out;
        for (std::size_t row = 0; row < cur->size(); ++row) {
            auto key = cur->value(0, row);
            auto value = cur->value(1, row);
            std::string key_text{key.value<std::string_view>()};
            std::string value_text{value.value<std::string_view>()};
            REQUIRE(out.emplace(std::move(key_text), std::move(value_text)).second);
        }
        return out;
    }

    std::size_t cache_rows(otterbrix::wrapper_dispatcher_t* dispatcher) {
        return rows_of(dispatcher, "SELECT oid FROM pg_catalog.pg_class WHERE relkind = 'f';") +
               rows_of(dispatcher, "SELECT oid FROM pg_catalog.pg_foreign_namespace;") +
               rows_of(dispatcher, "SELECT ftrelid FROM pg_catalog.pg_foreign_table;");
    }

    bool any_user_table_file(const std::filesystem::path& disk_root) {
        auto numeric_oid = [](const std::filesystem::path& dir) -> unsigned long {
            const auto name = dir.filename().string();
            char* end = nullptr;
            const unsigned long oid = std::strtoul(name.c_str(), &end, 10);
            return (end != nullptr && *end == '\0' && !name.empty()) ? oid : 0;
        };
        for (const auto& ns_entry : std::filesystem::directory_iterator(disk_root)) {
            if (!ns_entry.is_directory() || numeric_oid(ns_entry.path()) < catalog::FIRST_USER_OID) {
                continue;
            }
            for (const auto& tbl_entry : std::filesystem::directory_iterator(ns_entry.path())) {
                if (tbl_entry.is_directory() && numeric_oid(tbl_entry.path()) >= catalog::FIRST_USER_OID &&
                    std::filesystem::exists(tbl_entry.path() / "table.otbx")) {
                    return true;
                }
            }
        }
        return false;
    }

    components::cursor::cursor_t_ptr execute_node(otterbrix::wrapper_dispatcher_t* dispatcher,
                                                  const otterbrix::session_id_t& session,
                                                  logical_plan::node_ptr node) {
        auto* resource = dispatcher->resource();
        return dispatcher->execute_plan(
            session,
            logical_plan::execution_plan_t{resource, std::move(node), logical_plan::make_parameter_node(resource)});
    }

    // What federation's describe records: the canonical path inside the server and the remote columns.
    components::cursor::cursor_t_ptr record(otterbrix::wrapper_dispatcher_t* dispatcher,
                                            const otterbrix::session_id_t& session,
                                            const std::string& server,
                                            std::vector<std::string> path) {
        std::vector<table::column_definition_t> columns;
        columns.emplace_back("id", types::complex_logical_type(types::logical_type::BIGINT));
        columns.emplace_back("name", types::complex_logical_type(types::logical_type::STRING_LITERAL));
        auto node = logical_plan::make_node_record_remote_table(dispatcher->resource(),
                                                                server,
                                                                std::move(path),
                                                                std::move(columns));
        REQUIRE_FALSE(node.has_error());
        return execute_node(dispatcher, session, std::move(node.value()));
    }

    void record_ok(otterbrix::wrapper_dispatcher_t* dispatcher,
                   const std::string& server,
                   std::vector<std::string> path) {
        auto cur = record(dispatcher, otterbrix::session_id_t(), server, std::move(path));
        INFO(error_of(cur));
        REQUIRE(cur->is_success());
    }

    void create_server(otterbrix::wrapper_dispatcher_t* dispatcher) {
        auto server = exec(dispatcher,
                           "CREATE SERVER m2 TYPE 'mysql' OPTIONS (host 'db.local', port '3306', version '8.0');");
        INFO(error_of(server));
        REQUIRE(server->is_success());
    }

    // server.name leaves the schema to be guessed: refused before any cache lookup or connector call.
    void require_full_path_demanded(const components::cursor::cursor_t_ptr& cur, const std::string& sql) {
        INFO(sql << ": " << error_of(cur));
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::invalid_parameter);
        REQUIRE(error_of(cur).find("write the full path as server.schema.table") != std::string::npos);
    }

    void require_no_connector(const components::cursor::cursor_t_ptr& cur, const std::string& sql) {
        INFO(sql << ": " << error_of(cur));
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::connector_not_exists);
        REQUIRE(error_of(cur).find("no connector for server type 'mysql'") != std::string::npos);
    }

    struct written_slots_t {
        std::string uid;
        std::string dbname;
        std::string schema;
        std::string relname;
        bool operator==(const written_slots_t&) const = default;
    };

    struct captured_leaves_t {
        std::vector<written_slots_t> from_slots;
        std::set<catalog::oid_t> table_oids;
        catalog::oid_t server_oid{catalog::INVALID_OID};
        std::string server_type;
        char relkind{0};
    };
    captured_leaves_t& captured() {
        static captured_leaves_t leaves;
        return leaves;
    }

    // Reads what resolve stamped on the FROM leaves of the query, after enrich and before physgen.
    logical_plan::node_ptr capture_leaf_metadata_pass(std::pmr::memory_resource*, logical_plan::node_ptr node) {
        if (!node) {
            return node;
        }
        if (node->type() == logical_plan::node_type::aggregate_t) {
            const auto* agg = static_cast<const logical_plan::node_aggregate_t*>(node.get());
            if (agg->relname().t == "orders") {
                captured().from_slots.push_back({agg->uid().t, agg->dbname().t, agg->schema(), agg->relname().t});
            }
        }
        if (const auto* md = node->table_metadata(); md != nullptr && md->name == "orders") {
            captured().table_oids.insert(md->table_oid);
            captured().server_oid = md->server_oid;
            captured().server_type = md->server_type;
            captured().relkind = md->relkind;
        }
        for (auto& child : node->children()) {
            child = capture_leaf_metadata_pass(nullptr, child);
        }
        return node;
    }

} // namespace

TEST_CASE("integration::cpp::foreign_tables::recorded_description_is_catalog_only") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/record"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_server(dispatcher);
    const auto server_oid = oid_of_server(dispatcher, "m2");

    record_ok(dispatcher, "m2", {"shop", "orders"});

    REQUIRE(relkind_of(dispatcher, "orders") == "f");
    REQUIRE_FALSE(any_user_table_file(config.disk.path));
    const auto table_oid = oid_of_relation(dispatcher, "orders");
    REQUIRE(rows_of(dispatcher,
                    "SELECT attname FROM pg_catalog.pg_attribute WHERE attrelid = " + std::to_string(table_oid) +
                        ";") == 2);
    auto ft = exec(dispatcher,
                   "SELECT ftserver FROM pg_catalog.pg_foreign_table WHERE ftrelid = " + std::to_string(table_oid) +
                       ";");
    REQUIRE(ft->is_success());
    REQUIRE(ft->size() == 1);
    REQUIRE(ft->value(0, 0).value<std::uint32_t>() == server_oid);
    REQUIRE(rows_of(dispatcher,
                    "SELECT oid FROM pg_catalog.pg_foreign_namespace WHERE nspserver = " + std::to_string(server_oid) +
                        " AND nspname = 'shop';") == 1);

    // A second table in the same remote schema shares its namespace; another schema gets its own.
    record_ok(dispatcher, "m2", {"shop", "customers"});
    record_ok(dispatcher, "m2", {"crm", "shop", "orders"});
    REQUIRE(rows_of(dispatcher, "SELECT oid FROM pg_catalog.pg_foreign_namespace;") == 2);
    REQUIRE(rows_of(dispatcher, "SELECT oid FROM pg_catalog.pg_class WHERE relkind = 'f';") == 3);

    auto again = record(dispatcher, otterbrix::session_id_t(), "m2", {"shop", "orders"});
    REQUIRE(again->is_error());
}

TEST_CASE("integration::cpp::foreign_tables::server_and_cache_survive_restart") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/restart"));
    test_clear_directory(config);
    {
        test_spaces space(config);
        create_server(space.dispatcher());
        record_ok(space.dispatcher(), "m2", {"shop", "orders"});
    }
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(relkind_of(dispatcher, "orders") == "f");
    REQUIRE_FALSE(any_user_table_file(config.disk.path));
    const auto server_oid = oid_of_server(dispatcher, "m2");
    const auto table_oid = oid_of_relation(dispatcher, "orders");
    REQUIRE(rows_of(dispatcher,
                    "SELECT attname FROM pg_catalog.pg_attribute WHERE attrelid = " + std::to_string(table_oid) +
                        ";") == 2);
    auto server = exec(dispatcher, "SELECT srvtype FROM pg_catalog.pg_foreign_server WHERE srvname = 'm2';");
    REQUIRE(server->is_success());
    REQUIRE(server->size() == 1);
    {
        auto type = server->value(0, 0);
        REQUIRE(type.value<std::string_view>() == "mysql");
    }
    REQUIRE(options_of(dispatcher, server_oid) ==
            std::map<std::string, std::string>{{"host", "db.local"}, {"port", "3306"}, {"version", "8.0"}});
    auto again = record(dispatcher, otterbrix::session_id_t(), "m2", {"shop", "orders"});
    REQUIRE(again->is_error());
}

TEST_CASE("integration::cpp::foreign_tables::aborted_record_leaves_nothing") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/abort_record"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_server(dispatcher);

    auto session = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());
    auto recorded = record(dispatcher, session, "m2", {"shop", "orders"});
    INFO(error_of(recorded));
    REQUIRE(recorded->is_success());
    REQUIRE(run(dispatcher, session, "ROLLBACK;")->is_success());

    REQUIRE(cache_rows(dispatcher) == 0);
    REQUIRE_FALSE(any_user_table_file(config.disk.path));
    record_ok(dispatcher, "m2", {"shop", "orders"});
}

TEST_CASE("integration::cpp::foreign_tables::aborted_create_server_leaves_no_option_rows") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/abort_options"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto session = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, session, "BEGIN;")->is_success());
    REQUIRE(run(dispatcher, session, "CREATE SERVER m2 TYPE 'mysql' OPTIONS (host 'db.local', q 'a,b=c\\d');")
                ->is_success());
    REQUIRE(run(dispatcher, session, "ROLLBACK;")->is_success());

    REQUIRE(rows_of(dispatcher, "SELECT oid FROM pg_catalog.pg_foreign_server;") == 0);
    REQUIRE(rows_of(dispatcher, "SELECT owner_oid FROM pg_catalog.pg_foreign_option;") == 0);
}

TEST_CASE("integration::cpp::foreign_tables::leaf_metadata_carries_server_oid_and_type") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/leaf_metadata"));
    test_clear_directory(config);
    captured() = captured_leaves_t{};
    test_spaces space(config, &services::planner::no_custom_lowering, &capture_leaf_metadata_pass);
    auto* dispatcher = space.dispatcher();
    create_server(dispatcher);
    record_ok(dispatcher, "m2", {"shop", "orders"});
    const auto server_oid = oid_of_server(dispatcher, "m2");

    const std::string sql = "SELECT id, name FROM m2.shop.orders;";
    require_no_connector(exec(dispatcher, sql), sql);
    REQUIRE(captured().table_oids.size() == 1);
    REQUIRE(captured().relkind == catalog::relkind::foreign);
    REQUIRE(captured().server_oid == server_oid);
    REQUIRE(captured().server_type == "mysql");
}

TEST_CASE("integration::cpp::foreign_tables::same_relname_in_two_remote_schemas_stays_apart") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/two_schemas"));
    test_clear_directory(config);
    captured() = captured_leaves_t{};
    test_spaces space(config, &services::planner::no_custom_lowering, &capture_leaf_metadata_pass);
    auto* dispatcher = space.dispatcher();
    create_server(dispatcher);
    record_ok(dispatcher, "m2", {"sales", "orders"});
    record_ok(dispatcher, "m2", {"archive", "orders"});

    const std::string sql = "SELECT a.id FROM m2.sales.orders AS a JOIN m2.archive.orders AS b ON a.id = b.id;";
    require_no_connector(exec(dispatcher, sql), sql);
    REQUIRE(captured().table_oids.size() == 2);
}

TEST_CASE("integration::cpp::foreign_tables::read_without_a_description_needs_a_connector") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/no_description"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_server(dispatcher);

    for (const std::string sql : {"SELECT * FROM m2.shop.orders;",
                                  "SELECT * FROM m2.crm.shop.orders;",
                                  "SELECT count(*) FROM m2.shop.orders;"}) {
        require_no_connector(exec(dispatcher, sql), sql);
    }
    REQUIRE(cache_rows(dispatcher) == 0);
}

TEST_CASE("integration::cpp::foreign_tables::server_and_one_part_is_not_a_remote_path") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/one_part"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_server(dispatcher);
    record_ok(dispatcher, "m2", {"shop", "orders"});

    for (const std::string sql : {"SELECT * FROM m2.orders;",
                                  "SELECT count(*) FROM m2.orders;",
                                  "INSERT INTO m2.orders (id, name) VALUES (1, 'a');",
                                  "UPDATE m2.orders SET name = 'b' WHERE id = 1;",
                                  "DELETE FROM m2.orders WHERE id = 1;",
                                  "CREATE TABLE m2.t (id BIGINT);",
                                  "CREATE VIEW m2.v AS SELECT 1 AS a;",
                                  "CREATE SEQUENCE m2.s;",
                                  "CREATE TYPE m2.pair AS (a BIGINT, b BIGINT);",
                                  "DROP TABLE m2.orders;",
                                  "DROP VIEW m2.v;",
                                  "CREATE INDEX ft_id ON m2.orders (id);"}) {
        require_full_path_demanded(exec(dispatcher, sql), sql);
    }
    auto cur = exec(dispatcher, "SELECT * FROM m2.orders;");
    REQUIRE(error_of(cur).find("remote table \"m2.orders\"") != std::string::npos);
    // The cached m2.shop.orders is not what a two-part name means.
    REQUIRE(relkind_of(dispatcher, "orders") == "f");
}

// Until a connector for the server type exists, a read of a cached table is refused — never a disk scan
// or a disk reduce of storage the table does not have.
TEST_CASE("integration::cpp::foreign_tables::read_without_connector_is_refused") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/no_connector"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_server(dispatcher);
    record_ok(dispatcher, "m2", {"shop", "orders"});

    for (const std::string sql : {"SELECT id, name FROM m2.shop.orders;",
                                  "SELECT count(*) FROM m2.shop.orders;",
                                  "SELECT id FROM m2.shop.orders WHERE id = 1;",
                                  "SELECT name, count(*) FROM m2.shop.orders GROUP BY name;",
                                  "SELECT id FROM m2.shop.orders ORDER BY id LIMIT 1;"}) {
        auto cur = exec(dispatcher, sql);
        require_no_connector(cur, sql);
        REQUIRE(error_of(cur).find("orders") != std::string::npos);
    }
}

TEST_CASE("integration::cpp::foreign_tables::cache_is_invisible_to_short_names") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/short_names"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_server(dispatcher);
    record_ok(dispatcher, "m2", {"shop", "orders"});

    auto bare = exec(dispatcher, "SELECT * FROM orders;");
    INFO(error_of(bare));
    REQUIRE(bare->is_error());
    REQUIRE(bare->get_error().type == core::error_code_t::table_not_exists);
    auto schema = exec(dispatcher, "SELECT * FROM shop.orders;");
    INFO(error_of(schema));
    REQUIRE(schema->is_error());
    REQUIRE(schema->get_error().type == core::error_code_t::database_not_exists);

    REQUIRE(exec(dispatcher, "CREATE TABLE orders (id BIGINT);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO orders (id) VALUES (7);")->is_success());
    auto local = exec(dispatcher, "SELECT id FROM orders;");
    INFO(error_of(local));
    REQUIRE(local->is_success());
    REQUIRE(local->size() == 1);
    REQUIRE(exec(dispatcher, "SELECT id FROM public.orders;")->size() == 1);
}

TEST_CASE("integration::cpp::foreign_tables::drop_server_drops_its_cache_without_cascade") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/drop_server"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_server(dispatcher);
    record_ok(dispatcher, "m2", {"shop", "orders"});
    record_ok(dispatcher, "m2", {"crm", "shop", "orders"});
    std::vector<std::uint32_t> table_oids;
    {
        auto cur = exec(dispatcher, "SELECT oid FROM pg_catalog.pg_class WHERE relkind = 'f';");
        REQUIRE(cur->size() == 2);
        for (std::size_t row = 0; row < cur->size(); ++row) {
            table_oids.push_back(cur->value(0, row).value<std::uint32_t>());
        }
    }

    auto dropped = exec(dispatcher, "DROP SERVER m2;");
    INFO(error_of(dropped));
    REQUIRE(dropped->is_success());
    REQUIRE(cache_rows(dispatcher) == 0);
    REQUIRE(rows_of(dispatcher, "SELECT oid FROM pg_catalog.pg_foreign_server;") == 0);
    REQUIRE(rows_of(dispatcher, "SELECT owner_oid FROM pg_catalog.pg_foreign_option;") == 0);
    for (const auto oid : table_oids) {
        REQUIRE(rows_of(dispatcher,
                        "SELECT attname FROM pg_catalog.pg_attribute WHERE attrelid = " + std::to_string(oid) + ";") ==
                0);
    }

    create_server(dispatcher);
    record_ok(dispatcher, "m2", {"shop", "orders"});
    auto cascaded = exec(dispatcher, "DROP SERVER m2 CASCADE;");
    INFO(error_of(cascaded));
    REQUIRE(cascaded->is_success());
    REQUIRE(cache_rows(dispatcher) == 0);
}

TEST_CASE("integration::cpp::foreign_tables::open_transaction_keeps_seeing_a_dropped_server") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/drop_while_running"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_server(dispatcher);

    auto reader = otterbrix::session_id_t();
    REQUIRE(run(dispatcher, reader, "BEGIN;")->is_success());
    REQUIRE(run(dispatcher, reader, "SELECT srvname FROM pg_catalog.pg_foreign_server;")->size() == 1);

    auto dropped = exec(dispatcher, "DROP SERVER m2;");
    INFO(error_of(dropped));
    REQUIRE(dropped->is_success());

    REQUIRE(run(dispatcher, reader, "SELECT srvname FROM pg_catalog.pg_foreign_server;")->size() == 1);
    REQUIRE(run(dispatcher, reader, "COMMIT;")->is_success());

    REQUIRE(rows_of(dispatcher, "SELECT srvname FROM pg_catalog.pg_foreign_server;") == 0);
    auto gone = exec(dispatcher, "SELECT * FROM m2.shop.orders;");
    REQUIRE(gone->is_error());
    REQUIRE(gone->get_error().type == core::error_code_t::database_not_exists);
}

TEST_CASE("integration::cpp::foreign_tables::forget_table_and_forget_server_cache") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/forget"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_server(dispatcher);
    const auto server_oid = oid_of_server(dispatcher, "m2");
    record_ok(dispatcher, "m2", {"shop", "orders"});
    record_ok(dispatcher, "m2", {"shop", "customers"});

    {
        auto node = logical_plan::make_node_forget_remote_table(dispatcher->resource(), "m2", {"shop", "orders"});
        REQUIRE_FALSE(node.has_error());
        auto cur = execute_node(dispatcher, otterbrix::session_id_t(), std::move(node.value()));
        INFO(error_of(cur));
        REQUIRE(cur->is_success());
    }
    REQUIRE(relkind_of(dispatcher, "orders").empty());
    REQUIRE(relkind_of(dispatcher, "customers") == "f");

    {
        auto node = logical_plan::make_node_forget_server_cache(dispatcher->resource(), "m2");
        auto cur = execute_node(dispatcher, otterbrix::session_id_t(), std::move(node));
        INFO(error_of(cur));
        REQUIRE(cur->is_success());
    }
    REQUIRE(cache_rows(dispatcher) == 0);
    REQUIRE(oid_of_server(dispatcher, "m2") == server_oid);
    REQUIRE(options_of(dispatcher, server_oid).size() == 3);
    record_ok(dispatcher, "m2", {"shop", "orders"});
}

TEST_CASE("integration::cpp::foreign_tables::server_statement_refusals") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/refusals"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE DATABASE fdb;")->is_success());

    for (const std::string sql : {"CREATE SERVER m2 OPTIONS (host 'h');",
                                  "CREATE SERVER m2 TYPE 'mysql' VERSION '8.0';",
                                  "CREATE SERVER m2 TYPE 'mysql' VERSION '8.0' OPTIONS (host 'h');",
                                  "CREATE SERVER m2 FOREIGN DATA WRAPPER mysql_fdw;",
                                  "CREATE SERVER m2 TYPE 'mysql' FOREIGN DATA WRAPPER mysql_fdw;",
                                  "CREATE FOREIGN TABLE fdb.ft (id BIGINT) SERVER m2;",
                                  "DROP FOREIGN TABLE fdb.ft;"}) {
        auto cur = exec(dispatcher, sql);
        INFO(sql);
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::sql_parse_error);
    }

    REQUIRE(exec(dispatcher, "CREATE SERVER m2 TYPE 'mysql';")->is_success());
    auto duplicate = exec(dispatcher, "CREATE SERVER m2 TYPE 'postgres';");
    REQUIRE(duplicate->is_error());
    REQUIRE(duplicate->get_error().type == core::error_code_t::server_already_exists);

    auto repeated_option = exec(dispatcher, "CREATE SERVER m3 TYPE 'mysql' OPTIONS (host 'a', host 'b');");
    REQUIRE(repeated_option->is_error());

    auto drop_missing = exec(dispatcher, "DROP SERVER missing;");
    REQUIRE(drop_missing->is_error());
    REQUIRE(drop_missing->get_error().type == core::error_code_t::server_not_exists);
    REQUIRE(exec(dispatcher, "DROP SERVER IF EXISTS missing;")->is_success());
}

TEST_CASE("integration::cpp::foreign_tables::server_and_database_names_do_not_collide") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/collisions"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE DATABASE fdb;")->is_success());
    create_server(dispatcher);

    for (const std::string name : {"fdb", "public", "pg_catalog", "information_schema"}) {
        auto cur = exec(dispatcher, "CREATE SERVER " + name + " TYPE 'mysql';");
        INFO(name << ": " << error_of(cur));
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::already_exists);
    }
    auto db = exec(dispatcher, "CREATE DATABASE m2;");
    INFO(error_of(db));
    REQUIRE(db->is_error());
    REQUIRE(db->get_error().type == core::error_code_t::already_exists);
    REQUIRE(rows_of(dispatcher, "SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'm2';") == 0);

    auto pg_prefix = exec(dispatcher, "CREATE SERVER pg_remote TYPE 'mysql';");
    INFO(error_of(pg_prefix));
    REQUIRE(pg_prefix->is_success());
}

TEST_CASE("integration::cpp::foreign_tables::writes_through_a_server_need_a_connector") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/dml"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_server(dispatcher);
    record_ok(dispatcher, "m2", {"shop", "orders"});

    for (const std::string target : {"m2.shop.orders", "m2.crm.shop.orders"}) {
        for (const auto& sql : {"INSERT INTO " + target + " (id, name) VALUES (1, 'a');",
                                      "UPDATE " + target + " SET name = 'b' WHERE id = 1;",
                                      "DELETE FROM " + target + " WHERE id = 1;"}) {
            require_no_connector(exec(dispatcher, sql), sql);
        }
    }
    REQUIRE(relkind_of(dispatcher, "orders") == "f");
}

TEST_CASE("integration::cpp::foreign_tables::ddl_through_a_server_needs_a_connector") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/ddl"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_server(dispatcher);
    record_ok(dispatcher, "m2", {"shop", "orders"});

    for (const std::string sql : {"CREATE TABLE m2.shop.t (id BIGINT);",
                                  "CREATE TABLE m2.crm.shop.t (id BIGINT);",
                                  "ALTER TABLE m2.shop.orders ADD COLUMN z BIGINT;",
                                  "CREATE VIEW m2.shop.v AS SELECT 1 AS a;",
                                  "CREATE SEQUENCE m2.shop.s;",
                                  "CREATE TYPE m2.shop.pair AS (a BIGINT, b BIGINT);",
                                  "DROP TABLE m2.shop.orders;",
                                  "DROP VIEW m2.shop.v;"}) {
        require_no_connector(exec(dispatcher, sql), sql);
    }
    REQUIRE(relkind_of(dispatcher, "orders") == "f");
    REQUIRE(rows_of(dispatcher, "SELECT oid FROM pg_catalog.pg_class WHERE relname = 't';") == 0);
}

TEST_CASE("integration::cpp::foreign_tables::indexes_on_remote_tables_are_refused_by_otterbrix") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/no_index"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_server(dispatcher);
    record_ok(dispatcher, "m2", {"shop", "orders"});

    for (const std::string sql : {"CREATE INDEX ft_id ON m2.shop.orders (id);",
                                  "CREATE INDEX ft_id ON m2.crm.shop.orders (id);",
                                  "DROP INDEX m2.shop.orders.ft_id;"}) {
        auto cur = exec(dispatcher, sql);
        INFO(sql << ": " << error_of(cur));
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::invalid_parameter);
        REQUIRE(error_of(cur).find("remote") != std::string::npos);
    }
    REQUIRE(relkind_of(dispatcher, "ft_id").empty());
}

TEST_CASE("integration::cpp::foreign_tables::functions_of_a_server_are_refused") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/functions"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_server(dispatcher);

    for (const std::string sql : {"SELECT m2.f(1);", "SELECT * FROM m2.f(1);", "SELECT m2.shop.f(1);"}) {
        auto cur = exec(dispatcher, sql);
        INFO(sql << ": " << error_of(cur));
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::unimplemented_yet);
        REQUIRE(error_of(cur).find("functions of a remote server are not supported") != std::string::npos);
    }
}

TEST_CASE("integration::cpp::foreign_tables::names_without_a_server_resolve_as_before") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/unchanged"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_server(dispatcher);
    REQUIRE(exec(dispatcher, "CREATE DATABASE d;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE d.t (id BIGINT);")->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO d.t (id) VALUES (1), (2);")->is_success());

    REQUIRE(rows_of(dispatcher, "SELECT id FROM d.t;") == 2);
    auto read_schema = exec(dispatcher, "SELECT id FROM d.s.t;");
    REQUIRE(read_schema->is_error());
    REQUIRE(read_schema->get_error().type == core::error_code_t::invalid_parameter);
    REQUIRE(rows_of(dispatcher, "WITH m2 AS (SELECT 1 AS a) SELECT a FROM m2;") == 1);

    auto one_part = exec(dispatcher, "SELECT * FROM m2;");
    REQUIRE(one_part->is_error());
    REQUIRE(one_part->get_error().type == core::error_code_t::table_not_exists);
    auto no_database = exec(dispatcher, "SELECT * FROM nodb.t;");
    REQUIRE(no_database->is_error());
    REQUIRE(no_database->get_error().type == core::error_code_t::database_not_exists);
    auto write_schema = exec(dispatcher, "INSERT INTO d.s.t (id) VALUES (3);");
    REQUIRE(write_schema->is_error());
    REQUIRE(write_schema->get_error().type == core::error_code_t::invalid_parameter);
}

TEST_CASE("integration::cpp::foreign_tables::server_names_fold_like_identifiers") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/case"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE SERVER \"M2\" TYPE 'mysql';")->is_success());

    const std::string quoted = "SELECT * FROM \"M2\".shop.orders;";
    require_no_connector(exec(dispatcher, quoted), quoted);
    auto folded = exec(dispatcher, "SELECT * FROM M2.shop.orders;");
    REQUIRE(folded->is_error());
    REQUIRE(folded->get_error().type == core::error_code_t::database_not_exists);
}

// The server is the uid: once the first part is known to be a server, the name is carried as uid = server and the
// remote path in the remaining slots, so downstream only an empty uid means local.
TEST_CASE("integration::cpp::foreign_tables::a_server_name_is_carried_as_the_uid") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/server_uid"));
    test_clear_directory(config);
    captured() = captured_leaves_t{};
    test_spaces space(config, &services::planner::no_custom_lowering, &capture_leaf_metadata_pass);
    auto* dispatcher = space.dispatcher();
    create_server(dispatcher);
    record_ok(dispatcher, "m2", {"shop", "orders"});
    record_ok(dispatcher, "m2", {"crm", "shop", "orders"});

    require_no_connector(exec(dispatcher, "SELECT id FROM m2.shop.orders;"), "m2.shop.orders");
    require_no_connector(exec(dispatcher, "SELECT id FROM m2.crm.shop.orders;"), "m2.crm.shop.orders");
    REQUIRE(captured().from_slots.size() == 2);
    CHECK(captured().from_slots[0] == written_slots_t{"m2", "", "shop", "orders"});
    CHECK(captured().from_slots[1] == written_slots_t{"m2", "crm", "shop", "orders"});
    REQUIRE(captured().table_oids.size() == 2);

    auto recorded = logical_plan::make_node_record_remote_table(dispatcher->resource(), "m2", {"shop", "items"}, {});
    REQUIRE_FALSE(recorded.has_error());
    const auto* create = static_cast<const logical_plan::node_create_collection_t*>(recorded.value().get());
    CHECK(create->uid_slot() == "m2");
    CHECK(create->dbname().empty());
    CHECK(create->schema_slot() == "shop");
    auto forget = logical_plan::make_node_forget_remote_table(dispatcher->resource(), "m2", {"crm", "shop", "orders"});
    REQUIRE_FALSE(forget.has_error());
    const auto* drop = static_cast<const logical_plan::node_drop_t*>(forget.value().get());
    CHECK(drop->uid() == "m2");
    CHECK(drop->dbname() == "crm");
    CHECK(drop->schema() == "shop");
}

// A foreign key cannot point at a remote table, cached or not, whatever the connector (PostgreSQL 18 refuses a
// foreign table as a referenced relation).
TEST_CASE("integration::cpp::foreign_tables::a_foreign_key_to_a_remote_table_is_refused") {
    auto config = test_create_config(integration_fixture_path("test_foreign_tables/fk_remote"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    create_server(dispatcher);
    record_ok(dispatcher, "m2", {"shop", "customers"});
    REQUIRE(exec(dispatcher, "CREATE DATABASE d;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE d.o (cid BIGINT);")->is_success());

    const std::vector<std::pair<std::string, std::string>> cases{
        {"CREATE TABLE d.c1 (cid BIGINT REFERENCES m2.shop.customers (id));", "m2.shop.customers"},
        {"CREATE TABLE d.c2 (cid BIGINT, FOREIGN KEY (cid) REFERENCES m2.crm.shop.customers (id));",
         "m2.crm.shop.customers"},
        {"CREATE TABLE d.c3 (cid BIGINT REFERENCES m2.customers (id));", "m2.customers"},
        {"ALTER TABLE d.o ADD CONSTRAINT fk FOREIGN KEY (cid) REFERENCES m2.shop.customers (id);",
         "m2.shop.customers"},
        {"ALTER TABLE d.o ADD CONSTRAINT fk FOREIGN KEY (cid) REFERENCES m2.shop.missing (id);", "m2.shop.missing"}};
    for (const auto& [sql, target] : cases) {
        auto cur = exec(dispatcher, sql);
        INFO(sql << ": " << error_of(cur));
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::invalid_constraint);
        REQUIRE(error_of(cur).find("referenced relation \"" + target + "\" is not a table") != std::string::npos);
    }
    REQUIRE(rows_of(dispatcher, "SELECT oid FROM pg_catalog.pg_class WHERE relname = 'c1';") == 0);
}
