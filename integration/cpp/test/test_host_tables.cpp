// First-class host tables: an embedding host (OtterStax, the kafka connector)
// stores ITS OWN metadata in ordinary engine tables and must be able to drive
// them with plain SQL DML — plus read them back after a restart (.otbx + WAL).
//
// The kafka connector's `kafka.__sources` table (8 STRING_LITERAL columns,
// created node-based like KafkaManager::ensure_sources_table) crashed the engine
// when written with a SQL INSERT string (see
// otterstax integration/kafka/detail/kafka_manager_persistence.cpp: "Insert the
// row via node_insert (a SQL INSERT string through kafka_query crashes the
// engine on this table)"). This test pins the exact scenario as a regression.

#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>

using namespace components;

namespace {
    std::vector<components::table::column_definition_t> kafka_sources_columns() {
        using types::complex_logical_type;
        using types::logical_type;
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("name", complex_logical_type(logical_type::STRING_LITERAL));
        cols.emplace_back("kind", complex_logical_type(logical_type::STRING_LITERAL));
        cols.emplace_back("topic", complex_logical_type(logical_type::STRING_LITERAL));
        cols.emplace_back("bootstrap", complex_logical_type(logical_type::STRING_LITERAL));
        cols.emplace_back("group_id", complex_logical_type(logical_type::STRING_LITERAL));
        cols.emplace_back("offset_reset", complex_logical_type(logical_type::STRING_LITERAL));
        cols.emplace_back("transactional", complex_logical_type(logical_type::STRING_LITERAL));
        cols.emplace_back("as_select", complex_logical_type(logical_type::STRING_LITERAL));
        return cols;
    }
} // namespace

TEST_CASE("integration::cpp::host_tables::sql_insert_into_node_created_table") {
    auto config = test_create_config("/tmp/test_host_tables/kafka_repro");
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE kafka;")->is_success());
    }
    {
        // Node-based create, exactly like KafkaManager::ensure_sources_table.
        auto session = otterbrix::session_id_t();
        test_create_collection(dispatcher, session, "kafka", "__sources", kafka_sources_columns());
    }

    {
        // The operation that crashed the engine on a13/b2-rc-1: a SQL INSERT
        // string into the host-created table.
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            "INSERT INTO kafka.__sources (name, kind, topic, bootstrap, group_id, offset_reset, transactional, "
            "as_select) VALUES ('s1', 'source', 'topic-a', 'broker:9092', 'g1', 'earliest', 'false', '');");
        REQUIRE(cur->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM kafka.__sources;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
    {
        // Second row + WHERE round-trip.
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(
            session,
            "INSERT INTO kafka.__sources (name, kind, topic, bootstrap, group_id, offset_reset, transactional, "
            "as_select) VALUES ('st1', 'stream', 'topic-b', 'broker:9092', 'g2', 'latest', 'true', 'SELECT 1');");
        REQUIRE(cur->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT name, kind FROM kafka.__sources WHERE kind = 'stream';");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
    {
        // DELETE by key (the kafka delete_source_meta path, as SQL).
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "DELETE FROM kafka.__sources WHERE name = 's1';");
        REQUIRE(cur->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM kafka.__sources;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
}

TEST_CASE("integration::cpp::host_tables::restart_readback") {
    auto config = test_create_config("/tmp/test_host_tables/restart");
    test_clear_directory(config);

    INFO("phase 1: host DB + metadata table via plain DDL/DML");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE __otterstax;")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "CREATE TABLE __otterstax.meta_connections "
                                               "(uid TEXT, backend_kind TEXT, dsn TEXT);");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session,
                                        "INSERT INTO __otterstax.meta_connections (uid, backend_kind, dsn) VALUES "
                                        "('uid_pg1', 'postgresql', 'host=pg1 port=5432 dbname=db1'), "
                                        "('uid_my1', 'mysql', 'host=my1 port=3306 dbname=db2');");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM __otterstax.meta_connections;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }
    }

    INFO("phase 2: restart — metadata readable with zero re-registration");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM __otterstax.meta_connections;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session,
                                        "SELECT uid FROM __otterstax.meta_connections WHERE backend_kind = 'mysql';");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }
        {
            // The cache-rebuild write path stays usable after restart too.
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_sql(session,
                                        "INSERT INTO __otterstax.meta_connections (uid, backend_kind, dsn) VALUES "
                                        "('uid_ch1', 'clickhouse', 'host=ch1 port=9000 dbname=db3');");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM __otterstax.meta_connections;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }
    }
}
