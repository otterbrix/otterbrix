#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/table_storage.hpp>
#include <components/session/session.hpp>
#include <components/types/types.hpp>
#include <core/pmr.hpp>
#include <core/result_wrapper.hpp>

#include <cstdint>
#include <memory_resource>
#include <span>
#include <string_view>

// What a host plugs in for the databases of its connections. otterbrix keeps them in its own catalog (a pg_database
// row whose datconnection names the connection), resolves the names and plans; a connector gives the operators that
// read, write and change a table of such a database, and otterbrix places them in the ordinary plan. otterbrix keeps
// nothing of the backend's: a host keys what it needs (the remote name, the location) by the table's oid and its
// columns' attoids in its own tables.
namespace components::planner {

    // A statement's session and transaction as a connector sees them; a copy per call.
    struct statement_ctx_t {
        session::session_id_t session;
        std::uint64_t transaction_id;
        // BEGIN ... COMMIT or autocommit off: a ROLLBACK does not undo what a backend wrote (#663).
        bool explicit_transaction;
    };

    // connection.database.schema.table as otterbrix's catalog has it.
    struct remote_name_t {
        std::string_view connection;
        std::string_view database;
        std::string_view schema;
        std::string_view table;
    };

    struct remote_column_t {
        catalog::oid_t attoid{catalog::INVALID_OID};
        std::string_view name;
        types::complex_logical_type type;
        bool not_null{false};
    };

    // A table of a connection's database as the catalog has it: its oid, its name and its columns in attnum order.
    // The views are valid for the call; an operator that keeps them copies them.
    struct remote_table_t {
        catalog::oid_t oid{catalog::INVALID_OID};
        remote_name_t name;
        std::span<const remote_column_t> columns;
    };

    // A schema of a connection's database as the catalog has it: its pg_namespace oid and its full name (no table).
    struct remote_schema_t {
        catalog::oid_t oid{catalog::INVALID_OID};
        remote_name_t name;
    };

    // ALTER TABLE of a connection's table: one column added (its new attoid, a fresh one even for a column dropped
    // earlier under the same name) or dropped (its attoid).
    struct remote_alter_t {
        enum class kind_t : std::uint8_t
        {
            add_column,
            drop_column
        };
        kind_t kind;
        remote_column_t column;
    };

    // One per executor: the executor owns it and is its only caller, so it needs no thread safety and its methods are
    // not const. NVI: the public calls are not virtual. The contract of its operators: docs/embedding-host-api.md,
    // "The connector".
    class connector_t {
    public:
        connector_t(const connector_t&) = delete;
        connector_t& operator=(const connector_t&) = delete;
        virtual ~connector_t() = default;

        // Once per table and statement; otterbrix owns the storage until the statement ends.
        core::result_wrapper_t<logical_plan::table_storage_ptr>
        storage_for(std::pmr::memory_resource* resource, const remote_table_t& table, const statement_ctx_t& context) {
            return storage_for_impl(resource, table, context);
        }

        // Defined with the operators (components/physical_plan/connector.cpp): the result owns an operator.
        logical_plan::storage_operator_t make_create_table(const services::context_storage_t& context,
                                                           const remote_table_t& table);
        logical_plan::storage_operator_t make_drop_table(const services::context_storage_t& context,
                                                         const remote_table_t& table);
        // `table` as this step finds it: the earlier subcommands of the statement applied, this one not yet.
        logical_plan::storage_operator_t make_alter_table(const services::context_storage_t& context,
                                                          const remote_table_t& table,
                                                          const remote_alter_t& alter);
        logical_plan::storage_operator_t make_create_schema(const services::context_storage_t& context,
                                                            const remote_schema_t& schema);
        logical_plan::storage_operator_t make_drop_schema(const services::context_storage_t& context,
                                                          const remote_schema_t& schema);

    protected:
        connector_t() = default;

    private:
        virtual core::result_wrapper_t<logical_plan::table_storage_ptr>
        storage_for_impl(std::pmr::memory_resource* resource,
                         const remote_table_t& table,
                         const statement_ctx_t& context) = 0;
        virtual logical_plan::storage_operator_t make_create_table_impl(const services::context_storage_t& context,
                                                                        const remote_table_t& table) = 0;
        virtual logical_plan::storage_operator_t make_drop_table_impl(const services::context_storage_t& context,
                                                                      const remote_table_t& table) = 0;
        virtual logical_plan::storage_operator_t make_alter_table_impl(const services::context_storage_t& context,
                                                                       const remote_table_t& table,
                                                                       const remote_alter_t& alter) = 0;
        virtual logical_plan::storage_operator_t make_create_schema_impl(const services::context_storage_t& context,
                                                                         const remote_schema_t& schema) = 0;
        virtual logical_plan::storage_operator_t make_drop_schema_impl(const services::context_storage_t& context,
                                                                       const remote_schema_t& schema) = 0;
    };

    // Made with core::pmr::make_polymorphic_unique<the host's connector>(resource, ...).
    using connector_ptr = core::pmr::polymorphic_unique_ptr<connector_t>;

    // The host owns it and keeps it, and whatever it shares between the connectors it makes (a connection pool),
    // alive longer than the engine. open_engine calls make once per executor.
    class connector_factory_t {
    public:
        connector_factory_t(const connector_factory_t&) = delete;
        connector_factory_t& operator=(const connector_factory_t&) = delete;
        virtual ~connector_factory_t() = default;

        core::result_wrapper_t<connector_ptr> make(std::pmr::memory_resource* resource) const {
            return make_impl(resource);
        }

    protected:
        connector_factory_t() = default;

    private:
        virtual core::result_wrapper_t<connector_ptr> make_impl(std::pmr::memory_resource* resource) const = 0;
    };

} // namespace components::planner
