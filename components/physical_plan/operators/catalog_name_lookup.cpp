#include "catalog_name_lookup.hpp"

#include <components/catalog/helpers.hpp>
#include <components/context/context.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <services/disk/manager_disk.hpp>

#include <cstdint>
#include <string_view>
#include <utility>

namespace components::operators {
    namespace {
        // The columns `projected` of every row of the catalog table `table_oid` whose `key_columns` are `keys`.
        actor_zeta::unique_future<core::result_wrapper_t<chunks_vector_t>>
        read_keyed(pipeline::context_t* ctx,
                   catalog::oid_t table_oid,
                   std::pmr::vector<std::uint64_t> key_columns,
                   vector::data_chunk_t keys,
                   std::pmr::vector<std::uint64_t> projected) {
            components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
            auto [_r, rf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                        &services::disk::manager_disk_t::read_chunks_by_key,
                                                        exec_ctx,
                                                        table_oid,
                                                        std::move(key_columns),
                                                        std::move(keys),
                                                        std::move(projected));
            co_return co_await std::move(rf);
        }

        // The first row's column 0 of a keyed read as an oid; INVALID_OID for no row.
        catalog::oid_t first_oid(const chunks_vector_t& batches) {
            for (const auto& chunk : batches) {
                if (chunk.size() != 0 && chunk.column_count() >= 1 && !chunk.is_null(0, 0)) {
                    return static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, 0));
                }
            }
            return catalog::INVALID_OID;
        }

        std::uint64_t row_count(const chunks_vector_t& batches) {
            std::uint64_t rows = 0;
            for (const auto& chunk : batches) {
                rows += chunk.size();
            }
            return rows;
        }
    } // namespace

    actor_zeta::unique_future<core::result_wrapper_t<catalog::oid_t>>
    read_oid(std::pmr::memory_resource* resource,
             pipeline::context_t* ctx,
             catalog::oid_t table_oid,
             std::pmr::vector<std::uint64_t> key_columns,
             vector::data_chunk_t keys,
             std::uint64_t column) {
        std::pmr::vector<std::uint64_t> projected(resource);
        projected.emplace_back(column);
        auto batches =
            co_await read_keyed(ctx, table_oid, std::move(key_columns), std::move(keys), std::move(projected));
        if (batches.has_error()) {
            co_return batches.error();
        }
        co_return first_oid(batches.value());
    }

    namespace {
        actor_zeta::unique_future<core::result_wrapper_t<catalog::oid_t>>
        namespace_of_database(std::pmr::memory_resource* resource,
                              pipeline::context_t* ctx,
                              catalog::oid_t database_oid,
                              core::schema_t schema) {
            std::pmr::vector<std::uint64_t> keys(resource);
            keys.emplace_back(catalog::pg_namespace_col::nspname);
            keys.emplace_back(catalog::pg_namespace_col::nspdatabase);
            co_return co_await read_oid(resource,
                                        ctx,
                                        catalog::well_known_oid::pg_namespace_table,
                                        std::move(keys),
                                        make_key_chunk(resource, std::string_view{schema.t}, database_oid));
        }
    } // namespace

    namespace {
        actor_zeta::unique_future<core::result_wrapper_t<catalog::oid_t>>
        database_of(std::pmr::memory_resource* resource,
                    pipeline::context_t* ctx,
                    core::connection_name_t connection,
                    core::dbname_t name) {
            std::pmr::vector<std::uint64_t> keys(resource);
            keys.emplace_back(catalog::pg_database_col::datname);
            keys.emplace_back(catalog::pg_database_col::datconnection);
            co_return co_await read_oid(
                resource,
                ctx,
                catalog::well_known_oid::pg_database_table,
                std::move(keys),
                make_key_chunk(resource, std::string_view{name.t}, std::string_view{connection.t}));
        }

        actor_zeta::unique_future<core::result_wrapper_t<chunks_vector_t>>
        databases_of_connection(std::pmr::memory_resource* resource,
                                pipeline::context_t* ctx,
                                core::connection_name_t connection) {
            std::pmr::vector<std::uint64_t> keys(resource);
            keys.emplace_back(catalog::pg_database_col::datconnection);
            std::pmr::vector<std::uint64_t> projected(resource);
            projected.emplace_back(catalog::pg_database_col::oid);
            projected.emplace_back(catalog::pg_database_col::datname);
            co_return co_await read_keyed(ctx,
                                          catalog::well_known_oid::pg_database_table,
                                          std::move(keys),
                                          make_key_chunk(resource, std::string_view{connection.t}),
                                          std::move(projected));
        }

        // The first row's datname (column 1) of databases_of_connection.
        core::dbname_t first_datname(const chunks_vector_t& batches) {
            for (const auto& chunk : batches) {
                if (chunk.size() != 0) {
                    return core::dbname_t{std::string{chunk.get_value<std::string_view>(1, 0)}};
                }
            }
            return {};
        }

        core::error_t ambiguous(std::pmr::memory_resource* resource, const std::string& what) {
            return core::error_t{core::error_code_t::ambiguous_alias, std::pmr::string{what, resource}};
        }
    } // namespace

    actor_zeta::unique_future<core::result_wrapper_t<found_database_t>>
    lookup_database(std::pmr::memory_resource* resource,
                    pipeline::context_t* ctx,
                    core::connection_name_t connection,
                    core::dbname_t name) {
        if (!connection.t.empty()) {
            auto oid = co_await database_of(resource, ctx, connection, name);
            if (oid.has_error()) {
                co_return oid.error();
            }
            co_return found_database_t{oid.value(), std::move(connection), std::move(name)};
        }
        if (name.t.empty()) {
            co_return found_database_t{};
        }
        if (name.t == catalog::well_known_oid::main_database_name) {
            co_return found_database_t{catalog::well_known_oid::main_database, {}, std::move(name)};
        }
        // Before it is a database's name, a bare name is a connection's.
        core::connection_name_t as_connection{std::string{name.t}};
        auto of_connection = co_await databases_of_connection(resource, ctx, as_connection);
        if (of_connection.has_error()) {
            co_return of_connection.error();
        }
        auto local = co_await database_of(resource, ctx, core::connection_name_t{}, name);
        if (local.has_error()) {
            co_return local.error();
        }
        const auto databases = row_count(of_connection.value());
        if (databases == 0) {
            co_return found_database_t{local.value(), {}, std::move(name)};
        }
        if (local.value() != catalog::INVALID_OID) {
            co_return ambiguous(resource,
                                "database \"" + name.t + "\" is ambiguous: a connection and a local database");
        }
        if (databases > 1) {
            co_return ambiguous(resource, "connection \"" + name.t + "\" has more than one database");
        }
        co_return found_database_t{first_oid(of_connection.value()),
                                   std::move(as_connection),
                                   first_datname(of_connection.value())};
    }

    actor_zeta::unique_future<core::result_wrapper_t<catalog::oid_t>>
    lookup_database_oid(std::pmr::memory_resource* resource,
                        pipeline::context_t* ctx,
                        core::connection_name_t connection,
                        core::dbname_t name) {
        auto found = co_await lookup_database(resource, ctx, std::move(connection), std::move(name));
        if (found.has_error()) {
            co_return found.error();
        }
        co_return found.value().oid;
    }

    actor_zeta::unique_future<core::result_wrapper_t<found_schema_t>>
    lookup_schema(std::pmr::memory_resource* resource, pipeline::context_t* ctx, catalog::place_t name) {
        found_schema_t found;
        auto database = co_await lookup_database(resource, ctx, name.connection, name.database);
        if (database.has_error()) {
            co_return database.error();
        }
        found.database_oid = database.value().oid;
        if (found.database_oid == catalog::INVALID_OID) {
            co_return found;
        }
        found.connection = std::move(database.value().connection);
        found.database = std::move(database.value().name);
        if (found.connection.t.empty() && name.schema.t == "pg_catalog") {
            found.namespace_oid = catalog::well_known_oid::pg_catalog_namespace;
            co_return found;
        }
        // main's schemas made at bootstrap have fixed oids.
        if (found.database_oid == catalog::well_known_oid::main_database && name.schema.t == "public") {
            found.namespace_oid = catalog::well_known_oid::public_namespace;
            co_return found;
        }
        if (found.database_oid == catalog::well_known_oid::main_database && name.schema.t == "information_schema") {
            found.namespace_oid = catalog::well_known_oid::information_schema_namespace;
            co_return found;
        }
        auto schema = co_await namespace_of_database(resource, ctx, found.database_oid, name.schema);
        if (schema.has_error()) {
            co_return schema.error();
        }
        found.namespace_oid = schema.value();
        co_return found;
    }

    actor_zeta::unique_future<core::result_wrapper_t<catalog::oid_t>>
    lookup_relation_oid(std::pmr::memory_resource* resource,
                        pipeline::context_t* ctx,
                        catalog::oid_t namespace_oid,
                        core::relname_t relname) {
        std::pmr::vector<std::uint64_t> keys(resource);
        keys.emplace_back(catalog::pg_class_col::relname);
        keys.emplace_back(catalog::pg_class_col::relnamespace);
        co_return co_await read_oid(
            resource,
            ctx,
            catalog::well_known_oid::pg_class_table,
            std::move(keys),
            make_key_chunk(resource, std::string_view{relname.t}, static_cast<std::uint32_t>(namespace_oid)));
    }

    namespace {
        std::string text_of(const vector::data_chunk_t& chunk, std::uint64_t column) {
            return chunk.is_null(column, 0) ? std::string{} : std::string{chunk.get_value<std::string_view>(column, 0)};
        }

    } // namespace

    actor_zeta::unique_future<core::result_wrapper_t<catalog::place_t>>
    names_of_namespace(std::pmr::memory_resource* resource, pipeline::context_t* ctx, catalog::oid_t namespace_oid) {
        catalog::place_t named;
        std::pmr::vector<std::uint64_t> ns_keys(resource);
        ns_keys.emplace_back(catalog::pg_namespace_col::oid);
        std::pmr::vector<std::uint64_t> ns_cols(resource);
        ns_cols.emplace_back(catalog::pg_namespace_col::nspname);
        ns_cols.emplace_back(catalog::pg_namespace_col::nspdatabase);
        auto schema_rows = co_await read_keyed(ctx,
                                               catalog::well_known_oid::pg_namespace_table,
                                               std::move(ns_keys),
                                               make_key_chunk(resource, namespace_oid),
                                               std::move(ns_cols));
        if (schema_rows.has_error()) {
            co_return schema_rows.error();
        }
        auto database_oid = catalog::INVALID_OID;
        for (const auto& chunk : schema_rows.value()) {
            if (chunk.size() != 0 && !chunk.is_null(1, 0)) {
                named.schema = text_of(chunk, 0);
                database_oid = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(1, 0));
                break;
            }
        }
        if (database_oid == catalog::well_known_oid::main_database) {
            named.database = catalog::well_known_oid::main_database_name;
            co_return named;
        }
        std::pmr::vector<std::uint64_t> db_keys(resource);
        db_keys.emplace_back(catalog::pg_database_col::oid);
        std::pmr::vector<std::uint64_t> db_cols(resource);
        db_cols.emplace_back(catalog::pg_database_col::datname);
        db_cols.emplace_back(catalog::pg_database_col::datconnection);
        auto database_rows = co_await read_keyed(ctx,
                                                 catalog::well_known_oid::pg_database_table,
                                                 std::move(db_keys),
                                                 make_key_chunk(resource, database_oid),
                                                 std::move(db_cols));
        if (database_rows.has_error()) {
            co_return database_rows.error();
        }
        for (const auto& chunk : database_rows.value()) {
            if (chunk.size() != 0) {
                named.database = text_of(chunk, 0);
                named.connection = text_of(chunk, 1);
                break;
            }
        }
        co_return named;
    }

} // namespace components::operators
