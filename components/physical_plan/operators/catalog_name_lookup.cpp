#include "catalog_name_lookup.hpp"

#include <components/catalog/helpers.hpp>
#include <components/context/context.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <services/disk/manager_disk.hpp>

#include <cstdint>
#include <string_view>
#include <utility>

namespace components::operators {

    namespace catalog = components::catalog;

    namespace {
        // The first row's column 0 of a keyed read as an oid; INVALID_OID for no row.
        catalog::oid_t first_oid(const std::pmr::vector<vector::data_chunk_t>& batches) {
            for (const auto& chunk : batches) {
                if (chunk.size() != 0 && chunk.column_count() >= 1 && !chunk.is_null(0, 0)) {
                    return static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, 0));
                }
            }
            return catalog::INVALID_OID;
        }
    } // namespace

    actor_zeta::unique_future<core::result_wrapper_t<catalog::oid_t>>
    read_oid(std::pmr::memory_resource* resource,
             pipeline::context_t* ctx,
             catalog::oid_t table_oid,
             std::pmr::vector<std::uint64_t> key_columns,
             vector::data_chunk_t keys,
             std::uint64_t column) {
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
        std::pmr::vector<std::uint64_t> projected(resource);
        projected.emplace_back(column);
        auto [_r, rf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                    &services::disk::manager_disk_t::read_chunks_by_key,
                                                    exec_ctx,
                                                    table_oid,
                                                    std::move(key_columns),
                                                    std::move(keys),
                                                    std::move(projected));
        auto batches = co_await std::move(rf);
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
                              std::string schema) {
            std::pmr::vector<std::uint64_t> keys(resource);
            keys.emplace_back(catalog::pg_namespace_col::nspname);
            keys.emplace_back(catalog::pg_namespace_col::nspdatabase);
            co_return co_await read_oid(resource,
                                        ctx,
                                        catalog::well_known_oid::pg_namespace_table,
                                        std::move(keys),
                                        make_key_chunk(resource, std::string_view{schema}, database_oid));
        }
    } // namespace

    actor_zeta::unique_future<core::result_wrapper_t<catalog::oid_t>>
    lookup_database_oid(std::pmr::memory_resource* resource,
                        pipeline::context_t* ctx,
                        std::string connection,
                        std::string name) {
        if (connection.empty() && name == catalog::well_known_oid::main_database_name) {
            co_return catalog::well_known_oid::main_database;
        }
        std::pmr::vector<std::uint64_t> keys(resource);
        keys.emplace_back(catalog::pg_database_col::datname);
        keys.emplace_back(catalog::pg_database_col::datconnection);
        co_return co_await read_oid(resource,
                                    ctx,
                                    catalog::well_known_oid::pg_database_table,
                                    std::move(keys),
                                    make_key_chunk(resource, std::string_view{name}, std::string_view{connection}));
    }

    actor_zeta::unique_future<core::result_wrapper_t<found_schema_t>>
    lookup_schema(std::pmr::memory_resource* resource, pipeline::context_t* ctx, catalog::place_t name) {
        found_schema_t found;
        auto database = co_await lookup_database_oid(resource, ctx, name.connection, name.database);
        if (database.has_error()) {
            co_return database.error();
        }
        found.database_oid = database.value();
        if (found.database_oid == catalog::INVALID_OID) {
            co_return found;
        }
        if (name.connection.empty() && name.schema == "pg_catalog") {
            found.namespace_oid = catalog::well_known_oid::pg_catalog_namespace;
            co_return found;
        }
        // main's schemas made at bootstrap have fixed oids.
        if (found.database_oid == catalog::well_known_oid::main_database && name.schema == "public") {
            found.namespace_oid = catalog::well_known_oid::public_namespace;
            co_return found;
        }
        if (found.database_oid == catalog::well_known_oid::main_database && name.schema == "information_schema") {
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
                        std::string relname) {
        std::pmr::vector<std::uint64_t> keys(resource);
        keys.emplace_back(catalog::pg_class_col::relname);
        keys.emplace_back(catalog::pg_class_col::relnamespace);
        co_return co_await read_oid(
            resource,
            ctx,
            catalog::well_known_oid::pg_class_table,
            std::move(keys),
            make_key_chunk(resource, std::string_view{relname}, static_cast<std::uint32_t>(namespace_oid)));
    }

    namespace {
        std::string text_of(const vector::data_chunk_t& chunk, std::uint64_t column) {
            return chunk.is_null(column, 0) ? std::string{} : std::string{chunk.get_value<std::string_view>(column, 0)};
        }

    } // namespace

    actor_zeta::unique_future<core::result_wrapper_t<catalog::place_t>>
    names_of_namespace(std::pmr::memory_resource* resource, pipeline::context_t* ctx, catalog::oid_t namespace_oid) {
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
        catalog::place_t named;
        std::pmr::vector<std::uint64_t> ns_keys(resource);
        ns_keys.emplace_back(catalog::pg_namespace_col::oid);
        std::pmr::vector<std::uint64_t> ns_cols(resource);
        ns_cols.emplace_back(catalog::pg_namespace_col::nspname);
        ns_cols.emplace_back(catalog::pg_namespace_col::nspdatabase);
        auto [_ns, nsf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                      &services::disk::manager_disk_t::read_chunks_by_key,
                                                      exec_ctx,
                                                      catalog::well_known_oid::pg_namespace_table,
                                                      std::move(ns_keys),
                                                      make_key_chunk(resource, namespace_oid),
                                                      std::move(ns_cols));
        auto schema_rows = co_await std::move(nsf);
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
        auto [_db, dbf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                      &services::disk::manager_disk_t::read_chunks_by_key,
                                                      exec_ctx,
                                                      catalog::well_known_oid::pg_database_table,
                                                      std::move(db_keys),
                                                      make_key_chunk(resource, database_oid),
                                                      std::move(db_cols));
        auto database_rows = co_await std::move(dbf);
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
