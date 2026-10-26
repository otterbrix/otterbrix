#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/place.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <string>

namespace components::operators {

    // What a completed [connection.]database.schema is: INVALID_OID from the first missing level.
    struct found_schema_t {
        catalog::oid_t database_oid{catalog::INVALID_OID};
        catalog::oid_t namespace_oid{catalog::INVALID_OID};
    };

    // A database is its pg_database row, keyed by (datconnection, datname): a connection's database is otterbrix's
    // row like a local one. pg_catalog of a local database is main's: there is one.
    actor_zeta::unique_future<core::result_wrapper_t<found_schema_t>>
    lookup_schema(std::pmr::memory_resource* resource, pipeline::context_t* ctx, catalog::place_t name);

    // The column `column` (an oid) of the first row of the catalog table `table_oid` whose `key_columns` are `keys`;
    // INVALID_OID when there is none.
    actor_zeta::unique_future<core::result_wrapper_t<catalog::oid_t>>
    read_oid(std::pmr::memory_resource* resource,
             pipeline::context_t* ctx,
             catalog::oid_t table_oid,
             std::pmr::vector<std::uint64_t> key_columns,
             vector::data_chunk_t keys,
             std::uint64_t column = 0);

    // The oid of the database `name` of `connection` (empty: a local database); INVALID_OID when there is none.
    actor_zeta::unique_future<core::result_wrapper_t<catalog::oid_t>>
    lookup_database_oid(std::pmr::memory_resource* resource,
                        pipeline::context_t* ctx,
                        std::string connection,
                        std::string name);

    // The connection, database and schema names of the schema `namespace_oid`: its pg_namespace row and its
    // database's pg_database row.
    actor_zeta::unique_future<core::result_wrapper_t<catalog::place_t>>
    names_of_namespace(std::pmr::memory_resource* resource, pipeline::context_t* ctx, catalog::oid_t namespace_oid);

    // The oid of the relation `relname` in the schema `namespace_oid`; INVALID_OID when there is none.
    actor_zeta::unique_future<core::result_wrapper_t<catalog::oid_t>>
    lookup_relation_oid(std::pmr::memory_resource* resource,
                        pipeline::context_t* ctx,
                        catalog::oid_t namespace_oid,
                        std::string relname);

} // namespace components::operators
