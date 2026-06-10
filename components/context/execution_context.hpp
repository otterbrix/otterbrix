#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/session/session.hpp>
#include <components/table/row_version_manager.hpp>
#include <core/date/date_types.hpp>

namespace components {

    // oid-only routing. cfn lives only at parser/wrapper-API
    // boundary; it is not a routing identity. The execution_context_t passed
    // through every disk/index actor call carries the resolved table_oid (or
    // INVALID_OID for ops that don't target a single table — bulk pg_catalog
    // commit batches, vacuum, register_udf, DDL operators that resolve their
    // own oid from the node).
    //
    // database_oid is populated by catalog_resolve_database_t /
    // operator_resolve_database_t. WAL routes by this oid — until
    // catalog_resolve_database fires, callers may still pass main_database
    // explicitly (operator_insert/delete/update do so today). INVALID_OID is
    // also tolerated and routed to main_database by manager_wal_replicate's
    // get_or_create_worker.
    struct execution_context_t {
        session::session_id_t session;
        table::transaction_data txn{0, 0};
        core::date::timezone_offset_t session_tz{};
        catalog::oid_t table_oid{catalog::INVALID_OID};
        catalog::oid_t database_oid{catalog::INVALID_OID};
    };

} // namespace components
