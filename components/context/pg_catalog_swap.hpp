#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <cstdint>

namespace components {

    // Tracks one pg_catalog.* append made under a real txn. Operators record
    // these on the pipeline context; executor returns them via execute_result_t;
    // dispatcher aggregates into transaction_t; commit/abort operators apply
    // storage_commit_appends / storage_revert_appends after txn_manager_.commit().
    struct pg_catalog_append_range_t {
        catalog::oid_t table_oid{catalog::INVALID_OID};
        int64_t start_row{0};
        uint64_t count{0};
    };

} // namespace components
