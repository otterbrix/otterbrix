#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <cstdint>

namespace components {

    // Tracks one pg_catalog.* append made under a real txn. Operators record
    // these on the pipeline context; executor returns them via execute_result_t;
    // dispatcher aggregates into transaction_t; commit/abort operators apply
    // storage_publish_commits / storage_revert_appends after txn_manager_.commit().
    struct pg_catalog_append_range_t {
        catalog::oid_t table_oid{catalog::INVALID_OID};
        int64_t start_row{0};
        uint64_t count{0};
    };

    // Backfill marker for pg_attribute MVCC commit_id fields. ALTER operators
    // cannot stamp added_at/dropped_at_commit_id at execute time: the commit_id
    // is allocated later by transaction_manager_t::commit(). They write the row
    // with placeholder 0 and emit this marker; the commit operator drains them
    // post-commit and patches the column in place. `kind` selects the column
    // (added_at = index 10 for ADD/RENAME, dropped_at = index 11 for DROP tombstone).
    struct pg_attribute_commit_id_backfill_t {
        enum class kind_t : std::uint8_t
        {
            added_at,
            dropped_at
        };
        catalog::oid_t attoid{catalog::INVALID_OID};
        kind_t kind{kind_t::added_at};
    };

} // namespace components
