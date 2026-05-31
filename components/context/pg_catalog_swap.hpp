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

    // Backfill marker for pg_attribute MVCC commit_id fields.
    // operator_alter_column_{add,drop,rename} cannot stamp
    // added_at_commit_id / dropped_at_commit_id at execute time because the
    // commit_id is allocated by transaction_manager_t::commit() later in the
    // pipeline (see operator_commit_transaction.cpp). The operators write the
    // pg_attribute row with placeholder 0 and emit one of these markers; the
    // commit operator drains them after commit() and patches the row's
    // commit_id column in place. `kind` selects which of the two columns to
    // patch (index 10 = added_at_commit_id for ADD/RENAME identity row,
    // index 11 = dropped_at_commit_id for DROP tombstone).
    struct pg_attribute_commit_id_backfill_t {
        enum class kind_t : std::uint8_t { added_at, dropped_at };
        catalog::oid_t attoid{catalog::INVALID_OID};
        kind_t kind{kind_t::added_at};
    };

} // namespace components
