#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <cstdint>
#include <string>

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
    //
    // B3c1 — a dropped_at marker carries a SECOND piece of unfinished business, and it rides
    // here rather than on a channel of its own because it is the same event with the same
    // lifetime. The marker already IS "an ALTER wrote a pg_attribute row this txn has to
    // finish applying once the commit_id exists"; for a DROP, finishing it means both patching
    // dropped_at_commit_id AND releasing the column's physical storage. Both are legal only
    // after the same instant (the commit), and both must vanish on the same event (an ABORT —
    // txn_abort_drain_t discards backfill markers outright, which is exactly the semantics an
    // un-undoable storage rebuild needs). A parallel {table_oid, attname} channel would have
    // duplicated that flow through six txn-plumbing files to express a lifetime this marker
    // already has.
    //
    // release_table_oid / release_attname are populated ONLY for kind_t::dropped_at, and only
    // by operator_alter_column_drop_t (relkind='r'); an empty release_attname means "nothing
    // physical to release", which is how every added_at marker reads.
    struct pg_attribute_commit_id_backfill_t {
        enum class kind_t : std::uint8_t
        {
            added_at,
            dropped_at
        };
        catalog::oid_t attoid{catalog::INVALID_OID};
        kind_t kind{kind_t::added_at};
        catalog::oid_t release_table_oid{catalog::INVALID_OID};
        std::string release_attname;
    };

} // namespace components
