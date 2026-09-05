#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/types/types.hpp>
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
    // A dropped_at marker carries a SECOND piece of unfinished business, and it rides
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
    // release_table_oid / release_attname name the TABLE and the COLUMN this marker's second
    // half acts on, and what that half is depends on the kind:
    //   * dropped_at     — the physical release of the column's blocks (operator_alter_column_drop_t,
    //                      relkind='r'); an empty release_attname means "nothing physical to release";
    //   * storage_rename — the OLD name, with rename_to_attname carrying the new one (below);
    //   * added_at       — ALTER TABLE ADD COLUMN writes pg_attribute and stops; the
    //                      STORAGE column is materialised later, by the schema-growth stage of
    //                      an INSERT, on an agent that cannot read pg_attribute. So the marker
    //                      carries the (table, attname) the freshly minted `attoid` belongs to,
    //                      and manager_disk_t::update_pg_attribute_commit_id_fields parks that
    //                      identity on the owning agent. Without it the materialised column is
    //                      born with attoid 0 and the bootstrap reconciliation — which compares
    //                      on the oid, never on the name — has to refuse the whole table.
    //
    // kind_t::storage_rename is the ONE kind that patches no commit_id column at all, and it
    // rides this struct for the same reason the release does: it is the same event with the
    // same lifetime. RENAME re-appends the pg_attribute row under the new name with the SAME
    // added_at_commit_id (renaming is identity-preserving — it must not widen visibility), so
    // it has no commit_id to backfill; what it does have is a second half that is legal only
    // once the commit is irrevocable and must vanish on the same ABORT — renaming the column
    // in the STORAGE, without which the next bootstrap reads the storage's old name as a drop
    // and physically deletes a surviving column (see manager_disk_t::rename_storage_column).
    // For this kind release_table_oid names the table, release_attname the OLD name, and
    // rename_to_attname the NEW one.
    //
    // Because it patches nothing, operator_commit_transaction_t must keep it OUT of the batch
    // it hands to update_pg_attribute_commit_id_fields: that handler maps kind onto a column
    // index (added_at -> 10, anything else -> 11) and would stamp dropped_at over a live row.
    struct pg_attribute_commit_id_backfill_t {
        enum class kind_t : std::uint8_t
        {
            added_at,
            dropped_at,
            storage_rename
        };

        // What the storage needs in order to ANSWER a column pg_attribute publishes and no row
        // group holds yet. It is a PAIR, not a type, because PostgreSQL parity makes it one:
        // since PG 11, ALTER TABLE ADD COLUMN ... DEFAULT does not rewrite the table — the
        // constant is filed next to the type (pg_attribute.attmissingval beside atttypid) and
        // every row that predates the column reads it from there. atttypid <-> `type`,
        // attmissingval <-> `default_spec`, and the two travel together for the same reason PG
        // stores them in one row: a reader that has one and not the other answers the column
        // WRONG (NULL instead of the default) rather than not at all.
        //
        // `default_spec` is the encoded form catalog::encode_default_spec produces and
        // catalog::decode_default_spec reads back — the SAME text pg_attribute.attdefspec holds,
        // so the value that crosses to the agent and the value the catalog persisted are one
        // string, not two encodings that can drift. Empty = the column has no DEFAULT.
        struct added_column_type_t {
            types::complex_logical_type type;
            std::string default_spec;

            added_column_type_t() = default;
            // IMPLICIT on purpose, and it is what keeps the pair from being a burden on the two
            // kinds that create no column: a bare type IS this pair for a column with no DEFAULT,
            // so `... , types::complex_logical_type{}}` at a dropped_at / storage_rename call site
            // goes on meaning exactly what it meant.
            added_column_type_t(types::complex_logical_type t)
                : type(std::move(t)) {}
            added_column_type_t(types::complex_logical_type t, std::string spec)
                : type(std::move(t))
                , default_spec(std::move(spec)) {}
        };
        catalog::oid_t attoid{catalog::INVALID_OID};
        kind_t kind{kind_t::added_at};
        catalog::oid_t release_table_oid{catalog::INVALID_OID};
        std::string release_attname;
        std::string rename_to_attname;
        // added_at only: the type of the column the ALTER created, and the DEFAULT that goes with
        // it (see added_column_type_t). It travels with the identity to the owning agent, because
        // the storage that has not materialised the column yet has to be able to ANSWER it — with
        // the default where there is one, with NULLs of this type where there is not — until an
        // INSERT does (table_storage_adapter_t), and then to BACKFILL the rows that predate the
        // column with the same constant when that INSERT materialises it (row_group_t::add_column).
        // Unset for the other two kinds.
        added_column_type_t added_column_type;
    };

} // namespace components
