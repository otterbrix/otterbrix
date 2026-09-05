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
    // The release_* fields below are a SECOND piece of unfinished business that rides the same
    // marker rather than a channel of its own, because it shares the marker's lifetime exactly:
    // legal only after commit, and must vanish together on ABORT (txn_abort_drain_t discards
    // markers outright -- the right semantics for an un-undoable storage rebuild).
    //   * dropped_at     — release_attname is the column to physically free (empty = nothing to
    //                      free); operator_alter_column_drop_t, relkind='r'.
    //   * storage_rename — release_attname is the OLD name, rename_to_attname the NEW one.
    //                      RENAME re-appends under the SAME added_at_commit_id (identity-preserving,
    //                      must not widen visibility), so it patches no commit_id column and must
    //                      be excluded from the batch operator_commit_transaction_t hands to
    //                      update_pg_attribute_commit_id_fields (which maps kind to column 10/11
    //                      and would stamp dropped_at over a live row). See also
    //                      manager_disk_t::rename_storage_column.
    //   * added_at       — release_table_oid/release_attname carry the (table, attname) of the
    //                      freshly minted attoid to the agent that materialises the STORAGE column
    //                      later, at INSERT's schema-growth stage (which cannot read pg_attribute).
    struct pg_attribute_commit_id_backfill_t {
        enum class kind_t : std::uint8_t
        {
            added_at,
            dropped_at,
            storage_rename
        };

        // A pair, not a bare type: PG11+ ADD COLUMN DEFAULT doesn't rewrite the table (the
        // constant sits beside the type, pg_attribute.attmissingval next to atttypid), so a row
        // predating the column needs both to answer right instead of wrongly reading NULL.
        // `default_spec` is the SAME encoded text as pg_attribute.attdefspec
        // (catalog::encode/decode_default_spec); empty = no DEFAULT.
        struct added_column_type_t {
            types::complex_logical_type type;
            std::string default_spec;

            added_column_type_t() = default;
            // Implicit: keeps `..., types::complex_logical_type{}}` meaning "no default" at the
            // dropped_at / storage_rename call sites that create no column.
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
        // added_at only: type + DEFAULT of the new column, so the owning agent can answer
        // pre-existing rows before an INSERT materialises it (table_storage_adapter_t) and then
        // backfill them (row_group_t::add_column). Unset for the other two kinds.
        added_column_type_t added_column_type;
    };

} // namespace components
