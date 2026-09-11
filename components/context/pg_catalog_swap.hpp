#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/types/types.hpp>
#include <cstdint>
#include <string>

namespace components {

    struct pg_catalog_append_range_t {
        catalog::oid_t table_oid{catalog::INVALID_OID};
        int64_t start_row{0};
        uint64_t count{0};
    };

    // ALTER placeholders commit_id as 0 (transaction_manager_t::commit() allocates it later) and emits this marker for
    // post-commit patching; `kind` selects column 10 (added_at) or 11 (dropped_at).
    //
    // release_* fields ride this marker (legal only after commit, discarded on ABORT); for storage_rename,
    // release_attname/rename_to_attname (old/new name) must be excluded from the commit_id-patch batch, or it stamps
    // dropped_at over a live row.
    struct pg_attribute_commit_id_backfill_t {
        enum class kind_t : std::uint8_t
        {
            added_at,
            dropped_at,
            storage_rename
        };

        // A pair, not a bare type: PG11+ ADD COLUMN DEFAULT doesn't rewrite the table, so old rows need both.
        struct added_column_type_t {
            types::complex_logical_type type;
            std::string default_spec;

            added_column_type_t() = default;
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
        added_column_type_t added_column_type;
    };

} // namespace components
