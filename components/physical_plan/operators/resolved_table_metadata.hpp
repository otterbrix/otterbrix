#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/vector/data_chunk.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace components::operators {

    // Column metadata for a single resolved table column.
    // Mirrors one row of the operator_resolve_table_t output chunk
    // (position int32, attoid uint32, attname string, atttypid uint32,
    //  atttypspec string). Stored in a flat vector keyed by 0-based
    // "table position" (= position-1 from the chunk row).
    struct resolved_column_t {
        std::int32_t position{0}; // 1-based ordinal from resolver
        catalog::oid_t attoid{catalog::INVALID_OID};
        std::string attname;
        catalog::oid_t atttypid{catalog::INVALID_OID};
        std::string atttypspec;
    };

    // Parsed form of operator_resolve_table_t's output chunk.
    // Produced once by a resolve sibling and handed to the DML consumer
    // (insert/update/delete) via the set_resolved_metadata hook. The DML
    // operator uses the column list to build a chunk_position -> table_position
    // translation by alias matching just before storage_append.
    struct resolved_table_metadata_t {
        catalog::oid_t table_oid{catalog::INVALID_OID};
        std::vector<resolved_column_t> columns;

        // True when at least one column row was emitted by the resolver.
        bool has_columns() const noexcept { return !columns.empty(); }

        // Return the table position (0-based) of the column whose attname
        // matches `alias`. Returns std::nullopt when not found.
        std::optional<std::size_t> find_position_by_alias(std::string_view alias) const noexcept {
            for (std::size_t i = 0; i < columns.size(); ++i) {
                if (columns[i].attname == alias) {
                    return i;
                }
            }
            return std::nullopt;
        }
    };

    // chunk_position -> table_position translation. Entry == -1 means the
    // chunk column has no matching alias in the resolved schema (the disk
    // actor's existing alias/positional fallback then fills the gap). Length
    // equals data_chunk's column_count.
    using column_key_translation_t = std::vector<std::int32_t>;

    // Parse a metadata data_chunk produced by operator_resolve_table_t into
    // resolved_table_metadata_t. The chunk layout is fixed: 5 columns
    // [position int32, attoid uint32, attname string, atttypid uint32,
    // atttypspec string]. Returns std::nullopt when the chunk shape doesn't
    // match (defensive; we'd rather skip than crash on shape drift).
    std::optional<resolved_table_metadata_t> parse_resolved_table_metadata(catalog::oid_t table_oid,
                                                                           const operator_data_ptr& resolve_output);

    // Build a chunk_position -> table_position translation for `data_chunk`
    // by alias matching against the resolver's column list. Returns a vector
    // of length data_chunk.column_count with -1 entries for unmatched chunk
    // columns. When metadata.columns is empty this returns an all-(-1)
    // vector — caller can treat that as "no translation needed".
    column_key_translation_t build_column_key_translation(const resolved_table_metadata_t& metadata,
                                                          const vector::data_chunk_t& data_chunk);

} // namespace components::operators
