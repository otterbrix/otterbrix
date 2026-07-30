#include "scan_identity_projection.hpp"

#include <components/catalog/catalog_codes.hpp>

#include <utility>
#include <vector>

namespace components::operators {

    scan_identity_projection_t::scan_identity_projection_t(std::pmr::memory_resource* resource)
        : resource_(resource)
        , attoids_(resource)
        , names_(resource)
        , types_(resource) {}

    bool scan_identity_projection_t::displaced(
        const components::logical_plan::resolved_table_metadata_t* md) noexcept {
        if (!md || md->relkind == components::catalog::relkind::computed) {
            return false;
        }
        for (std::size_t i = 0; i < md->columns.size(); ++i) {
            // chunk_position is the storage slot this column was MEASURED to occupy — resolve
            // joins the live catalog columns to the storage's own column identities to get it
            // (operator_resolve_table.cpp). The logical ordinal is where the plan thinks the
            // column is. A tombstone in the middle is what makes them differ.
            //
            // A column with NO slot (chunk_position < 0) is not evidence either way. ALTER TABLE
            // ADD COLUMN gives a relation one of those — it widens pg_attribute and leaves the
            // storage alone — and it says nothing about whether the columns that DO have a slot
            // still sit where their ordinals say. Treating it as displacement would cost every
            // ADD COLUMN its column pruning, filter pushdown, aggregate pushdown and index
            // probes; treating a genuinely displaced relation as fine is the silent wrong answer
            // this exists to remove. Only a column that has a slot can testify about one.
            if (md->columns[i].chunk_position >= 0 &&
                md->columns[i].chunk_position != static_cast<std::int32_t>(i)) {
                return true;
            }
        }
        return false;
    }

    void scan_identity_projection_t::adopt(const components::logical_plan::resolved_table_metadata_t* md) {
        if (!displaced(md)) {
            return;
        }
        attoids_.reserve(md->columns.size());
        names_.reserve(md->columns.size());
        types_.reserve(md->columns.size());
        for (const auto& column : md->columns) {
            attoids_.push_back(column.attoid);
            // Uses-allocator construction: the pmr::vector appends its own resource, so the name
            // is built on `resource_` without naming it here (naming it too is a 4th argument).
            names_.emplace_back(column.attname.data(), column.attname.size());
            types_.push_back(column.type);
        }
    }

    core::error_t scan_identity_projection_t::apply(vector::data_chunk_t& chunk) const {
        if (attoids_.empty()) {
            return core::error_t::no_error();
        }
        // A 0-column chunk is the pipeline's drain sentinel, not a row of this relation. It
        // addresses nothing and must stay exactly as wide as it is.
        if (chunk.data.empty()) {
            return core::error_t::no_error();
        }

        // Resolve EVERY logical column before moving anything. Moving a vector_t clears the
        // source's attoid (vector.cpp: a moved-from column that still answered with its identity
        // would let a later logical column claim it a second time), so a single interleaved pass
        // would make the join depend on the order it happened to run in. `claimed` says the same
        // thing out loud for the columns that have not moved yet.
        std::vector<std::int64_t> source_column(attoids_.size(), -1);
        std::vector<bool> claimed(chunk.data.size(), false);
        for (std::size_t logical = 0; logical < attoids_.size(); ++logical) {
            const auto wanted = attoids_[logical];
            if (wanted == components::catalog::INVALID_OID) {
                continue; // reported below — there is no identity to look for
            }
            for (std::size_t column = 0; column < chunk.data.size(); ++column) {
                if (claimed[column] || chunk.data[column].attoid() != wanted) {
                    continue;
                }
                source_column[logical] = static_cast<std::int64_t>(column);
                claimed[column] = true;
                break;
            }
        }
        for (std::size_t logical = 0; logical < attoids_.size(); ++logical) {
            if (source_column[logical] >= 0) {
                continue;
            }
            // R6: no positional fallback. A column the catalog says is live, whose identity the
            // scanned chunk does not carry, means the storage and the catalog disagree about what
            // this relation IS — answering with whatever sits at that ordinal is how a dropped
            // column's neighbour started returning the dropped column's values.
            std::pmr::string message{"scan: column '", resource_};
            message.append(names_[logical]);
            message += "' (attoid ";
            const auto attoid_text = std::to_string(static_cast<unsigned>(attoids_[logical]));
            message.append(attoid_text.begin(), attoid_text.end());
            message += ") has no identity match in the scanned chunk";
            return core::error_t{core::error_code_t::field_not_exists, std::move(message)};
        }

        std::vector<vector::vector_t> projected;
        projected.reserve(attoids_.size());
        for (std::size_t logical = 0; logical < attoids_.size(); ++logical) {
            // vector_t's move carries both the identity and the name across, so the projected
            // column keeps saying exactly what the storage said about it. Restating the name
            // from the catalog here would be a second, disagreeing answer: `SELECT *` labels a
            // column by the PHYSICAL name today (a RENAME does not touch storage — pinned as a
            // known gap in test_sql_features), and the storage-side write matcher falls back to
            // that same name when a chunk reaches it without identity.
            projected.push_back(std::move(chunk.data[static_cast<std::size_t>(source_column[logical])]));
        }
        chunk.data = std::move(projected);
        return core::error_t::no_error();
    }

    vector::data_chunk_t scan_identity_projection_t::make_guard_chunk() const {
        vector::data_chunk_t guard{resource_, types_, 0};
        for (std::size_t i = 0; i < attoids_.size(); ++i) {
            guard.set_column_attoid(i, attoids_[i]);
            guard.set_column_name(i, std::string_view{names_[i]});
        }
        guard.set_cardinality(0);
        return guard;
    }

} // namespace components::operators
