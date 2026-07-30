#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <string>

namespace components::operators {

    // Projects a scan chunk onto the relation's LIVE LOGICAL schema, joining the two on each
    // column's catalog identity.
    //
    // A chunk leaves storage in PHYSICAL layout and at full storage width, every column carrying
    // the attoid of the table column it was read from (data_table_t::stamp_column_identity). The
    // logical schema — pg_attribute, minus tombstones — is what every ordinal ABOVE the scan was
    // resolved against (validate_schema builds its ordinals from resolved_table_metadata_t). While
    // a relation has never had a column dropped those two lists are the same list in the same
    // order, and this projection stays DISENGAGED: not a single chunk is touched.
    //
    // ALTER TABLE ... DROP COLUMN is what separates them. pg_attribute loses the column
    // immediately; the physical column list does not, because dropping a physical column rewrites
    // every row group and is VACUUM's job (PostgreSQL keeps the same split, attisdropped and all).
    // So the storage stays N columns wide while the logical schema becomes N-1, and every column
    // after the hole sits one slot further right in the chunk than its logical ordinal. Reading it
    // positionally is what made `SELECT c` answer with b's values on (a,b,c) after dropping b.
    //
    // The join key is the attoid, never the position: identity is what survives the hole, and it
    // is durable across a restart on all three storage paths (.otbx column record, IN_MEMORY
    // rehydrate, and restamp_user_storage_attoids_sync after WAL replay). A logical column with no
    // identity match in the chunk is an ERROR (R6) — answering it by position is exactly the
    // silent wrong answer this exists to remove.
    //
    // relkind='g' (computing tables) is deliberately out of scope: its storage columns are created
    // by append-time schema growth and get their attoid afterwards, from a different catalog
    // (pg_computed_column), so a 'g' chunk has no identity to join on. Those relations keep
    // routing through chunk_position, which is resolved against the live storage type list.
    class scan_identity_projection_t final {
    public:
        explicit scan_identity_projection_t(std::pmr::memory_resource* resource);

        scan_identity_projection_t(const scan_identity_projection_t&) = delete;
        scan_identity_projection_t& operator=(const scan_identity_projection_t&) = delete;
        scan_identity_projection_t(scan_identity_projection_t&&) = delete;
        scan_identity_projection_t& operator=(scan_identity_projection_t&&) = delete;
        ~scan_identity_projection_t() = default;

        // Take the live column list off `md` — but only when it no longer describes the storage
        // layout 1:1, i.e. when some live column's chunk_position is not its logical ordinal. That
        // is the whole and only condition: it is a statement about the STORAGE (a slot the logical
        // schema no longer names), not about which column is which, and the answer to the latter
        // question comes from the identity join alone. `md` may be null (no catalog metadata
        // reached this scan — a no-table sentinel, a unit-test construction); it is read here and
        // not retained.
        void adopt(const components::logical_plan::resolved_table_metadata_t* md);

        // THE condition, shared with plan generation so both sides read one definition. A
        // displaced relation cannot be pruned by logical ordinal, cannot carry a pushed-down
        // filter or a pushed aggregate, and cannot be probed through an index — every one of
        // those addresses a storage column by the ordinal the validator resolved. False for a
        // null `md` and for relkind='g'.
        [[nodiscard]] static bool displaced(const components::logical_plan::resolved_table_metadata_t* md) noexcept;

        [[nodiscard]] bool engaged() const noexcept { return !attoids_.empty(); }

        // Rewrite `chunk` from storage layout into logical layout, in place. No-op when
        // disengaged. Returns field_not_exists when a logical column has no identity match among
        // the chunk's columns.
        [[nodiscard]] core::error_t apply(vector::data_chunk_t& chunk) const;

        // The 0-row drained guard in LOGICAL layout: one column per live logical column, carrying
        // its plan-resolved type, its name and its identity. A scan builds its guard chunk from
        // the STORAGE type list, which is the wrong width and the wrong order once engaged.
        [[nodiscard]] vector::data_chunk_t make_guard_chunk() const;

    private:
        std::pmr::memory_resource* resource_;
        // Parallel by design: a struct with a std::pmr::string member is not uses-allocator
        // constructible, so a std::pmr::vector of it would re-home every name onto the default
        // resource (R8/R14). Three vectors keep every element on `resource_`.
        std::pmr::vector<components::catalog::oid_t> attoids_;
        std::pmr::vector<std::pmr::string> names_;
        std::pmr::vector<types::complex_logical_type> types_;
    };

} // namespace components::operators
