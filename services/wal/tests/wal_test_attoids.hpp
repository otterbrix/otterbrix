#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/wal/record.hpp>

namespace wal_test {

    // One attoid per column of `chunk`, distinct and in the user range.
    inline services::wal::column_attoids_t attoids_for(const components::vector::data_chunk_t& chunk) {
        services::wal::column_attoids_t attoids(chunk.resource());
        for (uint64_t column = 0; column < chunk.column_count(); column++) {
            attoids.push_back(components::catalog::FIRST_USER_OID + 100 +
                              static_cast<components::catalog::oid_t>(column));
        }
        return attoids;
    }

    // A batch's chunks all share one width.
    inline services::wal::column_attoids_t
    attoids_for(const std::pmr::vector<components::vector::data_chunk_t>& batch) {
        if (batch.empty()) {
            return services::wal::column_attoids_t(batch.get_allocator().resource());
        }
        return attoids_for(batch.front());
    }

} // namespace wal_test
