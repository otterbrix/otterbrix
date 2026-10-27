#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/vector/data_chunk.hpp>

#include <cstdint>
#include <memory_resource>

namespace components {

    // Rows of a host's own otterbrix table that a DDL of one of its tables brings: `insert` rows are appended in the
    // table's declared column order, and every row whose column `delete_column` holds one of `delete_keys` is deleted.
    // The statement writes them in its own transaction.
    struct host_rows_t {
        catalog::oid_t table{catalog::INVALID_OID};
        std::pmr::vector<vector::data_chunk_t> insert;
        uint64_t delete_column{0};
        std::pmr::vector<vector::data_chunk_t> delete_keys;

        bool empty() const noexcept { return insert.empty() && delete_keys.empty(); }
    };

} // namespace components
