#pragma once

#include "catalog_oids.hpp"
#include "catalog_write.hpp"
#include "oid_batch.hpp"

#include <cstddef>
#include <memory_resource>
#include <string>
#include <vector>

namespace components::catalog {

    // The oids build_information_schema_writes takes from its batch.
    [[nodiscard]] std::size_t information_schema_oid_demand();

    // The views of `database` (of oid `database_oid`) in its information_schema `namespace_oid`.
    [[nodiscard]] std::vector<catalog_write_t> build_information_schema_writes(std::pmr::memory_resource* resource,
                                                                               const std::string& database,
                                                                               oid_t database_oid,
                                                                               oid_t namespace_oid,
                                                                               oid_batch_t& oid_batch);

} // namespace components::catalog
