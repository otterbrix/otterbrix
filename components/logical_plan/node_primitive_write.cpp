#include "node_primitive_write.hpp"

namespace components::logical_plan {

    node_primitive_write_t::node_primitive_write_t(std::pmr::memory_resource* resource,
                                                   catalog::oid_t catalog_table_oid,
                                                   vector::data_chunk_t row)
        : node_t(resource, node_type::primitive_write_t)
        , catalog_table_oid_(catalog_table_oid)
        , row_(std::move(row)) {
        set_table_oid(catalog_table_oid);
    }

    hash_t node_primitive_write_t::hash_impl() const { return 0; }

    std::string node_primitive_write_t::to_string_impl() const {
        return "$primitive_write[oid=" + std::to_string(catalog_table_oid_) + "]";
    }

} // namespace components::logical_plan
