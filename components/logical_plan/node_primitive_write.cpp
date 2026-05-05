#include "node_primitive_write.hpp"

namespace components::logical_plan {

    node_primitive_write_t::node_primitive_write_t(std::pmr::memory_resource* resource,
                                                    collection_full_name_t     catalog_table,
                                                    vector::data_chunk_t       row)
        : node_t(resource, node_type::primitive_write_t, catalog_table)
        , catalog_table_(std::move(catalog_table))
        , row_(std::move(row)) {}

    hash_t node_primitive_write_t::hash_impl() const { return 0; }

    std::string node_primitive_write_t::to_string_impl() const {
        return "$primitive_write[" + catalog_table_.collection + "]";
    }

} // namespace components::logical_plan