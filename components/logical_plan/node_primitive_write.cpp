#include "node_primitive_write.hpp"

#include <sstream>

namespace components::logical_plan {

    node_primitive_write_t::node_primitive_write_t(std::pmr::memory_resource*       resource,
                                                    const collection_full_name_t&    collection,
                                                    components::catalog::oid_t       table_oid,
                                                    components::vector::data_chunk_t row)
        : node_t(resource, node_type::primitive_write_t, collection)
        , table_oid_(table_oid)
        , row_(std::move(row)) {}

    hash_t      node_primitive_write_t::hash_impl()      const { return 0; }
    std::string node_primitive_write_t::to_string_impl() const {
        std::ostringstream s;
        s << "$primitive_write: " << collection_name() << " table_oid=" << table_oid_;
        return s.str();
    }

} // namespace components::logical_plan
