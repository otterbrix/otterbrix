#include "node_create_sequence.hpp"

#include <sstream>

namespace components::logical_plan {

    node_create_sequence_t::node_create_sequence_t(std::pmr::memory_resource* resource,
                                                   const collection_full_name_t& name,
                                                   int64_t start,
                                                   int64_t increment,
                                                   int64_t min_value,
                                                   int64_t max_value)
        : node_t(resource, node_type::create_sequence_t, name)
        , start_(start)
        , increment_(increment)
        , min_value_(min_value)
        , max_value_(max_value) {}

    hash_t node_create_sequence_t::hash_impl() const { return 0; }

    std::string node_create_sequence_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$create_sequence: " << database_name() << "." << collection_name();
        return stream.str();
    }

    node_create_sequence_ptr make_node_create_sequence(std::pmr::memory_resource* resource,
                                                       const collection_full_name_t& name,
                                                       int64_t start,
                                                       int64_t increment,
                                                       int64_t min_value,
                                                       int64_t max_value) {
        return {new node_create_sequence_t{resource, name, start, increment, min_value, max_value}};
    }

} // namespace components::logical_plan
