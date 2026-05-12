#include "node_drop_sequence.hpp"

#include <sstream>

namespace components::logical_plan {

    node_drop_sequence_t::node_drop_sequence_t(std::pmr::memory_resource* resource,
                                               std::string dbname,
                                               std::string seqname)
        : node_t(resource, node_type::drop_sequence_t)
        , dbname_(std::move(dbname))
        , seqname_(std::move(seqname)) {}

    hash_t node_drop_sequence_t::hash_impl() const { return 0; }

    std::string node_drop_sequence_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$drop_sequence: " << dbname_ << "." << seqname_;
        return stream.str();
    }

    node_drop_sequence_ptr make_node_drop_sequence(std::pmr::memory_resource* resource,
                                                   std::string dbname,
                                                   std::string seqname) {
        return {new node_drop_sequence_t{resource, std::move(dbname), std::move(seqname)}};
    }

} // namespace components::logical_plan
