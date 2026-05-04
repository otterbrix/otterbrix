#include "node_create_constraint.hpp"

#include <sstream>

namespace components::logical_plan {

    node_create_constraint_t::node_create_constraint_t(std::pmr::memory_resource* resource,
                                                          const collection_full_name_t& collection,
                                                          std::string name,
                                                          constraint_kind kind,
                                                          collection_full_name_t ref_collection)
        : node_t(resource, node_type::create_constraint_t, collection)
        , name_(std::move(name))
        , kind_(kind)
        , ref_collection_(std::move(ref_collection)) {}

    hash_t node_create_constraint_t::hash_impl() const { return 0; }

    std::string node_create_constraint_t::to_string_impl() const {
        std::stringstream s;
        s << "$create_constraint: " << database_name() << "." << collection_name()
          << " name=" << name_ << " kind=" << static_cast<char>(kind_);
        if (!ref_collection_.empty()) {
            s << " ref=" << ref_collection_.database << "." << ref_collection_.collection;
        }
        return s.str();
    }

    node_create_constraint_ptr
    make_node_create_constraint(std::pmr::memory_resource* resource,
                                  const collection_full_name_t& collection,
                                  std::string name,
                                  constraint_kind kind,
                                  collection_full_name_t ref_collection) {
        return {new node_create_constraint_t{resource, collection, std::move(name), kind,
                                              std::move(ref_collection)}};
    }

} // namespace components::logical_plan
