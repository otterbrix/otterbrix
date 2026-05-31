#include "node_begin_transaction.hpp"

namespace components::logical_plan {

    node_begin_transaction_t::node_begin_transaction_t(std::pmr::memory_resource* resource)
        : node_t(resource, node_type::begin_transaction_t) {}

    hash_t node_begin_transaction_t::hash_impl() const { return 0; }

    std::string node_begin_transaction_t::to_string_impl() const { return "$begin_transaction"; }

} // namespace components::logical_plan
