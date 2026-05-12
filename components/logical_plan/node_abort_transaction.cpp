#include "node_abort_transaction.hpp"

namespace components::logical_plan {

    node_abort_transaction_t::node_abort_transaction_t(std::pmr::memory_resource* resource)
        : node_t(resource, node_type::abort_transaction_t) {}

    hash_t node_abort_transaction_t::hash_impl() const { return 0; }

    std::string node_abort_transaction_t::to_string_impl() const { return "$abort_transaction"; }

} // namespace components::logical_plan
