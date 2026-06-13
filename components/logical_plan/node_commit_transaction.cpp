#include "node_commit_transaction.hpp"

namespace components::logical_plan {

    node_commit_transaction_t::node_commit_transaction_t(std::pmr::memory_resource* resource)
        : node_t(resource, node_type::commit_transaction_t) {}

    hash_t node_commit_transaction_t::hash_impl() const { return 0; }

    std::string node_commit_transaction_t::to_string_impl() const { return "$commit_transaction"; }

} // namespace components::logical_plan
