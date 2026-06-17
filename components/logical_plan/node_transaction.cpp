#include "node_transaction.hpp"

#include <boost/container_hash/hash.hpp>

namespace components::logical_plan {

    node_transaction_t::node_transaction_t(std::pmr::memory_resource* resource, transaction_op op)
        : node_t(resource, node_type::transaction_t)
        , op_(op) {}

    // op_ is folded into the hash so begin/commit/abort land in distinct
    // buckets of any node-keyed container despite sharing node_type::transaction_t.
    hash_t node_transaction_t::hash_impl() const {
        hash_t hash_value{0};
        boost::hash_combine(hash_value, static_cast<uint8_t>(op_));
        return hash_value;
    }

    std::string node_transaction_t::to_string_impl() const {
        switch (op_) {
            case transaction_op::begin:
                return "$begin_transaction";
            case transaction_op::commit:
                return "$commit_transaction";
            case transaction_op::abort:
                return "$abort_transaction";
        }
        return "$transaction";
    }

    node_transaction_ptr make_node_transaction(std::pmr::memory_resource* resource, transaction_op op) {
        return {new node_transaction_t{resource, op}};
    }

} // namespace components::logical_plan
