#pragma once

#include "node.hpp"

namespace components::logical_plan {

    // ABORT (ROLLBACK) TRANSACTION — leaf node carrying no fields. The
    // session id flows through pipeline::context_t::session; operator-side
    // resolution against the txn_manager is done in
    // operator_abort_transaction_t.
    class node_abort_transaction_t final : public node_t {
    public:
        explicit node_abort_transaction_t(std::pmr::memory_resource* resource);

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_abort_transaction_ptr = boost::intrusive_ptr<node_abort_transaction_t>;

} // namespace components::logical_plan
