#pragma once

#include "node.hpp"

namespace components::logical_plan {

    // BEGIN / START TRANSACTION — leaf node, carries no fields. Lowered to
    // operator_begin_transaction_t, which ensures an active transaction exists
    // (idempotent begin_transaction) and marks it explicit. The executor's
    // commit phase reads is_explicit() to decide whether DML publishes ranges
    // per-statement (implicit) or accumulates into pending_base_appends_/deletes_
    // for COMMIT-time drain (explicit BEGIN..COMMIT).
    class node_begin_transaction_t final : public node_t {
    public:
        explicit node_begin_transaction_t(std::pmr::memory_resource* resource);

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_begin_transaction_ptr = boost::intrusive_ptr<node_begin_transaction_t>;

} // namespace components::logical_plan
