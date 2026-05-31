#pragma once

#include "node.hpp"

namespace components::logical_plan {

    // BEGIN / START TRANSACTION — leaf node, carries no fields.
    //
    // Block C §3.5 dec 22 Central accumulation: lowered to
    // operator_begin_transaction_t which (a) ensures an active transaction
    // exists for the session via txn_manager_->begin_transaction (idempotent)
    // and (b) calls transaction_t::mark_explicit(). The executor's commit
    // phase then consults transaction_t::is_explicit() to decide whether DML
    // statements publish their ranges per-statement (implicit txns) or
    // accumulate into pending_base_appends_/deletes_ for COMMIT-time drain
    // (explicit BEGIN..COMMIT txns).
    class node_begin_transaction_t final : public node_t {
    public:
        explicit node_begin_transaction_t(std::pmr::memory_resource* resource);

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_begin_transaction_ptr = boost::intrusive_ptr<node_begin_transaction_t>;

} // namespace components::logical_plan
