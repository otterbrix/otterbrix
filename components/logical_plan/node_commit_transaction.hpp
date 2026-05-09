#pragma once

#include "node.hpp"

namespace components::logical_plan {

    // COMMIT TRANSACTION — leaf node carrying no fields. The session id flows
    // through pipeline::context_t::session at execution time, and the
    // transaction lookup is done inside operator_commit_transaction_t against
    // the txn_manager pointer also stamped on the context.
    //
    // Phase 4 #56 of the pipeline-unification refactor: replaces the inline
    // manager_dispatcher_t::commit_transaction body with the standard
    // operator pipeline used elsewhere (mirrors get_schema #54, register_udf
    // #55).
    class node_commit_transaction_t final : public node_t {
    public:
        explicit node_commit_transaction_t(std::pmr::memory_resource* resource);

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_commit_transaction_ptr = boost::intrusive_ptr<node_commit_transaction_t>;

} // namespace components::logical_plan
