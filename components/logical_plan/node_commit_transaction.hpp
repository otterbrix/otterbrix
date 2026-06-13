#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>

#include <cstdint>

namespace components::logical_plan {

    // COMMIT TRANSACTION — leaf node. The session id flows through
    // pipeline::context_t::session; transaction lookup is done in
    // operator_commit_transaction_t against the txn_manager on context.
    //
    // Optional is_ddl_commit flag — when true, the operator additionally
    // performs manager_disk_t::flush + manager_wal_replicate_t::commit_txn
    // before the standard MVCC commit. txn_id and database_oid carry the WAL
    // coordinates needed for that prefix; ignored when is_ddl_commit=false
    // (RPC mode).
    class node_commit_transaction_t final : public node_t {
    public:
        explicit node_commit_transaction_t(std::pmr::memory_resource* resource);

        bool is_ddl_commit() const noexcept { return is_ddl_commit_; }
        void set_is_ddl_commit(bool v) noexcept { is_ddl_commit_ = v; }
        std::uint64_t txn_id() const noexcept { return txn_id_; }
        void set_txn_id(std::uint64_t v) noexcept { txn_id_ = v; }
        components::catalog::oid_t database_oid() const noexcept { return database_oid_; }
        void set_database_oid(components::catalog::oid_t v) noexcept { database_oid_ = v; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        bool is_ddl_commit_{false};
        std::uint64_t txn_id_{0};
        components::catalog::oid_t database_oid_{components::catalog::INVALID_OID};
    };

    using node_commit_transaction_ptr = boost::intrusive_ptr<node_commit_transaction_t>;

} // namespace components::logical_plan
