#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>

#include <cstdint>

namespace components::logical_plan {

    enum class transaction_op : uint8_t
    {
        begin,
        commit,
        abort
    };

    // Unified transaction-control leaf node (BEGIN / COMMIT / ROLLBACK),
    // selected by op(). The session id flows through pipeline::context_t::session;
    // operator-side resolution against the txn_manager happens at execution time.
    //
    // begin/abort carry no payload. commit optionally carries is_ddl_commit —
    // when true the commit operator additionally performs manager_disk_t::flush +
    // manager_wal_replicate_t::commit_txn before the standard MVCC commit; txn_id
    // and database_oid carry the WAL coordinates for that prefix (ignored when
    // is_ddl_commit=false / RPC mode).
    //
    // Lowered by create_plan (switch on op()) into the existing
    // operator_{begin,commit,abort}_transaction_t.
    class node_transaction_t final : public node_t {
    public:
        node_transaction_t(std::pmr::memory_resource* resource, transaction_op op);

        transaction_op op() const noexcept { return op_; }

        // commit-only payload (ignored for begin/abort)
        bool is_ddl_commit() const noexcept { return is_ddl_commit_; }
        void set_is_ddl_commit(bool v) noexcept { is_ddl_commit_ = v; }
        std::uint64_t txn_id() const noexcept { return txn_id_; }
        void set_txn_id(std::uint64_t v) noexcept { txn_id_ = v; }
        components::catalog::oid_t database_oid() const noexcept { return database_oid_; }
        void set_database_oid(components::catalog::oid_t v) noexcept { database_oid_ = v; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        transaction_op op_;
        bool is_ddl_commit_{false};
        std::uint64_t txn_id_{0};
        components::catalog::oid_t database_oid_{components::catalog::INVALID_OID};
    };

    using node_transaction_ptr = boost::intrusive_ptr<node_transaction_t>;
    node_transaction_ptr make_node_transaction(std::pmr::memory_resource* resource, transaction_op op);

} // namespace components::logical_plan
