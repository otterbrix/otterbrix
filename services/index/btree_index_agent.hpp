#pragma once

// Holds btree_index_disk_t by value and by its concrete type, not through a virtual interface
// (see index_agent_contract.hpp for why). Also holds the uncommitted buffer beside the tree, so
// read_rows below returns both halves already merged instead of stitching them post-hoc.

#include "btree_index_disk.hpp"
#include "index_agent_contract.hpp"

#include <core/result_wrapper.hpp>

#include <actor-zeta.hpp>
#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/actor/implements.hpp>
#include <actor-zeta/detail/future.hpp>

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <memory_resource>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace services::index {

    // No separate DROP TABLE handler: manager_index_t's unregister_collection sends the same
    // drop() to every agent when the owning table is dropped (services/index/manager_index.cpp).
    class btree_index_agent_t final : public actor_zeta::basic_actor<btree_index_agent_t> {
        using path_t = std::filesystem::path;
        using session_id_t = index_agent_contract::session_id_t;
        using value_t = index_agent_contract::value_t;

    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        // Must name the concrete class: actor_zeta::pmr::deleter_t deallocates sizeof(static type).
        using agent_ptr_t = std::unique_ptr<btree_index_agent_t, actor_zeta::pmr::deleter_t>;

        // Copied by manager_index_t into its per-index record; the only thing that tells an
        // ordered index from a hashed one over the same column. `single` because SQL's only
        // explicit spelling is `USING hash`; everything else this family builds is `single`.
        static constexpr components::logical_plan::index_type index_type_v =
            components::logical_plan::index_type::single;

        // Copied into the manager's record; can_use_index refuses lt/lte/gt/gte unless a
        // non-hashed index covers the key.
        static constexpr bool supports_ordered_probe_v = true;

        // No segment-record limit or WAL committed-txn set here: this family owns no segments
        // and no txn log, so there's no recover gate to arm. index_oid = pg_index.indexrelid;
        // the on-disk directory is ${path_db}/${table_oid}/${index_oid}/, oid-keyed.
        [[nodiscard]] static core::result_wrapper_t<agent_ptr_t> create(std::pmr::memory_resource* resource,
                                                                        const path_t& path_db,
                                                                        components::catalog::oid_t table_oid,
                                                                        components::catalog::oid_t index_oid,
                                                                        uint64_t flush_threshold,
                                                                        log_t& log);

        // No deferred open unlike the hashed family: btree_t::load() is void and cannot fail,
        // so an open() step here would only ever answer no_error(). The store is opened directly
        // in the member initializer list. Public because actor_zeta::spawn placement-news the actor.
        btree_index_agent_t(std::pmr::memory_resource* resource,
                            const path_t& path_db,
                            components::catalog::oid_t table_oid,
                            components::catalog::oid_t index_oid,
                            uint64_t flush_threshold,
                            log_t& log);
        ~btree_index_agent_t();

        [[nodiscard]] components::catalog::oid_t table_oid() const noexcept { return table_oid_; }

        unique_future<void> drop(session_id_t session);
        unique_future<core::error_t> clear(session_id_t session);
        unique_future<core::error_t>
        stage_inserts(session_id_t session, uint64_t txn_id, std::vector<std::pair<value_t, size_t>> values);
        unique_future<core::error_t>
        stage_deletes(session_id_t session, uint64_t txn_id, std::vector<std::pair<value_t, size_t>> values);
        // commit_id is unused here: this family keeps no journal, but the contract is positional
        // by msg_id, so both classes must carry the parameter or neither does.
        unique_future<core::error_t> commit_inserts(session_id_t session, uint64_t txn_id, uint64_t commit_id);
        unique_future<core::error_t> commit_deletes(session_id_t session, uint64_t txn_id, uint64_t commit_id);
        unique_future<core::error_t> revert_inserts(session_id_t session, uint64_t txn_id);
        unique_future<core::error_t> revert_deletes(session_id_t session, uint64_t txn_id);
        unique_future<core::result_wrapper_t<std::pmr::vector<int64_t>>>
        read_rows(session_id_t session, components::expressions::compare_type compare, value_t key, uint64_t txn_id);
        unique_future<core::error_t> force_flush(session_id_t session);

        // Bound in the contract's order, so msg_id is the same number across both families --
        // the only reason manager_index_t may send to a bare address.
        using dispatch_traits = actor_zeta::implements<index_agent_contract,
                                                       &btree_index_agent_t::drop,
                                                       &btree_index_agent_t::clear,
                                                       &btree_index_agent_t::stage_inserts,
                                                       &btree_index_agent_t::stage_deletes,
                                                       &btree_index_agent_t::commit_inserts,
                                                       &btree_index_agent_t::commit_deletes,
                                                       &btree_index_agent_t::revert_inserts,
                                                       &btree_index_agent_t::revert_deletes,
                                                       &btree_index_agent_t::read_rows,
                                                       &btree_index_agent_t::force_flush>;

        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

    private:
        log_t log_;
        components::catalog::oid_t table_oid_;
        // By value, not unique_ptr: this agent is the sole owner and the type is fixed at
        // compile time, which is what keeps the ordered scan_range contract reachable
        // without a runtime question.
        btree_index_disk_t store_;
        bool is_dropped_{false};

        // Per-transaction buckets of uncommitted rows. Keys are kept encoded in the b+tree's
        // own record format (codec::append_logical_value) so a staged key compares the same
        // way the committed half does, via the same decoder the tree's key getter uses.
        //
        // Bucket 0 is "committed for everyone but not yet durable": the rebuild feed stages
        // into it, and commit_inserts publishes it alongside whatever transaction is committing.
        using pending_row_t = std::pair<std::pmr::string, int64_t>;
        using pending_rows_t = std::pmr::vector<pending_row_t>;
        using pending_txn_map_t = std::pmr::unordered_map<uint64_t, pending_rows_t>;
        pending_txn_map_t pending_inserts_;
        pending_txn_map_t pending_deletes_;

        // No normalization: the tree stores and compares the column's own type, which is the
        // hashed family's step, not this one's.
        [[nodiscard]] std::pmr::string encode_key(const value_t& key) const;

        // Publish one bucket pair ({txn_id} and 0) into the tree and erase them. Shared by
        // the insert and delete legs, which differ only in which map they take from and
        // which store call they make -- that difference is the `apply` callable.
        template<typename ApplyFn>
        [[nodiscard]] core::error_t publish_buckets(pending_txn_map_t& buckets, uint64_t txn_id, ApplyFn&& apply);
    };

    // Catches a forgotten/mistyped/misordered handler here, not at the send site in manager_index.cpp.
    static_assert(index_agent_impl<btree_index_agent_t>,
                  "btree_index_agent_t does not satisfy the index agent contract");

    using btree_index_agent_ptr = btree_index_agent_t::agent_ptr_t;

} // namespace services::index
