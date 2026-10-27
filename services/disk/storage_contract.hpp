#pragma once

#include <actor-zeta/actor/address.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/future.hpp>

#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/execution_context.hpp>
#include <components/context/host_rows.hpp>
#include <components/logical_plan/detached_block.hpp>
#include <components/session/session.hpp>
#include <components/table/column_definition.hpp>
#include <components/table/row_version_manager.hpp>
#include <components/table/storage_address.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/storage_parameters.hpp>
#include <core/executor.hpp>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <set>
#include <vector>

// What every storage of a table understands: otterbrix's disk and a host's backend actor alike (docs/embedding-host-api.md,
// "The backend actor"). A table's operators reach its storage only through these messages, by the table's address.
namespace services::disk {

    using session_id_t = components::session::session_id_t;
    using execution_context_t = components::execution_context_t;

    // Reply of storage_fetch_next_batch and storage_run_block: OPEN returns the minted cursor_id, never 0 (as the
    // disk's and PostgreSQL's portals); a drained cursor replies an EMPTY chunk. A page with cursor_id 0 is refused.
    struct fetch_batch_t {
        std::unique_ptr<components::vector::data_chunk_t> batch;
        uint64_t cursor_id{0};

        fetch_batch_t() = default;
        fetch_batch_t(std::unique_ptr<components::vector::data_chunk_t>&& b, uint64_t id)
            : batch(std::move(b))
            , cursor_id(id) {}
    };

    // What one write did. `awaits_publish`: the rows wait for otterbrix's MVCC publication at COMMIT (the disk);
    // a storage that commits them itself answers false. `returned`: when the write asked for them, the values of every
    // column of each written row (or of each deleted row, as it was), in the order the rows were sent.
    struct write_receipt_t {
        int64_t start_row{0};
        uint64_t count{0};
        bool awaits_publish{false};
        std::pmr::vector<components::vector::data_chunk_t> returned;
    };

    // UPDATE's receipt; `written` / `attoids` are the new versions as the disk journals them (empty for a storage
    // that keeps no otterbrix journal).
    struct update_receipt_t {
        write_receipt_t receipt;
        std::pmr::vector<components::vector::data_chunk_t> written;
        std::pmr::vector<std::uint32_t> attoids;
    };

    using host_rows_t = components::host_rows_t;

    // `awaits_publish`: the change waits for otterbrix's COMMIT (the disk); a storage that commits it itself answers
    // false and is a participant of the transaction.
    struct ddl_reply_t {
        bool awaits_publish{false};
        host_rows_t rows;
    };

    // One relation node of an offered block the storage takes: its preorder position in the block, the storage's
    // own description of how to run it, the lines EXPLAIN prints under it, and the WHERE conjuncts (by their position
    // in the node's WHERE) the storage leaves to otterbrix.
    struct block_take_t {
        uint32_t preorder{0};
        std::pmr::string request;
        std::pmr::vector<std::pmr::string> explain;
        std::pmr::vector<uint32_t> residual;
    };

    // A write message (storage_append, storage_update, storage_delete_rows) to a connection's table carries at most
    // the connection's max_rows_per_write rows (backend_address_t); the backend may run it as several remote
    // statements of the same transaction.
    struct storage_contract {
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        // `cursor_id` 0 opens. `projected_cols` empty: every column; a column outside the set is a buffer-less stub.
        unique_future<core::result_wrapper_t<fetch_batch_t>>
        storage_fetch_next_batch(session_id_t session,
                                 components::catalog::oid_t table_oid,
                                 uint64_t cursor_id,
                                 int64_t limit,
                                 std::vector<size_t> projected_cols,
                                 components::table::transaction_data txn);
        unique_future<void>
        storage_close_cursor(session_id_t session, components::catalog::oid_t table_oid, uint64_t cursor_id);
        // An estimate is an answer too: it only orders the sides of a hash join.
        unique_future<core::result_wrapper_t<uint64_t>> storage_total_rows(session_id_t session,
                                                                           components::catalog::oid_t table_oid);
        // `attoids` name the chunk's columns: the ones the statement wrote, the others the storage fills itself.
        unique_future<core::result_wrapper_t<write_receipt_t>>
        storage_append(execution_context_t ctx,
                       components::catalog::oid_t table_oid,
                       std::pmr::vector<components::catalog::oid_t> attoids,
                       std::pmr::vector<components::vector::data_chunk_t> data,
                       bool returning);
        unique_future<core::result_wrapper_t<update_receipt_t>>
        storage_update(execution_context_t ctx,
                       components::catalog::oid_t table_oid,
                       std::pmr::vector<components::vector::vector_t> row_ids,
                       std::pmr::vector<components::vector::data_chunk_t> data,
                       bool returning);
        // A count smaller than requested is legitimate (a row this transaction already deleted).
        unique_future<core::result_wrapper_t<write_receipt_t>> storage_delete_rows(execution_context_t ctx,
                                                                                   components::catalog::oid_t table_oid,
                                                                                   components::vector::vector_t row_ids,
                                                                                   uint64_t count,
                                                                                   bool returning);
        unique_future<core::result_wrapper_t<ddl_reply_t>>
        create_storage_disk(execution_context_t ctx,
                            components::catalog::oid_t table_oid,
                            components::catalog::oid_t database_oid,
                            qualified_name_t name,
                            std::vector<components::table::column_definition_t> columns,
                            bool is_computed);
        // DROP of tables, in the statement; ctx.txn's id stands for the commit until COMMIT.
        unique_future<core::result_wrapper_t<ddl_reply_t>>
        mark_storage_dropped_many(execution_context_t ctx,
                                  std::pmr::vector<components::catalog::oid_t> table_oids,
                                  std::pmr::vector<qualified_name_t> names);
        unique_future<core::result_wrapper_t<ddl_reply_t>>
        add_storage_column(execution_context_t ctx,
                           components::catalog::oid_t table_oid,
                           components::table::column_definition_t column);
        unique_future<core::result_wrapper_t<ddl_reply_t>> stamp_column_dropped(execution_context_t ctx,
                                                                                components::catalog::oid_t table_oid,
                                                                                components::catalog::oid_t attoid);
        // ctx.txn's COMMIT of `tables`: the last step that may refuse.
        unique_future<core::error_t> storage_prepare(execution_context_t ctx,
                                                     std::pmr::set<components::catalog::oid_t> tables);
        // ctx.txn ended: what it kept for `tables` goes.
        unique_future<void> storage_release_prepared(execution_context_t ctx,
                                                     std::pmr::set<components::catalog::oid_t> tables);
        // ROLLBACK of ctx.txn (from_sub_id 0) or ROLLBACK TO its savepoint `from_sub_id`: the writes of that level and
        // above to `tables` are undone.
        unique_future<core::error_t> abort_transaction(execution_context_t ctx,
                                                       uint64_t from_sub_id,
                                                       std::pmr::set<components::catalog::oid_t> tables);

        // The host's part: the disk is never offered a block.
        // Which relation nodes of `block` the storage takes; `parameters` are the statement's, a correlated one
        // (kind execution) is known only when the node runs.
        unique_future<core::result_wrapper_t<std::pmr::vector<block_take_t>>>
        storage_offer(execution_context_t ctx,
                      components::logical_plan::detached_block_t block,
                      components::vector::storage_parameters parameters);
        // A taken read: `cursor_id` 0 runs `request` with `parameters` (a rescan runs it again); a drained cursor
        // replies an EMPTY chunk.
        unique_future<core::result_wrapper_t<fetch_batch_t>>
        storage_run_block(execution_context_t ctx,
                          std::pmr::string request,
                          uint64_t cursor_id,
                          components::vector::storage_parameters parameters);
        unique_future<void> storage_close_block(execution_context_t ctx, uint64_t cursor_id);
        // A taken UPDATE / DELETE: the receipt counts the rows it changed.
        unique_future<core::result_wrapper_t<write_receipt_t>>
        storage_run_write(execution_context_t ctx,
                          std::pmr::string request,
                          components::vector::storage_parameters parameters,
                          bool returning);

        // The order IS the message-id space; disk_contract repeats storage_contract's prefix first, number for
        // number (static_assert below).
        using dispatch_traits = actor_zeta::dispatch_traits<&storage_contract::storage_fetch_next_batch,
                                                            &storage_contract::storage_close_cursor,
                                                            &storage_contract::storage_total_rows,
                                                            &storage_contract::storage_append,
                                                            &storage_contract::storage_update,
                                                            &storage_contract::storage_delete_rows,
                                                            &storage_contract::create_storage_disk,
                                                            &storage_contract::mark_storage_dropped_many,
                                                            &storage_contract::add_storage_column,
                                                            &storage_contract::stamp_column_dropped,
                                                            &storage_contract::storage_prepare,
                                                            &storage_contract::storage_release_prepared,
                                                            &storage_contract::abort_transaction,
                                                            &storage_contract::storage_offer,
                                                            &storage_contract::storage_run_block,
                                                            &storage_contract::storage_close_block,
                                                            &storage_contract::storage_run_write>;

        // The methods the disk shares: positions 0 .. prefix_size - 1.
        static constexpr std::size_t prefix_size = 13;

        storage_contract() = delete;
    };

} // namespace services::disk

namespace actor_zeta::otterbrix {

    template<>
    inline constexpr bool needs_typed_address<services::disk::storage_contract> = true;

    // The only send to a table's storage: a bare actor address does not compile. An empty address (a connection the
    // host's map lost) is a caller's bug.
    template<auto MethodPtr, typename... Args>
        requires std::is_same_v<typename type_traits::callable_trait<decltype(MethodPtr)>::class_type,
                                services::disk::storage_contract>
    [[nodiscard]] inline auto send(const components::table::storage_address_t& target, Args&&... args) {
        using contract = services::disk::storage_contract;
        static_assert(contract_declares_method<MethodPtr, typename contract::dispatch_traits::methods>::value,
                      "send<&storage_contract::method>(): the method is not in the contract's dispatch_traits list");
        constexpr uint64_t action_id =
            action_id_impl<contract, MethodPtr, typename contract::dispatch_traits::methods>::value;
        if (target.empty()) {
            std::fputs("actor_zeta::otterbrix::send: a table's storage message to an empty address\n", stderr);
            std::abort();
        }
        return detail::dispatch_method_impl_address<contract, MethodPtr, action_id>(target.actor(),
                                                                                    std::forward<Args>(args)...);
    }

} // namespace actor_zeta::otterbrix
