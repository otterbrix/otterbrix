#include "transfer_scan.hpp"
#include "guard_chunk.hpp"

#include <services/disk/manager_disk.hpp>

#include <utility>
#include <vector>

namespace components::operators {

    transfer_scan::transfer_scan(std::pmr::memory_resource* resource,
                                 components::catalog::oid_t table_oid,
                                 table::storage_address_t address,
                                 std::pmr::string name,
                                 logical_plan::row_cap_t cap,
                                 std::vector<size_t> projected_cols)
        : read_only_operator_t(resource, log_t{}, operator_type::transfer_scan)
        , table_oid_(table_oid)
        , address_(std::move(address))
        , name_(std::move(name), resource)
        , cap_(cap)
        , projected_cols_(std::move(projected_cols)) {}

    std::pmr::string transfer_scan::explain_label_impl() const {
        // explain_plan names the relation after the line, as for every scan.
        return foreign_label(resource_, address_, "Foreign Scan", {});
    }

    actor_zeta::unique_future<core::error_t> transfer_scan::open_impl(pipeline::context_t* ctx) {
        opened_ = true;
        if (table_oid_ == components::catalog::INVALID_OID) {
            co_return core::error_t::no_error();
        }
        // The read-cap (offset+limit) is pushed down as the scan's row-count cap; SELECT OFFSET is applied by
        // operator_limit above.
        ctx->join_transaction(address_, table_oid_, false);
        auto [_s, sf] =
            actor_zeta::otterbrix::send<&services::disk::storage_contract::storage_fetch_next_batch>(address_,
                                                                                                     ctx->session,
                                                                                                     table_oid_,
                                                                                                     uint64_t{0},
                                                                                                     cap_.to_disk(),
                                                                                                     projected_cols_,
                                                                                                     ctx->txn);
        auto reply = co_await std::move(sf);
        if (reply.has_error()) {
            set_error(reply.error());
            mark_failed();
            co_return reply.error();
        }
        cursor_id_ = reply.value().cursor_id;
        first_batch_ = std::move(reply.value().batch);
        if (cursor_id_ == 0 && first_batch_ && first_batch_->size() != 0) {
            std::pmr::string what{"the storage of table \"", resource_};
            what += name_;
            what += "\" answered a page without a cursor";
            core::error_t refused{core::error_code_t::invalid_parameter, std::move(what)};
            set_error(refused);
            mark_failed();
            co_return refused;
        }
        co_return core::error_t::no_error();
    }

    // --- Push-based streaming pipeline source (PER-BATCH FETCH-NEXT, bounded) ---
    // open_impl OPENs a position-only cursor (no filter); each later call ADVANCEs it, one batch
    // each. At most one cross-actor fetch await per call, sequential across calls in this nested
    // operator coroutine — no lost-wakeup. Peak scan memory = one batch.
    actor_zeta::unique_future<void> transfer_scan::release_cursor(pipeline::context_t* ctx) {
        if (cursor_id_ == 0 || drained_) {
            co_return;
        }
        const uint64_t id = cursor_id_;
        cursor_id_ = 0;
        drained_ = true;
        auto [_s, cf] =
            actor_zeta::otterbrix::send<&services::disk::storage_contract::storage_close_cursor>(address_,
                                                                                                 ctx->session,
                                                                                                 table_oid_,
                                                                                                 id);
        co_await std::move(cf);
        co_return;
    }

    actor_zeta::unique_future<core::result_wrapper_t<std::optional<vector::data_chunk_t>>>
    transfer_scan::source_next(pipeline::context_t* ctx) {
        if (drained_) {
            co_return std::nullopt;
        }

        // No-table sentinel (no-FROM SELECT): emit ONE synthetic single-row batch that
        // carries one placeholder column, then drain. operator_select_t projects its constant/arithmetic columns over
        // this one row to produce the single constants row (the placeholder is ignored). No disk round-trip.
        if (table_oid_ == components::catalog::INVALID_OID) {
            drained_ = true;
            std::pmr::vector<types::complex_logical_type> types(resource_);
            types.emplace_back(types::logical_type::BOOLEAN);
            vector::data_chunk_t row{resource_, types, 1};
            row.set_cardinality(1);
            co_return std::move(row);
        }

        if (first_batch_) {
            co_return emit_or_skip(std::move(first_batch_));
        }

#ifdef DEV_MODE
        // Between-batches pause gate (see services::disk::scan_advance_gate_t). Polling by one
        // cross-actor round-trip per ask keeps every mailbox free while the gate holds: the
        // await parks this nested coroutine, it never blocks an actor thread.
        while (true) {
            auto* gate = services::disk::dev_scan_advance_gate();
            if (gate == nullptr || !gate->hold(table_oid_, cursor_id_)) {
                break;
            }
            auto [_g, gf] =
                actor_zeta::otterbrix::send<&services::disk::storage_contract::storage_total_rows>(address_,
                                                                                                   ctx->session,
                                                                                                   table_oid_);
            auto ping = co_await std::move(gf);
            if (ping.has_error()) {
                // The poll target is gone — the world is tearing down; holding forever
                // would hang it. Let the fetch below answer for the cursor.
                break;
            }
        }
#endif
        // ADVANCE.
        auto [_s, sf] =
            actor_zeta::otterbrix::send<&services::disk::storage_contract::storage_fetch_next_batch>(address_,
                                                                                                     ctx->session,
                                                                                                     table_oid_,
                                                                                                     cursor_id_,
                                                                                                     int64_t{-1},
                                                                                                     projected_cols_,
                                                                                                     ctx->txn);
        auto fetch_result = co_await std::move(sf);
        if (fetch_result.has_error()) {
            set_error(fetch_result.error());
            mark_failed();
            co_return fetch_result.convert_error<std::optional<vector::data_chunk_t>>();
        }
        co_return emit_or_skip(std::move(fetch_result.value().batch));
    }

    // The drained empty-guard for one fetched batch. (OFFSET is applied by operator_limit above; a scan never skips
    // rows.)
    core::result_wrapper_t<std::optional<vector::data_chunk_t>>
    transfer_scan::emit_or_skip(std::unique_ptr<vector::data_chunk_t> batch) {
        const uint64_t sz = batch ? batch->size() : 0;

        if (sz == 0) {
            drained_ = true;
            if (!emitted_any_) {
                emitted_any_ = true;
                return make_guard_chunk(resource_, page_types_, {});
            }
            return std::optional<vector::data_chunk_t>{};
        }
        if (auto refused = check_chunk_width(resource_, *batch); refused.contains_error()) {
            return refused;
        }
        if (auto refused = check_scan_page(resource_, name_, page_types_, *batch); refused.contains_error()) {
            return refused;
        }
        emitted_any_ = true;
        return std::optional<vector::data_chunk_t>{vector::adopted(resource_, std::move(*batch))};
    }

} // namespace components::operators
