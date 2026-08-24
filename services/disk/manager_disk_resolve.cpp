#include "manager_disk_impl.hpp"

namespace services::disk {

    using namespace core::filesystem;
    namespace catalog = components::catalog;
    using namespace detail;

    // Every catalog read here goes through scan_table → agent-0's
    // storage_scan_inner (catalog oids route to agent-0). Reading via the
    // mailbox — not a borrowed storage_entry_sync pointer — serialises against
    // agent-0's compact path (checkpoint/vacuum/maybe_cleanup_inner) running on the
    // scheduler_disk_ threads, avoiding a borrowed-pointer race. transaction_data{}
    // = "see all committed".

    // The three resolve_* readers below flow through this funnel; the C++-side row
    // filtering stays in each caller (it differs per table: equality on name,
    // name-match collect, enumerate).
    manager_disk_t::unique_future<std::pmr::vector<components::vector::data_chunk_t>>
    manager_disk_t::scan_table(components::catalog::oid_t table_oid,
                               std::unique_ptr<components::table::table_filter_t> filter,
                               std::vector<std::size_t> projected_cols,
                               components::table::transaction_data txn) {
        std::pmr::vector<components::vector::data_chunk_t> empty(resource());
        if (agents_.empty()) {
            co_return empty;
        }
        const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
        auto& agent = agents_[idx];
        if (agent == nullptr) {
            co_return empty;
        }
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                              &agent_disk_t::storage_scan_inner,
                                                              table_oid,
                                                              std::move(filter),
                                                              int64_t{-1},
                                                              std::move(projected_cols),
                                                              txn);
        if (needs_sched) {
            scheduler_disk_->enqueue(agent.get());
        }
        // Catalog-read funnel: the agent reply carries the scan_error. A buffer-pool OOM on a
        // catalog scan degrades to an empty batch set here, matching the no-agent / not-owned
        // fallbacks above (resolve callers already tolerate empty).
        auto scan_r = co_await std::move(fut);
        if (scan_r.has_error()) {
            co_return empty;
        }
        co_return std::move(scan_r.value());
    }

    manager_disk_t::unique_future<resolve_namespace_result_t>
    manager_disk_t::resolve_namespace(execution_context_t /*ctx*/, std::string name, std::uint64_t /*since_version*/) {
        resolve_namespace_result_t out(resource());

        auto batches = co_await scan_table(pg_namespace_oid_tbl,
                                           std::unique_ptr<components::table::table_filter_t>{},
                                           std::vector<std::size_t>{0, 1});
        for (auto& chunk : batches) {
            bool stop = false;
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (chunk.is_null(0, i) || chunk.is_null(1, i))
                    continue;
                if (chunk.get_value<std::string_view>(1, i) != name)
                    continue;
                out.found = true;
                out.oid = static_cast<components::catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
                out.name = name;
                stop = true;
                break;
            }
            if (stop)
                break;
        }
        co_return out;
    }

    manager_disk_t::unique_future<std::pmr::vector<resolve_function_result_t>>
    manager_disk_t::resolve_function_by_name(execution_context_t /*ctx*/,
                                             std::string name,
                                             std::uint64_t /*since_version*/) {
        std::pmr::vector<resolve_function_result_t> out(resource());
        auto batches = co_await scan_table(pg_proc_oid,
                                           std::unique_ptr<components::table::table_filter_t>{},
                                           std::vector<std::size_t>{0, 1, 2, 3, 4, 5, 6});
        for (auto& chunk : batches) {
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (!str_equals(chunk.value(1, i), name))
                    continue;
                resolve_function_result_t r(resource());
                r.found = true;
                r.name = name;
                r.oid = static_cast<components::catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
                if (!chunk.is_null(2, i))
                    r.namespace_oid = static_cast<components::catalog::oid_t>(chunk.get_value<std::uint32_t>(2, i));
                if (!chunk.is_null(3, i))
                    r.pronargs = chunk.get_value<std::int32_t>(3, i);
                if (!chunk.is_null(4, i))
                    r.prouid = chunk.get_value<std::uint64_t>(4, i);
                if (!chunk.is_null(5, i))
                    r.proargmatchers = std::string(chunk.get_value<std::string_view>(5, i));
                if (!chunk.is_null(6, i))
                    r.prorettype = std::string(chunk.get_value<std::string_view>(6, i));
                out.push_back(std::move(r));
            }
        }
        co_return out;
    }

    manager_disk_t::unique_future<components::catalog::oid_t>
    manager_disk_t::find_cast_oid(execution_context_t /*ctx*/,
                                  components::catalog::oid_t source_oid,
                                  components::catalog::oid_t target_oid) {
        auto batches = co_await scan_table(pg_cast_oid,
                                           std::unique_ptr<components::table::table_filter_t>{},
                                           std::vector<std::size_t>{0, 1, 2});
        for (auto& chunk : batches) {
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (chunk.is_null(1, i) || chunk.is_null(2, i)) {
                    continue;
                }
                const auto castsource = static_cast<components::catalog::oid_t>(chunk.get_value<std::uint32_t>(1, i));
                const auto casttarget = static_cast<components::catalog::oid_t>(chunk.get_value<std::uint32_t>(2, i));
                if (castsource == source_oid && casttarget == target_oid) {
                    co_return static_cast<components::catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
                }
            }
        }
        co_return components::catalog::INVALID_OID;
    }

    manager_disk_t::unique_future<std::pmr::vector<std::string>>
    manager_disk_t::list_namespaces(execution_context_t /*ctx*/) {
        std::pmr::vector<std::string> out(resource());
        auto batches = co_await scan_table(pg_namespace_oid_tbl,
                                           std::unique_ptr<components::table::table_filter_t>{},
                                           std::vector<std::size_t>{0, 1});
        for (auto& chunk : batches) {
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (!chunk.is_null(1, i)) {
                    out.emplace_back(std::string(chunk.get_value<std::string_view>(1, i)));
                }
            }
        }
        co_return out;
    }

    // --- Direct replay methods (synchronous, no MVCC, for physical WAL replay) ---

    manager_disk_t::unique_future<std::vector<components::catalog::oid_t>>
    manager_disk_t::allocate_oids_batch(std::size_t count) {
        std::vector<components::catalog::oid_t> batch;
        batch.reserve(count);
        for (std::size_t i = 0; i < count; ++i) {
            batch.push_back(oid_gen_.allocate());
        }
        co_return batch;
    }

    // Batched keyed scan for one table_oid. Every key routes to the SAME owning
    // agent (keyed by table_oid), so the per-key loop runs intra-agent: one
    // scan_by_keys_inner message carries the whole batch and the agent resolves the
    // shared key column names to indices once. result[i] corresponds to keys[i].
    manager_disk_t::unique_future<core::result_wrapper_t<std::pmr::vector<std::pmr::vector<std::int64_t>>>>
    manager_disk_t::scan_by_keys(execution_context_t ctx,
                                 components::catalog::oid_t table_oid,
                                 std::pmr::vector<std::string> key_col_names,
                                 components::vector::data_chunk_t keys) {
        std::pmr::vector<std::pmr::vector<std::int64_t>> out(resource());
        // INVARIANT on SUCCESS: result.size() == keys.size() — one (possibly empty) row per
        // input key, in input order, so result[i] always maps to keys[i]. Consumers
        // (operator_fk_check / operator_fk_cascade) index result[i] positionally and treat an
        // empty row as "no parent match", so a short outer vector would silently skip checks —
        // and a routing failure reported as keys.size() empty rows is a constraint check that
        // passes because the check could not run. Hence the errors below. Zero keys is not a
        // failure: an empty request has an empty answer, and keys.size() == 0 keeps the invariant.
        if (keys.empty()) {
            co_return out;
        }
        if (key_col_names.empty()) {
            co_return core::error_t{core::error_code_t::invalid_parameter,
                                    std::pmr::string{"scan_by_keys: no key columns given", resource()}};
        }
        if (agents_.empty()) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"scan_by_keys: no disk agents", resource()}};
        }
        const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
        auto& agent = agents_[idx];
        if (agent == nullptr) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"scan_by_keys: owning disk agent is null", resource()}};
        }

        auto [scan_ns, scan_fut] = actor_zeta::otterbrix::send(agent->address(),
                                                               &agent_disk_t::scan_by_keys_inner,
                                                               table_oid,
                                                               std::move(key_col_names),
                                                               std::move(keys),
                                                               ctx.txn);
        if (scan_ns) {
            scheduler_disk_->enqueue(agent.get());
        }
        co_return co_await std::move(scan_fut);
    }

    manager_disk_t::unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
    manager_disk_t::read_chunks_by_key(execution_context_t ctx,
                                       components::catalog::oid_t table_oid,
                                       std::pmr::vector<std::uint64_t> key_col_indices,
                                       components::vector::data_chunk_t keys,
                                       std::pmr::vector<std::uint64_t> projected_cols) {
        // Thin router: the caller passes storage column ORDINALS and the eq-AND filtered
        // scan runs intra-agent in read_chunks_by_key_inner (no row-major flatten, no
        // column-name resolution hop at all). Callers read cells via chunk.value(col, row).
        // These used to return an empty vector, which the resolve operators read as
        // "no such row" — a misrouted or agent-less read then surfaced as "Database does
        // not exist". A read that never ran is an error.
        if (key_col_indices.empty()) {
            co_return core::error_t{core::error_code_t::invalid_parameter,
                                    std::pmr::string{"read_chunks_by_key: no key columns given", resource()}};
        }
        if (agents_.empty()) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"read_chunks_by_key: no disk agents", resource()}};
        }
        const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
        auto& agent = agents_[idx];
        if (agent == nullptr) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"read_chunks_by_key: owning disk agent is null", resource()}};
        }

        auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                              &agent_disk_t::read_chunks_by_key_inner,
                                                              table_oid,
                                                              std::move(key_col_indices),
                                                              std::move(keys),
                                                              std::move(projected_cols),
                                                              ctx.txn);
        if (needs_sched) {
            scheduler_disk_->enqueue(agent.get());
        }
        co_return co_await std::move(fut);
    }

    manager_disk_t::unique_future<
        core::result_wrapper_t<std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>>>>
    manager_disk_t::read_chunks_by_keys(execution_context_t ctx,
                                        components::catalog::oid_t table_oid,
                                        std::pmr::vector<std::uint64_t> key_col_indices,
                                        components::vector::data_chunk_t keys,
                                        std::pmr::vector<std::uint64_t> projected_cols) {
        // Thin router for the multi-key batch: the caller passes storage column ORDINALS and the
        // filtered scan runs intra-agent in read_chunks_by_keys_inner (one mailbox hop for the
        // whole batch). INVARIANT on SUCCESS: result.size() == keys.size() — one (possibly
        // empty) entry per input key, in input order, so result[i] always maps to keys[i], which
        // is how consumers index it.
        // Routing failures are NOT keys.size() empty entries — that shape is indistinguishable
        // from "every key matched nothing", which is how a failed FK attribute read silently
        // produced empty fk.child_col_names.
        std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>> out(resource());
        if (keys.empty()) {
            co_return out;
        }
        if (key_col_indices.empty()) {
            co_return core::error_t{core::error_code_t::invalid_parameter,
                                    std::pmr::string{"read_chunks_by_keys: no key columns given", resource()}};
        }
        if (agents_.empty()) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"read_chunks_by_keys: no disk agents", resource()}};
        }
        const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
        auto& agent = agents_[idx];
        if (agent == nullptr) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"read_chunks_by_keys: owning disk agent is null", resource()}};
        }

        auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                              &agent_disk_t::read_chunks_by_keys_inner,
                                                              table_oid,
                                                              std::move(key_col_indices),
                                                              std::move(keys),
                                                              std::move(projected_cols),
                                                              ctx.txn);
        if (needs_sched) {
            scheduler_disk_->enqueue(agent.get());
        }
        co_return co_await std::move(fut);
    }

} // namespace services::disk
