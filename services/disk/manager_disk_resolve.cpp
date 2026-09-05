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

    // The four resolve_* readers below flow through this funnel; the C++-side row
    // filtering stays in each caller (it differs per table: equality on name,
    // name-match collect, enumerate).
    //
    // A READ THAT COULD NOT BE PERFORMED IS AN ERROR, NEVER AN EMPTY ANSWER. All three legs
    // below used to return an empty batch list and a comment called that a fallback "matching
    // the no-agent / not-owned fallbacks above". An empty batch list is also exactly what "no
    // matching rows" looks like, so every reader upstream turned an unreadable catalog into a
    // negative fact about it: no such namespace, no such function, no such cast. Same shape,
    // same words, as read_chunks_by_key three functions below.
    manager_disk_t::unique_future<core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>>
    manager_disk_t::scan_table(components::catalog::oid_t table_oid,
                               std::unique_ptr<components::table::table_filter_t> filter,
                               std::vector<std::size_t> projected_cols,
                               components::table::transaction_data txn) {
        if (agents_.empty()) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"scan_table: no disk agents", resource()}};
        }
        const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
        auto& agent = agents_[idx];
        if (agent == nullptr) {
            co_return core::error_t{core::error_code_t::io_error,
                                    std::pmr::string{"scan_table: owning disk agent is null", resource()}};
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
        // The agent reply carries the scan_error (buffer-pool OOM, data_corruption, a block the
        // device would not give back). Pass it through untouched — the reader that asked is the
        // one that has to fail, and it needs the reason, not a synthesized one.
        co_return co_await std::move(fut);
    }

    // ctx.txn, NOT the default snapshot — the same fix resolve_function_by_name and
    // find_cast_oid below already carry. On transaction_data{} this scan saw only committed
    // rows, so a namespace created inside an open transaction was invisible to ITS OWN
    // resolve and one dropped in it still answered found=true; any verdict built on the
    // negative ("no such namespace" collision checks, follow-up DDL in the txn) read a lie.
    // A zero-txn ctx carries transaction_data{0,...}, which sees exactly the committed
    // state — the behaviour every existing caller had.
    manager_disk_t::unique_future<core::result_wrapper_t<resolve_namespace_result_t>>
    manager_disk_t::resolve_namespace(execution_context_t ctx, std::string name, std::uint64_t /*since_version*/) {
        resolve_namespace_result_t out(resource());

        auto batches_r = co_await scan_table(pg_namespace_oid_tbl,
                                             std::unique_ptr<components::table::table_filter_t>{},
                                             std::vector<std::size_t>{0, 1},
                                             ctx.txn);
        if (batches_r.has_error()) {
            co_return batches_r.convert_error<resolve_namespace_result_t>();
        }
        for (auto& chunk : batches_r.value()) {
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

    // ctx.txn, NOT the default snapshot. operator_unregister_udf_t reads this answer and then
    // scrubs each m.oid through delete_pg_catalog_rows_many, treating a spec that deleted
    // nothing as a refusal — "the function is still in the catalog". That verdict is only sound
    // while the two see the same pg_proc: the delete's scan carries the caller's transaction
    // (agent_disk_t::delete_pg_catalog_rows_inner), so a read on transaction_data{} would list
    // rows this transaction has already deleted, and the scrub of a row that is gone would be
    // reported as a catalog that refused to give it up.
    manager_disk_t::unique_future<core::result_wrapper_t<std::pmr::vector<resolve_function_result_t>>>
    manager_disk_t::resolve_function_by_name(execution_context_t ctx,
                                             std::string name,
                                             std::uint64_t /*since_version*/) {
        std::pmr::vector<resolve_function_result_t> out(resource());
        auto batches_r = co_await scan_table(pg_proc_oid,
                                             std::unique_ptr<components::table::table_filter_t>{},
                                             std::vector<std::size_t>{0, 1, 2, 3, 4, 5, 6},
                                             ctx.txn);
        if (batches_r.has_error()) {
            co_return batches_r.convert_error<std::pmr::vector<resolve_function_result_t>>();
        }
        for (auto& chunk : batches_r.value()) {
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

    // INVALID_OID stays the in-band "there is no such cast" INSIDE the wrapper: DROP CAST has
    // to tell "no pg_cast row exists" (do_not_exists) from "the read failed", and collapsing
    // the former into an error would destroy exactly that distinction.
    //
    // ctx.txn, for the reason given on resolve_function_by_name above: operator_unregister_cast_t
    // turns THIS oid into a delete spec and reads a zero count as "the cast is still in the
    // catalog". The delete sees the caller's transaction, so this read has to as well — both so
    // a cast created in the open transaction can be found at all, and so one already dropped in
    // it is reported as absent (do_not_exists) instead of as a scrub that was refused.
    manager_disk_t::unique_future<core::result_wrapper_t<components::catalog::oid_t>>
    manager_disk_t::find_cast_oid(execution_context_t ctx,
                                  components::catalog::oid_t source_oid,
                                  components::catalog::oid_t target_oid) {
        auto batches_r = co_await scan_table(pg_cast_oid,
                                             std::unique_ptr<components::table::table_filter_t>{},
                                             std::vector<std::size_t>{0, 1, 2},
                                             ctx.txn);
        if (batches_r.has_error()) {
            co_return batches_r.convert_error<components::catalog::oid_t>();
        }
        for (auto& chunk : batches_r.value()) {
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

    // ctx.txn, for the reason on resolve_namespace above: the enumeration must show a txn
    // its own uncommitted namespaces and hide the ones it dropped, or the next verdict
    // built on this list lies the same way the resolve did.
    manager_disk_t::unique_future<core::result_wrapper_t<std::pmr::vector<std::string>>>
    manager_disk_t::list_namespaces(execution_context_t ctx) {
        std::pmr::vector<std::string> out(resource());
        auto batches_r = co_await scan_table(pg_namespace_oid_tbl,
                                             std::unique_ptr<components::table::table_filter_t>{},
                                             std::vector<std::size_t>{0, 1},
                                             ctx.txn);
        if (batches_r.has_error()) {
            co_return batches_r.convert_error<std::pmr::vector<std::string>>();
        }
        for (auto& chunk : batches_r.value()) {
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
