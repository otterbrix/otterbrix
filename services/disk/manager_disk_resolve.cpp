#include "manager_disk_impl.hpp"

namespace services::disk {

    using namespace core::filesystem;
    namespace catalog = components::catalog;
    using namespace detail;

    // Every catalog read here goes through agent-0's storage_scan_batched_inner
    // (catalog oids route to agent-0). Reading via the mailbox — not a borrowed
    // storage_entry_sync pointer — serialises against agent-0's compact path
    // (checkpoint/vacuum/maybe_cleanup_inner) running on the scheduler_disk_
    // threads, avoiding a borrowed-pointer race. transaction_data{} = "see all
    // committed". read_chunks_by_key is a thin router: the column NAME→index
    // resolution and the eq-AND filtered scan both run intra-agent in
    // read_chunks_by_key_inner.

    manager_disk_t::unique_future<resolve_namespace_result_t>
    manager_disk_t::resolve_namespace(execution_context_t /*ctx*/, std::string name, std::uint64_t /*since_version*/) {
        resolve_namespace_result_t out(resource());

        if (!agents_.empty() && agents_[0] != nullptr) {
            const std::size_t idx = pool_idx_for_oid(pg_namespace_oid_tbl, agents_.size());
            std::vector<size_t> projected{0, 1};
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agents_[idx]->address(),
                                                                  &agent_disk_t::storage_scan_batched_inner,
                                                                  pg_namespace_oid_tbl,
                                                                  std::unique_ptr<components::table::table_filter_t>{},
                                                                  int64_t{-1},
                                                                  std::move(projected),
                                                                  components::table::transaction_data{});
            if (needs_sched) {
                scheduler_disk_->enqueue(agents_[idx].get());
            }
            auto batches = co_await std::move(fut);
            for (auto& chunk : batches) {
                bool stop = false;
                for (uint64_t i = 0; i < chunk.size(); ++i) {
                    auto oid_v = chunk.value(0, i);
                    auto name_v = chunk.value(1, i);
                    if (oid_v.is_null() || name_v.is_null())
                        continue;
                    if (name_v.value<std::string_view>() != name)
                        continue;
                    out.found = true;
                    out.oid = static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>());
                    out.name = name;
                    stop = true;
                    break;
                }
                if (stop)
                    break;
            }
        }
        co_return out;
    }

    manager_disk_t::unique_future<std::pmr::vector<resolve_function_result_t>>
    manager_disk_t::resolve_function_by_name(execution_context_t /*ctx*/,
                                             std::string name,
                                             std::uint64_t /*since_version*/) {
        std::pmr::vector<resolve_function_result_t> out(resource());
        if (agents_.empty() || agents_[0] == nullptr) {
            co_return out;
        }
        const std::size_t idx = pool_idx_for_oid(pg_proc_oid, agents_.size());
        std::vector<size_t> projected{0, 1, 2, 3, 4, 5, 6};
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(agents_[idx]->address(),
                                                              &agent_disk_t::storage_scan_batched_inner,
                                                              pg_proc_oid,
                                                              std::unique_ptr<components::table::table_filter_t>{},
                                                              int64_t{-1},
                                                              std::move(projected),
                                                              components::table::transaction_data{});
        if (needs_sched) {
            scheduler_disk_->enqueue(agents_[idx].get());
        }
        auto batches = co_await std::move(fut);
        for (auto& chunk : batches) {
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (!str_equals(chunk.value(1, i), name))
                    continue;
                resolve_function_result_t r(resource());
                r.found = true;
                r.name = name;
                r.oid = static_cast<components::catalog::oid_t>(chunk.value(0, i).value<std::uint32_t>());
                auto ns_v = chunk.value(2, i);
                if (!ns_v.is_null())
                    r.namespace_oid = static_cast<components::catalog::oid_t>(ns_v.value<std::uint32_t>());
                auto nargs_v = chunk.value(3, i);
                if (!nargs_v.is_null())
                    r.pronargs = nargs_v.value<std::int32_t>();
                auto uid_v = chunk.value(4, i);
                if (!uid_v.is_null())
                    r.prouid = uid_v.value<std::uint64_t>();
                auto args_v = chunk.value(5, i);
                if (!args_v.is_null())
                    r.proargmatchers = std::string(args_v.value<std::string_view>());
                auto ret_v = chunk.value(6, i);
                if (!ret_v.is_null())
                    r.prorettype = std::string(ret_v.value<std::string_view>());
                out.push_back(std::move(r));
            }
        }
        co_return out;
    }

    manager_disk_t::unique_future<std::pmr::vector<std::string>>
    manager_disk_t::list_namespaces(execution_context_t /*ctx*/) {
        std::pmr::vector<std::string> out(resource());
        if (agents_.empty() || agents_[0] == nullptr) {
            co_return out;
        }
        const std::size_t idx = pool_idx_for_oid(pg_namespace_oid_tbl, agents_.size());
        std::vector<size_t> projected{0, 1};
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(agents_[idx]->address(),
                                                              &agent_disk_t::storage_scan_batched_inner,
                                                              pg_namespace_oid_tbl,
                                                              std::unique_ptr<components::table::table_filter_t>{},
                                                              int64_t{-1},
                                                              std::move(projected),
                                                              components::table::transaction_data{});
        if (needs_sched) {
            scheduler_disk_->enqueue(agents_[idx].get());
        }
        auto batches = co_await std::move(fut);
        for (auto& chunk : batches) {
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                auto name_v = chunk.value(1, i);
                if (!name_v.is_null()) {
                    out.emplace_back(std::string(name_v.value<std::string_view>()));
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
    manager_disk_t::unique_future<std::pmr::vector<std::pmr::vector<std::int64_t>>>
    manager_disk_t::scan_by_keys(execution_context_t ctx,
                                 components::catalog::oid_t table_oid,
                                 std::pmr::vector<std::string> key_col_names,
                                 components::vector::data_chunk_t keys) {
        std::pmr::vector<std::pmr::vector<std::int64_t>> out(resource());
        // INVARIANT: result.size() == keys.size() on EVERY path — one (possibly
        // empty) row per input key, in input order, so result[i] always maps to
        // keys[i]. Consumers (operator_fk_check / operator_fk_cascade) index
        // result[i] positionally and treat an empty row as "no parent match", so a
        // short outer vector would silently skip checks. No-key-columns, no-agents
        // and null-agent paths therefore still emit keys.size() empty rows rather
        // than a 0-size vector. (Per-key arity / unknown column is handled
        // agent-side, also yielding empty rows in result order.) keys.empty()
        // collapses to an empty result, which is keys.size()==0 — still the invariant.
        auto fill_empty_rows = [&]() {
            for (std::size_t i = 0; i < keys.size(); ++i) {
                out.emplace_back();
            }
        };
        if (keys.empty() || key_col_names.empty()) {
            fill_empty_rows();
            co_return out;
        }
        if (agents_.empty()) {
            fill_empty_rows();
            co_return out;
        }
        const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
        auto& agent = agents_[idx];
        if (agent == nullptr) {
            fill_empty_rows();
            co_return out;
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

    manager_disk_t::unique_future<std::pmr::vector<components::vector::data_chunk_t>>
    manager_disk_t::read_chunks_by_key(execution_context_t ctx,
                                       components::catalog::oid_t table_oid,
                                       std::pmr::vector<std::string> key_col_names,
                                       components::vector::data_chunk_t keys) {
        // Thin router: name→index resolution + the eq-AND filtered scan now run
        // intra-agent in read_chunks_by_key_inner (no row-major flatten, no
        // separate column-name resolution hop). Callers read cells via
        // chunk.value(col, row).
        std::pmr::vector<components::vector::data_chunk_t> empty(resource());
        if (key_col_names.empty())
            co_return empty;

        if (agents_.empty())
            co_return empty;
        const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
        auto& agent = agents_[idx];
        if (agent == nullptr)
            co_return empty;

        auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                              &agent_disk_t::read_chunks_by_key_inner,
                                                              table_oid,
                                                              std::move(key_col_names),
                                                              std::move(keys),
                                                              ctx.txn);
        if (needs_sched) {
            scheduler_disk_->enqueue(agent.get());
        }
        co_return co_await std::move(fut);
    }

    manager_disk_t::unique_future<std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>>>
    manager_disk_t::read_chunks_by_keys(execution_context_t ctx,
                                        components::catalog::oid_t table_oid,
                                        std::pmr::vector<std::string> key_col_names,
                                        components::vector::data_chunk_t keys) {
        // Thin router for the multi-key batch: name→index resolution + the per-key eq-AND
        // filtered scans run intra-agent in read_chunks_by_keys_inner (one mailbox hop for the
        // whole batch). INVARIANT: result.size() == keys.size() on EVERY path — one (possibly
        // empty) entry per input key, in input order, so result[i] always maps to keys[i].
        // Consumers index result[i] positionally, so the no-key-columns / no-agents / null-agent
        // paths still emit keys.size() empty entries rather than a 0-size vector. (Per-key arity
        // / unknown column is handled agent-side, also yielding empty entries in result order.)
        std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>> out(resource());
        auto fill_empty_entries = [&]() {
            for (std::size_t i = 0; i < keys.size(); ++i) {
                // Nested pmr vector: bare emplace_back() lets uses-allocator construction
                // propagate `out`'s resource to the inner vector. emplace_back(resource())
                // would resolve to the 2-arg (resource, allocator) form — no such ctor.
                out.emplace_back();
            }
        };
        if (keys.empty() || key_col_names.empty()) {
            fill_empty_entries();
            co_return out;
        }
        if (agents_.empty()) {
            fill_empty_entries();
            co_return out;
        }
        const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
        auto& agent = agents_[idx];
        if (agent == nullptr) {
            fill_empty_entries();
            co_return out;
        }

        auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                              &agent_disk_t::read_chunks_by_keys_inner,
                                                              table_oid,
                                                              std::move(key_col_names),
                                                              std::move(keys),
                                                              ctx.txn);
        if (needs_sched) {
            scheduler_disk_->enqueue(agent.get());
        }
        co_return co_await std::move(fut);
    }

} // namespace services::disk
