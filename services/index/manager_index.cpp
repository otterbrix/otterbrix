#include "manager_index.hpp"

#include <actor-zeta/spawn.hpp>
#include <algorithm>
#include <components/vector/data_chunk.hpp>
#include <core/executor.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/wal/record.hpp>
#include <set>

namespace {
    using value_t = components::types::logical_value_t;

    // Rebuild feeds (repopulate_table) key every re-staged entry by the PHYSICAL row id
    // the scan stamped into chunk.row_ids — a visibility-filtered scan compacts POSITIONS,
    // not ids, so counting positions would shift every entry after a surviving tombstone.
    // A non-empty chunk without a readable FLAT row_ids buffer is therefore a PRODUCER
    // defect: fail loudly (rule 6), never guess and never fall back to positional
    // numbering.
    [[nodiscard]] core::error_t
    check_rebuild_chunks_have_row_ids(const std::pmr::vector<components::vector::data_chunk_t>& chunks,
                                      std::pmr::memory_resource* resource) {
        for (const auto& chunk : chunks) {
            if (chunk.size() == 0) {
                continue;
            }
            if (chunk.row_ids.data() == nullptr ||
                chunk.row_ids.get_vector_type() != components::vector::vector_type::FLAT) {
                return core::error_t{
                    core::error_code_t::invalid_parameter,
                    std::pmr::string{"index rebuild received a scan chunk without physical row_ids", resource}};
            }
        }
        return core::error_t::no_error();
    }

} // anonymous namespace

namespace services::index {

#ifdef DEV_MODE
    namespace {
        std::atomic<uint64_t> g_index_repopulations{0};
        std::atomic<uint64_t> g_index_agent_reads{0};
        std::atomic<uint64_t> g_index_key_column_probes{0};
        // How many held-back erases exist across every live manager_index_t, kept as a
        // DELTA (add on defer, subtract on publish / forget / destruction) rather than as
        // an assignment of one manager's size: a test binary can hold two managers in
        // sequence, and a store() from either would report the other's queue as empty.
        // Written ONLY where deferred_deletes_ is, never read on a decision path -- the
        // sweep walks the vector itself, so this cannot become a second source of truth.
        std::atomic<uint64_t> g_index_deferred_deletes{0};
    } // namespace
    uint64_t index_repopulations() noexcept { return g_index_repopulations.load(std::memory_order_relaxed); }
    void reset_index_repopulations() noexcept { g_index_repopulations.store(0, std::memory_order_relaxed); }

    uint64_t index_agent_reads() noexcept { return g_index_agent_reads.load(std::memory_order_relaxed); }
    void reset_index_agent_reads() noexcept { g_index_agent_reads.store(0, std::memory_order_relaxed); }

    uint64_t index_key_column_probes() noexcept { return g_index_key_column_probes.load(std::memory_order_relaxed); }
    void reset_index_key_column_probes() noexcept { g_index_key_column_probes.store(0, std::memory_order_relaxed); }

    uint64_t index_deferred_deletes() noexcept { return g_index_deferred_deletes.load(std::memory_order_relaxed); }
#endif

    // --- Routing lookups (declared in the header; pure, no actor state) --------------

    const index_record_t* match_index_relid(const index_records_t& records,
                                            components::catalog::oid_t index_oid) noexcept {
        for (const auto& record : records) {
            if (record.index_oid == index_oid) {
                return &record;
            }
        }
        return nullptr;
    }

    const index_record_t* match_index(const index_records_t& records,
                                      const components::index::keys_base_storage_t& keys,
                                      components::logical_plan::index_type type) {
        for (const auto& record : records) {
            if (record.type == type && record.keys == keys) {
                return &record;
            }
        }
        return nullptr;
    }

    const index_record_t* match_index(const index_records_t& records,
                                      const components::index::keys_base_storage_t& keys) {
        // ORDERED FIRST, and that is a decision (see the declaration), not an artefact of
        // registration order.
        const index_record_t* unordered_match = nullptr;
        for (const auto& record : records) {
            if (record.keys != keys) {
                continue;
            }
            if (record.ordered) {
                return &record;
            }
            if (unordered_match == nullptr) {
                unordered_match = &record;
            }
        }
        return unordered_match;
    }

    std::pmr::vector<components::index::keys_base_storage_t> indexed_keys(const index_records_t& records,
                                                                          std::pmr::memory_resource* resource) {
        // DEDUPLICATED, deliberately -- see the declaration. Linear scan over the result:
        // an index count per table is 1-3, and the alternative is the key-keyed map this
        // whole line of work removed.
        std::pmr::vector<components::index::keys_base_storage_t> result(resource);
        result.reserve(records.size());
        for (const auto& record : records) {
            bool already_listed = false;
            for (const auto& listed : result) {
                if (listed == record.keys) {
                    already_listed = true;
                    break;
                }
            }
            if (!already_listed) {
                result.push_back(record.keys);
            }
        }
        return result;
    }

    std::pmr::vector<components::index::index_description_t>
    indexed_descriptions(const index_records_t& records, std::pmr::memory_resource* resource) {
        std::pmr::vector<components::index::index_description_t> result(resource);
        result.reserve(records.size());
        for (const auto& record : records) {
            components::index::index_description_t desc{components::index::keys_base_storage_t(resource),
                                                        record.type};
            for (const auto& key : record.keys) {
                desc.keys.push_back(key);
            }
            result.push_back(std::move(desc));
        }
        return result;
    }

    std::size_t resolve_key_column(const components::index::keys_base_storage_t& keys,
                                   const components::vector::data_chunk_t& chunk) {
        if (keys.empty()) {
            return key_column_absent;
        }
        std::size_t first_key_column = key_column_absent;
        for (const auto& key : keys) {
            const auto key_name = key.as_string();
            std::size_t found = key_column_absent;
            for (std::size_t column = 0; column < chunk.data.size(); ++column) {
#ifdef DEV_MODE
                g_index_key_column_probes.fetch_add(1, std::memory_order_relaxed);
#endif
                if (chunk.data[column].type().alias() == key_name) {
                    found = column;
                    break;
                }
            }
            if (found == key_column_absent) {
                return key_column_absent;
            }
            if (first_key_column == key_column_absent) {
                first_key_column = found;
            }
        }
        return first_key_column;
    }

    namespace {
        // A statement's worth of (key, row id) pairs for ONE index -- the unit an agent
        // takes. The manager builds it because it is the sole owner of the key sets, and
        // it is what keeps a data_chunk_t off the mailbox: forwarding the chunk would
        // clone every column of it, per agent, to read one.
        using key_batch_t = std::vector<std::pair<value_t, size_t>>;

        // The key column is resolved ONCE PER CHUNK per index -- it is a property of the
        // chunk's column layout, not of a row. A chunk that does not carry this index's
        // key contributes nothing and is skipped whole.
        //
        // Three collectors rather than one with a policy: the three DML shapes differ only
        // in where a row's physical id comes from, and that difference is one line each.

        // Rows appended contiguously from `start_row_id`, stopping after `count` of them
        // (INSERT, and the new half of UPDATE).
        key_batch_t collect_contiguous(std::pmr::memory_resource* resource,
                                       const components::index::keys_base_storage_t& keys,
                                       const std::pmr::vector<components::vector::data_chunk_t>& chunks,
                                       int64_t start_row_id,
                                       uint64_t count) {
            key_batch_t batch;
            uint64_t seen = 0;
            for (const auto& chunk : chunks) {
                const auto column = resolve_key_column(keys, chunk);
                for (uint64_t i = 0; i < chunk.size() && seen < count; ++i) {
                    if (column != key_column_absent) {
                        const auto cell = chunk.data[column].value(i);
                        batch.emplace_back(value_t(resource, cell),
                                           static_cast<size_t>(start_row_id + static_cast<int64_t>(seen)));
                    }
                    ++seen;
                }
                if (seen >= count) {
                    break;
                }
            }
            return batch;
        }

        // Rows named by an explicit id vector walked in lockstep with the chunks (DELETE,
        // and the old half of UPDATE).
        key_batch_t collect_by_row_ids(std::pmr::memory_resource* resource,
                                       const components::index::keys_base_storage_t& keys,
                                       const std::pmr::vector<components::vector::data_chunk_t>& chunks,
                                       const std::pmr::vector<int64_t>& row_ids) {
            key_batch_t batch;
            size_t seen = 0;
            for (const auto& chunk : chunks) {
                const auto column = resolve_key_column(keys, chunk);
                for (uint64_t i = 0; i < chunk.size() && seen < row_ids.size(); ++i) {
                    if (column != key_column_absent) {
                        const auto cell = chunk.data[column].value(i);
                        batch.emplace_back(value_t(resource, cell), static_cast<size_t>(row_ids[seen]));
                    }
                    ++seen;
                }
                if (seen >= row_ids.size()) {
                    break;
                }
            }
            return batch;
        }

        // Rows carrying their own physical id in chunk.row_ids (the rebuild feed). The
        // scan is visibility-filtered, so it compacts POSITIONS while ids keep their gaps:
        // counting positions would point every post-tombstone key one row low.
        key_batch_t collect_by_chunk_row_ids(std::pmr::memory_resource* resource,
                                             const components::index::keys_base_storage_t& keys,
                                             const std::pmr::vector<components::vector::data_chunk_t>& chunks) {
            key_batch_t batch;
            for (const auto& chunk : chunks) {
                const auto column = resolve_key_column(keys, chunk);
                if (column == key_column_absent) {
                    continue;
                }
                const auto* chunk_row_ids = chunk.row_ids.data<int64_t>();
                for (uint64_t i = 0; i < chunk.size(); ++i) {
                    const auto cell = chunk.data[column].value(i);
                    batch.emplace_back(value_t(resource, cell), static_cast<size_t>(chunk_row_ids[i]));
                }
            }
            return batch;
        }
    } // namespace
    manager_index_t::manager_index_t(std::pmr::memory_resource* resource,
                                     actor_zeta::scheduler_raw scheduler,
                                     log_t& log,
                                     std::filesystem::path path_db,
                                     uint64_t bitcask_flush_threshold,
                                     uint64_t bitcask_segment_record_limit,
                                     uint64_t btree_flush_threshold)
        : actor_zeta::actor::actor_mixin<manager_index_t>()
        , resource_(resource)
        , scheduler_(scheduler)
        , log_(log)
        , path_db_(std::move(path_db))
        , bitcask_flush_threshold_(bitcask_flush_threshold)
        , bitcask_segment_record_limit_(bitcask_segment_record_limit)
        , btree_flush_threshold_(btree_flush_threshold)
        , indexes_per_oid_(resource)
        , dropped_table_agents_(resource)
        , deferred_deletes_(resource)
        , bitcask_agents_owned_(resource)
        , btree_agents_owned_(resource)
        , pending_void_(resource) {
        if (!path_db_.empty()) {
            std::filesystem::create_directories(path_db_);
        }

        // Event-loop thread: the sole processor of behaviors. Senders only
        // deliver into inbox_ and wake pump_cv_.
        loop_thread_ = std::thread([this] {
            // Loop-thread-owned list. std::pmr::list for iterator stability:
            // behavior_t is move-only and a resume can re-suspend on a new await
            // without us touching the node.
            // this->resource() is qualified because the ctor param `resource` shadows the member fn.
            std::pmr::list<in_flight_entry_t> in_flight(this->resource());

            while (loop_running_.load(std::memory_order_acquire)) {
                // Drain inbox_, re-wrapping each raw message* into a message_ptr
                // parked in a fresh slot. pending_msg STAYS in its slot: the
                // coroutine holds a raw pointer to the message across suspension
                // points, so msg must outlive its behavior.
                {
                    actor_zeta::mailbox::message* raw = nullptr;
                    while (inbox_.pop(raw)) {
                        in_flight.emplace_back();
                        in_flight.back().pending_msg = actor_zeta::mailbox::message_ptr{raw};
                    }
                }

                bool made_progress = false;

                // (a) Materialize a behavior for the first entry that has a
                //     pending_msg but no behavior yet.
                for (auto& e : in_flight) {
                    if (e.pending_msg && !e.behavior) {
                        e.behavior = behavior(e.pending_msg.get());
                        poll_pending();
                        made_progress = true;
                        break;
                    }
                }

                // (b) Resume one whose awaited unique_future is ready.
                //     take_awaited_continuation atomically claims the cont; a
                //     null result means another resume already took it.
                if (!made_progress) {
                    actor_zeta::detail::coroutine_handle<> cont{};
                    for (auto& e : in_flight) {
                        if (e.behavior.is_awaited_ready()) {
                            cont = e.behavior.take_awaited_continuation();
                            if (cont) {
                                break;
                            }
                        }
                    }
                    if (cont) {
#ifdef DEV_MODE
                        services::dispatcher::note_pump_hop();
#endif
                        cont.resume();
                        poll_pending();
                        made_progress = true;
                    }
                }

                // (c) Erase one done entry. Its behavior_t and message destruct
                //     here on the loop thread, which is safe: ~behavior_t releases
                //     the promise only after the awaiter is gone.
                if (!made_progress) {
                    for (auto it = in_flight.begin(); it != in_flight.end(); ++it) {
                        if (it->behavior && it->behavior.done()) {
                            it = in_flight.erase(it);
                            made_progress = true;
                            break;
                        }
                    }
                }

                if (made_progress) {
                    continue;
                }

                // Bounded-staleness idle wait: future completion only sets an
                // atomic flag (no notify), so the wait must time out to re-poll
                // readiness; enqueue notifies pump_cv_ early.
                std::unique_lock<std::mutex> lk(mutex_);
                // Readiness is discovered by this wait TIMING OUT, so while work is in
                // flight that expiry IS the per-hop latency and a statement crosses ~20
                // hops. Idle is the opposite case: only a new message can arrive and that
                // DOES notify, so the idle tick is left alone — shortening it burns CPU for
                // nothing, lengthening it would expose the push-notify race to the first
                // statement after a pause.
                if (inbox_.empty()) {
                    pump_cv_.wait_for(lk,
                                      in_flight.empty() ? std::chrono::microseconds(100)
                                                        : std::chrono::microseconds(5));
                }
            }
            // in_flight destructs here, on the loop thread — never on a sender.
        });
    }

    manager_index_t::~manager_index_t() {
#ifdef DEV_MODE
        // Whatever is still queued dies with this manager -- the erases are moot once its
        // agents are gone. Subtracted so the process-wide meter does not carry a dead
        // manager's backlog into the next one's.
        g_index_deferred_deletes.fetch_sub(deferred_deletes_.size(), std::memory_order_relaxed);
#endif
        loop_running_.store(false, std::memory_order_release);
        pump_cv_.notify_all();
        if (loop_thread_.joinable()) {
            loop_thread_.join();
        }
        // Drain messages delivered after the loop exited so each deleter runs.
        actor_zeta::mailbox::message* raw = nullptr;
        while (inbox_.pop(raw)) {
            actor_zeta::mailbox::message_ptr reclaim{raw};
        }
    }

    auto manager_index_t::make_type() const noexcept -> const char* { return "manager_index"; }

    std::pair<bool, actor_zeta::detail::enqueue_result>
    manager_index_t::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
        // Deliver only: release into inbox_ and wake the loop. ALL processing
        // happens on loop_thread_.
        inbox_.push(msg.release());
        pump_cv_.notify_one();
        return {false, actor_zeta::detail::enqueue_result::success};
    }

    actor_zeta::behavior_t manager_index_t::behavior(actor_zeta::mailbox::message* msg) {
        poll_pending();

        switch (msg->command()) {
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::register_collection>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::register_collection, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::unregister_collection>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::unregister_collection, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::create_index>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::create_index, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::drop_index>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::drop_index, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::insert_rows>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::insert_rows, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::delete_rows>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::delete_rows, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::update_rows>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::update_rows, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::commit_inserts>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::commit_inserts, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::commit_deletes>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::commit_deletes, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::revert_insert>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::revert_insert, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::revert_delete>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::revert_delete, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::cleanup_all_versions>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::cleanup_all_versions, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::all_indexed_oids>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::all_indexed_oids, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::repopulate_table>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::repopulate_table, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::search>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::search, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::search_with_preferred_type>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::search_with_preferred_type, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::flush_all_indexes>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::flush_all_indexes, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::tables_without_indexes>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::tables_without_indexes, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::get_indexed_keys>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::get_indexed_keys, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::get_indexed_descriptions>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::get_indexed_descriptions, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::on_horizon_advanced>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::on_horizon_advanced, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::mark_table_dropped>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::mark_table_dropped, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::table_dropped_committed>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::table_dropped_committed, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::table_drop_aborted>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::table_drop_aborted, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::apply_wal_record_for_index>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::apply_wal_record_for_index, msg);
                break;
            }
            default:
                break;
        }
    }

    void manager_index_t::poll_pending() {
        // No mutex: pending_void_ is touched only by the loop thread (here and
        // from handlers running on it).
        pending_void_.erase(
            std::remove_if(pending_void_.begin(), pending_void_.end(), [](auto& f) { return f.is_ready(); }),
            pending_void_.end());
    }

    void manager_index_t::sync(index_sync_pack_t pack) {
        disk_address_ = pack.disk;
        trace(log_, "manager_index_t::sync: disk_address set");
    }

    void manager_index_t::mark_table_dropped_sync(components::catalog::oid_t oid, uint64_t dropped_at_commit_id) {
        dropped_table_agents_[oid] = dropped_at_commit_id;
    }

    manager_index_t::unique_future<void> manager_index_t::mark_table_dropped(session_id_t /*session*/,
                                                                             components::catalog::oid_t table_oid,
                                                                             uint64_t dropped_at_commit_id) {
        // Wrapper so the operator co_awaits a future and the dropped_table_agents_
        // mutation runs on this actor's thread, not synchronously cross-actor.
        trace(log_,
              "manager_index_t::mark_table_dropped , oid : {} , commit_id : {}",
              static_cast<unsigned>(table_oid),
              dropped_at_commit_id);
        mark_table_dropped_sync(table_oid, dropped_at_commit_id);
        co_return;
    }

    manager_index_t::unique_future<void>
    manager_index_t::table_dropped_committed(session_id_t /*session*/, uint64_t txn_id, uint64_t commit_id) {
        // DROP-GC value-space remap. mark_table_dropped_sync stored the entry's value
        // in TXN-ID space (>= 2^62), the only id the cascade-delete operator had.
        // on_horizon_advanced reclaims entries by comparing the stored value against a
        // commit-id horizon, so a TXN-ID placeholder would never satisfy
        // value < new_horizon. After the transaction commits and a real commit_id is
        // allocated, rewrite every dropped_table_agents_ entry whose value still
        // equals txn_id, moving it into commit-id space.
        trace(log_, "manager_index_t::table_dropped_committed , txn_id : {} , commit_id : {}", txn_id, commit_id);
        for (auto& kv : dropped_table_agents_) {
            if (kv.second == txn_id) {
                kv.second = commit_id;
            }
        }
        co_return;
    }

    manager_index_t::unique_future<void> manager_index_t::table_drop_aborted(session_id_t /*session*/,
                                                                             uint64_t txn_id) {
        // DROP-rollback un-mark — the abort mirror of table_dropped_committed.
        // mark_table_dropped_sync stored the entry's value in TXN-ID space (>= 2^62),
        // the only id the cascade-delete operator had. If the transaction ABORTS the
        // table must stay indexed, so ERASE (not remap) every dropped_table_agents_
        // entry whose value still equals txn_id, un-marking the DROP so
        // on_horizon_advanced never reaps the engine.
        trace(log_, "manager_index_t::table_drop_aborted , txn_id : {}", txn_id);
        for (auto it = dropped_table_agents_.begin(); it != dropped_table_agents_.end();) {
            if (it->second == txn_id) {
                trace(log_,
                      "manager_index_t::table_drop_aborted: un-marked DROP for oid {} (txn_id {})",
                      static_cast<unsigned>(it->first),
                      txn_id);
                it = dropped_table_agents_.erase(it);
            } else {
                ++it;
            }
        }
        co_return;
    }

    void manager_index_t::set_manager_dispatcher_sync(actor_zeta::address_t address) {
        manager_dispatcher_ = std::move(address);
    }

    // ---------------- Bootstrap helpers (called pre-scheduler-start) ----------------

    void manager_index_t::bootstrap_engine_sync(components::catalog::oid_t oid) {
        // Mirrors register_collection's lazy init without the co_return wrapper: an EMPTY
        // record list, which is how the manager says "this table is known and carries no
        // index yet".
        indexes_per_oid_.try_emplace(oid, index_records_t(resource_));
    }

    core::error_t manager_index_t::bootstrap_index_sync(components::catalog::oid_t table_oid,
                                                        components::catalog::oid_t index_oid,
                                                        components::logical_plan::index_type type,
                                                        components::index::keys_base_storage_t keys,
                                                        std::pmr::set<std::uint64_t> committed_txn_ids) {
        // Steady-state equivalent of create_index below, minus the mailbox wrapper (see
        // the declaration). Every gate, and the AGENT-FIRST order, is the same one
        // create_index uses -- that identity is the point: an index restored at startup
        // and an index created by a statement must be the same object, raised the same
        // way, over a store opened with the same thresholds.
        //
        // base_spaces runs bootstrap_engine_sync for every live oid first, so a missing
        // entry here is a bootstrap-order bug.
        auto it = indexes_per_oid_.find(table_oid);
        if (it == indexes_per_oid_.end()) {
            return core::error_t{core::error_code_t::index_create_fail,
                                 std::pmr::string{"index bootstrap: the table is not registered with the index "
                                                  "manager (bootstrap order violated)",
                                                  resource_}};
        }

        // Refuse duplicate registration -- base_spaces should only call once per alive
        // pg_index row, but be defensive against rescan paths.
        if (match_index_relid(it->second, index_oid) != nullptr) {
            return core::error_t{core::error_code_t::index_create_fail,
                                 std::pmr::string{"index bootstrap: the index is already registered", resource_}};
        }

        if (type != components::logical_plan::index_type::single &&
            type != components::logical_plan::index_type::hashed) {
            return core::error_t{core::error_code_t::index_create_fail,
                                 std::pmr::string{"index bootstrap: unsupported index type", resource_}};
        }

        // NO CATALOG DIRECTORY, NO INDEX -- see create_index for the whole argument.
        if (path_db_.empty()) {
            return core::error_t{
                core::error_code_t::index_create_fail,
                std::pmr::string{"index bootstrap: this index manager has no on-disk catalog path, and an "
                                 "index keeps its rows on disk",
                                 resource_}};
        }

        // THE AGENT FIRST, the registration second. The store is opened by the AGENT
        // (rule 10), so its failure is only knowable once the agent exists; registering
        // the record first would mean unwinding a live index out of the registry on that
        // failure, and this way there is nothing to unwind.
        auto spawned = spawn_disk_agent(table_oid, index_oid, type, std::move(committed_txn_ids));
        if (spawned.has_error()) {
            return spawned.error();
        }
        // Only reachable past the check above -- result_wrapper_t::value() is what makes
        // that a compiler-enforced order rather than a convention.
        const auto agent = spawned.value();

        // ONE RECORD, holding every routing fact: the key set (the manager's alone), the
        // backend the agent actually IS, whether it can answer an ordered probe, and the
        // mailbox to reach it through. `it` is still valid -- spawn_disk_agent touches the
        // owner vectors, never this map, and nothing suspended.
        it->second.push_back(index_record_t{index_oid,
                                            std::move(keys),
                                            agent.type,
                                            agent.ordered,
                                            agent.address});

        // NOTHING IS REHYDRATED HERE, and the absence is deliberate. What stood in this
        // spot opened a SECOND core::b_plus_tree::btree_t on ${path_db}/${table}/${index}
        // -- the very directory this agent's store has open -- from the manager's own
        // thread, and replayed every entry into an in-memory twin, because reads were
        // answered from that twin. Reads go to the agent that owns the tree, so the twin
        // is gone and with it the second owner of those files (rule 10).
        trace(log_,
              "manager_index_t::bootstrap_index_sync: wired index_oid={} on oid={} type={}",
              static_cast<unsigned>(index_oid),
              static_cast<unsigned>(table_oid),
              static_cast<unsigned>(agent.type));
        return core::error_t::no_error();
    }

    void manager_index_t::bootstrap_dropped_sync(components::catalog::oid_t oid, uint64_t delete_id) {
        mark_table_dropped_sync(oid, delete_id);
    }

#ifdef DEV_MODE
    std::pmr::vector<bitcask_index_agent_t*> manager_index_t::owned_bitcask_agents_sync() {
        std::pmr::vector<bitcask_index_agent_t*> agents(resource_);
        agents.reserve(bitcask_agents_owned_.size());
        for (auto& agent : bitcask_agents_owned_) {
            if (agent) {
                agents.emplace_back(agent.get());
            }
        }
        return agents;
    }

    std::pmr::vector<btree_index_agent_t*> manager_index_t::owned_btree_agents_sync() {
        std::pmr::vector<btree_index_agent_t*> agents(resource_);
        agents.reserve(btree_agents_owned_.size());
        for (auto& agent : btree_agents_owned_) {
            if (agent) {
                agents.emplace_back(agent.get());
            }
        }
        return agents;
    }
#endif

    void manager_index_t::schedule_agent(const actor_zeta::address_t& addr, bool needs_sched) {
        if (!needs_sched)
            return;
        // Both families, because an address does not say which one it belongs to and does
        // not have to: the scheduler takes the agent, and only the owner knows the type.
        // An address that matches NEITHER vector is an agent this manager no longer owns
        // (a reap in progress holds it in a handler frame), and that handler schedules the
        // pointer it holds itself -- see send_drop_to_detached.
        for (auto& agent : bitcask_agents_owned_) {
            if (agent && agent->address() == addr) {
                scheduler_->enqueue(agent.get());
                return;
            }
        }
        for (auto& agent : btree_agents_owned_) {
            if (agent && agent->address() == addr) {
                scheduler_->enqueue(agent.get());
                return;
            }
        }
    }

    // --- Disk agents: raising, detaching, reaping ---

    core::result_wrapper_t<manager_index_t::spawned_agent_t>
    manager_index_t::spawn_disk_agent(components::catalog::oid_t table_oid,
                                      components::catalog::oid_t index_oid,
                                      components::logical_plan::index_type type,
                                      std::pmr::set<std::uint64_t> committed_txn_ids) {
        // THE ONE PLACE pg_index.indtype picks a class. index_type::hashed -> the bitcask
        // LSM agent; everything else (single / composite / multikey / wildcard) -> the
        // ordered b+tree agent. Every other line of this manager works through an
        // actor_zeta::address_t and never asks again.
        //
        // The thresholds are the manager's CONFIGURED ones, not each backend's static
        // defaults, and that is true on both roads into this function (bootstrap and
        // runtime CREATE INDEX). Nothing else may build an agent.
        if (type == components::logical_plan::index_type::hashed) {
            // Only this family owns a txn log, so only it receives the WAL committed-txn
            // set for the recover gate (M1.1).
            auto agent = bitcask_index_agent_t::create(resource_,
                                                       path_db_,
                                                       table_oid,
                                                       index_oid,
                                                       bitcask_flush_threshold_,
                                                       bitcask_segment_record_limit_,
                                                       log_,
                                                       std::move(committed_txn_ids));
            if (agent.has_error()) {
                return agent.error();
            }
            auto addr = agent.value()->address();
            bitcask_agents_owned_.emplace_back(std::move(agent.value()));
            // The two routing facts come from the CLASS, not from the `type` argument:
            // what the catalog asked for and what the family is are not the same word.
            return spawned_agent_t{addr,
                                   bitcask_index_agent_t::index_type_v,
                                   bitcask_index_agent_t::supports_ordered_probe_v};
        }
        auto agent =
            btree_index_agent_t::create(resource_, path_db_, table_oid, index_oid, btree_flush_threshold_, log_);
        if (agent.has_error()) {
            return agent.error();
        }
        auto addr = agent.value()->address();
        btree_agents_owned_.emplace_back(std::move(agent.value()));
        return spawned_agent_t{addr, btree_index_agent_t::index_type_v, btree_index_agent_t::supports_ordered_probe_v};
    }

    manager_index_t::detached_agents_t manager_index_t::detach_table_agents(components::catalog::oid_t table_oid) {
        detached_agents_t dying(resource_);
        // The records go first: from this point nothing that consults the manager can
        // reach these agents.
        indexes_per_oid_.erase(table_oid);
        // Which agents belong to the table is asked of the AGENTS, not of the registry
        // that could disagree with the owners -- the entry has just been erased, and an
        // address left behind in it after a partial teardown would leave an owner here
        // forever (which is the leak this whole path exists to close).
        auto take = [&](auto& owned, auto& into) {
            for (auto agent_it = owned.begin(); agent_it != owned.end();) {
                if (*agent_it && (*agent_it)->table_oid() == table_oid) {
                    into.emplace_back(std::move(*agent_it));
                    agent_it = owned.erase(agent_it);
                } else {
                    ++agent_it;
                }
            }
        };
        take(bitcask_agents_owned_, dying.bitcask);
        take(btree_agents_owned_, dying.btree);
        return dying;
    }

    manager_index_t::detached_agents_t manager_index_t::detach_index(components::catalog::oid_t table_oid,
                                                                     components::catalog::oid_t index_oid) {
        detached_agents_t dying(resource_);
        // ONE index, so the per-oid entry is TRIMMED rather than erased: a DROP INDEX must
        // leave its table's sibling indexes registered, and the table itself known.
        auto oid_it = indexes_per_oid_.find(table_oid);
        if (oid_it == indexes_per_oid_.end()) {
            return dying;
        }
        auto& records = oid_it->second;
        auto record_it = std::find_if(records.begin(), records.end(), [&](const index_record_t& record) {
            return record.index_oid == index_oid;
        });
        if (record_it == records.end()) {
            return dying;
        }
        const auto agent_addr = record_it->address;
        records.erase(record_it);
        auto take = [&](auto& owned, auto& into) {
            for (auto agent_it = owned.begin(); agent_it != owned.end(); ++agent_it) {
                if (*agent_it && (*agent_it)->address() == agent_addr) {
                    into.emplace_back(std::move(*agent_it));
                    owned.erase(agent_it);
                    return true;
                }
            }
            return false;
        };
        if (!take(bitcask_agents_owned_, dying.bitcask)) {
            take(btree_agents_owned_, dying.btree);
        }
        return dying;
    }

    std::pmr::vector<manager_index_t::unique_future<void>>
    manager_index_t::send_drop_to_detached(detached_agents_t& dying, session_id_t session) {
        std::pmr::vector<unique_future<void>> futures(resource_);
        futures.reserve(dying.bitcask.size() + dying.btree.size());
        // Two-phase by construction: this function only SENDS, so the caller can await
        // every reply afterwards and the agents drop in parallel.
        auto send_drop = [&](auto& owned) {
            for (auto& agent : owned) {
                if (!agent) {
                    continue;
                }
                auto [needs_sched, fut] =
                    actor_zeta::otterbrix::send<&index_agent_contract::drop>(agent->address(), session);
                // schedule_agent() searches the manager's vectors, which this agent has
                // just left -- schedule the pointer we hold, or the drop would never be
                // processed and the caller's await would never return.
                if (needs_sched) {
                    scheduler_->enqueue(agent.get());
                }
                futures.emplace_back(std::move(fut));
            }
        };
        send_drop(dying.bitcask);
        send_drop(dying.btree);
        return futures;
    }

    // --- Collection lifecycle ---

    manager_index_t::unique_future<void> manager_index_t::register_collection(session_id_t /*session*/,
                                                                              components::catalog::oid_t table_oid) {
        trace(log_, "manager_index_t::register_collection: oid={}", static_cast<unsigned>(table_oid));

        // An EMPTY record list, not an absent entry: "known, carries no index yet".
        indexes_per_oid_.try_emplace(table_oid, index_records_t(resource_));
        co_return;
    }

    manager_index_t::unique_future<void> manager_index_t::unregister_collection(session_id_t session,
                                                                                components::catalog::oid_t table_oid) {
        trace(log_, "manager_index_t::unregister_collection: oid={}", static_cast<unsigned>(table_oid));

        // AND THE AGENTS GO WITH IT. detach_table_agents erases the registry entry, so the
        // table's records and its agents leave together -- one container, one removal. This handler is the commit-time (and abort-time)
        // physical teardown of a dropped table: operator_commit_transaction awaits it for
        // every dropped oid and only THEN tells manager_disk_t to free the table's files.
        // It used to erase the routing entries and leave the owning pointers standing, so
        // every dropped indexed table left behind an agent holding its store OPEN, for the
        // life of the process -- while the disk manager unlinked the very directory that
        // store had open, and the comment promising a later reaper described something
        // that did not exist.
        //
        // The order is the one drop_index proved out: take the ownership into THIS frame
        // BEFORE the terminal drop is sent, so nothing can address the agent behind it,
        // then await the reply and let the frame destroy it. Awaiting is also what keeps
        // the index-before-disk invariant real: the store is closed before the caller
        // proceeds to the disk drop.
        // The held-back erases go BEFORE the agents that own their buckets do (see
        // deferred_deletes_): nothing is left to publish once the agents are gone, and an
        // entry that outlived them would be a lookup into an erased registry entry on the
        // next horizon sweep.
        forget_deferred_deletes(table_oid);
        auto dying = detach_table_agents(table_oid);
        auto drop_futures = send_drop_to_detached(dying, session);
        for (auto& f : drop_futures) {
            co_await std::move(f);
        }
        // `dying` is destroyed with this frame: after the drop replies each mailbox is
        // provably empty, so closing them cancels nothing.
        co_return;
    }

    // --- DDL: index management ---

    manager_index_t::unique_future<core::error_t> manager_index_t::create_index(
        session_id_t /*session*/,
        components::catalog::oid_t table_oid,
        components::catalog::oid_t index_oid,
        components::index::keys_base_storage_t keys,
        components::logical_plan::index_type type,
        // Unused since the btree replay below it was removed: nothing in CREATE INDEX
        // interprets a key any more. It stays in the signature because it is part of
        // index_contract::create_index, which every caller sends.
        core::date::timezone_offset_t /*session_tz*/) {
        trace(log_,
              "manager_index_t::create_index: index_oid={} on oid={}",
              static_cast<unsigned>(index_oid),
              static_cast<unsigned>(table_oid));

        auto it = indexes_per_oid_.find(table_oid);
        if (it == indexes_per_oid_.end()) {
            co_return core::error_t{core::error_code_t::index_create_fail,
                                    std::pmr::string{"the table is not registered with the index manager", resource_}};
        }

        // Duplicate detection. A SECOND index over the same (keys, type) would be a
        // pure cost — same keys answered by the same backend, maintained twice on
        // every DML — so the registry holds at most one, and the pair is what makes
        // `CREATE INDEX ... (k)` beside `CREATE INDEX ... USING hash (k)` legal.
        // A duplicate CREATE INDEX by NAME also lands here: the retry allocates a
        // fresh indexrelid, but its (keys, type) collide with the live index.
        // index_create_fail, not already_exists: test_index pins this code for a
        // duplicate CREATE INDEX.
        if (match_index_relid(it->second, index_oid) != nullptr ||
            match_index(it->second, keys, type) != nullptr) {
            co_return core::error_t{core::error_code_t::index_create_fail,
                                    std::pmr::string{"index already exists", resource_}};
        }

        if (type != components::logical_plan::index_type::single &&
            type != components::logical_plan::index_type::hashed) {
            co_return core::error_t{core::error_code_t::index_create_fail,
                                    std::pmr::string{"unsupported index type", resource_}};
        }

        // NO CATALOG DIRECTORY, NO INDEX. Every index this manager can build keeps its
        // committed rows in a store its own agent opens under path_db_, and every read of
        // those rows is a message to that agent. With no path there is no store, no agent
        // and no reachable committed half — so the only honest answer to CREATE INDEX is a
        // refusal.
        //
        // What stood here built one in memory instead and reported success. The statement
        // asked for an index; the caller got an object holding rows nothing else could see,
        // that no restart would find and that the read path would later have to guess about.
        // Rule 6: the failure is LOUD and it happens at the statement, not silently at the
        // first read.
        if (path_db_.empty()) {
            co_return core::error_t{
                core::error_code_t::index_create_fail,
                std::pmr::string{"index create: this index manager has no on-disk catalog path, and an index "
                                 "keeps its rows on disk",
                                 resource_}};
        }

        // THE AGENT FIRST, the registration second. The storage this index needs is opened
        // by the AGENT (rule 10) — nothing is created here and handed across — so its
        // failure is only knowable once the agent exists. Registering the record first
        // would mean unwinding a live index out of the registry on that failure; this way
        // there is nothing to unwind.
        //
        // Which class of agent is spawn_disk_agent's decision, and the THRESHOLDS it uses
        // are this manager's configured ones. They used to be each backend's static
        // default_* here, which is why `bitcask_segment_record_limit` (and its two
        // siblings) held for indexes restored at startup and was silently ignored by every
        // index a statement created afterwards.
        //
        // Runtime DDL path: a fresh index dir with no txn-log to gate, so the recover-gate
        // set is EMPTY (correct value, not a fallback). Built on resource_ — the resource
        // the agent and its store use.
        auto spawned = spawn_disk_agent(table_oid, index_oid, type, std::pmr::set<std::uint64_t>(resource_));
        if (spawned.has_error()) {
            // The statement asked for a disk index. Handing back a memory one would report
            // success for something the user did not ask for and cannot see, so the
            // failure goes back to the caller. No agent was built, so there is nothing
            // registered and nothing scheduled to unwind.
            error(log_,
                  "manager_index_t::create_index: index_oid={} on oid={} failed, "
                  "disk storage could not be opened: {}",
                  static_cast<unsigned>(index_oid),
                  static_cast<unsigned>(table_oid),
                  spawned.error().what);
            co_return spawned.error();
        }
        // Only reachable past the check above -- result_wrapper_t::value() is what makes
        // that a compiler-enforced order rather than a convention. The owner is already in
        // this manager's per-family vector; what comes back is the address every other
        // line works through, plus the two facts the record keeps.
        const auto agent = spawned.value();

        // ONE RECORD. Nothing suspended between the find() above and here, so `it` is
        // still valid: spawn_disk_agent touches the owner vectors, never this map.
        it->second.push_back(index_record_t{index_oid,
                                            std::move(keys),
                                            agent.type,
                                            agent.ordered,
                                            agent.address});

        // No btree replay into an in-memory twin — see the note in bootstrap_index_sync.
        // Whatever a pre-existing store at this oid pair holds is already loaded by the
        // agent's own backend constructor, which is the only owner of those files.
        co_return core::error_t::no_error();
    }

    manager_index_t::unique_future<void> manager_index_t::drop_index(session_id_t session,
                                                                     components::catalog::oid_t table_oid,
                                                                     components::catalog::oid_t index_oid) {
        trace(log_,
              "manager_index_t::drop_index: index_oid={} on oid={}",
              static_cast<unsigned>(index_oid),
              static_cast<unsigned>(table_oid));

        // EVERYTHING THAT CAN STILL REACH THIS AGENT IS TAKEN AWAY BEFORE THE DROP IS
        // SENT, AND THE AGENT ITSELF IS TAKEN INTO THIS FRAME.
        //
        // The reply to drop() is what tells us the agent is finished: its mailbox is
        // FIFO and every index-agent message is posted from this one thread, so any
        // request posted BEFORE the drop has already been answered by the time the drop
        // answers. What that argument cannot cover is a request posted AFTER it — and
        // the old order left exactly that door open, because the index stayed registered
        // and its address stayed reachable for the whole duration of the await. A search
        // materialised in that window found the index, sent read_rows() behind the drop,
        // and suspended; the drop reply then arrived first and the erase destroyed the
        // agent underneath it. Closing an actor's mailbox deletes the messages still in
        // it and each deletion sets operation_canceled on the caller's shared_state — and
        // state_flags::result_set is value_set|error_set, so the waiter is RESUMED, its
        // await_resume() asserts the error away (nothing under NDEBUG) and moves a value
        // out of storage that was never written. There is no way to check for that after
        // the fact: an actor coroutine can only co_await a unique_future, and by the time
        // it is resumed the take has already happened. The only fix is that the agent
        // still be there — so nothing may be able to address it after its last message is
        // posted.
        //
        // detach_index does BOTH halves in one step: the record leaves the registry (so no
        // search materialised from here on can find this index) and the owning pointer
        // leaves the manager into `dying`. It TRIMS the table's entry rather than erasing
        // it: DROP INDEX, not DROP TABLE — the sibling indexes must stay registered.
        //
        // Unregistering first also removes the stale-record hazard the await used to
        // carry: on_horizon_advanced can erase the table's entry while we are suspended.
        // ONE index's held-back erases, dropped with the index they were owed to — the
        // sibling indexes of the same table keep theirs (see deferred_deletes_).
        forget_deferred_deletes(table_oid, index_oid);
        auto dying = detach_index(table_oid, index_oid);
        if (dying.empty()) {
            // No such index on this table (or no such table). Nothing was registered and
            // nothing is owned, so there is nothing to tear down.
            co_return;
        }

        auto drop_futures = send_drop_to_detached(dying, session);
        for (auto& f : drop_futures) {
            co_await std::move(f);
        }

        // `dying` is destroyed with this frame: after the drop reply the agent's mailbox is
        // provably empty, so nothing is cancelled by closing it.
        co_return;
    }

    // --- Txn-aware DML ---
    //
    // ALL THREE HANDLERS HAVE THE SAME SHAPE, and it is no longer a loop over ROWS. What
    // stood here walked every row of every chunk and called into a per-index object once
    // per row, which is how the write path looked when that object lived in this actor.
    // It does not: the buffer is the agent's now. So each handler resolves the key column
    // ONCE PER CHUNK per index, builds ONE batch of (key, row id) pairs per index, and
    // sends it as ONE message.
    //
    // Two-phase, deliberately: every batch is SENT before anything is awaited, so the N
    // agents of a table stage in parallel. Awaiting matters even though staging cannot
    // fail on its own — a DROPPED agent refuses, and that refusal must fail the statement
    // rather than leave it believing rows are indexed that are not.
    //
    // Both `data` chunks and the batches stay OFF the mailbox as chunks: a data_chunk_t
    // forwarded to N agents is N clones of every column to read one.

    manager_index_t::unique_future<core::error_t>
    manager_index_t::insert_rows(execution_context_t ctx,
                                 components::catalog::oid_t table_oid,
                                 std::pmr::vector<components::vector::data_chunk_t> data,
                                 uint64_t start_row_id,
                                 uint64_t count) {
        if (count == 0)
            co_return core::error_t::no_error();

        auto txn_id = ctx.txn.transaction_id;
        auto it = indexes_per_oid_.find(table_oid);
        if (it == indexes_per_oid_.end())
            co_return core::error_t::no_error();

        std::pmr::vector<unique_future<core::error_t>> futures(resource_);
        futures.reserve(it->second.size());
        for (const auto& record : it->second) {
            // Rows are indexed in vector order with contiguous row-ids based at
            // start_row_id, stopping after `count` of them (the committed/appended total).
            auto batch = collect_contiguous(resource_, record.keys, data, static_cast<int64_t>(start_row_id), count);
            if (batch.empty()) {
                // No chunk carries this index's key — the index does not apply to this
                // statement. Not an error and not a silent drop: there is no key to index.
                continue;
            }
            auto [needs_sched, f] = actor_zeta::otterbrix::send<&index_agent_contract::stage_inserts>(
                record.address,
                ctx.session,
                txn_id,
                std::move(batch));
            schedule_agent(record.address, needs_sched);
            futures.emplace_back(std::move(f));
        }

        // Drain EVERY future even past the first failure so none is dropped; the first
        // failure is what the statement is told about.
        core::error_t first_error = core::error_t::no_error();
        for (auto& f : futures) {
            auto err = co_await std::move(f);
            if (err.contains_error() && !first_error.contains_error()) {
                first_error = std::move(err);
            }
        }
        // The statement fails: an index entry that could not be staged is not a detail to
        // log and move past — the index would disagree with the table.
        co_return first_error;
    }

    manager_index_t::unique_future<core::error_t>
    manager_index_t::delete_rows(execution_context_t ctx,
                                 components::catalog::oid_t table_oid,
                                 std::pmr::vector<components::vector::data_chunk_t> data,
                                 std::pmr::vector<int64_t> row_ids) {
        if (row_ids.empty())
            co_return core::error_t::no_error();

        auto txn_id = ctx.txn.transaction_id;
        auto it = indexes_per_oid_.find(table_oid);
        if (it == indexes_per_oid_.end())
            co_return core::error_t::no_error();

        std::pmr::vector<unique_future<core::error_t>> futures(resource_);
        futures.reserve(it->second.size());
        for (const auto& record : it->second) {
            // The chunks are walked in lockstep with the flat row_ids: row_ids[k] is the
            // storage row-id of the k-th row across the concatenated chunks.
            auto batch = collect_by_row_ids(resource_, record.keys, data, row_ids);
            if (batch.empty()) {
                continue;
            }
            auto [needs_sched, f] = actor_zeta::otterbrix::send<&index_agent_contract::stage_deletes>(
                record.address,
                ctx.session,
                txn_id,
                std::move(batch));
            schedule_agent(record.address, needs_sched);
            futures.emplace_back(std::move(f));
        }

        core::error_t first_error = core::error_t::no_error();
        for (auto& f : futures) {
            auto err = co_await std::move(f);
            if (err.contains_error() && !first_error.contains_error()) {
                first_error = std::move(err);
            }
        }
        co_return first_error;
    }

    manager_index_t::unique_future<core::error_t>
    manager_index_t::update_rows(execution_context_t ctx,
                                 components::catalog::oid_t table_oid,
                                 std::pmr::vector<components::vector::data_chunk_t> old_data,
                                 std::pmr::vector<components::vector::data_chunk_t> new_data,
                                 std::pmr::vector<int64_t> row_ids,
                                 int64_t new_start_row_id) {
        if (row_ids.empty())
            co_return core::error_t::no_error();

        auto txn_id = ctx.txn.transaction_id;
        auto it = indexes_per_oid_.find(table_oid);
        if (it == indexes_per_oid_.end())
            co_return core::error_t::no_error();

        std::pmr::vector<unique_future<core::error_t>> futures(resource_);
        futures.reserve(it->second.size() * 2);
        for (const auto& record : it->second) {
            // The OLD half first, then the NEW one. Within one agent's FIFO the two land
            // in that order, which is the order the row versions happened in; across
            // agents they are independent.
            auto old_batch = collect_by_row_ids(resource_, record.keys, old_data, row_ids);
            if (!old_batch.empty()) {
                auto [needs_sched, f] = actor_zeta::otterbrix::send<&index_agent_contract::stage_deletes>(
                    record.address,
                    ctx.session,
                    txn_id,
                    std::move(old_batch));
                schedule_agent(record.address, needs_sched);
                futures.emplace_back(std::move(f));
            }
            // New rows are appended contiguously from new_start_row_id, aligned
            // positionally to the deleted old rows.
            auto new_batch =
                collect_contiguous(resource_, record.keys, new_data, new_start_row_id, row_ids.size());
            if (!new_batch.empty()) {
                auto [needs_sched, f] = actor_zeta::otterbrix::send<&index_agent_contract::stage_inserts>(
                    record.address,
                    ctx.session,
                    txn_id,
                    std::move(new_batch));
                schedule_agent(record.address, needs_sched);
                futures.emplace_back(std::move(f));
            }
        }

        core::error_t first_error = core::error_t::no_error();
        for (auto& f : futures) {
            auto err = co_await std::move(f);
            if (err.contains_error() && !first_error.contains_error()) {
                first_error = std::move(err);
            }
        }
        co_return first_error;
    }

    // --- MVCC commit/revert/cleanup ---

    manager_index_t::unique_future<core::error_t>
    manager_index_t::commit_inserts(execution_context_t ctx,
                                    std::pmr::vector<components::catalog::oid_t> table_oids,
                                    // The commit id is no longer carried down. It stamped
                                    // insert_id / delete_id on in-memory index entries, and
                                    // those stamps died with the last in-memory index: a
                                    // committed row is simply in the store and an
                                    // uncommitted one is simply in a bucket. It stays in the
                                    // signature because it is part of index_contract.
                                    uint64_t /*commit_id*/) {
        auto session = ctx.session;
        auto txn_id = ctx.txn.transaction_id;

        // Two-phase fan-out across the WHOLE batch: send every oid's commit with no
        // intervening co_await, then await them all. Tables with no registry entry are
        // skipped silently — the table is not indexed, so there is nothing to publish.
        //
        // THE PENDING ENTRIES ARE NOT READ HERE ANY MORE. They used to be pulled out of
        // per-index facade objects in this actor, batched per agent and shipped back down;
        // the agent holds them now, so a commit is one message that names the transaction
        // and nothing else. Which buckets it publishes ({txn_id} and 0) is the agent's
        // business, and it is the same pair this loop used to fold together by hand.
        //
        // Every future is drained even after the first failure; the first
        // contains_error() across the batch is what is returned.
        std::pmr::vector<unique_future<core::error_t>> futures(resource_);
        for (auto table_oid : table_oids) {
            auto it = indexes_per_oid_.find(table_oid);
            if (it == indexes_per_oid_.end())
                continue;
            for (const auto& record : it->second) {
                auto [needs_sched, f] =
                    actor_zeta::otterbrix::send<&index_agent_contract::commit_inserts>(record.address,
                                                                                       session,
                                                                                       txn_id);
                schedule_agent(record.address, needs_sched);
                futures.emplace_back(std::move(f));
            }
        }

        core::error_t first_error = core::error_t::no_error();
        for (auto& f : futures) {
            auto err = co_await std::move(f);
            if (err.contains_error() && !first_error.contains_error()) {
                first_error = std::move(err);
            }
        }
        // NO POST-AWAIT FLIP. The in-memory half that had to be flipped after the disk
        // fan-out — and re-looked-up by oid first, because the awaits suspend this
        // single-threaded loop and a neighbouring handler may have erased the engine — was
        // the facades' pending buckets. The agent clears its own bucket as part of
        // publishing it, in its own thread, so there is nothing here to re-find and
        // nothing that can go stale across the await.
        co_return first_error;
    }

    // NOT the mirror of commit_inserts, and the asymmetry is the whole of C5c.
    //
    // An INSERT may reach the store the moment it commits: the index is allowed to name
    // rows a reader must not see and the fetch drops them (C4b). A DELETE may not, because
    // the mistake it makes points the other way. A reader whose snapshot predates this
    // commit still OWNS the row -- row_version_manager keeps it alive while
    // delete_id > snapshot_horizon -- and if the id has left the index, storage_fetch is
    // never asked for it and no filter downstream can put it back. That is a SHORT answer
    // to a correct query, from two ordinary overlapping transactions: no checkpoint, no
    // restart, no crash.
    //
    // So this handler PUBLISHES NOTHING. It records (table, index, txn, commit) and hands
    // the erase to on_horizon_advanced, which sends it once no live snapshot can still
    // want the rows. The rows themselves stay exactly where stage_deletes left them, in
    // each agent's own bucket: this queue carries the SCHEDULE, never a copy of the data
    // (see deferred_deletes_).
    //
    // ZERO CROSS-ACTOR AWAITS -- the two-phase send/await shape is gone with the sends,
    // and the reply is now unconditionally no_error(): there is no IO here left to fail.
    manager_index_t::unique_future<core::error_t>
    manager_index_t::commit_deletes(execution_context_t ctx,
                                    std::pmr::vector<components::catalog::oid_t> table_oids,
                                    uint64_t commit_id) {
        auto txn_id = ctx.txn.transaction_id;

        // The transition, not the state: the dispatcher's flag is cleared ONLY by this
        // manager's own ack, and that ack requires deferred_deletes_ to be empty, so a
        // non-empty queue proves the flag is still set. Marking again would be a message
        // per committing DELETE for a boolean that is already true.
        const bool was_empty = deferred_deletes_.empty();
        const auto queued_before = deferred_deletes_.size();

        // Tables with no registry entry are skipped: not indexed, nothing was staged,
        // nothing to publish later.
        for (auto table_oid : table_oids) {
            auto it = indexes_per_oid_.find(table_oid);
            if (it == indexes_per_oid_.end())
                continue;
            for (const auto& record : it->second) {
                deferred_deletes_.emplace_back(
                    deferred_delete_t{table_oid, record.index_oid, txn_id, commit_id});
            }
        }
#ifdef DEV_MODE
        g_index_deferred_deletes.fetch_add(deferred_deletes_.size() - queued_before, std::memory_order_relaxed);
#endif

        if (was_empty && !deferred_deletes_.empty() &&
            manager_dispatcher_ != actor_zeta::address_t::empty_address()) {
            // Arm the selective broadcast. Without this the dispatcher only fans
            // on_horizon_advanced out to subscribers with DROPPED RESOURCES, and a database
            // that never drops a table would never hear a horizon -- so the erases above
            // would wait forever. The flag means "this subscriber has reclaimable state
            // pending a horizon", which a held-back erase is exactly as much as a dropped
            // table's tombstone is.
            //
            // Fire-and-forget, parked in pending_void_ so flush_all_indexes drains it
            // rather than dropping it -- the same handling the ack gets. It is ENQUEUED
            // before this handler replies, and operator_commit_transaction awaits that
            // reply before it sends txn_publish_msg, so the mark is in the dispatcher's
            // mailbox ahead of the publish that triggers the broadcast.
            constexpr uint8_t INDEX_KIND = 2;
            pending_void_.emplace_back(
                std::move(actor_zeta::send(manager_dispatcher_,
                                           &services::dispatcher::manager_dispatcher_t::on_drop_resource_marked,
                                           INDEX_KIND)
                              .second));
        }
        co_return core::error_t::no_error();
    }

    void manager_index_t::forget_deferred_deletes(components::catalog::oid_t table_oid) {
        const auto queued_before = deferred_deletes_.size();
        deferred_deletes_.erase(std::remove_if(deferred_deletes_.begin(),
                                               deferred_deletes_.end(),
                                               [table_oid](const deferred_delete_t& entry) {
                                                   return entry.table_oid == table_oid;
                                               }),
                                deferred_deletes_.end());
#ifdef DEV_MODE
        g_index_deferred_deletes.fetch_sub(queued_before - deferred_deletes_.size(), std::memory_order_relaxed);
#endif
    }

    void manager_index_t::forget_deferred_deletes(components::catalog::oid_t table_oid,
                                                  components::catalog::oid_t index_oid) {
        const auto queued_before = deferred_deletes_.size();
        deferred_deletes_.erase(std::remove_if(deferred_deletes_.begin(),
                                               deferred_deletes_.end(),
                                               [table_oid, index_oid](const deferred_delete_t& entry) {
                                                   return entry.table_oid == table_oid &&
                                                          entry.index_oid == index_oid;
                                               }),
                                deferred_deletes_.end());
#ifdef DEV_MODE
        g_index_deferred_deletes.fetch_sub(queued_before - deferred_deletes_.size(), std::memory_order_relaxed);
#endif
    }

    manager_index_t::unique_future<void> manager_index_t::revert_insert(execution_context_t ctx,
                                                                        components::catalog::oid_t table_oid) {
        auto txn_id = ctx.txn.transaction_id;
        auto it = indexes_per_oid_.find(table_oid);
        if (it == indexes_per_oid_.end())
            co_return;

        // Nothing durable was written for this transaction (owner decision 16), so the
        // abort is a bucket erase in each agent and touches no store.
        std::pmr::vector<unique_future<core::error_t>> futures(resource_);
        futures.reserve(it->second.size());
        for (const auto& record : it->second) {
            auto [needs_sched, f] =
                actor_zeta::otterbrix::send<&index_agent_contract::revert_inserts>(record.address,
                                                                                   ctx.session,
                                                                                   txn_id);
            schedule_agent(record.address, needs_sched);
            futures.emplace_back(std::move(f));
        }
        for (auto& f : futures) {
            // The contract gives an abort no error channel of its own (there is no
            // statement left to fail), so a refusal is RECORDED rather than propagated —
            // but the future is still awaited, never dropped.
            auto err = co_await std::move(f);
            if (err.contains_error()) {
                error(log_, "manager_index_t::revert_insert: {}", err.what);
            }
        }
        co_return;
    }

    manager_index_t::unique_future<void> manager_index_t::revert_delete(execution_context_t ctx,
                                                                        components::catalog::oid_t table_oid) {
        auto txn_id = ctx.txn.transaction_id;
        auto it = indexes_per_oid_.find(table_oid);
        if (it == indexes_per_oid_.end())
            co_return;

        // The abort mirror of revert_insert: aborted DELETE markers never reached disk.
        std::pmr::vector<unique_future<core::error_t>> futures(resource_);
        futures.reserve(it->second.size());
        for (const auto& record : it->second) {
            auto [needs_sched, f] =
                actor_zeta::otterbrix::send<&index_agent_contract::revert_deletes>(record.address,
                                                                                   ctx.session,
                                                                                   txn_id);
            schedule_agent(record.address, needs_sched);
            futures.emplace_back(std::move(f));
        }
        for (auto& f : futures) {
            auto err = co_await std::move(f);
            if (err.contains_error()) {
                error(log_, "manager_index_t::revert_delete: {}", err.what);
            }
        }
        co_return;
    }

    manager_index_t::unique_future<void> manager_index_t::cleanup_all_versions(session_id_t /*session*/,
                                                                               uint64_t /*lowest_active*/) {
        // NOTHING TO RECLAIM, and that is the true answer rather than a stub.
        //
        // Version stamps — insert_id / delete_id on an index entry — were an IN-MEMORY
        // index concept, and there has been no in-memory index for two stages. In a
        // disk-backed index a committed row is simply in the store and an uncommitted one
        // is simply in a per-transaction bucket, which its own commit or abort removes.
        // There is no old version left anywhere for a snapshot floor to free.
        //
        // The handler stays because it is part of index_contract and its callers send it;
        // what it used to do was walk every index and call a cleanup_versions_impl whose
        // body, on both surviving classes, was empty.
        co_return;
    }

    manager_index_t::unique_future<std::pmr::vector<components::catalog::oid_t>>
    manager_index_t::all_indexed_oids(session_id_t /*session*/) {
        // Every oid whose registry entry holds >= 1 index (an entry is created empty for
        // every table), EXCLUDING oids mid-GC (in dropped_table_agents_) — repopulating a
        // dropping table would resurrect entries about to be reaped by
        // on_horizon_advanced.
        std::pmr::vector<components::catalog::oid_t> result(resource_);
        result.reserve(indexes_per_oid_.size());
        for (auto& [oid, records] : indexes_per_oid_) {
            if (records.empty()) {
                continue;
            }
            if (dropped_table_agents_.find(oid) != dropped_table_agents_.end()) {
                continue;
            }
            result.emplace_back(oid);
        }
        co_return result;
    }

    manager_index_t::unique_future<core::error_t>
    manager_index_t::repopulate_table(session_id_t session,
                                      components::catalog::oid_t table_oid,
                                      std::pmr::vector<components::vector::data_chunk_t> chunks,
                                      uint64_t row_count,
                                      core::date::timezone_offset_t /*session_tz*/) {
#ifdef DEV_MODE
        g_index_repopulations.fetch_add(1, std::memory_order_relaxed);
#endif
        trace(log_, "manager_index_t::repopulate_table: oid={} rows={}", static_cast<unsigned>(table_oid), row_count);

        auto it = indexes_per_oid_.find(table_oid);
        if (it == indexes_per_oid_.end() || it->second.empty()) {
            // Table dropped, never registered, or registered with no index — a legal
            // no-op (correct semantics, not a fallback): nothing to clear, nothing to
            // rebuild.
            co_return core::error_t::no_error();
        }
        // Producer-defect gate BEFORE any mutation: a bad feed must fail the statement,
        // not leave the index cleared-but-unrebuilt.
        if (auto chunk_error = check_rebuild_chunks_have_row_ids(chunks, resource_); chunk_error.contains_error()) {
            co_return chunk_error;
        }

        // ONE PASS OF SENDS PER AGENT, in FIFO order: clear, then the rebuilt batch, then
        // the commit that publishes it. Nothing is awaited between them, and nothing needs
        // to be: an agent's mailbox is FIFO and every one of these is posted from this one
        // thread, so an agent cannot see the batch before the clear or the commit before
        // the batch.
        //
        // That FIFO is also what closed the window this path used to have. The clear and
        // the rebuild used to be separated by a co_await IN THIS ACTOR, and a read landing
        // in between saw an index that had been wiped and not yet refilled; the only thing
        // covering it was that the refilled entries sat in bucket 0 up here. They sit in
        // bucket 0 down there now, behind the same FIFO as the clear, so there is no
        // window to cover.
        //
        // Empty chunks (table emptied by compact, or nothing visible) are valid: the
        // clears still run and nothing is staged after them.
        //
        // AND THE HELD-BACK ERASES GO WITH THE CLEAR. clear() wipes every pending bucket
        // along with the store, so the rows a deferred entry would publish stop existing
        // here; the rebuild feed below already reflects the deletes, because it is a scan
        // of what survived them. Keeping the entries would leave the queue holding a
        // schedule for data that is gone (see deferred_deletes_).
        forget_deferred_deletes(table_oid);
        std::pmr::vector<unique_future<core::error_t>> futures(resource_);
        futures.reserve(it->second.size() * 3);
        for (const auto& record : it->second) {
            auto [clear_sched, clear_future] =
                actor_zeta::otterbrix::send<&index_agent_contract::clear>(record.address, session);
            schedule_agent(record.address, clear_sched);
            futures.emplace_back(std::move(clear_future));

            if (chunks.empty()) {
                continue;
            }
            // Each row is keyed by the PHYSICAL row id the scan stamped into
            // chunk.row_ids. The rebuild stream is visibility-filtered
            // (storage_fetch_next_batch under the statement's snapshot), so it compacts
            // POSITIONS while ids keep their gaps whenever compact() was refused (an open
            // snapshot or an active scan cursor on this oid) — counting positions would
            // point every post-tombstone key one row low.
            auto batch = collect_by_chunk_row_ids(resource_, record.keys, chunks);
            if (batch.empty()) {
                continue;
            }
            // txn_id 0: committed-for-everyone. The stage/commit pair is the SAME write
            // path a statement takes, which is the point — there is no second, rebuild-only
            // route into a store for the two to drift apart on.
            auto [stage_sched, stage_future] =
                actor_zeta::otterbrix::send<&index_agent_contract::stage_inserts>(record.address,
                                                                                  session,
                                                                                  uint64_t{0},
                                                                                  std::move(batch));
            schedule_agent(record.address, stage_sched);
            futures.emplace_back(std::move(stage_future));

            auto [commit_sched, commit_future] =
                actor_zeta::otterbrix::send<&index_agent_contract::commit_inserts>(record.address,
                                                                                   session,
                                                                                   uint64_t{0});
            schedule_agent(record.address, commit_sched);
            futures.emplace_back(std::move(commit_future));
        }

        core::error_t first_error = core::error_t::no_error();
        for (auto& f : futures) {
            auto err = co_await std::move(f);
            if (err.contains_error() && !first_error.contains_error()) {
                first_error = std::move(err);
            }
        }
        // The rebuild is a statement (VACUUM / CHECKPOINT drives it), so a refusal is
        // returned rather than logged: an index that could not be rebuilt disagrees with
        // its table, and the caller is the only one that can act on that.
        co_return first_error;
    }

    // --- Txn-aware Query ---

    manager_index_t::unique_future<core::result_wrapper_t<std::pmr::vector<int64_t>>>
    manager_index_t::search_with_preferred_type(session_id_t session,
                                                components::catalog::oid_t table_oid,
                                                components::index::keys_base_storage_t keys,
                                                components::types::logical_value_t value,
                                                components::expressions::compare_type compare,
                                                components::logical_plan::index_type preferred_type,
                                                // The snapshot floor is not consulted here
                                                // and has not been since the last in-memory
                                                // index went away: which committed rows a
                                                // reader may SEE is the TABLE's decision and
                                                // storage_fetch applies it, so an index
                                                // answers a superset and never filters by
                                                // visibility. Kept because it is part of
                                                // index_contract::search.
                                                uint64_t /*start_time*/,
                                                uint64_t txn_id,
                                                core::date::timezone_offset_t /*session_tz*/) {
        auto it = indexes_per_oid_.find(table_oid);
        if (it == indexes_per_oid_.end()) {
            // A planner invariant, not a data answer: an index_scan is only built for a
            // predicate the planner already saw an index for, so arriving here means the
            // plan and the index manager disagree about the table. Reporting an empty
            // match would turn that into "no rows" — the exact silent wrong answer the
            // no-fallback rule forbids.
            co_return core::error_t{core::error_code_t::index_not_exists,
                                    std::pmr::string{"index search: no index engine for the table oid", resource_}};
        }

        // The plan's PREFERRED backend first (index_type::no_valid — "no preference" —
        // matches no registered index by construction), then the untyped lookup, whose
        // ordered-before-unordered priority is declared at match_index.
        const auto* record = match_index(it->second, keys, preferred_type);
        if (record == nullptr) {
            record = match_index(it->second, keys);
        }
        if (record == nullptr) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"index search: the table has no index on the predicate key", resource_}};
        }

        // `WHERE indexed_col <op> NULL` is UNKNOWN for every row, so it selects nothing.
        // Answered before the send, not after: an index stores only NON-NULL keys, so
        // there is no round trip to make and no key for a backend to encode. The ONE rule
        // (index_key_is_null), called rather than re-derived — the agents call the same
        // function on the other side of the mailbox.
        if (index_key_is_null(value)) {
            co_return std::pmr::vector<int64_t>(resource_);
        }

        // AN INDEX WITH NO ORDERING CAN ANSWER EQUALITY AND NOTHING ELSE. The planner
        // enforces that upstream (can_use_index refuses a range predicate unless a
        // NON-hashed index also covers the key), so a range arriving here is a routing
        // bug — and one that used to end in `not supported` raised as a C-string from
        // inside an actor coroutine, where the exception is swallowed and the statement
        // reports success over zero rows.
        //
        // Read off the RECORD, which copied it from the agent class's static
        // supports_ordered_probe_v at spawn. That is what keeps the refusal HERE, before
        // any send: the agent refuses too, but only after a round trip, and the store
        // underneath it has no ordering to answer with at all.
        if (compare != components::expressions::compare_type::eq && !record->ordered) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"index search: this index has no ordering and cannot answer a range predicate",
                                 resource_}};
        }

        auto agent_addr = record->address;
#ifdef DEV_MODE
        g_index_agent_reads.fetch_add(1, std::memory_order_relaxed);
#endif
        // THE PREDICATE AND THE TRANSACTION BOTH TRAVEL WITH THE KEY, and the transaction
        // is what changed. The agent owns BOTH halves of the answer now — the committed
        // rows in its store and this transaction's own staged writes in its buckets — so
        // it merges them itself and what comes back is the whole answer.
        //
        // That is also why nothing is re-resolved after the await. What used to follow it
        // was a re-lookup of the engine AND of the index, by oid, because the merge had to
        // be done up here against an object a neighbouring handler could have destroyed
        // while this coroutine was suspended (unregister_collection and
        // on_horizon_advanced erase the table; drop_index removes the index). There is
        // nothing left up here to go stale: the reply is the result.
        auto [needs_sched, agent_future] = actor_zeta::otterbrix::send<&index_agent_contract::read_rows>(
            agent_addr,
            session,
            compare,
            components::types::logical_value_t(resource_, value),
            txn_id);
        schedule_agent(agent_addr, needs_sched);
        auto agent_result = co_await std::move(agent_future);
        if (agent_result.has_error()) {
            // Includes the DROP INDEX race: an index dropped while the read was in flight
            // answers "the index has been dropped". The ROWS are still all there — only
            // the index that was going to name them is gone — so an empty vector would say
            // "no row matches", which is a wrong answer and not a missing one.
            co_return agent_result.error();
        }
        // A SUPERSET filter, deliberately, not a visibility one: which committed rows a
        // reader may SEE is the table's decision, and storage_fetch applies it.
        co_return std::move(agent_result.value());
    }

    manager_index_t::unique_future<core::result_wrapper_t<std::pmr::vector<int64_t>>>
    manager_index_t::search(session_id_t session,
                            components::catalog::oid_t table_oid,
                            components::index::keys_base_storage_t keys,
                            components::types::logical_value_t value,
                            components::expressions::compare_type compare,
                            uint64_t start_time,
                            uint64_t txn_id,
                            core::date::timezone_offset_t session_tz) {
        // The two handlers differ only in whether a preferred backend is named, and
        // index_type::no_valid matches no registered index — so search IS
        // search_with_preferred_type with nothing preferred, and the read path exists
        // once. Invoked directly rather than self-sent: we are already on this actor
        // (same shape as manager_wal_replicate_t co_awaiting its own truncate_before).
        co_return co_await search_with_preferred_type(session,
                                                      table_oid,
                                                      std::move(keys),
                                                      std::move(value),
                                                      compare,
                                                      components::logical_plan::index_type::no_valid,
                                                      start_time,
                                                      txn_id,
                                                      session_tz);
    }

    manager_index_t::unique_future<std::pmr::vector<components::index::keys_base_storage_t>>
    manager_index_t::get_indexed_keys(session_id_t /*session*/, components::catalog::oid_t table_oid) {
        auto it = indexes_per_oid_.find(table_oid);
        if (it == indexes_per_oid_.end()) {
            co_return std::pmr::vector<components::index::keys_base_storage_t>(resource_);
        }
        co_return indexed_keys(it->second, resource_);
    }

    manager_index_t::unique_future<std::pmr::vector<components::index::index_description_t>>
    manager_index_t::get_indexed_descriptions(session_id_t /*session*/, components::catalog::oid_t table_oid) {
        auto it = indexes_per_oid_.find(table_oid);
        if (it == indexes_per_oid_.end()) {
            co_return std::pmr::vector<components::index::index_description_t>(resource_);
        }
        co_return indexed_descriptions(it->second, resource_);
    }

    manager_index_t::unique_future<std::pmr::vector<components::catalog::oid_t>>
    manager_index_t::tables_without_indexes(session_id_t /*session*/,
                                            std::pmr::vector<components::catalog::oid_t> table_oids) {
        // Compact gate (see index_contract.hpp): a registry entry is created for EVERY
        // table at bootstrap/register_collection, so its presence alone does not mean the
        // table is indexed — the entry starts EMPTY and only CREATE INDEX puts a record in
        // it. A table is therefore safe to compact when it has no entry, or when its entry
        // holds ZERO records. Return the subset that is safe to compact, input order
        // preserved.
        std::pmr::vector<components::catalog::oid_t> result(resource_);
        result.reserve(table_oids.size());
        for (auto table_oid : table_oids) {
            auto it = indexes_per_oid_.find(table_oid);
            if (it == indexes_per_oid_.end() || it->second.empty()) {
                result.emplace_back(table_oid);
            }
        }
        co_return result;
    }

    manager_index_t::unique_future<core::error_t> manager_index_t::flush_all_indexes(session_id_t session) {
        trace(log_, "manager_index_t::flush_all_indexes, session: {}", session.data());

        // Await all pending agent operations first: this is the cross-handler
        // ordering barrier (e.g. the agent-drop futures parked by
        // on_horizon_advanced) — a force_flush must never start before an
        // in-flight drop finishes.
        for (auto& f : pending_void_) {
            co_await std::move(f);
        }
        pending_void_.clear();

        // Fan out force_flush as a mailbox op per owned disk agent (no direct
        // cross-actor synchronous call). Two-phase: send every message with no
        // intervening co_await so the agents flush in parallel, then await all
        // futures. Each force_flush naturally orders behind any pending
        // insert/remove already queued in that agent's FIFO, and the is_dropped
        // guard now lives inside the agent handler.
        std::pmr::vector<unique_future<core::error_t>> futures(resource_);
        futures.reserve(bitcask_agents_owned_.size() + btree_agents_owned_.size());
        // Both families. The agent is scheduled through the pointer we already hold rather
        // than through schedule_agent's search, which would only find it again.
        auto flush_all = [&](auto& owned) {
            for (auto& agent : owned) {
                if (!agent) {
                    continue;
                }
                auto [needs_sched, fut] =
                    actor_zeta::otterbrix::send<&index_agent_contract::force_flush>(agent->address(), session);
                if (needs_sched) {
                    scheduler_->enqueue(agent.get());
                }
                futures.emplace_back(std::move(fut));
            }
        };
        flush_all(bitcask_agents_owned_);
        flush_all(btree_agents_owned_);
        // Every future is drained, and the FIRST refusal is the one reported: the fan-out is
        // parallel and abandoning the tail would leave replies addressed to a finished frame.
        // The remaining agents still flush -- a checkpoint that stops half way through is
        // worse than one that finishes and then refuses.
        core::error_t first_error = core::error_t::no_error();
        for (auto& f : futures) {
            auto err = co_await std::move(f);
            if (err.contains_error() && !first_error.contains_error()) {
                first_error = std::move(err);
            }
        }
        co_return first_error;
    }

    // GC subscriber (see declaration): erases per-oid state for tables whose
    // dropped_at_commit_id is below the new snapshot floor, publishes the committed index
    // erases the floor has now made safe, then acks once BOTH queues are empty.
    //
    // Receiving half of the horizon GC sweep — a DECLARED maintenance bypass of the rule-3 pipeline
    // (core/pipeline_bypass.hpp lists it; the declaration itself sits at the only sender in the
    // tree, manager_dispatcher_t::try_trigger_cleanup_if_horizon_advanced). Do NOT add a second
    // sender: the horizon this argument carries is the one thing keeping the deferred erases off
    // entries a live snapshot is still entitled to read.
    manager_index_t::unique_future<void> manager_index_t::on_horizon_advanced(uint64_t new_horizon) {
        trace(log_, "manager_index_t::on_horizon_advanced , horizon : {}", new_horizon);

        // THE WHOLE MAP WALK RUNS WITHOUT SUSPENDING, and that is why the reaping is
        // split in two below: the co_await further down hands this single-threaded loop to
        // another handler, which may itself erase from dropped_table_agents_ /
        // indexes_per_oid_ / the owner vectors. An iterator held across that await would be
        // dangling. So: take everything first, then send, then await.
        detached_agents_t dying(resource_);
        for (auto it = dropped_table_agents_.begin(); it != dropped_table_agents_.end();) {
            if (it->second < new_horizon) {
                auto oid = it->first;
                // AND TAKE THE AGENTS. detach_table_agents erases the table's registry
                // entry and takes its owners in one step, so the records and the agents go
                // together. This used to send the terminal drop and leave the
                // owning pointers standing, on a comment promising they were "reaped later
                // (next force_flush pass or base_spaces shutdown)" — there was no such
                // reaper, so every GC'd indexed table leaked an agent holding its store
                // open for the life of the process. Ownership moves into this frame BEFORE
                // the drop is sent, exactly as drop_index does it, so nothing can address
                // an agent behind its own terminal message.
                //
                // AND ITS HELD-BACK ERASES GO FIRST. The agents about to be detached own
                // the buckets those erases would publish, and both die with this frame; an
                // entry left behind would be a lookup into a registry entry that no longer
                // exists on the next sweep. Before the detach, so the sweep below cannot
                // see an entry whose index is already gone.
                forget_deferred_deletes(oid);
                auto oid_agents = detach_table_agents(oid);
                for (auto& agent : oid_agents.bitcask) {
                    dying.bitcask.emplace_back(std::move(agent));
                }
                for (auto& agent : oid_agents.btree) {
                    dying.btree.emplace_back(std::move(agent));
                }
                it = dropped_table_agents_.erase(it);
            } else {
                ++it;
            }
        }

        // THE ERASES THE FLOOR HAS MADE SAFE (see deferred_deletes_). Also a walk with no
        // suspension in it, for the same reason: the sends go out first and everything is
        // awaited together at the bottom.
        //
        // `commit_id <= new_horizon`, and the boundary is not a hedge. A reader hides a
        // deleted row exactly when use_inserted_version(txn, delete_id) holds, which fails
        // only for delete_id > snapshot_horizon; a snapshot sitting AT commit_id therefore
        // already hides the row, and new_horizon is the lowest such horizon among live
        // snapshots. (The dropped-table sweep above keeps its strict `<`: reclaiming a
        // table's whole engine one horizon later costs nothing, and that condition predates
        // this one.)
        //
        // THE ADDRESS IS LOOKED UP HERE, NOT STORED. An index dropped between the commit
        // and this sweep is simply not found, and its entry leaves the queue with the erase
        // undone -- which is correct: the index it belonged to no longer exists.
        std::pmr::vector<unique_future<core::error_t>> delete_futures(resource_);
        const auto queued_before_sweep = deferred_deletes_.size();
        for (auto entry = deferred_deletes_.begin(); entry != deferred_deletes_.end();) {
            if (entry->commit_id > new_horizon) {
                ++entry;
                continue;
            }
            auto table_it = indexes_per_oid_.find(entry->table_oid);
            const index_record_t* record =
                table_it == indexes_per_oid_.end() ? nullptr : match_index_relid(table_it->second, entry->index_oid);
            if (record != nullptr) {
                auto [needs_sched, f] =
                    actor_zeta::otterbrix::send<&index_agent_contract::commit_deletes>(record->address,
                                                                                       session_id_t{},
                                                                                       entry->txn_id);
                schedule_agent(record->address, needs_sched);
                delete_futures.emplace_back(std::move(f));
            }
            entry = deferred_deletes_.erase(entry);
        }
#ifdef DEV_MODE
        g_index_deferred_deletes.fetch_sub(queued_before_sweep - deferred_deletes_.size(),
                                           std::memory_order_relaxed);
#endif

        // Sends only, no suspension: the drops go out before the ack below exactly as they
        // did when they were fire-and-forget, so nothing about the ack's timing moves.
        auto drop_futures = send_drop_to_detached(dying, session_id_t{});

        // BOTH queues, and deferred_deletes_ is the one that must not be forgotten here:
        // acking clears the dispatcher's broadcast flag, and the broadcast is the ONLY
        // thing that can ever publish a held-back erase. Reading the queues at this point
        // is exact -- nothing has suspended since they were drained, so no neighbouring
        // handler can have refilled either behind us.
        if (dropped_table_agents_.empty() && deferred_deletes_.empty() &&
            manager_dispatcher_ != actor_zeta::address_t::empty_address()) {
            // Ack so the dispatcher stops broadcasting on_horizon_advanced until
            // a new DROP TABLE re-marks the subscriber. The ack future is parked in
            // pending_void_ so flush_all_indexes drains it rather than dropping it.
            //
            // It no longer has to act as the agent-drop barrier: the drops are awaited
            // HERE, below, and the agents they belong to have already left the vectors
            // flush_all_indexes fans out over, so a force_flush cannot reach one of them
            // in the first place.
            constexpr uint8_t INDEX_KIND = 2;
            pending_void_.emplace_back(
                std::move(actor_zeta::send(manager_dispatcher_,
                                           &services::dispatcher::manager_dispatcher_t::on_subscriber_empty,
                                           INDEX_KIND)
                              .second));
        }

        for (auto& f : delete_futures) {
            // There is no statement left to fail: the transaction that asked for this
            // delete committed long ago and the caller of this handler is a fire-and-forget
            // broadcast. So a refusal is RECORDED, loudly — and the future is awaited, never
            // dropped. What it leaves behind is an index that still names a deleted row: a
            // SUPERSET, which the fetch filters, and which is the safe direction to fail in.
            auto err = co_await std::move(f);
            if (err.contains_error()) {
                error(log_, "manager_index_t::on_horizon_advanced: deferred index delete failed: {}", err.what);
            }
        }
        for (auto& f : drop_futures) {
            co_await std::move(f);
        }
        // `dying` is destroyed with this frame — THIS is where a GC'd table's agents are
        // actually freed and their stores closed. After the drop replies each mailbox is
        // provably empty, so closing them cancels nothing.
        co_return;
    }

    // Apply one WAL record's effect to the build's engine during CREATE INDEX
    // catchup (single record per call; see index_contract for param semantics).
    //
    // PHYSICAL_DELETE/UPDATE records ship only row_ids, so the operator does a
    // storage_fetch(row_ids) and forwards the recovered chunk in physical_data;
    // the same engine->mark_delete_row loop the DML path uses then applies it
    // (no engine API change, no row_id->key reverse map). UPDATE is split by the
    // operator into a PHYSICAL_UPDATE message (NEW chunk, insert half) followed
    // by a PHYSICAL_DELETE message (recovered OLD chunk, delete half). See
    // operator_create_index_backfill.cpp.
    manager_index_t::unique_future<void>
    manager_index_t::apply_wal_record_for_index(session_id_t session,
                                                components::catalog::oid_t table_oid,
                                                components::catalog::oid_t index_oid,
                                                uint64_t wal_record_id,
                                                uint8_t record_type,
                                                std::pmr::vector<int64_t> row_ids,
                                                std::pmr::vector<components::vector::data_chunk_t> physical_data,
                                                uint64_t physical_row_start,
                                                uint64_t txn_id,
                                                core::date::timezone_offset_t /*session_tz*/) {
        auto it = indexes_per_oid_.find(table_oid);
        if (it == indexes_per_oid_.end()) {
            // The entry should exist from the operator's earlier register_collection /
            // create_index; a miss is a bookkeeping bug. Log and skip (no exceptions
            // across the actor boundary).
            trace(log_,
                  "manager_index_t::apply_wal_record_for_index: no registry entry for "
                  "table_oid={} (index_oid={} wal_id={} type={}), skipping",
                  static_cast<unsigned>(table_oid),
                  static_cast<unsigned>(index_oid),
                  wal_record_id,
                  static_cast<unsigned>(record_type));
            co_return;
        }

        uint64_t total_rows = 0;
        for (const auto& chunk : physical_data) {
            total_rows += chunk.size();
        }
        if (total_rows == 0) {
            // An empty chunk is legal on every leg. On the DELETE leg it means the
            // operator's storage_fetch recovered nothing, so the rows are gone and the
            // convergence guard upstream is what catches persistent divergence.
            trace(log_,
                  "manager_index_t::apply_wal_record_for_index: empty chunk "
                  "(table_oid={} index_oid={} wal_id={} type={} row_ids={})",
                  static_cast<unsigned>(table_oid),
                  static_cast<unsigned>(index_oid),
                  wal_record_id,
                  static_cast<unsigned>(record_type),
                  row_ids.size());
            co_return;
        }

        // Which leg, and where a row's physical id comes from, is the ONLY difference
        // between the three record types:
        //   PHYSICAL_INSERT / PHYSICAL_UPDATE  rows appended from physical_row_start; the
        //                                      insert leg. UPDATE ships the NEW chunk only
        //                                      and its OLD-row delete half arrives as a
        //                                      separate PHYSICAL_DELETE message, which is
        //                                      what lets the operator run the storage_fetch
        //                                      with its own disk_address instead of this
        //                                      manager needing one (rule 10).
        //   PHYSICAL_DELETE                    rows named by the row_ids that travelled
        //                                      with the record; the delete leg.
        const bool is_delete_leg =
            record_type == static_cast<uint8_t>(services::wal::wal_record_type::PHYSICAL_DELETE);
        const bool is_insert_leg =
            record_type == static_cast<uint8_t>(services::wal::wal_record_type::PHYSICAL_INSERT) ||
            record_type == static_cast<uint8_t>(services::wal::wal_record_type::PHYSICAL_UPDATE);
        if (!is_delete_leg && !is_insert_leg) {
            trace(log_,
                  "manager_index_t::apply_wal_record_for_index: ignoring "
                  "record_type={} (table_oid={} wal_id={})",
                  static_cast<unsigned>(record_type),
                  static_cast<unsigned>(table_oid),
                  wal_record_id);
            co_return;
        }

        // Entries are tagged with the CREATE INDEX txn_id, so they stay in that
        // transaction's bucket until the post-pipeline commit publishes them with the rest
        // of the build.
        std::pmr::vector<unique_future<core::error_t>> futures(resource_);
        futures.reserve(it->second.size());
        for (const auto& record : it->second) {
            if (is_delete_leg) {
                auto batch = collect_by_row_ids(resource_, record.keys, physical_data, row_ids);
                if (batch.empty()) {
                    continue;
                }
                auto [needs_sched, f] = actor_zeta::otterbrix::send<&index_agent_contract::stage_deletes>(
                    record.address,
                    session,
                    txn_id,
                    std::move(batch));
                schedule_agent(record.address, needs_sched);
                futures.emplace_back(std::move(f));
                continue;
            }
            auto batch = collect_contiguous(resource_,
                                            record.keys,
                                            physical_data,
                                            static_cast<int64_t>(physical_row_start),
                                            total_rows);
            if (batch.empty()) {
                continue;
            }
            auto [needs_sched, f] = actor_zeta::otterbrix::send<&index_agent_contract::stage_inserts>(record.address,
                                                                                                      session,
                                                                                                      txn_id,
                                                                                                      std::move(batch));
            schedule_agent(record.address, needs_sched);
            futures.emplace_back(std::move(f));
        }

        for (auto& f : futures) {
            // Catchup has no statement to fail — the operator's convergence guard is what
            // notices a build that never caught up — so a refusal is RECORDED. The future
            // is still awaited, never dropped.
            auto err = co_await std::move(f);
            if (err.contains_error()) {
                error(log_,
                      "manager_index_t::apply_wal_record_for_index: table_oid={} index_oid={} wal_id={}: {}",
                      static_cast<unsigned>(table_oid),
                      static_cast<unsigned>(index_oid),
                      wal_record_id,
                      err.what);
            }
        }
        trace(log_,
              "manager_index_t::apply_wal_record_for_index: table_oid={} index_oid={} wal_id={} type={} rows={}",
              static_cast<unsigned>(table_oid),
              static_cast<unsigned>(index_oid),
              wal_record_id,
              static_cast<unsigned>(record_type),
              total_rows);
        co_return;
    }

} // namespace services::index
