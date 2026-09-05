#include "manager_index.hpp"

#include "bitcask_index_disk.hpp"
#include "btree_index_disk.hpp"
#include "btree_record_codec.hpp"
#include "disk_hash_table.hpp"

#include <actor-zeta/spawn.hpp>
#include <algorithm>
#include <components/index/disk_hash_single_field_index.hpp>
#include <components/index/hash_single_field_index.hpp>
#include <components/index/index_engine.hpp>
#include <components/index/single_field_index.hpp>
#include <core/b_plus_tree/b_plus_tree.hpp>
#include <core/executor.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/wal/record.hpp>
#include <set>
#include <unordered_map>

namespace {
    using namespace core::b_plus_tree;

    using value_t = components::types::logical_value_t;
    using namespace components::types;

    // Index maintenance must never fail SILENTLY.
    //
    // These loops run inside actor coroutines whose promise discards exceptions: its
    // unhandled_exception() is empty under NDEBUG (actor-zeta detail/future.hpp), so a throw from
    // one row would abandon the rest of the batch while the caller still observed success.
    //
    // Nothing throws here: the disk hash table reports failures by value and the key codec
    // asserts on its own invariants while reporting corrupt payloads as NA. A failure travels
    // the error channel instead — insert_rows/delete_rows/update_rows return core::error_t.
    template<typename Fn>
    [[nodiscard]] core::error_t
    guarded_index_row(log_t&, const char*, components::catalog::oid_t, int64_t, std::pmr::memory_resource*, Fn&& fn) {
        fn();
        return core::error_t::no_error();
    }

    // Rebuild feeds (bootstrap_repopulate_sync / repopulate_table) key every
    // re-inserted entry by the PHYSICAL row id the scan stamped into
    // chunk.row_ids — a visibility-filtered scan compacts POSITIONS, not ids,
    // so counting positions would shift every entry after a surviving
    // tombstone. A non-empty chunk without a readable FLAT row_ids buffer is
    // therefore a PRODUCER defect: fail loudly (rule 6), never guess and never
    // fall back to positional numbering.
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

    // Rehydrate decode: the b+tree hands stored keys back as physical_value (via
    // item_key_getter), which carries only the PHYSICAL type — the logical tag was erased.
    // A DATE key therefore comes back as INT32 and re-enters the in-memory index as
    // INTEGER (TIME/TIMESTAMP/TIMESTAMP_TZ as BIGINT). That is deliberate, not a gap this
    // switch could close: the raw counters order and equal-compare identically, and
    // single_field_index_t casts every later probe into its locked key domain, so lookups
    // stay exact. Restoring the true temporal type would need the logical tag, i.e.
    // decoding the raw item bytes with codec::read_logical_value instead of going through
    // physical_value at the full_scan call sites.
    value_t reverse_convert(std::pmr::memory_resource* r, const physical_value& pv) {
        switch (pv.type()) {
            case physical_type::NA:
                // NULL keys are indexed (convert() maps them to the NA physical_value), so a
                // rehydrated tree legitimately hands them back.
                return value_t(r, complex_logical_type{logical_type::NA});
            case physical_type::BOOL:
                return value_t(r, pv.value<physical_type::BOOL>());
            case physical_type::UINT8:
                return value_t(r, pv.value<physical_type::UINT8>());
            case physical_type::INT8:
                return value_t(r, pv.value<physical_type::INT8>());
            case physical_type::UINT16:
                return value_t(r, pv.value<physical_type::UINT16>());
            case physical_type::INT16:
                return value_t(r, pv.value<physical_type::INT16>());
            case physical_type::UINT32:
                return value_t(r, pv.value<physical_type::UINT32>());
            case physical_type::INT32:
                return value_t(r, pv.value<physical_type::INT32>());
            case physical_type::UINT64:
                return value_t(r, pv.value<physical_type::UINT64>());
            case physical_type::INT64:
                return value_t(r, pv.value<physical_type::INT64>());
            case physical_type::FLOAT:
                return value_t(r, pv.value<physical_type::FLOAT>());
            case physical_type::DOUBLE:
                return value_t(r, pv.value<physical_type::DOUBLE>());
            case physical_type::STRING: {
                auto sv = pv.value<physical_type::STRING>();
                return value_t(r, std::string(sv));
            }
            default:
                // Unreachable from user data: the CREATE INDEX gate
                // (is_representable_index_key_type) refuses every logical type whose physical
                // encoding this switch does not carry, so nothing else was ever written into
                // the tree. The old `return NA` collapsed such keys silently; die loudly.
                // NDEBUG coverage gap: the suite builds Debug+DEV_MODE (the assert aborts
                // first) — the std::abort() below is NOT exercised by any test.
                assert(false && "reverse_convert: physical key type not representable");
                std::abort();
        }
    }
} // anonymous namespace

namespace {
    // Batched disk operation types — collect per-agent ops, send once
    using disk_batch_t = std::vector<std::pair<value_t, size_t>>;
    using agent_batch_map_t = std::unordered_map<uintptr_t, disk_batch_t>;
    using agent_addr_map_t = std::unordered_map<uintptr_t, actor_zeta::address_t>;
} // anonymous namespace

namespace services::index {

#ifdef DEV_MODE
    namespace {
        std::atomic<uint64_t> g_index_repopulations{0};
    } // namespace
    uint64_t index_repopulations() noexcept { return g_index_repopulations.load(std::memory_order_relaxed); }
    void reset_index_repopulations() noexcept { g_index_repopulations.store(0, std::memory_order_relaxed); }
#endif
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
        , engines_(resource)
        , dropped_table_agents_(resource)
        , disk_agents_per_oid_(resource)
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
        // Mirrors register_collection's lazy init without the co_return wrapper.
        auto it = engines_.find(oid);
        if (it == engines_.end()) {
            engines_.emplace(oid, components::index::make_index_engine(resource_));
        }
    }

    void manager_index_t::bootstrap_index_sync(components::catalog::oid_t table_oid,
                                               components::catalog::oid_t index_oid,
                                               components::logical_plan::index_type type,
                                               components::index::keys_base_storage_t keys,
                                               actor_zeta::address_t disk_agent_addr,
                                               index_agent_disk_ptr disk_agent_owned,
                                               disk_hash_table_ptr shared_hash_storage) {
        // Steady-state equivalent of create_index below, minus the mailbox send
        // (see the declaration). base_spaces runs bootstrap_engine_sync for every
        // live oid first, so a missing engine here is a bootstrap-order bug:
        // log and return rather than papering over it with an emplace.
        auto it = engines_.find(table_oid);
        if (it == engines_.end()) {
            trace(log_,
                  "manager_index_t::bootstrap_index_sync: missing engine for oid={} "
                  "(index_oid={}), bootstrap order violated — skipping",
                  static_cast<unsigned>(table_oid),
                  static_cast<unsigned>(index_oid));
            return;
        }

        auto& engine = it->second;

        // Refuse duplicate registration — base_spaces should only call once
        // per alive pg_index row, but be defensive against rescan paths.
        if (engine->has_index(index_oid)) {
            trace(log_,
                  "manager_index_t::bootstrap_index_sync: index_oid={} already present on oid={}, skipping",
                  static_cast<unsigned>(index_oid),
                  static_cast<unsigned>(table_oid));
            return;
        }

        uint32_t id_index = components::index::INDEX_ID_UNDEFINED;
        switch (type) {
            case components::logical_plan::index_type::single: {
                id_index =
                    components::index::make_index<components::index::single_field_index_t>(engine, index_oid, keys);
                break;
            }
            case components::logical_plan::index_type::hashed: {
                if (path_db_.empty()) {
                    id_index = components::index::make_index<components::index::hash_single_field_index_t>(engine,
                                                                                                           index_oid,
                                                                                                           keys);
                } else if (shared_hash_storage) {
                    id_index = components::index::make_index<components::index::disk_hash_single_field_index_t>(
                        engine,
                        index_oid,
                        keys,
                        shared_hash_storage);
                } else {
                    const auto base = path_db_ / std::to_string(static_cast<unsigned>(table_oid)) /
                                      std::to_string(static_cast<unsigned>(index_oid));
                    std::filesystem::create_directories(base);
                    auto storage = services::index::disk_hash_table_t::create(
                        base / "hash_index.bin",
                        services::index::disk_hash_table_t::default_bucket_count,
                        resource_);
                    if (storage.has_error()) {
                        // Startup has no caller to answer, so the index is left
                        // UNREGISTERED rather than silently replaced by a memory
                        // one. An absent index costs a full scan; a memory index
                        // standing in for a disk index answers from an empty
                        // structure, i.e. returns wrong rows, and reports nothing.
                        error(log_,
                              "manager_index_t::bootstrap_index_sync: index_oid={} on oid={} not restored, "
                              "its disk storage could not be opened: {}",
                              static_cast<unsigned>(index_oid),
                              static_cast<unsigned>(table_oid),
                              storage.error().what);
                        return;
                    }
                    id_index = components::index::make_index<components::index::disk_hash_single_field_index_t>(
                        engine,
                        index_oid,
                        keys,
                        storage.value());
                }
                break;
            }
            default:
                trace(log_,
                      "manager_index_t::bootstrap_index_sync: unsupported index type for index_oid={} on oid={}",
                      static_cast<unsigned>(index_oid),
                      static_cast<unsigned>(table_oid));
                return;
        }

        if (id_index == components::index::INDEX_ID_UNDEFINED) {
            trace(log_,
                  "manager_index_t::bootstrap_index_sync: failed to construct index_oid={} on oid={}",
                  static_cast<unsigned>(index_oid),
                  static_cast<unsigned>(table_oid));
            return;
        }

        // Wire the in-memory index_t to its disk-persistence actor address
        // (mirrors create_index below).
        if (auto* idx = components::index::search_index(engine, keys); idx) {
            idx->set_disk_agent(disk_agent_addr, address());
            engine->add_disk_agent(id_index, disk_agent_addr);
        }

        // Rehydrate the in-memory btree from the on-disk b+tree. Without this the
        // engine is wired but its in-memory storage_ is empty, so post-restart
        // equality predicates (routed through index_scan) return 0 rows even
        // though the on-disk btree is intact.
        if (!path_db_.empty() && type == components::logical_plan::index_type::single) {
            auto btree_path = path_db_ / std::to_string(static_cast<unsigned>(table_oid)) /
                              std::to_string(static_cast<unsigned>(index_oid));
            if (std::filesystem::exists(btree_path / "metadata")) {
                core::filesystem::local_file_system_t fs;
                auto db = std::make_unique<core::b_plus_tree::btree_t>(resource_, fs, btree_path, item_key_getter);
                db->load();
                if (db->size() > 0) {
                    struct pv_entry {
                        components::types::physical_value key;
                        int64_t row_id;
                    };
                    std::pmr::vector<pv_entry> raw(resource_);
                    db->full_scan<pv_entry>(&raw, [](void* data, size_t sz) -> pv_entry {
                        auto item =
                            core::b_plus_tree::btree_t::item_data{static_cast<core::b_plus_tree::data_ptr_t>(data),
                                                                  static_cast<uint32_t>(sz)};
                        return {
                            item_key_getter(item),
                            static_cast<int64_t>(id_getter(item).value<components::types::physical_type::UINT64>())};
                    });
                    if (auto* idx = components::index::search_index(engine, keys); idx) {
                        // Bootstrap has no session — default-construct tz (UTC).
                        const core::date::timezone_offset_t bootstrap_tz{};
                        for (auto& e : raw) {
                            idx->insert(reverse_convert(resource_, e.key), e.row_id, bootstrap_tz);
                        }
                        trace(log_, "bootstrap_index_sync: loaded {} entries from btree", raw.size());
                    }
                }
            }
        }

        // Per-oid fan-out registration (used by commit_* and on_horizon_advanced
        // GC). Insertion order matches create_index for runtime/bootstrap parity.
        auto oid_it =
            disk_agents_per_oid_.try_emplace(table_oid, std::pmr::vector<actor_zeta::address_t>(resource_)).first;
        oid_it->second.emplace_back(disk_agent_addr);

        // Keep the agent alive for the manager's lifetime so the addresses
        // recorded above stay valid (reaped by on_horizon_advanced).
        disk_agents_owned_.emplace_back(std::move(disk_agent_owned));

        trace(log_,
              "manager_index_t::bootstrap_index_sync: wired index_oid={} (id={}) on oid={} type={}",
              static_cast<unsigned>(index_oid),
              id_index,
              static_cast<unsigned>(table_oid),
              static_cast<unsigned>(type));
    }

    void manager_index_t::bootstrap_dropped_sync(components::catalog::oid_t oid, uint64_t delete_id) {
        mark_table_dropped_sync(oid, delete_id);
    }

    void manager_index_t::schedule_agent(const actor_zeta::address_t& addr, bool needs_sched) {
        if (!needs_sched)
            return;
        for (auto& agent : disk_agents_owned_) {
            if (agent->address() == addr) {
                scheduler_->enqueue(agent.get());
                return;
            }
        }
    }

    // --- Collection lifecycle ---

    manager_index_t::unique_future<void> manager_index_t::register_collection(session_id_t /*session*/,
                                                                              components::catalog::oid_t table_oid) {
        trace(log_, "manager_index_t::register_collection: oid={}", static_cast<unsigned>(table_oid));

        auto it = engines_.find(table_oid);
        if (it == engines_.end()) {
            engines_.emplace(table_oid, components::index::make_index_engine(resource_));
        }
        co_return;
    }

    manager_index_t::unique_future<void> manager_index_t::unregister_collection(session_id_t /*session*/,
                                                                                components::catalog::oid_t table_oid) {
        trace(log_, "manager_index_t::unregister_collection: oid={}", static_cast<unsigned>(table_oid));

        engines_.erase(table_oid);
        disk_agents_per_oid_.erase(table_oid);
        co_return;
    }

    // --- DDL: index management ---

    manager_index_t::unique_future<core::result_wrapper_t<uint32_t>> manager_index_t::create_index(
        session_id_t /*session*/,
        components::catalog::oid_t table_oid,
        components::catalog::oid_t index_oid,
        components::index::keys_base_storage_t keys,
        components::logical_plan::index_type type,
        core::date::timezone_offset_t session_tz) {
        trace(log_,
              "manager_index_t::create_index: index_oid={} on oid={}",
              static_cast<unsigned>(index_oid),
              static_cast<unsigned>(table_oid));

        auto it = engines_.find(table_oid);
        if (it == engines_.end()) {
            co_return core::error_t{core::error_code_t::index_create_fail,
                                    std::pmr::string{"no index engine registered for the table", resource_}};
        }

        auto& engine = it->second;

        // Duplicate detection. The engine holds at most ONE index per (keys, type)
        // — mapper_ is keyed by the key set, and a colliding add_index would
        // silently half-register the new index. A duplicate CREATE INDEX by NAME
        // also lands here: the retry allocates a fresh indexrelid, but its keys
        // collide with the live index. index_create_fail, not already_exists:
        // test_index pins this code for a duplicate CREATE INDEX.
        if (engine->has_index(index_oid) || engine->matching(keys, type) != nullptr) {
            co_return core::error_t{core::error_code_t::index_create_fail,
                                    std::pmr::string{"index already exists", resource_}};
        }

        uint32_t id_index = components::index::INDEX_ID_UNDEFINED;
        services::index::disk_hash_table_ptr shared_hash_storage;
        switch (type) {
            case components::logical_plan::index_type::single: {
                id_index =
                    components::index::make_index<components::index::single_field_index_t>(engine, index_oid, keys);
                break;
            }
            case components::logical_plan::index_type::hashed: {
                if (path_db_.empty()) {
                    id_index = components::index::make_index<components::index::hash_single_field_index_t>(engine,
                                                                                                           index_oid,
                                                                                                           keys);
                } else {
                    const auto base = path_db_ / std::to_string(static_cast<unsigned>(table_oid)) /
                                      std::to_string(static_cast<unsigned>(index_oid));
                    std::filesystem::create_directories(base);
                    auto storage = services::index::disk_hash_table_t::create(
                        base / "hash_index.bin",
                        services::index::disk_hash_table_t::default_bucket_count,
                        resource_);
                    if (storage.has_error()) {
                        // The statement asked for a disk index. Handing back a
                        // memory one would report success for something the user
                        // did not ask for and cannot see, so the failure goes back
                        // to the caller, which drops the half-built index.
                        error(log_,
                              "manager_index_t::create_index: index_oid={} on oid={} failed, "
                              "disk storage could not be opened: {}",
                              static_cast<unsigned>(index_oid),
                              static_cast<unsigned>(table_oid),
                              storage.error().what);
                        co_return storage.error();
                    }
                    shared_hash_storage = storage.value();
                    id_index = components::index::make_index<components::index::disk_hash_single_field_index_t>(
                        engine,
                        index_oid,
                        keys,
                        shared_hash_storage);
                }
                break;
            }
            default:
                co_return core::error_t{core::error_code_t::index_create_fail,
                                        std::pmr::string{"unsupported index type", resource_}};
        }

        if (id_index != components::index::INDEX_ID_UNDEFINED) {
            // Load index data from btree (persistent storage). Path layout
            // mirrors disk-side ${path_db}/${table_oid}/${index_oid}/.
            if (!path_db_.empty() && type == components::logical_plan::index_type::single) {
                auto btree_path = path_db_ / std::to_string(static_cast<unsigned>(table_oid)) /
                                  std::to_string(static_cast<unsigned>(index_oid));
                if (std::filesystem::exists(btree_path / "metadata")) {
                    core::filesystem::local_file_system_t fs;
                    auto db = std::make_unique<core::b_plus_tree::btree_t>(resource_, fs, btree_path, item_key_getter);
                    db->load();

                    if (db->size() > 0) {
                        struct pv_entry {
                            components::types::physical_value key;
                            int64_t row_id;
                        };
                        std::pmr::vector<pv_entry> raw(resource_);
                        db->full_scan<pv_entry>(&raw, [](void* data, size_t sz) -> pv_entry {
                            auto item =
                                core::b_plus_tree::btree_t::item_data{static_cast<core::b_plus_tree::data_ptr_t>(data),
                                                                      static_cast<uint32_t>(sz)};
                            return {item_key_getter(item),
                                    static_cast<int64_t>(
                                        id_getter(item).value<components::types::physical_type::UINT64>())};
                        });

                        auto* idx = components::index::search_index(engine, keys);
                        if (idx) {
                            for (auto& e : raw) {
                                idx->insert(reverse_convert(resource_, e.key), e.row_id, session_tz);
                            }
                            trace(log_, "create_index: loaded {} entries from btree", raw.size());
                        }
                    }
                }
            }

            // Create disk agent for persistent storage
            if (!path_db_.empty()) {
                // Runtime DDL path: a fresh index dir with no txn-log to
                // gate, so the recover-gate set is EMPTY (correct value, not
                // a fallback). Built on resource_ — the resource the agent
                // and its bitcask store use.
                auto agent =
                    actor_zeta::spawn<index_agent_disk_t>(resource_,
                                                          path_db_,
                                                          table_oid,
                                                          index_oid,
                                                          type,
                                                          bitcask_index_disk_t::default_flush_threshold_,
                                                          bitcask_index_disk_t::default_segment_record_limit_,
                                                          btree_index_disk_t::default_flush_threshold_,
                                                          log_,
                                                          std::pmr::set<std::uint64_t>(resource_),
                                                          shared_hash_storage);

                // Link disk agent with in-memory index
                auto* idx = components::index::search_index(engine, keys);
                if (idx) {
                    idx->set_disk_agent(agent->address(), address());
                    engine->add_disk_agent(id_index, agent->address());
                }

                auto addr = agent->address();
                disk_agents_owned_.emplace_back(std::move(agent));
                // Register address in per-oid fan-out map for commit_inserts
                // / commit_deletes / on_horizon_advanced.
                auto oid_it =
                    disk_agents_per_oid_.try_emplace(table_oid, std::pmr::vector<actor_zeta::address_t>(resource_))
                        .first;
                oid_it->second.emplace_back(addr);
            }
        }

        co_return id_index;
    }

    manager_index_t::unique_future<void> manager_index_t::drop_index(session_id_t session,
                                                                     components::catalog::oid_t table_oid,
                                                                     components::catalog::oid_t index_oid) {
        trace(log_,
              "manager_index_t::drop_index: index_oid={} on oid={}",
              static_cast<unsigned>(index_oid),
              static_cast<unsigned>(table_oid));

        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return;

        auto& engine = it->second;
        auto* index = engine->matching_relid(index_oid);

        if (index) {
            // Drop disk agent if exists
            if (index->is_disk()) {
                auto agent_addr = index->disk_agent();
                auto [needs_sched, future] =
                    actor_zeta::otterbrix::send(agent_addr, &index_agent_disk_t::drop, session);
                schedule_agent(agent_addr, needs_sched);

                // Wait for drop to complete before destroying the agent
                co_await std::move(future);

                disk_agents_owned_.erase(
                    std::remove_if(disk_agents_owned_.begin(),
                                   disk_agents_owned_.end(),
                                   [&agent_addr](const auto& a) { return a->address() == agent_addr; }),
                    disk_agents_owned_.end());

                // Remove address from per-oid fan-out map (DROP INDEX, not DROP TABLE —
                // sibling indexes' addresses must stay).
                auto oid_it = disk_agents_per_oid_.find(table_oid);
                if (oid_it != disk_agents_per_oid_.end()) {
                    auto& vec = oid_it->second;
                    vec.erase(std::remove(vec.begin(), vec.end(), agent_addr), vec.end());
                    if (vec.empty()) {
                        disk_agents_per_oid_.erase(oid_it);
                    }
                }
            }

            components::index::drop_index(engine, index);
        }

        co_return;
    }

    // --- Txn-aware DML ---

    core::error_t manager_index_t::bootstrap_repopulate_sync(components::catalog::oid_t table_oid,
                                                             std::pmr::vector<components::vector::data_chunk_t> chunks,
                                                             uint64_t row_count) {
        if (chunks.empty() || row_count == 0) {
            return core::error_t::no_error();
        }
        auto it = engines_.find(table_oid);
        if (it == engines_.end()) {
            return core::error_t::no_error();
        }
        // Producer-defect gate BEFORE any mutation: a bad feed must not leave
        // the engine half-cleared.
        if (auto chunk_error = check_rebuild_chunks_have_row_ids(chunks, resource_); chunk_error.contains_error()) {
            return chunk_error;
        }
        auto& engine = it->second;
        // Clear in-memory storage_ for every index on this oid: entries loaded
        // by bootstrap_index_sync's rehydration carry pre-compact row_ids.
        for (auto idx_oid : engine->indexes()) {
            auto* idx = engine->matching_relid(idx_oid);
            if (idx) {
                idx->clean_memory_to_new_elements(0);
            }
        }
        // Re-insert each row with the PHYSICAL row id the scan stamped into
        // chunk.row_ids. The rebuild scan is visibility-filtered, so it compacts
        // POSITIONS, not ids: a tombstone that survived checkpoint (WAL-replayed
        // DELETE after a crash, or a compact refused by an open snapshot/cursor)
        // leaves a hole in the id sequence, and collection_t::fetch resolves ids
        // physically — numbering by position would point every entry after the
        // hole one row low.
        const core::date::timezone_offset_t bootstrap_tz{};
        for (const auto& chunk : chunks) {
            // Key->column resolution is a property of the CHUNK, not of the row: bind once here
            // instead of walking the columns (and building a key string per comparison) per row.
            const auto binding = engine->bind_chunk(chunk);
            const auto* chunk_row_ids = chunk.row_ids.data<int64_t>();
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                const auto row_id = chunk_row_ids[i];
                auto index_error = guarded_index_row(log_, "bootstrap_repopulate", table_oid, row_id, resource_, [&] {
                    engine->insert_row(binding, chunk, i, row_id, /*txn_id=*/0, bootstrap_tz);
                });
                if (index_error.contains_error()) {
                    // Bootstrap has no statement to fail, so a bad row is recorded and the
                    // rest of the batch continues.
                    trace(log_, "manager_index_t::bootstrap_repopulate: row {} left unindexed", row_id);
                }
            }
        }
        // Commit the txn-0 batch — the same tail repopulate_table runs. Without it
        // the rows above stay PENDING(txn=0) forever; for a disk-hash facade that
        // means every committed disk entry gains an in-memory twin and equality
        // lookups return each row twice (the disk store is authoritative for the
        // facade and already holds these rows, so no disk mirror fan-out is
        // needed here — commit only flips/clears the in-memory side).
        for (auto idx_oid : engine->indexes()) {
            auto* idx = engine->matching_relid(idx_oid);
            if (idx) {
                idx->commit_insert(0, 0);
            }
        }
        trace(log_,
              "manager_index_t::bootstrap_repopulate_sync: oid={} rows={}",
              static_cast<unsigned>(table_oid),
              row_count);
        return core::error_t::no_error();
    }

    manager_index_t::unique_future<core::error_t>
    manager_index_t::insert_rows(execution_context_t ctx,
                                 components::catalog::oid_t table_oid,
                                 std::pmr::vector<components::vector::data_chunk_t> data,
                                 uint64_t start_row_id,
                                 uint64_t count) {
        if (count == 0)
            co_return core::error_t::no_error();

        auto txn_id = ctx.txn.transaction_id;
        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return core::error_t::no_error();

        auto& engine = it->second;
        // Index every chunk's rows in vector order with contiguous row-ids based at
        // start_row_id, stopping after `count` rows (the committed/appended total).
        uint64_t inserted = 0;
        for (const auto& chunk : data) {
            const auto binding = engine->bind_chunk(chunk);
            for (uint64_t i = 0; i < chunk.size() && inserted < count; i++) {
                const auto row_id = static_cast<int64_t>(start_row_id + inserted);
                if (auto index_error = guarded_index_row(
                        log_,
                        "insert_rows",
                        table_oid,
                        row_id,
                        resource_,
                        [&] { engine->insert_row(binding, chunk, i, row_id, txn_id, ctx.session_tz); });
                    index_error.contains_error()) {
                    // The statement fails: an index entry that could not be written is not a
                    // detail to log and move past — the index would disagree with the table.
                    co_return index_error;
                }
                ++inserted;
            }
        }
        // No disk mirroring — uncommitted entries don't go to disk

        co_return core::error_t::no_error();
    }

    manager_index_t::unique_future<core::error_t>
    manager_index_t::delete_rows(execution_context_t ctx,
                                 components::catalog::oid_t table_oid,
                                 std::pmr::vector<components::vector::data_chunk_t> data,
                                 std::pmr::vector<int64_t> row_ids) {
        if (row_ids.empty())
            co_return core::error_t::no_error();

        auto txn_id = ctx.txn.transaction_id;
        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return core::error_t::no_error();

        auto& engine = it->second;
        // Walk the chunks in lockstep with the flat row_ids (row_ids[k] is the storage
        // row-id of the k-th row across the concatenated chunks).
        size_t deleted = 0;
        for (const auto& chunk : data) {
            const auto binding = engine->bind_chunk(chunk);
            for (uint64_t i = 0; i < chunk.size() && deleted < row_ids.size(); i++) {
                const auto row_id = row_ids[deleted];
                if (auto index_error = guarded_index_row(
                        log_,
                        "delete_rows",
                        table_oid,
                        row_id,
                        resource_,
                        [&] { engine->mark_delete_row(binding, chunk, i, row_id, txn_id, ctx.session_tz); });
                    index_error.contains_error()) {
                    co_return index_error;
                }
                ++deleted;
            }
        }
        // No disk mirroring — uncommitted deletes don't go to disk

        co_return core::error_t::no_error();
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
        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return core::error_t::no_error();

        auto& engine = it->second;

        // Mark old entries as deleted — walk the old chunks in lockstep with row_ids.
        size_t deleted = 0;
        for (const auto& chunk : old_data) {
            const auto binding = engine->bind_chunk(chunk);
            for (uint64_t i = 0; i < chunk.size() && deleted < row_ids.size(); i++) {
                const auto row_id = row_ids[deleted];
                if (auto index_error = guarded_index_row(
                        log_,
                        "update_rows(old)",
                        table_oid,
                        row_id,
                        resource_,
                        [&] { engine->mark_delete_row(binding, chunk, i, row_id, txn_id, ctx.session_tz); });
                    index_error.contains_error()) {
                    co_return index_error;
                }
                ++deleted;
            }
        }

        // Insert new entries — new chunks in vector order, contiguous row-ids from
        // new_start_row_id (aligned positionally to the deleted old rows).
        size_t inserted = 0;
        for (const auto& chunk : new_data) {
            const auto binding = engine->bind_chunk(chunk);
            for (uint64_t i = 0; i < chunk.size() && inserted < row_ids.size(); i++) {
                const auto row_id = new_start_row_id + static_cast<int64_t>(inserted);
                if (auto index_error = guarded_index_row(
                        log_,
                        "update_rows(new)",
                        table_oid,
                        row_id,
                        resource_,
                        [&] { engine->insert_row(binding, chunk, i, row_id, txn_id, ctx.session_tz); });
                    index_error.contains_error()) {
                    co_return index_error;
                }
                ++inserted;
            }
        }

        co_return core::error_t::no_error();
    }

    // --- MVCC commit/revert/cleanup ---

    manager_index_t::unique_future<core::error_t>
    manager_index_t::commit_inserts(execution_context_t ctx,
                                    std::pmr::vector<components::catalog::oid_t> table_oids,
                                    uint64_t commit_id) {
        auto session = ctx.session;
        auto txn_id = ctx.txn.transaction_id;

        // Two-phase fan-out across the WHOLE batch: collect every oid's pending
        // disk inserts (from both the txn-local and the global txn_id==0 maps),
        // send all insert_many messages with no intervening co_await, then await
        // the collected futures and only then flip the in-memory engines. Engines
        // with no entry in engines_ are skipped silently.
        //
        // M3.5: each insert_many now resolves to a core::error_t. We keep the
        // send-all-then-await-all shape — every future is still drained so none
        // is dropped — but record the FIRST contains_error() seen and return it.
        std::pmr::vector<unique_future<core::error_t>> futures(resource_);
        // Oids whose pending entries were actually fanned out, in batch order.
        // We record OIDS, not engine pointers: every co_await below suspends this
        // single-threaded loop, during which unregister_collection or
        // on_horizon_advanced may run and erase the engine from engines_ (table
        // dropped mid-commit), invalidating any cached engine pointer. We
        // re-lookup engines_.find(oid) after the awaits and flip only engines
        // that still exist; a vanished engine = table dropped, so skipping its
        // flip is correct — its entries died with it.
        std::pmr::vector<components::catalog::oid_t> oids_to_flip(resource_);
        oids_to_flip.reserve(table_oids.size());

        for (auto table_oid : table_oids) {
            auto it = engines_.find(table_oid);
            if (it == engines_.end())
                continue;

            auto& engine = it->second;

            // Batch this oid's pending disk inserts per agent and send all
            // insert_many messages immediately (no co_await in the loop) so the
            // N disk agents across all oids run in parallel.
            agent_batch_map_t insert_batches;
            agent_addr_map_t insert_addrs;
            engine->for_each_pending_disk_insert(
                txn_id,
                [&](const actor_zeta::address_t& agent_addr, const components::index::value_t& key, int64_t row_index) {
                    auto id = reinterpret_cast<uintptr_t>(agent_addr.get());
                    insert_addrs.try_emplace(id, agent_addr);
                    insert_batches[id].emplace_back(value_t(resource_, key), static_cast<size_t>(row_index));
                });
            if (txn_id != 0) {
                engine->for_each_pending_disk_insert(
                    0,
                    [&](const actor_zeta::address_t& agent_addr,
                        const components::index::value_t& key,
                        int64_t row_index) {
                        auto id = reinterpret_cast<uintptr_t>(agent_addr.get());
                        insert_addrs.try_emplace(id, agent_addr);
                        insert_batches[id].emplace_back(value_t(resource_, key), static_cast<size_t>(row_index));
                    });
            }

            for (auto& [id, batch] : insert_batches) {
                auto& addr = insert_addrs.at(id);
                auto [ns, f] = actor_zeta::otterbrix::send(addr,
                                                           &index_agent_disk_t::insert_many,
                                                           session,
                                                           txn_id,
                                                           std::move(batch));
                schedule_agent(addr, ns);
                futures.emplace_back(std::move(f));
            }
            oids_to_flip.emplace_back(table_oid);
        }

        // Await every disk batch across all oids before flipping any engine.
        // Drain ALL futures even after the first error so no future is dropped;
        // keep the first error to return (the batch-wide first-error contract).
        core::error_t first_error = core::error_t::no_error();
        for (auto& f : futures) {
            auto err = co_await std::move(f);
            if (err.contains_error() && !first_error.contains_error()) {
                first_error = std::move(err);
            }
        }

        // Re-lookup each oid: the awaits above suspended the loop, so an engine
        // recorded before the fan-out may have been erased meanwhile (see the
        // suspension-window note where oids_to_flip is declared). Flip only the
        // engines that still exist. The in-memory flip is otherwise unconditional:
        // a disk-agent IO failure surfaced through first_error but the engines
        // were already mutated in-memory by the DML pass, so the flip keeps the
        // in-memory MVCC state consistent with what the caller will publish/abort.
        for (auto oid : oids_to_flip) {
            auto it = engines_.find(oid);
            if (it == engines_.end())
                continue;
            auto& engine = it->second;
            engine->commit_insert(txn_id, commit_id);
            if (txn_id != 0) {
                engine->commit_insert(0, commit_id);
            }
        }

        // First contains_error() across the whole batch wins; no_error() if every
        // disk batch succeeded.
        co_return first_error;
    }

    manager_index_t::unique_future<core::error_t>
    manager_index_t::commit_deletes(execution_context_t ctx,
                                    std::pmr::vector<components::catalog::oid_t> table_oids,
                                    uint64_t commit_id) {
        auto session = ctx.session;
        auto txn_id = ctx.txn.transaction_id;

        // Batch mirror of commit_inserts: send all remove_many across every oid
        // before awaiting, then flip the in-memory engines (see commit_inserts).
        // As there, we record OIDS (not engine pointers): the co_awaits below
        // suspend this single-threaded loop, and unregister_collection /
        // on_horizon_advanced may erase an engine while suspended. We re-lookup
        // per oid after the awaits and skip any engine that vanished.
        //
        // M3.5: each remove_many resolves to a core::error_t; first error wins,
        // every future is still drained (see commit_inserts).
        std::pmr::vector<unique_future<core::error_t>> futures(resource_);
        std::pmr::vector<components::catalog::oid_t> oids_to_flip(resource_);
        oids_to_flip.reserve(table_oids.size());

        for (auto table_oid : table_oids) {
            auto it = engines_.find(table_oid);
            if (it == engines_.end())
                continue;

            auto& engine = it->second;

            agent_batch_map_t remove_batches;
            agent_addr_map_t remove_addrs;
            engine->for_each_pending_disk_delete(
                txn_id,
                [&](const actor_zeta::address_t& agent_addr, const components::index::value_t& key, int64_t row_index) {
                    auto id = reinterpret_cast<uintptr_t>(agent_addr.get());
                    remove_addrs.try_emplace(id, agent_addr);
                    remove_batches[id].emplace_back(value_t(resource_, key), static_cast<size_t>(row_index));
                });
            if (txn_id != 0) {
                engine->for_each_pending_disk_delete(
                    0,
                    [&](const actor_zeta::address_t& agent_addr,
                        const components::index::value_t& key,
                        int64_t row_index) {
                        auto id = reinterpret_cast<uintptr_t>(agent_addr.get());
                        remove_addrs.try_emplace(id, agent_addr);
                        remove_batches[id].emplace_back(value_t(resource_, key), static_cast<size_t>(row_index));
                    });
            }

            for (auto& [id, batch] : remove_batches) {
                auto& addr = remove_addrs.at(id);
                auto [ns, f] = actor_zeta::otterbrix::send(addr,
                                                           &index_agent_disk_t::remove_many,
                                                           session,
                                                           txn_id,
                                                           std::move(batch));
                schedule_agent(addr, ns);
                futures.emplace_back(std::move(f));
            }
            oids_to_flip.emplace_back(table_oid);
        }

        // Drain every future even past the first error; keep the first to return.
        core::error_t first_error = core::error_t::no_error();
        for (auto& f : futures) {
            auto err = co_await std::move(f);
            if (err.contains_error() && !first_error.contains_error()) {
                first_error = std::move(err);
            }
        }

        // Re-lookup each oid after the awaits; flip only engines that still
        // exist (see commit_inserts for the suspension-window invariant).
        for (auto oid : oids_to_flip) {
            auto it = engines_.find(oid);
            if (it == engines_.end())
                continue;
            auto& engine = it->second;
            engine->commit_delete(txn_id, commit_id);
            if (txn_id != 0) {
                engine->commit_delete(0, commit_id);
            }
        }

        co_return first_error;
    }

    manager_index_t::unique_future<void> manager_index_t::revert_insert(execution_context_t ctx,
                                                                        components::catalog::oid_t table_oid) {
        auto txn_id = ctx.txn.transaction_id;
        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return;

        it->second->revert_insert(txn_id);
        // No disk action — uncommitted entries never went to disk

        co_return;
    }

    manager_index_t::unique_future<void> manager_index_t::revert_delete(execution_context_t ctx,
                                                                        components::catalog::oid_t table_oid) {
        auto txn_id = ctx.txn.transaction_id;
        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return;

        it->second->revert_delete(txn_id);
        // No disk action — aborted delete markers never reached disk

        co_return;
    }

    manager_index_t::unique_future<void> manager_index_t::cleanup_all_versions(session_id_t /*session*/,
                                                                               uint64_t lowest_active) {
        for (auto& [oid, engine] : engines_) {
            engine->cleanup_versions(lowest_active);
        }

        co_return;
    }

    manager_index_t::unique_future<std::pmr::vector<components::catalog::oid_t>>
    manager_index_t::all_indexed_oids(session_id_t /*session*/) {
        // Every oid whose engine holds >= 1 index (size() counts indexes, not
        // entries; an engine is created empty for every table), EXCLUDING oids
        // mid-GC (in dropped_table_agents_) — repopulating a dropping table
        // would resurrect entries about to be reaped by on_horizon_advanced.
        std::pmr::vector<components::catalog::oid_t> result(resource_);
        result.reserve(engines_.size());
        for (auto& [oid, engine] : engines_) {
            if (engine->size() == 0) {
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
                                      core::date::timezone_offset_t session_tz) {
#ifdef DEV_MODE
        g_index_repopulations.fetch_add(1, std::memory_order_relaxed);
#endif
        trace(log_, "manager_index_t::repopulate_table: oid={} rows={}", static_cast<unsigned>(table_oid), row_count);

        auto it = engines_.find(table_oid);
        if (it == engines_.end()) {
            // Table dropped or never indexed — legal no-op (correct semantics,
            // not a fallback). row_count==0 with no engine: nothing to do.
            co_return core::error_t::no_error();
        }
        auto& engine = it->second;
        if (engine->size() == 0) {
            // Engine exists but holds zero indexes — nothing on disk to clear,
            // nothing in memory to rebuild. Legal no-op.
            co_return core::error_t::no_error();
        }
        // Producer-defect gate BEFORE any mutation: a bad feed must fail the
        // statement, not leave the index cleared-but-unrebuilt.
        if (auto chunk_error = check_rebuild_chunks_have_row_ids(chunks, resource_); chunk_error.contains_error()) {
            co_return chunk_error;
        }

        // (a) Clear each disk-backed index's on-disk backing FIRST: two-phase
        //     send clear() to every disk agent of this oid, then await all.
        //     disk_agents_per_oid_ holds exactly the agent addresses
        //     commit_inserts fans out to (same per-index wiring). Sending all
        //     before any co_await keeps the N agents clearing in parallel.
        std::pmr::vector<unique_future<void>> clear_futures(resource_);
        auto disk_it = disk_agents_per_oid_.find(table_oid);
        if (disk_it != disk_agents_per_oid_.end()) {
            clear_futures.reserve(disk_it->second.size());
            for (auto& addr : disk_it->second) {
                auto [needs_sched, fut] = actor_zeta::otterbrix::send(addr, &index_agent_disk_t::clear, session);
                schedule_agent(addr, needs_sched);
                clear_futures.emplace_back(std::move(fut));
            }
        }
        for (auto& f : clear_futures) {
            co_await std::move(f);
        }

        // The awaits above suspended this single-threaded loop; unregister_collection
        // or on_horizon_advanced may have erased the engine meanwhile. Re-lookup
        // and bail if it vanished (table dropped mid-repopulate — its entries
        // died with it, so skipping the rebuild is correct).
        it = engines_.find(table_oid);
        if (it == engines_.end()) {
            co_return core::error_t::no_error();
        }
        auto& engine_after = it->second;

        // (b) Clear the in-memory engine — the same clean_memory_to_new_elements(0)
        //     bootstrap_repopulate_sync uses. Entries carry pre-compact row_ids.
        for (auto idx_oid : engine_after->indexes()) {
            auto* idx = engine_after->matching_relid(idx_oid);
            if (idx) {
                idx->clean_memory_to_new_elements(0);
            }
        }

        // (c) Re-insert every chunk row with the PHYSICAL row id the scan stamped
        //     into chunk.row_ids, under txn_id=0 (committed-for-everyone — no
        //     commit needed). The rebuild stream is visibility-filtered
        //     (storage_fetch_next_batch under the statement's snapshot), so it
        //     compacts POSITIONS while ids keep their gaps whenever compact()
        //     was refused (open snapshot or active scan cursor on this oid) —
        //     counting positions would point every post-tombstone key one row
        //     low. Empty chunks (table emptied by compact, or nothing visible)
        //     are valid: the clears above ran, nothing is re-inserted here.
        if (!chunks.empty()) {
            for (const auto& chunk : chunks) {
                const auto binding = engine_after->bind_chunk(chunk);
                const auto* chunk_row_ids = chunk.row_ids.data<int64_t>();
                for (uint64_t i = 0; i < chunk.size(); ++i) {
                    const auto row_id = chunk_row_ids[i];
                    auto index_error = guarded_index_row(log_, "repopulate_table", table_oid, row_id, resource_, [&] {
                        engine_after->insert_row(binding, chunk, i, row_id, /*txn_id=*/0, session_tz);
                    });
                    if (index_error.contains_error()) {
                        trace(log_, "manager_index_t::repopulate_table: row {} left unindexed", row_id);
                    }
                }
            }

            agent_batch_map_t insert_batches;
            agent_addr_map_t insert_addrs;
            engine_after->for_each_pending_disk_insert(
                0,
                [&](const actor_zeta::address_t& agent_addr, const components::index::value_t& key, int64_t row_index) {
                    auto id = reinterpret_cast<uintptr_t>(agent_addr.get());
                    insert_addrs.try_emplace(id, agent_addr);
                    insert_batches[id].emplace_back(value_t(resource_, key), static_cast<size_t>(row_index));
                });

            std::pmr::vector<unique_future<core::error_t>> futures(resource_);
            futures.reserve(insert_batches.size());
            for (auto& [id, batch] : insert_batches) {
                auto& addr = insert_addrs.at(id);
                auto [needs_sched, f] = actor_zeta::otterbrix::send(addr,
                                                                    &index_agent_disk_t::insert_many,
                                                                    session,
                                                                    uint64_t{0},
                                                                    std::move(batch));
                schedule_agent(addr, needs_sched);
                futures.emplace_back(std::move(f));
            }
            for (auto& f : futures) {
                auto err = co_await std::move(f);
                if (err.contains_error()) {
                    trace(log_,
                          "manager_index_t::repopulate_table: disk mirror failed for oid={} error={}",
                          static_cast<unsigned>(table_oid),
                          err.what);
                }
            }

            for (auto idx_oid : engine_after->indexes()) {
                auto* idx = engine_after->matching_relid(idx_oid);
                if (idx) {
                    idx->commit_insert(0, 0);
                }
            }
        }

        co_return core::error_t::no_error();
    }

    // --- Txn-aware Query ---

    manager_index_t::unique_future<core::result_wrapper_t<std::pmr::vector<int64_t>>>
    manager_index_t::search_with_preferred_type(session_id_t session,
                                                components::catalog::oid_t table_oid,
                                                components::index::keys_base_storage_t keys,
                                                components::types::logical_value_t value,
                                                components::expressions::compare_type compare,
                                                components::logical_plan::index_type preferred_type,
                                                uint64_t start_time,
                                                uint64_t txn_id,
                                                core::date::timezone_offset_t session_tz) {
        auto it = engines_.find(table_oid);
        if (it == engines_.end()) {
            // A planner invariant, not a data answer: an index_scan is only built for a
            // predicate the planner already saw an index for, so arriving here means the
            // plan and the index manager disagree about the table. Reporting an empty
            // match would turn that into "no rows" — the exact silent wrong answer the
            // no-fallback rule forbids.
            co_return core::error_t{core::error_code_t::index_not_exists,
                                    std::pmr::string{"index search: no index engine for the table oid", resource_}};
        }

        auto* index = it->second->matching(keys, preferred_type);
        if (!index) {
            index = components::index::search_index(it->second, keys);
        }
        if (!index) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"index search: the table has no index on the predicate key", resource_}};
        }

        // `WHERE indexed_col <op> NULL` is UNKNOWN for every row, so it selects nothing.
        // Answered before the send, not after: an index stores only NON-NULL keys (see
        // index_t), so there is no round trip to make and no key for a backend to encode.
        if (value.is_null()) {
            co_return std::pmr::vector<int64_t>(resource_);
        }

        // An index with no ordering can answer equality and nothing else. The planner
        // enforces that upstream (can_use_index refuses a range predicate unless a
        // NON-hashed index also covers the key), so a range arriving here is a routing
        // bug — and one that used to end in `not supported` raised as a C-string from
        // inside an actor coroutine, where the exception is swallowed and the statement
        // reports success over zero rows.
        //
        // Asked OF the index rather than guessed from its type, and asked for EVERY
        // index rather than only the disk-backed ones: the in-memory hashed index has
        // exactly the same hole, and the guard that used to sit inside the disk-hash leg
        // below could not see it.
        if (compare != components::expressions::compare_type::eq && !index->supports_ordered_probe()) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"index search: this index has no ordering and cannot answer a range predicate",
                                 resource_}};
        }

        // Disk-backed HASHED index: the committed rows live in the agent and are read by
        // message. Everything else still answers from its in-memory structure. The
        // ordered b+tree backend now speaks the whole predicate set honestly
        // (btree_index_disk_t::scan_range — C2a), but ROUTING index_type::single onto it
        // is C2b: today that index is still constructed as an in-memory
        // single_field_index_t, whose own storage holds the answer.
        const bool read_through_agent =
            index->is_disk() && index->type() == components::logical_plan::index_type::hashed;
        if (!read_through_agent) {
            co_return index->search(compare, value, start_time, txn_id, session_tz);
        }

        // Remember the index's OWN oid before the await. The address is enough to send,
        // but not to come back to: see the re-lookup below.
        const auto index_oid = index->oid();
        auto agent_addr = index->disk_agent();
        auto [needs_sched, agent_future] =
            actor_zeta::otterbrix::send(agent_addr,
                                        &index_agent_disk_t::find_rows,
                                        session,
                                        components::types::logical_value_t(resource_, value));
        schedule_agent(agent_addr, needs_sched);
        auto agent_result = co_await std::move(agent_future);
        if (agent_result.has_error()) {
            co_return agent_result.error();
        }

        // THE AWAIT ABOVE SUSPENDED THIS SINGLE-THREADED LOOP. unregister_collection and
        // on_horizon_advanced both erase engines_[oid] (and the latter reaps the owning
        // agent), and drop_index removes the index from its engine, so `it` and `index`
        // are stale pointers from here on — the same hazard commit_inserts records where
        // it collects oids instead of engine pointers. Re-resolve BOTH, by oid.
        it = engines_.find(table_oid);
        if (it == engines_.end()) {
            // The table was dropped while the read was in flight. Its rows are gone with
            // it, so the empty answer is the true one rather than a fallback.
            co_return std::pmr::vector<int64_t>(resource_);
        }
        auto* index_after = it->second->matching_relid(index_oid);
        if (!index_after) {
            co_return std::pmr::vector<int64_t>(resource_);
        }

        // Rows on disk, plus this transaction's own uncommitted inserts, minus its own
        // uncommitted deletes. The disk half is what just came back; the txn-local half
        // never reaches disk at all and can only come from the index. Note the answer is
        // deliberately a SUPERSET filter, not a visibility one: which committed rows a
        // reader may SEE is the table's decision, and storage_fetch applies it.
        auto rows = std::move(agent_result.value());
        index_after->merge_uncommitted_rows(value, txn_id, session_tz, rows);
        co_return std::move(rows);
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
        auto it = engines_.find(table_oid);
        if (it == engines_.end()) {
            co_return std::pmr::vector<components::index::keys_base_storage_t>(resource_);
        }
        co_return it->second->all_indexed_keys();
    }

    manager_index_t::unique_future<std::pmr::vector<components::index::index_description_t>>
    manager_index_t::get_indexed_descriptions(session_id_t /*session*/, components::catalog::oid_t table_oid) {
        auto it = engines_.find(table_oid);
        if (it == engines_.end()) {
            co_return std::pmr::vector<components::index::index_description_t>(resource_);
        }
        co_return it->second->all_indexed_descriptions();
    }

    manager_index_t::unique_future<std::pmr::vector<components::catalog::oid_t>>
    manager_index_t::tables_without_indexes(session_id_t /*session*/,
                                            std::pmr::vector<components::catalog::oid_t> table_oids) {
        // Compact gate (see index_contract.hpp): an engine is created for EVERY
        // table at bootstrap/register_collection, so engine presence alone does
        // not mean the table is indexed — the engine starts EMPTY and only
        // CREATE INDEX adds indexes to it. A table is therefore safe to compact
        // when it has no engine, or when its engine holds ZERO indexes
        // (index_engine_t::size() counts registered indexes, not entries).
        // Return the subset that is safe to compact, input order preserved.
        std::pmr::vector<components::catalog::oid_t> result(resource_);
        result.reserve(table_oids.size());
        for (auto table_oid : table_oids) {
            auto it = engines_.find(table_oid);
            if (it == engines_.end() || it->second->size() == 0) {
                result.emplace_back(table_oid);
            }
        }
        co_return result;
    }

    manager_index_t::unique_future<void> manager_index_t::flush_all_indexes(session_id_t session) {
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
        std::pmr::vector<unique_future<void>> futures(resource_);
        futures.reserve(disk_agents_owned_.size());
        for (auto& agent : disk_agents_owned_) {
            if (!agent) {
                continue;
            }
            auto addr = agent->address();
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(addr, &index_agent_disk_t::force_flush, session);
            schedule_agent(addr, needs_sched);
            futures.emplace_back(std::move(fut));
        }
        for (auto& f : futures) {
            co_await std::move(f);
        }
        co_return;
    }

    // GC subscriber (see declaration): erases per-oid state for tables whose
    // dropped_at_commit_id is below the new snapshot floor, then acks on drain.
    manager_index_t::unique_future<void> manager_index_t::on_horizon_advanced(uint64_t new_horizon) {
        trace(log_, "manager_index_t::on_horizon_advanced , horizon : {}", new_horizon);
        for (auto it = dropped_table_agents_.begin(); it != dropped_table_agents_.end();) {
            if (it->second < new_horizon) {
                auto oid = it->first;
                // Drop the engine first — manager_index_t is its sole owner.
                engines_.erase(oid);
                // Best-effort terminal drop to each disk agent bound to this oid;
                // the owning pointers in disk_agents_owned_ are reaped later
                // (next force_flush pass or base_spaces shutdown).
                auto disk_it = disk_agents_per_oid_.find(oid);
                if (disk_it != disk_agents_per_oid_.end()) {
                    for (auto& addr : disk_it->second) {
                        auto [needs_sched, fut] =
                            actor_zeta::otterbrix::send(addr, &index_agent_disk_t::drop, session_id_t{});
                        schedule_agent(addr, needs_sched);
                        pending_void_.emplace_back(std::move(fut));
                    }
                    disk_agents_per_oid_.erase(disk_it);
                }
                it = dropped_table_agents_.erase(it);
            } else {
                ++it;
            }
        }
        if (dropped_table_agents_.empty() && manager_dispatcher_ != actor_zeta::address_t::empty_address()) {
            // Ack so the dispatcher stops broadcasting on_horizon_advanced until
            // a new DROP TABLE re-marks the subscriber. The ack future is parked
            // in pending_void_ as a cross-handler ordering barrier: flush_all_indexes
            // co_awaits every pending_void_ future (including the agent-drop
            // futures emitted above) BEFORE fanning out its force_flush messages,
            // so a force_flush can never race an in-flight agent drop (a btree-path
            // use-after-free).
            constexpr uint8_t INDEX_KIND = 2;
            pending_void_.emplace_back(
                std::move(actor_zeta::send(manager_dispatcher_,
                                           &services::dispatcher::manager_dispatcher_t::on_subscriber_empty,
                                           INDEX_KIND)
                              .second));
        }
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
    manager_index_t::apply_wal_record_for_index(session_id_t /*session*/,
                                                components::catalog::oid_t table_oid,
                                                components::catalog::oid_t index_oid,
                                                uint64_t wal_record_id,
                                                uint8_t record_type,
                                                std::pmr::vector<int64_t> row_ids,
                                                std::pmr::vector<components::vector::data_chunk_t> physical_data,
                                                uint64_t physical_row_start,
                                                uint64_t txn_id,
                                                core::date::timezone_offset_t session_tz) {
        auto it = engines_.find(table_oid);
        if (it == engines_.end()) {
            // Engine should exist from the operator's earlier register_collection
            // / create_index; a miss is a bookkeeping bug. Log and skip (no
            // exceptions across the actor boundary).
            trace(log_,
                  "manager_index_t::apply_wal_record_for_index: no engine for "
                  "table_oid={} (index_oid={} wal_id={} type={}), skipping",
                  static_cast<unsigned>(table_oid),
                  static_cast<unsigned>(index_oid),
                  wal_record_id,
                  static_cast<unsigned>(record_type));
            co_return;
        }

        auto& engine = it->second;

        uint64_t total_rows = 0;
        for (const auto& chunk : physical_data) {
            total_rows += chunk.size();
        }

        if (record_type == static_cast<uint8_t>(services::wal::wal_record_type::PHYSICAL_INSERT)) {
            // Replay the INSERT chunks, tagged with the CREATE INDEX txn_id so
            // entries stay PENDING until the post-pipeline commit_insert
            // publishes them with the rest of the build.
            if (total_rows == 0) {
                trace(log_,
                      "manager_index_t::apply_wal_record_for_index INSERT: empty chunk "
                      "(table_oid={} index_oid={} wal_id={})",
                      static_cast<unsigned>(table_oid),
                      static_cast<unsigned>(index_oid),
                      wal_record_id);
                co_return;
            }
            uint64_t global = 0;
            for (const auto& chunk : physical_data) {
                const auto binding = engine->bind_chunk(chunk);
                for (uint64_t i = 0; i < chunk.size(); ++i) {
                    const auto row_id = static_cast<int64_t>(physical_row_start + global);
                    auto index_error = guarded_index_row(log_, "wal_replay(insert)", table_oid, row_id, resource_, [&] {
                        engine->insert_row(binding, chunk, static_cast<size_t>(i), row_id, txn_id, session_tz);
                    });
                    if (index_error.contains_error()) {
                        trace(log_, "manager_index_t::wal_replay(insert): row {} left unindexed", row_id);
                    }
                    ++global;
                }
            }
            trace(log_,
                  "manager_index_t::apply_wal_record_for_index INSERT: "
                  "table_oid={} index_oid={} wal_id={} rows={}",
                  static_cast<unsigned>(table_oid),
                  static_cast<unsigned>(index_oid),
                  wal_record_id,
                  total_rows);
        } else if (record_type == static_cast<uint8_t>(services::wal::wal_record_type::PHYSICAL_DELETE)) {
            // physical_data is the operator-recovered key batch (see header).
            // Best-effort: if it's missing the rows are gone, skip and let the
            // operator's convergence guard catch persistent divergence; on
            // partial recovery apply only the row_ids/chunk prefix that agrees.
            if (total_rows == 0) {
                trace(log_,
                      "manager_index_t::apply_wal_record_for_index DELETE: no recovered chunk "
                      "(table_oid={} index_oid={} wal_id={} row_ids={}), skipping",
                      static_cast<unsigned>(table_oid),
                      static_cast<unsigned>(index_oid),
                      wal_record_id,
                      row_ids.size());
            } else {
                size_t global = 0;
                for (const auto& chunk : physical_data) {
                    const auto binding = engine->bind_chunk(chunk);
                    for (uint64_t i = 0; i < chunk.size() && global < row_ids.size(); ++i) {
                        const auto row_id = row_ids[global];
                        auto index_error =
                            guarded_index_row(log_, "wal_replay(delete)", table_oid, row_id, resource_, [&] {
                                engine->mark_delete_row(binding,
                                                        chunk,
                                                        static_cast<size_t>(i),
                                                        row_id,
                                                        txn_id,
                                                        session_tz);
                            });
                        if (index_error.contains_error()) {
                            trace(log_, "manager_index_t::wal_replay(delete): row {} left unindexed", row_id);
                        }
                        ++global;
                    }
                }
                trace(log_,
                      "manager_index_t::apply_wal_record_for_index DELETE: "
                      "table_oid={} index_oid={} wal_id={} rows={} (row_ids={} chunk_rows={})",
                      static_cast<unsigned>(table_oid),
                      static_cast<unsigned>(index_oid),
                      wal_record_id,
                      global,
                      row_ids.size(),
                      total_rows);
            }
        } else if (record_type == static_cast<uint8_t>(services::wal::wal_record_type::PHYSICAL_UPDATE)) {
            // Insert half only (NEW chunks in physical_data); the OLD-row delete
            // half arrives as a separate PHYSICAL_DELETE message (see header).
            // The two-message split lets the operator run the storage_fetch with
            // its own disk_address; this manager has none for the catchup path,
            // keeping inter-actor communication mailbox-only.
            if (total_rows > 0) {
                // New rows were appended from physical_row_start, so the g-th row in
                // vector order maps to physical_row_start + g (the engine's contract).
                uint64_t global = 0;
                for (const auto& chunk : physical_data) {
                    const auto binding = engine->bind_chunk(chunk);
                    for (uint64_t i = 0; i < chunk.size(); ++i) {
                        const auto row_id = static_cast<int64_t>(physical_row_start + global);
                        auto index_error =
                            guarded_index_row(log_, "wal_replay(update-new)", table_oid, row_id, resource_, [&] {
                                engine->insert_row(binding, chunk, static_cast<size_t>(i), row_id, txn_id, session_tz);
                            });
                        if (index_error.contains_error()) {
                            trace(log_, "manager_index_t::wal_replay(update-new): row {} left unindexed", row_id);
                        }
                        ++global;
                    }
                }
                trace(log_,
                      "manager_index_t::apply_wal_record_for_index UPDATE (insert new half): "
                      "table_oid={} index_oid={} wal_id={} new_rows={} old_row_ids={} "
                      "(OLD-delete half follows as a separate DELETE message)",
                      static_cast<unsigned>(table_oid),
                      static_cast<unsigned>(index_oid),
                      wal_record_id,
                      total_rows,
                      row_ids.size());
            } else {
                trace(log_,
                      "manager_index_t::apply_wal_record_for_index UPDATE: empty chunk "
                      "(table_oid={} index_oid={} wal_id={})",
                      static_cast<unsigned>(table_oid),
                      static_cast<unsigned>(index_oid),
                      wal_record_id);
            }
        } else {
            trace(log_,
                  "manager_index_t::apply_wal_record_for_index: ignoring "
                  "record_type={} (table_oid={} wal_id={})",
                  static_cast<unsigned>(record_type),
                  static_cast<unsigned>(table_oid),
                  wal_record_id);
        }
        co_return;
    }

} // namespace services::index
