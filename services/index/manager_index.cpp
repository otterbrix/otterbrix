#include "manager_index.hpp"

#include "bitcask_index_disk.hpp"
#include "btree_index_disk.hpp"
#include "disk_hash_table.hpp"

#include <actor-zeta/spawn.hpp>
#include <algorithm>
#include <components/index/hash_single_field_index.hpp>
#include <components/index/index_engine.hpp>
#include <components/index/disk_hash_single_field_index.hpp>
#include <components/index/single_field_index.hpp>
#include <core/b_plus_tree/b_plus_tree.hpp>
#include <core/b_plus_tree/msgpack_reader/msgpack_reader.hpp>
#include <core/executor.hpp>
#include <msgpack.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/wal/record.hpp>
#include <unordered_map>

namespace {
    using namespace core::b_plus_tree;

    auto item_key_getter = [](const btree_t::item_data& item) -> btree_t::index_t {
        msgpack::unpacked msg;
        msgpack::unpack(msg, item.data, item.size, [](msgpack::type::object_type, std::size_t, void*) { return true; });
        return get_field(msg.get(), "/0");
    };

    auto id_getter = [](const btree_t::item_data& item) -> btree_t::index_t {
        msgpack::unpacked msg;
        msgpack::unpack(msg, item.data, item.size, [](msgpack::type::object_type, std::size_t, void*) { return true; });
        return get_field(msg.get(), "/1");
    };

    using value_t = components::types::logical_value_t;
    using namespace components::types;

    value_t reverse_convert(std::pmr::memory_resource* r, const physical_value& pv) {
        switch (pv.type()) {
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
                return value_t(r, complex_logical_type{logical_type::NA});
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

        // Event-loop-in-thread. Senders deliver into inbox_ and wake pump_cv_;
        // this thread is the sole processor of behaviors. The in-flight list is
        // a thread-local pmr::list owned by the loop — node allocations and the
        // behavior_t coroutine frames live and die on this thread.
        loop_thread_ = std::thread([this] {
            // Local in-flight list (loop-thread-owned). Stable iterators
            // (std::pmr::list) are required because behavior_t is move-only and
            // resume can re-suspend on a new await without us touching the node.
            // this->resource(): the ctor parameter `resource` shadows the member fn.
            std::pmr::list<in_flight_entry_t> in_flight(this->resource());

            while (loop_running_.load(std::memory_order_acquire)) {
                // Drain inbox_: each raw message* released by a sender is
                // re-wrapped into a message_ptr and parked in a fresh slot.
                // pending_msg STAYS in the slot — the coroutine holds a raw
                // pointer to the message across suspension points, so msg must
                // outlive its behavior.
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
                        cont.resume();
                        poll_pending();
                        made_progress = true;
                    }
                }

                // (c) Cleanup phase — erase one done entry. The moved-out
                //     behavior (and its message) destruct here, on the loop
                //     thread (safe: ~behavior_t + Last-One-Out keeps the
                //     promise alive). pending_msg is released alongside.
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

                // Bounded-staleness idle wait: completion only sets an atomic
                // flag on the awaited future (no notify), so wake at least every
                // 100µs to re-poll readiness; enqueue notifies pump_cv_ early.
                std::unique_lock<std::mutex> lk(mutex_);
                if (inbox_.empty()) {
                    pump_cv_.wait_for(lk, std::chrono::microseconds(100));
                }
            }
            // Local in_flight destructs on the loop thread here — safe:
            // ~behavior_t + Last-One-Out keeps each promise alive.
        });
    }

    manager_index_t::~manager_index_t() {
        loop_running_.store(false, std::memory_order_release);
        pump_cv_.notify_all();
        if (loop_thread_.joinable()) {
            loop_thread_.join();
        }
        // Drain any messages a sender delivered after the loop exited — wrap
        // each leftover raw back into a message_ptr so its deleter runs.
        actor_zeta::mailbox::message* raw = nullptr;
        while (inbox_.pop(raw)) {
            actor_zeta::mailbox::message_ptr reclaim{raw};
        }
    }

    auto manager_index_t::make_type() const noexcept -> const char* { return "manager_index"; }

    std::pair<bool, actor_zeta::detail::enqueue_result>
    manager_index_t::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
        // Producer side: deliver only. Release the message into the lock-free
        // inbox_ (re-wrapped into a message_ptr by the loop thread) and wake the
        // loop. ALL processing happens on loop_thread_.
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
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::has_index>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::has_index, msg);
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
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::commit_insert>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::commit_insert, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::commit_delete>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::commit_delete, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::revert_insert>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::revert_insert, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::cleanup_all_versions>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::cleanup_all_versions, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::rebuild_indexes>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::rebuild_indexes, msg);
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
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::apply_wal_record_for_index>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::apply_wal_record_for_index, msg);
                break;
            }
            default:
                break;
        }
    }

    void manager_index_t::poll_pending() {
        // O(N) erase-remove. Single-threaded — pending_void_ is touched ONLY
        // by the loop thread (here, and from handlers running on the loop). No
        // mutex needed.
        pending_void_.erase(std::remove_if(pending_void_.begin(),
                                           pending_void_.end(),
                                           [](auto& f) { return f.is_ready(); }),
                            pending_void_.end());
    }

    void manager_index_t::sync(address_pack pack) {
        disk_address_ = std::get<0>(pack);
        trace(log_, "manager_index_t::sync: disk_address set");
    }

    void manager_index_t::mark_table_dropped_sync(components::catalog::oid_t oid, uint64_t dropped_at_commit_id) {
        // Bootstrap helper — catalog scan rebuild populates this. Also called
        // internally by the mark_table_dropped mailbox handler. NOT a mailbox
        // handler — single-threaded callers only (bootstrap path).
        dropped_table_agents_[oid] = dropped_at_commit_id;
    }

    manager_index_t::unique_future<void>
    manager_index_t::mark_table_dropped(session_id_t session,
                                        components::catalog::oid_t table_oid,
                                        uint64_t dropped_at_commit_id) {
        // Runtime DROP TABLE path — operator_dynamic_cascade_delete sends this
        // from inside the executor actor. Thin coroutine wrapper around
        // mark_table_dropped_sync so the operator can co_await a real future
        // and the dropped_table_agents_ mutation stays on the manager_index_t
        // actor's mailbox (rule 11 — no sync inter-actor mutation).
        trace(log_,
              "manager_index_t::mark_table_dropped , oid : {} , commit_id : {}",
              static_cast<unsigned>(table_oid),
              dropped_at_commit_id);
        mark_table_dropped_sync(table_oid, dropped_at_commit_id);
        co_return;
    }

    void manager_index_t::set_manager_dispatcher_sync(actor_zeta::address_t address) {
        // Bootstrap-only path: base_spaces wires this before scheduler.start.
        // Single-threaded by construction — no locking required.
        manager_dispatcher_ = std::move(address);
    }

    // ---------------- Bootstrap helpers (called pre-scheduler-start) ----------------

    void manager_index_t::bootstrap_engine_sync(components::catalog::oid_t oid) {
        // Pre-scheduler-start; single-threaded by construction. Mirrors the
        // lazy-init shape of register_collection without the co_return wrapper.
        auto it = engines_.find(oid);
        if (it == engines_.end()) {
            engines_.emplace(oid, components::index::make_index_engine(resource_));
        }
    }

    void manager_index_t::bootstrap_index_sync(components::catalog::oid_t table_oid,
                                               std::pmr::string name,
                                               components::logical_plan::index_type type,
                                               components::index::keys_base_storage_t keys,
                                               actor_zeta::address_t disk_agent_addr,
                                               index_agent_disk_ptr disk_agent_owned) {
        // Bootstrap path — base_spaces::bootstrap_indexes_sync invokes this
        // once per alive pg_index row recovered from the catalog scan, BEFORE
        // scheduler.start (rule 11 exception — single-threaded by
        // construction). Mirrors the runtime create_index handler below
        // (in-memory index_t construction + engine wiring + per-oid fan-out
        // registration) without the mailbox-send overhead, plus takes
        // ownership of the disk-persistence actor whose spawn responsibility
        // stays in base_spaces (it owns the path layout knowledge).
        //
        // No emergency engine create here: bootstrap_engine_sync is expected
        // to have run for every live oid before any bootstrap_index_sync call
        // (base_spaces sequences engines first, then per-index rows). A
        // missing entry is a bootstrap-order bug — log and return, do not
        // silently paper over it with an emplace.

        auto it = engines_.find(table_oid);
        if (it == engines_.end()) {
            trace(log_,
                  "manager_index_t::bootstrap_index_sync: missing engine for oid={} "
                  "(index name={}), bootstrap order violated — skipping",
                  static_cast<unsigned>(table_oid),
                  std::string_view(name.data(), name.size()));
            return;
        }

        auto& engine = it->second;
        const std::string index_name(name.data(), name.size());

        // Refuse duplicate registration — base_spaces should only call once
        // per alive pg_index row, but be defensive against rescan paths.
        if (engine->has_index(index_name)) {
            trace(log_,
                  "manager_index_t::bootstrap_index_sync: index {} already present on oid={}, skipping",
                  index_name,
                  static_cast<unsigned>(table_oid));
            return;
        }

        uint32_t id_index = components::index::INDEX_ID_UNDEFINED;
        switch (type) {
            case components::logical_plan::index_type::single: {
                id_index =
                    components::index::make_index<components::index::single_field_index_t>(engine, index_name, keys);
                break;
            }
            case components::logical_plan::index_type::hashed: {
                if (path_db_.empty()) {
                    id_index =
                        components::index::make_index<components::index::hash_single_field_index_t>(engine,
                                                                                                      index_name,
                                                                                                      keys);
                } else {
                    const auto base = path_db_ / std::to_string(static_cast<unsigned>(table_oid)) / index_name;
                    std::filesystem::create_directories(base);
                    try {
                        id_index = components::index::make_index<components::index::disk_hash_single_field_index_t>(
                            engine,
                            index_name,
                            keys,
                            std::make_unique<services::index::disk_hash_table_t>(base / "hash_index.bin",
                                                                                 services::index::disk_hash_table_t::default_bucket_count,
                                                                                 true,
                                                                                 resource_));
                    } catch (const std::exception& e) {
                        trace(log_,
                              "manager_index_t::bootstrap_index_sync: disk hash storage init failed, "
                              "fallback to memory: {}",
                              e.what());
                        id_index = components::index::make_index<components::index::hash_single_field_index_t>(
                            engine,
                            index_name,
                            keys);
                    }
                }
                break;
            }
            default:
                trace(log_,
                      "manager_index_t::bootstrap_index_sync: unsupported index type for {} on oid={}",
                      index_name,
                      static_cast<unsigned>(table_oid));
                return;
        }

        if (id_index == components::index::INDEX_ID_UNDEFINED) {
            trace(log_,
                  "manager_index_t::bootstrap_index_sync: failed to construct index {} on oid={}",
                  index_name,
                  static_cast<unsigned>(table_oid));
            return;
        }

        // Wire the in-memory index_t to its disk-persistence actor address
        // (mirrors the runtime create_index handler below). search_index by
        // keys is the same lookup the runtime path uses.
        if (auto* idx = components::index::search_index(engine, keys); idx) {
            idx->set_disk_agent(disk_agent_addr, address());
            engine->add_disk_agent(id_index, disk_agent_addr);
        }

        // Rehydrate in-memory btree from on-disk b+tree (same block as the
        // runtime create_index handler below). Without this, the engine is
        // wired but its in-memory storage_ is empty, so post-restart equality
        // predicates routed by the planner through index_scan return 0 rows
        // even though the disk-side btree is intact.
        if (!path_db_.empty() && type == components::logical_plan::index_type::single) {
            auto btree_path = path_db_ / std::to_string(static_cast<unsigned>(table_oid)) / index_name;
            if (std::filesystem::exists(btree_path / "metadata")) {
                try {
                    core::filesystem::local_file_system_t fs;
                    auto db =
                        std::make_unique<core::b_plus_tree::btree_t>(resource_, fs, btree_path, item_key_getter);
                    db->load();
                    if (db->size() > 0) {
                        struct pv_entry {
                            components::types::physical_value key;
                            int64_t row_id;
                        };
                        std::pmr::vector<pv_entry> raw(resource_);
                        db->full_scan<pv_entry>(&raw, [](void* data, size_t sz) -> pv_entry {
                            auto item = core::b_plus_tree::btree_t::item_data{
                                static_cast<core::b_plus_tree::data_ptr_t>(data),
                                static_cast<uint32_t>(sz)};
                            return {item_key_getter(item),
                                    static_cast<int64_t>(
                                        id_getter(item).value<components::types::physical_type::UINT64>())};
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
                } catch (const std::exception& e) {
                    trace(log_, "bootstrap_index_sync: btree load failed: {}", e.what());
                }
            }
        }

        // Per-oid fan-out registration — commit_insert / commit_delete /
        // on_horizon_advanced GC use this map. Keep insertion order matching
        // create_index for runtime/bootstrap parity.
        auto oid_it = disk_agents_per_oid_.try_emplace(
            table_oid, std::pmr::vector<actor_zeta::address_t>(resource_)).first;
        oid_it->second.emplace_back(disk_agent_addr);

        // Ownership transfer — the unique_ptr keeps the spawned agent alive
        // for the lifetime of the manager. Addresses recorded above stay
        // valid until reaped by on_horizon_advanced.
        disk_agents_owned_.emplace_back(std::move(disk_agent_owned));

        trace(log_,
              "manager_index_t::bootstrap_index_sync: wired index {} (id={}) on oid={} type={}",
              index_name,
              id_index,
              static_cast<unsigned>(table_oid),
              static_cast<unsigned>(type));
    }

    void manager_index_t::bootstrap_dropped_sync(components::catalog::oid_t oid, uint64_t delete_id) {
        // Pre-scheduler-start alias of mark_table_dropped_sync to keep the
        // bootstrap_* naming consistent at the call site.
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

    manager_index_t::unique_future<void> manager_index_t::register_collection(session_id_t session,
                                                                              components::catalog::oid_t table_oid) {
        trace(log_, "manager_index_t::register_collection: oid={}", static_cast<unsigned>(table_oid));

        auto it = engines_.find(table_oid);
        if (it == engines_.end()) {
            engines_.emplace(table_oid, components::index::make_index_engine(resource_));
        }
        co_return;
    }

    manager_index_t::unique_future<void> manager_index_t::unregister_collection(session_id_t session,
                                                                                components::catalog::oid_t table_oid) {
        trace(log_, "manager_index_t::unregister_collection: oid={}", static_cast<unsigned>(table_oid));

        engines_.erase(table_oid);
        disk_agents_per_oid_.erase(table_oid);
        co_return;
    }

    // --- DDL: index management ---

    manager_index_t::unique_future<uint32_t> manager_index_t::create_index(session_id_t session,
                                                                           components::catalog::oid_t table_oid,
                                                                           index_name_t index_name,
                                                                           components::index::keys_base_storage_t keys,
                                                                           components::logical_plan::index_type type,
                                                                           core::date::timezone_offset_t session_tz) {
        trace(log_, "manager_index_t::create_index: {} on oid={}", index_name, static_cast<unsigned>(table_oid));

        auto it = engines_.find(table_oid);
        if (it == engines_.end()) {
            co_return components::index::INDEX_ID_UNDEFINED;
        }

        auto& engine = it->second;

        if (engine->has_index(index_name)) {
            co_return components::index::INDEX_ID_UNDEFINED;
        }

        uint32_t id_index = components::index::INDEX_ID_UNDEFINED;
        switch (type) {
            case components::logical_plan::index_type::single: {
                id_index =
                    components::index::make_index<components::index::single_field_index_t>(engine, index_name, keys);
                break;
            }
            case components::logical_plan::index_type::hashed: {
                if (path_db_.empty()) {
                    id_index =
                        components::index::make_index<components::index::hash_single_field_index_t>(engine,
                                                                                                      index_name,
                                                                                                      keys);
                } else {
                    const auto base = path_db_ / std::to_string(static_cast<unsigned>(table_oid)) / index_name;
                    std::filesystem::create_directories(base);
                    try {
                        id_index = components::index::make_index<components::index::disk_hash_single_field_index_t>(
                            engine,
                            index_name,
                            keys,
                            std::make_unique<services::index::disk_hash_table_t>(base / "hash_index.bin",
                                                                                 services::index::disk_hash_table_t::default_bucket_count,
                                                                                 true,
                                                                                 resource_));
                    } catch (const std::exception& e) {
                        trace(log_,
                              "manager_index_t::create_index: disk hash storage init failed, fallback to memory: {}",
                              e.what());
                        id_index = components::index::make_index<components::index::hash_single_field_index_t>(
                            engine,
                            index_name,
                            keys);
                    }
                }
                break;
            }
            default:
                trace(log_, "manager_index_t::create_index: unsupported index type");
                co_return components::index::INDEX_ID_UNDEFINED;
        }

        if (id_index != components::index::INDEX_ID_UNDEFINED) {
            // Load index data from btree (persistent storage). Path layout
            // mirrors disk-side ${path_db}/${table_oid}/${index_name}/.
            if (!path_db_.empty() && type == components::logical_plan::index_type::single) {
                auto btree_path = path_db_ / std::to_string(static_cast<unsigned>(table_oid)) / index_name;
                if (std::filesystem::exists(btree_path / "metadata")) {
                    try {
                        core::filesystem::local_file_system_t fs;
                        auto db =
                            std::make_unique<core::b_plus_tree::btree_t>(resource_, fs, btree_path, item_key_getter);
                        db->load();

                        if (db->size() > 0) {
                            struct pv_entry {
                                components::types::physical_value key;
                                int64_t row_id;
                            };
                            std::pmr::vector<pv_entry> raw(resource_);
                            db->full_scan<pv_entry>(&raw, [](void* data, size_t sz) -> pv_entry {
                                auto item = core::b_plus_tree::btree_t::item_data{
                                    static_cast<core::b_plus_tree::data_ptr_t>(data),
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
                    } catch (const std::exception& e) {
                        trace(log_, "create_index: btree load failed: {}", e.what());
                    }
                }
            }

            // Create disk agent for persistent storage
            if (!path_db_.empty()) {
                try {
                    auto agent =
                        actor_zeta::spawn<index_agent_disk_t>(resource_,
                                                              path_db_,
                                                              table_oid,
                                                              std::string(index_name),
                                                              type,
                                                              bitcask_index_disk_t::default_flush_threshold_,
                                                              bitcask_index_disk_t::default_segment_record_limit_,
                                                              btree_index_disk_t::default_flush_threshold_,
                                                              log_);

                    // Link disk agent with in-memory index
                    auto* idx = components::index::search_index(engine, keys);
                    if (idx) {
                        idx->set_disk_agent(agent->address(), address());
                        engine->add_disk_agent(id_index, agent->address());
                    }

                    auto addr = agent->address();
                    disk_agents_owned_.emplace_back(std::move(agent));
                    // Register address in per-oid fan-out map for commit_insert
                    // / commit_delete / on_horizon_advanced.
                    auto oid_it = disk_agents_per_oid_.try_emplace(
                        table_oid, std::pmr::vector<actor_zeta::address_t>(resource_)).first;
                    oid_it->second.emplace_back(addr);
                } catch (const std::exception& e) {
                    trace(log_, "manager_index_t::create_index: disk agent creation failed: {}", e.what());
                }
            }
        }

        co_return id_index;
    }

    manager_index_t::unique_future<void>
    manager_index_t::drop_index(session_id_t session, components::catalog::oid_t table_oid, index_name_t index_name) {
        trace(log_, "manager_index_t::drop_index: {} on oid={}", index_name, static_cast<unsigned>(table_oid));

        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return;

        auto& engine = it->second;
        auto* index = components::index::search_index(engine, index_name);

        if (index) {
            // Drop disk agent if exists
            if (index->is_disk()) {
                auto agent_addr = index->disk_agent();
                auto [needs_sched, future] =
                    actor_zeta::otterbrix::send(agent_addr, &index_agent_disk_t::drop, session);
                schedule_agent(agent_addr, needs_sched);

                // Wait for drop to complete before destroying the agent
                co_await std::move(future);

                // Remove agent from disk_agents_owned_
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

    // --- Query ---

    manager_index_t::unique_future<bool> manager_index_t::has_index(session_id_t session,
                                                                    components::catalog::oid_t table_oid,
                                                                    index_name_t index_name) {
        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return false;

        co_return it->second->has_index(index_name);
    }

    // --- Txn-aware DML ---

    void manager_index_t::bootstrap_repopulate_sync(components::catalog::oid_t table_oid,
                                                    std::unique_ptr<components::vector::data_chunk_t> chunk,
                                                    uint64_t row_count) {
        if (!chunk || row_count == 0) {
            return;
        }
        auto it = engines_.find(table_oid);
        if (it == engines_.end()) {
            return;
        }
        auto& engine = it->second;
        // Clear in-memory storage_ for every index on this oid. Any entries
        // loaded earlier (e.g. via bootstrap_index_sync's disk-btree
        // rehydration) carry pre-compact row_ids and must be discarded.
        for (auto& idx_name : engine->indexes()) {
            auto* idx = components::index::search_index(engine, idx_name);
            if (idx) {
                idx->clean_memory_to_new_elements(0);
            }
        }
        // Re-insert each row with its CURRENT physical row_id (chunks
        // produced by storage_scan_segment after a checkpoint hold
        // post-compact, 0-based contiguous row_ids).
        const core::date::timezone_offset_t bootstrap_tz{};
        for (uint64_t i = 0; i < row_count; ++i) {
            engine->insert_row(*chunk, i, static_cast<int64_t>(i), /*txn_id=*/0, bootstrap_tz);
        }
        trace(log_,
              "manager_index_t::bootstrap_repopulate_sync: oid={} rows={}",
              static_cast<unsigned>(table_oid),
              row_count);
    }

    manager_index_t::unique_future<void>
    manager_index_t::insert_rows(execution_context_t ctx,
                                 components::catalog::oid_t table_oid,
                                 std::unique_ptr<components::vector::data_chunk_t> data,
                                 uint64_t start_row_id,
                                 uint64_t count) {
        if (!data || count == 0)
            co_return;

        auto txn_id = ctx.txn.transaction_id;
        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return;

        auto& engine = it->second;
        for (uint64_t i = 0; i < count; i++) {
            engine->insert_row(*data, i, static_cast<int64_t>(start_row_id + i), txn_id, ctx.session_tz);
        }
        // No disk mirroring — uncommitted entries don't go to disk

        co_return;
    }

    manager_index_t::unique_future<void>
    manager_index_t::delete_rows(execution_context_t ctx,
                                 components::catalog::oid_t table_oid,
                                 std::unique_ptr<components::vector::data_chunk_t> data,
                                 std::pmr::vector<int64_t> row_ids) {
        if (!data || row_ids.empty())
            co_return;

        auto txn_id = ctx.txn.transaction_id;
        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return;

        auto& engine = it->second;
        for (size_t i = 0; i < row_ids.size(); i++) {
            engine->mark_delete_row(*data, i, row_ids[i], txn_id, ctx.session_tz);
        }
        // No disk mirroring — uncommitted deletes don't go to disk

        co_return;
    }

    manager_index_t::unique_future<void>
    manager_index_t::update_rows(execution_context_t ctx,
                                 components::catalog::oid_t table_oid,
                                 std::unique_ptr<components::vector::data_chunk_t> old_data,
                                 std::unique_ptr<components::vector::data_chunk_t> new_data,
                                 std::pmr::vector<int64_t> row_ids,
                                 int64_t new_start_row_id) {
        if (!old_data || !new_data || row_ids.empty())
            co_return;

        auto txn_id = ctx.txn.transaction_id;
        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return;

        auto& engine = it->second;

        // Mark old entries as deleted
        for (size_t i = 0; i < row_ids.size(); i++) {
            engine->mark_delete_row(*old_data, i, row_ids[i], txn_id, ctx.session_tz);
        }

        // Insert new entries
        for (size_t i = 0; i < row_ids.size(); i++) {
            engine->insert_row(*new_data, i, new_start_row_id + static_cast<int64_t>(i), txn_id, ctx.session_tz);
        }

        co_return;
    }

    // --- MVCC commit/revert/cleanup ---

    manager_index_t::unique_future<core::error_t>
    manager_index_t::commit_insert(execution_context_t ctx, components::catalog::oid_t table_oid, uint64_t commit_id) {
        auto session = ctx.session;
        auto txn_id = ctx.txn.transaction_id;
        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return core::error_t::no_error();

        auto& engine = it->second;

        // Two-phase fan-out. Phase 1 — gather pending disk inserts from BOTH
        // the txn-local pending map and the global pending map (txn_id == 0
        // path), batch per disk-agent, then send all insert_many messages
        // without intervening co_await. Phase 2 — collect every future in one
        // pass. Decoupling send from await means N disk agents progress in
        // parallel instead of strictly serially.
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
                [&](const actor_zeta::address_t& agent_addr, const components::index::value_t& key, int64_t row_index) {
                    auto id = reinterpret_cast<uintptr_t>(agent_addr.get());
                    insert_addrs.try_emplace(id, agent_addr);
                    insert_batches[id].emplace_back(value_t(resource_, key), static_cast<size_t>(row_index));
                });
        }

        // Phase 1: send all insert_many messages, collect futures.
        std::pmr::vector<unique_future<void>> futures(resource_);
        futures.reserve(insert_batches.size());
        for (auto& [id, batch] : insert_batches) {
            auto& addr = insert_addrs.at(id);
            auto [ns, f] =
                actor_zeta::otterbrix::send(addr, &index_agent_disk_t::insert_many, session, txn_id, std::move(batch));
            schedule_agent(addr, ns);
            futures.emplace_back(std::move(f));
        }

        // Phase 2: collect every future. Underlying disk methods are
        // assert+abort terminal today, so reaching the end of this loop
        // implies success for all agents.
        for (auto& f : futures) {
            co_await std::move(f);
        }

        // In-memory flip is unconditional: if any disk agent had failed, the
        // process would already have been aborted by the assert in bitcask.
        engine->commit_insert(txn_id, commit_id);
        if (txn_id != 0) {
            engine->commit_insert(0, commit_id);
        }

        co_return core::error_t::no_error();
    }

    manager_index_t::unique_future<core::error_t>
    manager_index_t::commit_delete(execution_context_t ctx, components::catalog::oid_t table_oid, uint64_t commit_id) {
        auto session = ctx.session;
        auto txn_id = ctx.txn.transaction_id;
        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return core::error_t::no_error();

        auto& engine = it->second;

        // Two-phase fan-out (mirror of commit_insert). Phase 1 batches
        // pending disk deletes from both the txn-local pending map and the
        // global (txn_id == 0) map, then sends every remove_many message
        // before any co_await. Phase 2 awaits the collected futures.
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
                [&](const actor_zeta::address_t& agent_addr, const components::index::value_t& key, int64_t row_index) {
                    auto id = reinterpret_cast<uintptr_t>(agent_addr.get());
                    remove_addrs.try_emplace(id, agent_addr);
                    remove_batches[id].emplace_back(value_t(resource_, key), static_cast<size_t>(row_index));
                });
        }

        // Phase 1: send all remove_many messages, collect futures.
        std::pmr::vector<unique_future<void>> futures(resource_);
        futures.reserve(remove_batches.size());
        for (auto& [id, batch] : remove_batches) {
            auto& addr = remove_addrs.at(id);
            auto [ns, f] =
                actor_zeta::otterbrix::send(addr, &index_agent_disk_t::remove_many, session, txn_id, std::move(batch));
            schedule_agent(addr, ns);
            futures.emplace_back(std::move(f));
        }

        // Phase 2: collect every future. Same assert+abort terminal contract
        // as commit_insert applies — reaching this point means all succeeded.
        for (auto& f : futures) {
            co_await std::move(f);
        }

        engine->commit_delete(txn_id, commit_id);
        if (txn_id != 0) {
            engine->commit_delete(0, commit_id);
        }

        co_return core::error_t::no_error();
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

    manager_index_t::unique_future<void> manager_index_t::cleanup_all_versions(session_id_t session,
                                                                               uint64_t lowest_active) {
        for (auto& [oid, engine] : engines_) {
            engine->cleanup_versions(lowest_active);
        }

        co_return;
    }

    manager_index_t::unique_future<void> manager_index_t::rebuild_indexes(session_id_t session,
                                                                          components::catalog::oid_t table_oid) {
        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return;

        auto& engine = it->second;

        // Clear all indexes in this engine
        for (auto& idx_name : engine->indexes()) {
            auto* idx = components::index::search_index(engine, idx_name);
            if (idx) {
                idx->clean_memory_to_new_elements(0);
            }
        }

        // Rebuild will be triggered by executor sending scan data to
        // manager_index for index rebuild.
        trace(log_, "manager_index_t::rebuild_indexes: cleared indexes for oid={}", static_cast<unsigned>(table_oid));

        co_return;
    }

    // --- Txn-aware Query ---

    manager_index_t::unique_future<std::pmr::vector<int64_t>>
    manager_index_t::search_with_preferred_type(session_id_t session,
                                                components::catalog::oid_t table_oid,
                                                components::index::keys_base_storage_t keys,
                                                components::types::logical_value_t value,
                                                components::expressions::compare_type compare,
                                                components::logical_plan::index_type preferred_type,
                                                uint64_t start_time,
                                                uint64_t txn_id,
                                                core::date::timezone_offset_t session_tz) {
        std::pmr::vector<int64_t> result(resource_);
        auto it = engines_.find(table_oid);
        if (it == engines_.end()) {
            co_return result;
        }

        auto* index = it->second->matching(keys, preferred_type);
        if (!index) {
            index = components::index::search_index(it->second, keys);
        }
        if (!index) {
            co_return result;
        }
        co_return index->search(compare, value, start_time, txn_id, session_tz);
    }

    manager_index_t::unique_future<std::pmr::vector<int64_t>>
    manager_index_t::search(session_id_t session,
                            components::catalog::oid_t table_oid,
                            components::index::keys_base_storage_t keys,
                            components::types::logical_value_t value,
                            components::expressions::compare_type compare,
                            uint64_t start_time,
                            uint64_t txn_id,
                            core::date::timezone_offset_t session_tz) {
        std::pmr::vector<int64_t> result(resource_);

        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return result;

        auto* index = components::index::search_index(it->second, keys);
        if (!index)
            co_return result;

        co_return index->search(compare, value, start_time, txn_id, session_tz);
    }

    manager_index_t::unique_future<std::pmr::vector<components::index::keys_base_storage_t>>
    manager_index_t::get_indexed_keys(session_id_t session, components::catalog::oid_t table_oid) {
        auto it = engines_.find(table_oid);
        if (it == engines_.end()) {
            co_return std::pmr::vector<components::index::keys_base_storage_t>(resource_);
        }
        co_return it->second->all_indexed_keys();
    }

    manager_index_t::unique_future<std::pmr::vector<components::index::index_description_t>>
    manager_index_t::get_indexed_descriptions(session_id_t session, components::catalog::oid_t table_oid) {
        auto it = engines_.find(table_oid);
        if (it == engines_.end()) {
            co_return std::pmr::vector<components::index::index_description_t>(resource_);
        }
        co_return it->second->all_indexed_descriptions();
    }

    manager_index_t::unique_future<void> manager_index_t::flush_all_indexes(session_id_t session) {
        trace(log_, "manager_index_t::flush_all_indexes, session: {}", session.data());

        // Await all pending agent operations to ensure no in-flight writes
        for (auto& f : pending_void_) {
            co_await std::move(f);
        }
        pending_void_.clear();
        // Now safe to call synchronously — no actor messaging, avoids TSan race
        for (auto& agent : disk_agents_owned_) {
            if (agent && !agent->is_dropped()) {
                agent->force_flush_sync();
            }
        }
        co_return;
    }

    // event-driven GC subscriber. Walks dropped_table_agents_ and erases
    // the per-oid routing entries (engines_ + disk_agents_per_oid_) whose
    // dropped_at_commit_id is now strictly below the snapshot floor
    // (new_horizon). On drain (queue empty) sends on_subscriber_empty(INDEX_KIND)
    // ack to dispatcher so the selective-broadcast flag clears and no further
    // on_horizon_advanced broadcasts arrive until a new DROP TABLE re-marks
    // the subscriber.
    manager_index_t::unique_future<void> manager_index_t::on_horizon_advanced(uint64_t new_horizon) {
        trace(log_, "manager_index_t::on_horizon_advanced , horizon : {}", new_horizon);
        // symmetric GC pass: erase per-oid state for tables whose
        // dropped_at_commit_id is now below the snapshot floor.
        for (auto it = dropped_table_agents_.begin(); it != dropped_table_agents_.end();) {
            if (it->second < new_horizon) {
                auto oid = it->first;
                // Drop the engine first — manager_index_t is its sole owner.
                engines_.erase(oid);
                // Send terminal drop to each disk_agent_disk_t bound to this oid
                // (best-effort fire-and-forget — the agents are owned by
                // disk_agents_owned_; they will be reaped on next force_flush
                // pass or at base_spaces shutdown).
                auto disk_it = disk_agents_per_oid_.find(oid);
                if (disk_it != disk_agents_per_oid_.end()) {
                    for (auto& addr : disk_it->second) {
                        auto [needs_sched, fut] = actor_zeta::otterbrix::send(addr,
                                                                              &index_agent_disk_t::drop,
                                                                              session_id_t{});
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
            // ack flag flip — dispatcher clears index_has_dropped_ on
            // receipt so no further on_horizon_advanced broadcasts arrive
            // until a new DROP TABLE re-marks the subscriber. Park the
            // resulting future in pending_void_ as a CROSS-HANDLER ORDERING
            // BARRIER: flush_all_indexes co_awaits every pending_void_ future
            // (including the index_agent_disk_t::drop futures emitted above)
            // BEFORE it calls force_flush_sync(), so a force_flush can never
            // race an in-flight agent drop (which would be a use-after-free on
            // the btree path). Dropping the future itself is safe (Last-One-Out
            // keeps the promise alive); the barrier is the reason for parking.
            constexpr uint8_t INDEX_KIND = 2;
            pending_void_.emplace_back(std::move(
                actor_zeta::send(manager_dispatcher_,
                                 &services::dispatcher::manager_dispatcher_t::on_subscriber_empty,
                                 INDEX_KIND).second));
        }
        co_return;
    }

    // Apply a single WAL record's effect to the build's in-memory
    // index_engine_t during the CREATE INDEX catchup loop.
    //
    // The handler mirrors the steady-state DML path (insert_rows / delete_rows
    // / update_rows above) but drives a single record instead of a chunk batch.
    // INSERT (and the NEW-row half of UPDATE) are wired against engine->insert_row
    // using the WAL chunk + physical_row_start; the storage_row assigned to
    // each chunk row is reconstructed as physical_row_start + i so the index
    // keys land on the same row_ids the table-side writer used.
    //
    // DELETE (and the OLD-row half of UPDATE) are wired via a sibling
    // recovery path: the WAL PHYSICAL_DELETE / UPDATE records ship only
    // row_ids, so the operator side performs a storage_fetch(row_ids)
    // BEFORE sending this message and forwards the recovered chunk via
    // physical_data. The handler then runs the same engine->mark_delete_row
    // loop the steady-state DML path uses (no engine API change, no row_id
    // → key reverse map). PHYSICAL_UPDATE replay is split into two messages
    // by the operator: a PHYSICAL_UPDATE one carrying the NEW chunk for the
    // insert half, then a PHYSICAL_DELETE one carrying the recovered OLD
    // chunk for the delete half. See operator_create_index_backfill.cpp for
    // the catchup loop that drives this sequence.
    manager_index_t::unique_future<void>
    manager_index_t::apply_wal_record_for_index(session_id_t session,
                                                components::catalog::oid_t table_oid,
                                                components::catalog::oid_t index_oid,
                                                uint64_t wal_record_id,
                                                uint8_t record_type,
                                                std::pmr::vector<int64_t> row_ids,
                                                std::unique_ptr<components::vector::data_chunk_t> physical_data,
                                                uint64_t physical_row_start,
                                                uint64_t txn_id,
                                                core::date::timezone_offset_t session_tz) {
        auto it = engines_.find(table_oid);
        if (it == engines_.end()) {
            // The build's engine should already have been created by the
            // register_collection / create_index calls earlier in the operator.
            // Missing engine here means a bookkeeping mismatch; log and skip
            // (no exceptions thrown across the actor boundary).
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

        if (record_type == static_cast<uint8_t>(services::wal::wal_record_type::PHYSICAL_INSERT)) {
            // Replay the INSERT chunk into the build's engine. Entries are
            // tagged with the CREATE INDEX txn_id so they stay PENDING until
            // the post-pipeline commit_insert publishes them alongside the
            // rest of the build (operator_create_index_backfill wires
            // dml_append_row_count for the executor to fire that commit).
            if (!physical_data || physical_data->size() == 0) {
                trace(log_,
                      "manager_index_t::apply_wal_record_for_index INSERT: empty chunk "
                      "(table_oid={} index_oid={} wal_id={})",
                      static_cast<unsigned>(table_oid),
                      static_cast<unsigned>(index_oid),
                      wal_record_id);
                co_return;
            }
            const auto rows = physical_data->size();
            for (uint64_t i = 0; i < rows; ++i) {
                engine->insert_row(*physical_data,
                                   static_cast<size_t>(i),
                                   static_cast<int64_t>(physical_row_start + i),
                                   txn_id,
                                   session_tz);
            }
            trace(log_,
                  "manager_index_t::apply_wal_record_for_index INSERT: "
                  "table_oid={} index_oid={} wal_id={} rows={}",
                  static_cast<unsigned>(table_oid),
                  static_cast<unsigned>(index_oid),
                  wal_record_id,
                  rows);
        } else if (record_type == static_cast<uint8_t>(services::wal::wal_record_type::PHYSICAL_DELETE)) {
            // The PHYSICAL_DELETE WAL record itself ships only storage row_ids
            // — no key columns. The operator side (operator_create_index_backfill
            // catchup loop) closes that gap by performing a
            // storage_fetch(row_ids) before sending us the message and
            // forwards the recovered chunk via physical_data. This keeps the
            // engine API unchanged (no row_id → keys reverse map needed) at
            // the cost of one extra read-only fetch per PHYSICAL_DELETE
            // record during catchup — acceptable for the bounded-retry
            // CREATE INDEX loop.
            //
            // Defensive fall-throughs:
            //   - physical_data == nullptr or empty: the operator could not
            //     recover the chunk (rows physically gone, fetch failed).
            //     Best-effort skip + log; the operator-side convergence
            //     guard still fires if the index keeps diverging.
            //   - row_ids.size() != physical_data->size(): partial recovery
            //     (some rows missing from storage). Apply only the prefix
            //     where both sides agree and log the mismatch — same
            //     best-effort posture as the all-missing case.
            if (!physical_data || physical_data->size() == 0) {
                trace(log_,
                      "manager_index_t::apply_wal_record_for_index DELETE: no recovered chunk "
                      "(table_oid={} index_oid={} wal_id={} row_ids={}), skipping",
                      static_cast<unsigned>(table_oid),
                      static_cast<unsigned>(index_oid),
                      wal_record_id,
                      row_ids.size());
            } else {
                const auto rows = std::min<uint64_t>(physical_data->size(), row_ids.size());
                for (uint64_t i = 0; i < rows; ++i) {
                    engine->mark_delete_row(*physical_data,
                                            static_cast<size_t>(i),
                                            row_ids[static_cast<size_t>(i)],
                                            txn_id,
                                            session_tz);
                }
                trace(log_,
                      "manager_index_t::apply_wal_record_for_index DELETE: "
                      "table_oid={} index_oid={} wal_id={} rows={} (row_ids={} chunk={})",
                      static_cast<unsigned>(table_oid),
                      static_cast<unsigned>(index_oid),
                      wal_record_id,
                      rows,
                      row_ids.size(),
                      physical_data->size());
            }
        } else if (record_type == static_cast<uint8_t>(services::wal::wal_record_type::PHYSICAL_UPDATE)) {
            // PHYSICAL_UPDATE records carry the NEW chunk (in physical_data)
            // and the OLD storage row_ids (in row_ids). The new-row insert
            // half is replayed here. The OLD-row delete half is replayed by a
            // separate apply_wal_record_for_index call with
            // record_type=PHYSICAL_DELETE that the operator issues right
            // after this one — the operator owns the storage_fetch on the
            // OLD row_ids that recovers the key chunk for the delete branch
            // above (same recovery pattern as standalone PHYSICAL_DELETE).
            //
            // Splitting OLD-delete and NEW-insert into two messages keeps the
            // signature stable (one chunk per call) and lets the operator
            // schedule the fetch with its existing disk_address handle (the
            // index manager intentionally does not own a disk_address handle
            // for the catchup path — mailbox-only inter-actor communication).
            if (physical_data && physical_data->size() > 0) {
                // For UPDATE the new storage row-ids are appended starting at
                // physical_row_start (the table writer's append cursor at the
                // time the UPDATE landed); chunk row i maps to that base + i,
                // matching the row_id contract the rest of the engine assumes.
                const auto rows = physical_data->size();
                for (uint64_t i = 0; i < rows; ++i) {
                    engine->insert_row(*physical_data,
                                       static_cast<size_t>(i),
                                       static_cast<int64_t>(physical_row_start + i),
                                       txn_id,
                                       session_tz);
                }
                trace(log_,
                      "manager_index_t::apply_wal_record_for_index UPDATE (insert new half): "
                      "table_oid={} index_oid={} wal_id={} new_rows={} old_row_ids={} "
                      "(OLD-delete half follows as a separate DELETE message)",
                      static_cast<unsigned>(table_oid),
                      static_cast<unsigned>(index_oid),
                      wal_record_id,
                      rows,
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
