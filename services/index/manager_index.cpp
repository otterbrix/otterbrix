#include "manager_index.hpp"

#include <actor-zeta/spawn.hpp>
#include <algorithm>
#include <cstdint>
#include <components/vector/data_chunk.hpp>
#include <core/executor.hpp>
#include <core/file/local_file_system.hpp>
#include <fstream>
#include <services/dispatcher/dispatcher.hpp>
#include <services/wal/record.hpp>
#include <set>
#include <string>

namespace {
    using value_t = components::types::logical_value_t;

    // chunk.row_ids must be physical ids, not positions: rebuild scans compact positions but not ids.
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
        std::atomic<uint64_t> g_index_deferred_deletes{0};
        std::atomic<uint64_t> g_index_stage_insert_batches{0};
        std::atomic<const void*> g_stage_insert_owner{nullptr};
        std::atomic<uint64_t> g_index_stage_insert_foreign_batches{0};

        void note_stage_insert_batch(const void* manager) noexcept {
            g_index_stage_insert_batches.fetch_add(1, std::memory_order_relaxed);
            const void* owner = nullptr;
            if (!g_stage_insert_owner.compare_exchange_strong(owner, manager, std::memory_order_relaxed) &&
                owner != manager) {
                g_index_stage_insert_foreign_batches.fetch_add(1, std::memory_order_relaxed);
            }
        }
    } // namespace
    uint64_t index_repopulations() noexcept { return g_index_repopulations.load(std::memory_order_relaxed); }
    void reset_index_repopulations() noexcept { g_index_repopulations.store(0, std::memory_order_relaxed); }

    uint64_t index_agent_reads() noexcept { return g_index_agent_reads.load(std::memory_order_relaxed); }
    void reset_index_agent_reads() noexcept { g_index_agent_reads.store(0, std::memory_order_relaxed); }

    uint64_t index_key_column_probes() noexcept { return g_index_key_column_probes.load(std::memory_order_relaxed); }
    void reset_index_key_column_probes() noexcept { g_index_key_column_probes.store(0, std::memory_order_relaxed); }

    uint64_t index_deferred_deletes() noexcept { return g_index_deferred_deletes.load(std::memory_order_relaxed); }

    uint64_t index_stage_insert_batches() noexcept {
        return g_index_stage_insert_batches.load(std::memory_order_relaxed);
    }
    uint64_t index_stage_insert_foreign_batches() noexcept {
        return g_index_stage_insert_foreign_batches.load(std::memory_order_relaxed);
    }

    void reset_index_stage_insert_batches() noexcept {
        g_index_stage_insert_batches.store(0, std::memory_order_relaxed);
        g_index_stage_insert_foreign_batches.store(0, std::memory_order_relaxed);
        g_stage_insert_owner.store(nullptr, std::memory_order_relaxed);
    }
#endif

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
        // Linear scan is fine here: a table has 1-3 indexes, not enough to justify a map.
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
        // (key, row id) pairs for one index; avoids forwarding whole chunks, which clones every column per agent.
        using key_batch_t = std::vector<std::pair<value_t, size_t>>;

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
        , catchup_failures_(resource)
        , mirrored_ranges_(resource)
        , bitcask_agents_owned_(resource)
        , btree_agents_owned_(resource)
        , parked_agents_(resource)
        , pending_void_(resource) {
        if (!path_db_.empty()) {
            std::filesystem::create_directories(path_db_);
        }

        loop_thread_ = std::thread([this] {
            // pmr::list for iterator stability: behavior_t is move-only, and a resume can re-suspend in place.
            std::pmr::list<in_flight_entry_t> in_flight(this->resource());

            while (loop_running_.load(std::memory_order_acquire)) {
                    {
                    actor_zeta::mailbox::message* raw = nullptr;
                    while (inbox_.pop(raw)) {
                        in_flight.emplace_back();
                        in_flight.back().pending_msg = actor_zeta::mailbox::message_ptr{raw};
                    }
                }

                bool made_progress = false;

                for (auto& e : in_flight) {
                    if (e.pending_msg && !e.behavior) {
                        e.behavior = behavior(e.pending_msg.get());
                        poll_pending();
                        made_progress = true;
                        break;
                    }
                }

                // take_awaited_continuation atomically claims it; null means another resume already took it.
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

                std::unique_lock<std::mutex> lk(mutex_);
                if (inbox_.empty()) {
                    pump_cv_.wait_for(lk,
                                      in_flight.empty() ? std::chrono::microseconds(100)
                                                        : std::chrono::microseconds(5));
                }
            }
        });
    }

    manager_index_t::~manager_index_t() {
#ifdef DEV_MODE
        g_index_deferred_deletes.fetch_sub(deferred_deletes_.size(), std::memory_order_relaxed);
#endif
        loop_running_.store(false, std::memory_order_release);
        pump_cv_.notify_all();
        if (loop_thread_.joinable()) {
            loop_thread_.join();
        }
        actor_zeta::mailbox::message* raw = nullptr;
        while (inbox_.pop(raw)) {
            actor_zeta::mailbox::message_ptr reclaim{raw};
        }
    }

    auto manager_index_t::make_type() const noexcept -> const char* { return "manager_index"; }

    std::pair<bool, actor_zeta::detail::enqueue_result>
    manager_index_t::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
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
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::unmirrored_ranges>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::unmirrored_ranges, msg);
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
        // No mutex needed: pending_void_ is touched only by the loop thread (here and from handlers running on it).
        pending_void_.erase(
            std::remove_if(pending_void_.begin(), pending_void_.end(), [](auto& f) { return f.is_ready(); }),
            pending_void_.end());
    }

    void manager_index_t::mark_table_dropped_sync(components::catalog::oid_t oid, uint64_t dropped_at_commit_id) {
        dropped_table_agents_[oid] = dropped_at_commit_id;
    }

    manager_index_t::unique_future<void> manager_index_t::mark_table_dropped(session_id_t /*session*/,
                                                                             components::catalog::oid_t table_oid,
                                                                             uint64_t dropped_at_commit_id) {
        trace(log_,
              "manager_index_t::mark_table_dropped , oid : {} , commit_id : {}",
              static_cast<unsigned>(table_oid),
              dropped_at_commit_id);
        mark_table_dropped_sync(table_oid, dropped_at_commit_id);
        co_return;
    }

    manager_index_t::unique_future<void>
    manager_index_t::table_dropped_committed(session_id_t /*session*/, uint64_t txn_id, uint64_t commit_id) {
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

    // Bootstrap helpers: called pre-scheduler-start.

    void manager_index_t::bootstrap_engine_sync(components::catalog::oid_t oid) {
        indexes_per_oid_.try_emplace(oid, index_records_t(resource_));
    }

    core::error_t manager_index_t::bootstrap_index_sync(components::catalog::oid_t table_oid,
                                                        components::catalog::oid_t index_oid,
                                                        components::logical_plan::index_type type,
                                                        components::index::keys_base_storage_t keys,
                                                        std::pmr::set<std::uint64_t> commit_ids) {
        // Mirrors create_index's gates and agent-first order (see there for why each gate exists).
        auto it = indexes_per_oid_.find(table_oid);
        if (it == indexes_per_oid_.end()) {
            return core::error_t{core::error_code_t::index_create_fail,
                                 std::pmr::string{"index bootstrap: the table is not registered with the index "
                                                  "manager (bootstrap order violated)",
                                                  resource_}};
        }

        if (match_index_relid(it->second, index_oid) != nullptr) {
            return core::error_t{core::error_code_t::index_create_fail,
                                 std::pmr::string{"index bootstrap: the index is already registered", resource_}};
        }

        if (match_index(it->second, keys, type) != nullptr) {
            return core::error_t{
                core::error_code_t::index_create_fail,
                std::pmr::string{"index bootstrap: an index over the same keys and type is already registered",
                                 resource_}};
        }

        if (keys.size() != 1) {
            return core::error_t{
                core::error_code_t::index_create_fail,
                std::pmr::string{"index bootstrap: multi-column indexes are not supported by any backend; the "
                                 "index would silently store only its first key's column",
                                 resource_}};
        }

        if (type != components::logical_plan::index_type::single &&
            type != components::logical_plan::index_type::hashed) {
            return core::error_t{core::error_code_t::index_create_fail,
                                 std::pmr::string{"index bootstrap: unsupported index type", resource_}};
        }

        if (path_db_.empty()) {
            return core::error_t{
                core::error_code_t::index_create_fail,
                std::pmr::string{"index bootstrap: this index manager has no on-disk catalog path, and an "
                                 "index keeps its rows on disk",
                                 resource_}};
        }

        auto spawned = spawn_disk_agent(table_oid, index_oid, type, std::move(commit_ids));
        if (spawned.has_error()) {
            return spawned.error();
        }
        const auto agent = spawned.value();

        // built_compact_epoch 0: the table's own counter also restarts at 0 with the process.
        it->second.push_back(index_record_t{index_oid,
                                            std::move(keys),
                                            agent.type,
                                            agent.ordered,
                                            agent.address,
                                            /*built_compact_epoch=*/0});

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

    core::result_wrapper_t<manager_index_t::spawned_agent_t>
    manager_index_t::spawn_disk_agent(components::catalog::oid_t table_oid,
                                      components::catalog::oid_t index_oid,
                                      components::logical_plan::index_type type,
                                      std::pmr::set<std::uint64_t> commit_ids) {
        // The one place pg_index.indtype maps to a backend class: hashed -> bitcask LSM, else -> ordered b+tree.
        if (type == components::logical_plan::index_type::hashed) {
            auto agent = bitcask_index_agent_t::create(resource_,
                                                       path_db_,
                                                       table_oid,
                                                       index_oid,
                                                       bitcask_flush_threshold_,
                                                       bitcask_segment_record_limit_,
                                                       log_,
                                                       std::move(commit_ids));
            if (agent.has_error()) {
                return agent.error();
            }
            auto addr = agent.value()->address();
            bitcask_agents_owned_.emplace_back(std::move(agent.value()));
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
        indexes_per_oid_.erase(table_oid);
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
        // Trims the per-oid entry rather than erasing it, so sibling indexes stay registered.
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
        auto send_drop = [&](auto& owned) {
            for (auto& agent : owned) {
                if (!agent) {
                    continue;
                }
                auto [needs_sched, fut] =
                    actor_zeta::otterbrix::send<&index_agent_contract::drop>(agent->address(), session);
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

    void manager_index_t::park_detached(detached_agents_t&& dying) {
        for (auto& agent : dying.bitcask) {
            parked_agents_.bitcask.emplace_back(std::move(agent));
        }
        for (auto& agent : dying.btree) {
            parked_agents_.btree.emplace_back(std::move(agent));
        }
    }

    manager_index_t::unique_future<void> manager_index_t::register_collection(session_id_t /*session*/,
                                                                              components::catalog::oid_t table_oid) {
        trace(log_, "manager_index_t::register_collection: oid={}", static_cast<unsigned>(table_oid));

        // An empty record list, not an absent entry: known, carries no index yet.
        indexes_per_oid_.try_emplace(table_oid, index_records_t(resource_));
        co_return;
    }

    manager_index_t::unique_future<void> manager_index_t::unregister_collection(session_id_t session,
                                                                                components::catalog::oid_t table_oid) {
        trace(log_, "manager_index_t::unregister_collection: oid={}", static_cast<unsigned>(table_oid));

        // manager_disk_t frees the table's files only after this awaits every agent's drop.
        forget_deferred_deletes(table_oid);
        auto dying = detach_table_agents(table_oid);
        auto drop_futures = send_drop_to_detached(dying, session);
        for (auto& f : drop_futures) {
            co_await std::move(f);
        }
        co_return;
    }

    manager_index_t::unique_future<core::error_t> manager_index_t::create_index(
        session_id_t /*session*/,
        components::catalog::oid_t table_oid,
        components::catalog::oid_t index_oid,
        components::index::keys_base_storage_t keys,
        components::logical_plan::index_type type,
        // Unused; kept because it's part of index_contract::create_index, which every caller sends.
        core::date::timezone_offset_t /*session_tz*/,
        uint64_t built_compact_epoch) {
        trace(log_,
              "manager_index_t::create_index: index_oid={} on oid={}",
              static_cast<unsigned>(index_oid),
              static_cast<unsigned>(table_oid));

        auto it = indexes_per_oid_.find(table_oid);
        if (it == indexes_per_oid_.end()) {
            co_return core::error_t{core::error_code_t::index_create_fail,
                                    std::pmr::string{"the table is not registered with the index manager", resource_}};
        }

        // (keys, hash) beside (keys, single) is legal; index_create_fail not already_exists: test_index pins this code.
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

        if (keys.size() != 1) {
            co_return core::error_t{
                core::error_code_t::index_create_fail,
                std::pmr::string{"index create: multi-column indexes are not supported by any backend; the "
                                 "index would silently store only its first key's column",
                                 resource_}};
        }

        if (path_db_.empty()) {
            co_return core::error_t{
                core::error_code_t::index_create_fail,
                std::pmr::string{"index create: this index manager has no on-disk catalog path, and an index "
                                 "keeps its rows on disk",
                                 resource_}};
        }

        auto spawned = spawn_disk_agent(table_oid, index_oid, type, std::pmr::set<std::uint64_t>(resource_));
        if (spawned.has_error()) {
            error(log_,
                  "manager_index_t::create_index: index_oid={} on oid={} failed, "
                  "disk storage could not be opened: {}",
                  static_cast<unsigned>(index_oid),
                  static_cast<unsigned>(table_oid),
                  spawned.error().what);
            co_return spawned.error();
        }
        const auto agent = spawned.value();

        it->second.push_back(index_record_t{index_oid,
                                            std::move(keys),
                                            agent.type,
                                            agent.ordered,
                                            agent.address,
                                            built_compact_epoch});
        co_return core::error_t::no_error();
    }

    manager_index_t::unique_future<void> manager_index_t::drop_index(session_id_t session,
                                                                     components::catalog::oid_t table_oid,
                                                                     components::catalog::oid_t index_oid) {
        trace(log_,
              "manager_index_t::drop_index: index_oid={} on oid={}",
              static_cast<unsigned>(index_oid),
              static_cast<unsigned>(table_oid));

        if (auto marker_error = forget_rebuild_marker_entry_(table_oid, index_oid);
            marker_error.contains_error()) {
            error(log_,
                  "manager_index_t::drop_index: the rebuild guard still names index_oid={}: {}",
                  static_cast<unsigned>(index_oid),
                  marker_error.what);
        }

        // Registry entry and owning agent leave together: a read routed to the agent mid-drop
        // would swallow the cancellation under NDEBUG.
        forget_deferred_deletes(table_oid, index_oid);
        auto dying = detach_index(table_oid, index_oid);
        if (dying.empty()) {
            co_return;
        }

        auto drop_futures = send_drop_to_detached(dying, session);
        for (auto& f : drop_futures) {
            co_await std::move(f);
        }

        // Park, do not destroy: anything already queued behind drop() must still be ANSWERED, and
        // destroying the agent here cancels it instead. The next horizon advance reaps this generation.
        park_detached(std::move(dying));
        co_return;
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
        auto it = indexes_per_oid_.find(table_oid);
        if (it == indexes_per_oid_.end())
            co_return core::error_t::no_error();

        if (!it->second.empty()) {
            auto& per_oid = mirrored_ranges_.try_emplace(txn_id).first->second;
            per_oid.try_emplace(table_oid).first->second.push_back(index_row_range_t{start_row_id, count});
        }

        std::pmr::vector<unique_future<core::error_t>> futures(resource_);
        futures.reserve(it->second.size());
        for (const auto& record : it->second) {
            auto batch = collect_contiguous(resource_, record.keys, data, static_cast<int64_t>(start_row_id), count);
            if (batch.empty()) {
                continue;
            }
#ifdef DEV_MODE
            note_stage_insert_batch(this);
#endif
            auto [needs_sched, f] = actor_zeta::otterbrix::send<&index_agent_contract::stage_inserts>(
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

        if (!it->second.empty()) {
            auto& per_oid = mirrored_ranges_.try_emplace(txn_id).first->second;
            per_oid.try_emplace(table_oid).first->second.push_back(
                index_row_range_t{static_cast<uint64_t>(new_start_row_id), row_ids.size()});
        }

        std::pmr::vector<unique_future<core::error_t>> futures(resource_);
        futures.reserve(it->second.size() * 2);
        for (const auto& record : it->second) {
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
            auto new_batch =
                collect_contiguous(resource_, record.keys, new_data, new_start_row_id, row_ids.size());
            if (!new_batch.empty()) {
#ifdef DEV_MODE
                note_stage_insert_batch(this);
#endif
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

    manager_index_t::unique_future<std::pmr::vector<index_row_range_t>>
    manager_index_t::unmirrored_ranges(execution_context_t ctx,
                                       components::catalog::oid_t table_oid,
                                       std::pmr::vector<index_row_range_t> ranges) {
        std::pmr::vector<index_row_range_t> missing(resource_);
        auto it = indexes_per_oid_.find(table_oid);
        if (it == indexes_per_oid_.end() || it->second.empty()) {
            co_return missing;
        }

        std::pmr::vector<std::pair<uint64_t, uint64_t>> covered(resource_);
        if (auto ledger = mirrored_ranges_.find(ctx.txn.transaction_id); ledger != mirrored_ranges_.end()) {
            if (auto per_oid = ledger->second.find(table_oid); per_oid != ledger->second.end()) {
                covered.reserve(per_oid->second.size());
                for (const auto& r : per_oid->second) {
                    covered.emplace_back(r.row_start, r.row_start + r.row_count);
                }
            }
        }
        std::sort(covered.begin(), covered.end());

        for (const auto& q : ranges) {
            uint64_t pos = q.row_start;
            const uint64_t end = q.row_start + q.row_count;
            for (const auto& [cs, ce] : covered) {
                if (ce <= pos) {
                    continue;
                }
                if (cs >= end) {
                    break;
                }
                if (cs > pos) {
                    missing.push_back(index_row_range_t{pos, cs - pos});
                }
                pos = std::max(pos, ce);
                if (pos >= end) {
                    break;
                }
            }
            if (pos < end) {
                missing.push_back(index_row_range_t{pos, end - pos});
            }
        }
        co_return missing;
    }

    manager_index_t::unique_future<core::error_t>
    manager_index_t::commit_inserts(execution_context_t ctx,
                                    std::pmr::vector<components::catalog::oid_t> table_oids,
                                    // Must run before the WAL commit marker: the hashed family's
                                    // txn-log frame stays inert until then.
                                    uint64_t commit_id) {
        auto session = ctx.session;
        auto txn_id = ctx.txn.transaction_id;

        if (auto failed = catchup_failures_.find(txn_id); failed != catchup_failures_.end()) {
            co_return failed->second;
        }

        std::pmr::vector<unique_future<core::error_t>> futures(resource_);
        for (auto table_oid : table_oids) {
            auto it = indexes_per_oid_.find(table_oid);
            if (it == indexes_per_oid_.end())
                continue;
            for (const auto& record : it->second) {
                auto [needs_sched, f] =
                    actor_zeta::otterbrix::send<&index_agent_contract::commit_inserts>(record.address,
                                                                                       session,
                                                                                       txn_id,
                                                                                       commit_id);
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
        mirrored_ranges_.erase(txn_id);
        mirrored_ranges_.erase(uint64_t{0});
        co_return first_error;
    }

    // Not the mirror of commit_inserts: an early DELETE would remove an id from a row a reader
    // still owns, an unrecoverable short answer -- so this only records the erase for on_horizon_advanced.
    manager_index_t::unique_future<core::error_t>
    manager_index_t::commit_deletes(execution_context_t ctx,
                                    std::pmr::vector<components::catalog::oid_t> table_oids,
                                    uint64_t commit_id) {
        auto txn_id = ctx.txn.transaction_id;

        const bool was_empty = deferred_deletes_.empty();
        [[maybe_unused]] const auto queued_before = deferred_deletes_.size();

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
            constexpr uint8_t INDEX_KIND = 2;
            pending_void_.emplace_back(std::move(
                actor_zeta::otterbrix::send(manager_dispatcher_,
                                            &services::dispatcher::manager_dispatcher_t::on_drop_resource_marked,
                                            INDEX_KIND)
                    .second));
        }
        co_return core::error_t::no_error();
    }

    void manager_index_t::forget_deferred_deletes(components::catalog::oid_t table_oid) {
        [[maybe_unused]] const auto queued_before = deferred_deletes_.size();
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
        [[maybe_unused]] const auto queued_before = deferred_deletes_.size();
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
        catchup_failures_.erase(txn_id);
        if (auto ledger = mirrored_ranges_.find(txn_id); ledger != mirrored_ranges_.end()) {
            ledger->second.erase(table_oid);
            if (ledger->second.empty()) {
                mirrored_ranges_.erase(ledger);
            }
        }
        auto it = indexes_per_oid_.find(table_oid);
        if (it == indexes_per_oid_.end())
            co_return;

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
        catchup_failures_.erase(txn_id);
        auto it = indexes_per_oid_.find(table_oid);
        if (it == indexes_per_oid_.end())
            co_return;

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
        // Genuinely nothing to reclaim, not a stub: version stamps are an in-memory concept, and there is none here.
        co_return;
    }

    manager_index_t::unique_future<std::pmr::vector<components::catalog::oid_t>>
    manager_index_t::all_indexed_oids(session_id_t /*session*/) {
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
                                      core::date::timezone_offset_t /*session_tz*/,
                                      uint64_t built_compact_epoch) {
#ifdef DEV_MODE
        g_index_repopulations.fetch_add(1, std::memory_order_relaxed);
#endif
        trace(log_, "manager_index_t::repopulate_table: oid={} rows={}", static_cast<unsigned>(table_oid), row_count);

        auto it = indexes_per_oid_.find(table_oid);
        if (it == indexes_per_oid_.end() || it->second.empty()) {
            // Table dropped, never registered, or index-free — a legal no-op, not a fallback.
            co_return core::error_t::no_error();
        }
        if (auto chunk_error = check_rebuild_chunks_have_row_ids(chunks, resource_); chunk_error.contains_error()) {
            co_return chunk_error;
        }

        // Clear, batch, commit -- one FIFO pass per agent, so no read lands on a wiped index.
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
            auto batch = collect_by_chunk_row_ids(resource_, record.keys, chunks);
            if (batch.empty()) {
                continue;
            }
            // txn_id 0 means committed-for-everyone; same stage/commit write path a statement takes.
#ifdef DEV_MODE
            note_stage_insert_batch(this);
#endif
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
                                                                                   uint64_t{0},
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
        if (first_error.contains_error()) {
            co_return first_error;
        }

        auto stamped = indexes_per_oid_.find(table_oid);
        if (stamped != indexes_per_oid_.end()) {
            for (auto& record : stamped->second) {
                record.built_compact_epoch = built_compact_epoch;
            }
        }

        if (auto marker_error = clear_rebuild_marker_(table_oid, it->second); marker_error.contains_error()) {
            error(log_,
                  "manager_index_t::repopulate_table: table_oid={} was rebuilt, but the rebuild guard could "
                  "not be cleared: {}",
                  static_cast<unsigned>(table_oid),
                  marker_error.what);
            co_return marker_error;
        }
        co_return core::error_t::no_error();
    }

    manager_index_t::unique_future<core::result_wrapper_t<index_search_result_t>>
    manager_index_t::search_with_preferred_type(session_id_t session,
                                                components::catalog::oid_t table_oid,
                                                components::index::keys_base_storage_t keys,
                                                components::types::logical_value_t value,
                                                components::expressions::compare_type compare,
                                                components::logical_plan::index_type preferred_type,
                                                // Unused; kept because it's part of index_contract::search.
                                                uint64_t /*start_time*/,
                                                uint64_t txn_id,
                                                core::date::timezone_offset_t /*session_tz*/) {
        auto it = indexes_per_oid_.find(table_oid);
        if (it == indexes_per_oid_.end()) {
            // A planner invariant, not a data answer: an empty match would be the silent wrong
            // answer the no-fallback rule forbids.
            co_return core::error_t{core::error_code_t::index_not_exists,
                                    std::pmr::string{"index search: no index engine for the table oid", resource_}};
        }

        const auto* record = match_index(it->second, keys, preferred_type);
        if (record == nullptr) {
            record = match_index(it->second, keys);
        }
        if (record == nullptr) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"index search: the table has no index on the predicate key", resource_}};
        }

        if (index_key_is_null(value)) {
            co_return index_search_result_t{std::pmr::vector<int64_t>(resource_), record->built_compact_epoch};
        }

        if (compare != components::expressions::compare_type::eq && !record->ordered) {
            co_return core::error_t{
                core::error_code_t::index_not_exists,
                std::pmr::string{"index search: this index has no ordering and cannot answer a range predicate",
                                 resource_}};
        }

        auto agent_addr = record->address;
        // Captured before the send: `record` can be destroyed while this coroutine is suspended.
        const uint64_t built_epoch = record->built_compact_epoch;
#ifdef DEV_MODE
        g_index_agent_reads.fetch_add(1, std::memory_order_relaxed);
#endif
        auto [needs_sched, agent_future] = actor_zeta::otterbrix::send<&index_agent_contract::read_rows>(
            agent_addr,
            session,
            compare,
            components::types::logical_value_t(resource_, value),
            txn_id);
        schedule_agent(agent_addr, needs_sched);
        auto agent_result = co_await std::move(agent_future);
        if (agent_result.has_error()) {
            co_return agent_result.error();
        }
        // A superset filter, deliberately: which committed rows a reader may see is the table's decision.
        co_return index_search_result_t{std::move(agent_result.value()), built_epoch};
    }

    manager_index_t::unique_future<core::result_wrapper_t<index_search_result_t>>
    manager_index_t::search(session_id_t session,
                            components::catalog::oid_t table_oid,
                            components::index::keys_base_storage_t keys,
                            components::types::logical_value_t value,
                            components::expressions::compare_type compare,
                            uint64_t start_time,
                            uint64_t txn_id,
                            core::date::timezone_offset_t session_tz) {
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

    std::filesystem::path manager_index_t::rebuild_marker_path_() const {
        if (path_db_.empty()) {
            return {};
        }
        return path_db_ / "index_rebuild_pending";
    }

    std::pmr::vector<manager_index_t::pending_index_rebuild_t> manager_index_t::read_rebuild_marker_() const {
        std::pmr::vector<pending_index_rebuild_t> pending(resource_);
        const auto marker = rebuild_marker_path_();
        if (marker.empty()) {
            return pending;
        }
        std::error_code ec;
        if (!std::filesystem::exists(marker, ec) || ec) {
            return pending;
        }
        std::ifstream in(marker);
        if (!in.is_open()) {
            // Returns an empty list rather than aborting: refusing the database over one unreadable note is worse.
            return pending;
        }
        unsigned long long table_oid = 0;
        unsigned long long index_oid = 0;
        while (in >> table_oid >> index_oid) {
            pending.push_back(pending_index_rebuild_t{static_cast<components::catalog::oid_t>(table_oid),
                                                      static_cast<components::catalog::oid_t>(index_oid)});
        }
        return pending;
    }

    core::error_t
    manager_index_t::write_rebuild_marker_(const std::pmr::vector<pending_index_rebuild_t>& pending) const {
        const auto marker = rebuild_marker_path_();
        if (marker.empty()) {
            return core::error_t::no_error();
        }
        auto tmp_path = marker;
        tmp_path += ".tmp";

        core::filesystem::local_file_system_t fs;
        auto refuse = [&](std::string reason) {
            std::error_code rm_ec;
            std::filesystem::remove(tmp_path, rm_ec);
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"manager_index_t: the index rebuild marker " + marker.string() +
                                                      " was NOT updated: " + std::move(reason),
                                                  resource_});
        };

        if (pending.empty()) {
            std::error_code ec;
            std::filesystem::remove(marker, ec);
            if (ec) {
                return refuse("the empty marker could not be removed: " + ec.message());
            }
            return core::error_t::no_error();
        }

        std::string body;
        body.reserve(pending.size() * 16);
        for (const auto& entry : pending) {
            body += std::to_string(static_cast<unsigned>(entry.table_oid));
            body += ' ';
            body += std::to_string(static_cast<unsigned>(entry.index_oid));
            body += '\n';
        }

        std::error_code stale_ec;
        std::filesystem::remove(tmp_path, stale_ec);
        auto tmp = core::filesystem::open_file(fs,
                                               tmp_path,
                                               core::filesystem::file_flags::WRITE |
                                                   core::filesystem::file_flags::FILE_CREATE_NEW);
        if (tmp == nullptr) {
            return refuse("could not open the staging file " + tmp_path.string());
        }
        const auto want = static_cast<std::uint64_t>(body.size());
        const auto written = tmp->write(body.data(), want);
        if (!written.complete || written.bytes_written != want) {
            tmp.reset();
            return refuse("the staging write landed " + std::to_string(written.bytes_written) + " of " +
                          std::to_string(want) + " bytes");
        }
        if (!tmp->sync()) {
            tmp.reset();
            return refuse("the staging file could not be fsynced");
        }
        tmp.reset(); // closes the descriptor; the fsync above is what made the bytes durable
        if (!core::filesystem::move_files(fs, tmp_path, marker)) {
            return refuse("the rename over the live marker was refused");
        }
        // Stricter than agent_disk.cpp's sidecar: reverting to the old longer list just repeats a rebuild,
        // but a shorter one would silently drop an index that needs one.
        auto dir = core::filesystem::open_file(fs, marker.parent_path(), core::filesystem::file_flags::READ);
        if (dir == nullptr || !core::filesystem::file_sync(fs, *dir)) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"manager_index_t: the index rebuild marker " + marker.string() +
                                                      " was written and renamed, but its directory could not be "
                                                      "fsynced -- a crash may still surface the previous list",
                                                  resource_});
        }
        return core::error_t::no_error();
    }

    core::error_t manager_index_t::arm_rebuild_marker_() {
        if (rebuild_marker_path_().empty()) {
            return core::error_t::no_error();
        }
        auto pending = read_rebuild_marker_();
        const auto already_named = [&pending](components::catalog::oid_t table_oid,
                                              components::catalog::oid_t index_oid) {
            for (const auto& entry : pending) {
                if (entry.table_oid == table_oid && entry.index_oid == index_oid) {
                    return true;
                }
            }
            return false;
        };
        std::size_t before = pending.size();
        for (const auto& [table_oid, records] : indexes_per_oid_) {
            if (dropped_table_agents_.find(table_oid) != dropped_table_agents_.end()) {
                continue;
            }
            for (const auto& record : records) {
                if (!already_named(table_oid, record.index_oid)) {
                    pending.push_back(pending_index_rebuild_t{table_oid, record.index_oid});
                }
            }
        }
        if (pending.size() == before && before == 0) {
            return core::error_t::no_error();
        }
        return write_rebuild_marker_(pending);
    }

    core::error_t manager_index_t::clear_rebuild_marker_(components::catalog::oid_t table_oid,
                                                         const index_records_t& rebuilt) {
        if (rebuild_marker_path_().empty()) {
            return core::error_t::no_error();
        }
        auto pending = read_rebuild_marker_();
        if (pending.empty()) {
            return core::error_t::no_error();
        }
        std::pmr::vector<pending_index_rebuild_t> survivors(resource_);
        survivors.reserve(pending.size());
        for (const auto& entry : pending) {
            bool rebuilt_now = false;
            if (entry.table_oid == table_oid) {
                rebuilt_now = match_index_relid(rebuilt, entry.index_oid) != nullptr;
            }
            if (!rebuilt_now) {
                survivors.push_back(entry);
            }
        }
        if (survivors.size() == pending.size()) {
            return core::error_t::no_error();
        }
        return write_rebuild_marker_(survivors);
    }

    core::error_t manager_index_t::forget_rebuild_marker_entry_(components::catalog::oid_t table_oid,
                                                                components::catalog::oid_t index_oid) {
        if (rebuild_marker_path_().empty()) {
            return core::error_t::no_error();
        }
        auto pending = read_rebuild_marker_();
        if (pending.empty()) {
            return core::error_t::no_error();
        }
        std::pmr::vector<pending_index_rebuild_t> survivors(resource_);
        survivors.reserve(pending.size());
        for (const auto& entry : pending) {
            if (entry.table_oid == table_oid && entry.index_oid == index_oid) {
                continue;
            }
            survivors.push_back(entry);
        }
        if (survivors.size() == pending.size()) {
            return core::error_t::no_error();
        }
        return write_rebuild_marker_(survivors);
    }

    std::pmr::vector<manager_index_t::pending_index_rebuild_t> manager_index_t::pending_index_rebuilds_sync() const {
        return read_rebuild_marker_();
    }

    manager_index_t::unique_future<core::error_t> manager_index_t::flush_all_indexes(session_id_t session) {
        trace(log_, "manager_index_t::flush_all_indexes, session: {}", session.data());

        if (auto arm_error = arm_rebuild_marker_(); arm_error.contains_error()) {
            error(log_,
                  "manager_index_t::flush_all_indexes: the index rebuild guard could not be made durable, so "
                  "the round must not go on to renumber the rows it guards: {}",
                  arm_error.what);
            co_return arm_error;
        }

        for (auto& f : pending_void_) {
            co_await std::move(f);
        }
        pending_void_.clear();

        std::pmr::vector<unique_future<core::error_t>> futures(resource_);
        futures.reserve(bitcask_agents_owned_.size() + btree_agents_owned_.size());
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
        core::error_t first_error = core::error_t::no_error();
        for (auto& f : futures) {
            auto err = co_await std::move(f);
            if (err.contains_error() && !first_error.contains_error()) {
                first_error = std::move(err);
            }
        }
        co_return first_error;
    }

    // The dispatcher's horizon sweep is the only sender; do not add a second one.
    manager_index_t::unique_future<void> manager_index_t::on_horizon_advanced(uint64_t new_horizon) {
        trace(log_, "manager_index_t::on_horizon_advanced , horizon : {}", new_horizon);

        // Agents parked by drop_index since the last advance go here: nothing routes to them any more
        // (the registry entry left with the detach) and the scheduler has had the whole interval to
        // drain what was queued.
        parked_agents_.bitcask.clear();
        parked_agents_.btree.clear();

        detached_agents_t dying(resource_);
        for (auto it = dropped_table_agents_.begin(); it != dropped_table_agents_.end();) {
            if (it->second < new_horizon) {
                auto oid = it->first;
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

        // `commit_id <= new_horizon`, not `<`: a snapshot sitting at commit_id already hides the row.
        std::pmr::vector<unique_future<core::error_t>> delete_futures(resource_);
        std::pmr::vector<deferred_delete_t> swept_entries(resource_);
        [[maybe_unused]] const auto queued_before_sweep = deferred_deletes_.size();
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
                                                                                       entry->txn_id,
                                                                                       entry->commit_id);
                schedule_agent(record->address, needs_sched);
                delete_futures.emplace_back(std::move(f));
                swept_entries.emplace_back(*entry);
            }
            entry = deferred_deletes_.erase(entry);
        }
#ifdef DEV_MODE
        g_index_deferred_deletes.fetch_sub(queued_before_sweep - deferred_deletes_.size(),
                                           std::memory_order_relaxed);
#endif

        auto drop_futures = send_drop_to_detached(dying, session_id_t{});

        bool subscriber_acked = false;
        if (dropped_table_agents_.empty() && deferred_deletes_.empty() &&
            manager_dispatcher_ != actor_zeta::address_t::empty_address()) {
            constexpr uint8_t INDEX_KIND = 2;
            pending_void_.emplace_back(
                std::move(actor_zeta::otterbrix::send(manager_dispatcher_,
                                                      &services::dispatcher::manager_dispatcher_t::on_subscriber_empty,
                                                      INDEX_KIND)
                              .second));
            subscriber_acked = true;
        }

        size_t requeued = 0;
        for (size_t i = 0; i < delete_futures.size(); ++i) {
            auto err = co_await std::move(delete_futures[i]);
            if (err.contains_error()) {
                error(log_,
                      "manager_index_t::on_horizon_advanced: deferred index delete failed and is re-queued "
                      "for the next horizon: {}",
                      err.what);
                deferred_deletes_.emplace_back(swept_entries[i]);
                ++requeued;
            }
        }
#ifdef DEV_MODE
        g_index_deferred_deletes.fetch_add(requeued, std::memory_order_relaxed);
#endif
        if (requeued != 0 && subscriber_acked) {
            constexpr uint8_t INDEX_KIND = 2;
            pending_void_.emplace_back(std::move(
                actor_zeta::otterbrix::send(manager_dispatcher_,
                                            &services::dispatcher::manager_dispatcher_t::on_drop_resource_marked,
                                            INDEX_KIND)
                    .second));
        }
        for (auto& f : drop_futures) {
            co_await std::move(f);
        }
        co_return;
    }

    // Only ever adds; see index_contract for param semantics, and the leg guard below for why.
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
            error(log_,
                  "manager_index_t::apply_wal_record_for_index: no registry entry for "
                  "table_oid={} (index_oid={} wal_id={} type={}); the build's commit will refuse",
                  static_cast<unsigned>(table_oid),
                  static_cast<unsigned>(index_oid),
                  wal_record_id,
                  static_cast<unsigned>(record_type));
            catchup_failures_.try_emplace(
                txn_id,
                core::error_t{core::error_code_t::index_create_fail,
                              std::pmr::string{"CREATE INDEX catchup could not place a WAL record: the table has "
                                               "no registry entry; the build is missing rows and may not publish",
                                               resource_}});
            co_return;
        }

        uint64_t total_rows = 0;
        for (const auto& chunk : physical_data) {
            total_rows += chunk.size();
        }
        if (total_rows == 0) {
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

        // UPDATE ships only the new chunk; its old-row half arrives separately as PHYSICAL_DELETE.
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

        // Insert leg only, deliberately (same asymmetry commit_deletes is built around): staging the
        // delete leg would trap it in a bucket with no exit, so dropping it is the safe move.
        if (is_delete_leg) {
            trace(log_,
                  "manager_index_t::apply_wal_record_for_index: dropping the delete leg "
                  "(table_oid={} index_oid={} wal_id={} row_ids={}) -- an undecided "
                  "journal delete may not shrink an index",
                  static_cast<unsigned>(table_oid),
                  static_cast<unsigned>(index_oid),
                  wal_record_id,
                  row_ids.size());
            co_return;
        }

        const auto* target = match_index_relid(it->second, index_oid);
        if (target == nullptr) {
            error(log_,
                  "manager_index_t::apply_wal_record_for_index: table_oid={} has no registry entry for "
                  "index_oid={} (wal_id={} type={}); the build's commit will refuse",
                  static_cast<unsigned>(table_oid),
                  static_cast<unsigned>(index_oid),
                  wal_record_id,
                  static_cast<unsigned>(record_type));
            catchup_failures_.try_emplace(
                txn_id,
                core::error_t{core::error_code_t::index_create_fail,
                              std::pmr::string{"CREATE INDEX could not place a record: the index it names is not "
                                               "registered on this table; the build is missing rows and may not "
                                               "publish",
                                               resource_}});
            co_return;
        }

        std::pmr::vector<unique_future<core::error_t>> futures(resource_);
        {
            auto batch = collect_contiguous(resource_,
                                            target->keys,
                                            physical_data,
                                            static_cast<int64_t>(physical_row_start),
                                            total_rows);
            if (!batch.empty()) {
#ifdef DEV_MODE
                note_stage_insert_batch(this);
#endif
                auto [needs_sched, f] =
                    actor_zeta::otterbrix::send<&index_agent_contract::stage_inserts>(target->address,
                                                                                      session,
                                                                                      txn_id,
                                                                                      std::move(batch));
                schedule_agent(target->address, needs_sched);
                futures.emplace_back(std::move(f));
            }
        }

        for (auto& f : futures) {
            auto err = co_await std::move(f);
            if (err.contains_error()) {
                error(log_,
                      "manager_index_t::apply_wal_record_for_index: table_oid={} index_oid={} wal_id={}: {}",
                      static_cast<unsigned>(table_oid),
                      static_cast<unsigned>(index_oid),
                      wal_record_id,
                      err.what);
                catchup_failures_.try_emplace(txn_id, std::move(err));
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
