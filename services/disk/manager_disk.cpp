#include "manager_disk.hpp"
#include <actor-zeta/spawn.hpp>
#include <algorithm>
#include <array>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/dependency_walker.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <filesystem>
#include <fstream>
#include <limits>
#include <services/dispatcher/dispatcher.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <system_error>
#include <unordered_set>

namespace services::disk {

    using namespace core::filesystem;
    namespace catalog = components::catalog;

    // ---- behavior/implements sync check ----
    // Ensures behavior() handles every method registered in dispatch_traits.
    // When adding a new method:
    //   1. Add it to implements<> in manager_disk.hpp
    //   2. Add a case to the behavior() switch
    //   3. Add the corresponding msg_id to kBehaviorHandledIds below
    namespace {
        template<typename MethodList>
        struct behavior_expected_ids_t;

        template<auto... Ptrs>
        struct behavior_expected_ids_t<actor_zeta::type_traits::type_list<actor_zeta::method_map_entry<Ptrs>...>> {
            static constexpr std::array<actor_zeta::mailbox::message_id, sizeof...(Ptrs)> value{
                actor_zeta::msg_id<manager_disk_t, Ptrs>...};
        };

        constexpr auto kImplementedIds = behavior_expected_ids_t<manager_disk_t::dispatch_traits::methods>::value;

        constexpr std::array kBehaviorHandledIds{
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::flush>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::checkpoint_all>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::vacuum_all>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::maybe_cleanup_many>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::create_storage>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::create_storage_with_columns>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::create_storage_disk>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::drop_storage_many>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_types>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_total_rows>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_scan>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_scan_batched>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_fetch>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_scan_segment>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_append>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_update>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_delete_rows>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_publish_commits>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_publish_deletes>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_revert_appends>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_revert_deletes>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::resolve_namespace>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::resolve_function_by_name>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::list_namespaces>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::allocate_oids_batch>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::append_pg_catalog_row>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::delete_pg_catalog_rows>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::delete_pg_catalog_rows_many>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::update_pg_attribute_commit_id_fields>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::scan_by_keys>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::read_chunks_by_key>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::read_chunks_by_keys>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::compact_relkind_g_storage>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::on_horizon_advanced>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::mark_storage_dropped_many>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_dropped_committed>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_drop_aborted>,
        };

        constexpr bool behavior_covers_all_implements() noexcept {
            if (kImplementedIds.size() != kBehaviorHandledIds.size())
                return false;
            for (auto id : kImplementedIds) {
                bool found = false;
                for (auto hid : kBehaviorHandledIds) {
                    if (id == hid) {
                        found = true;
                        break;
                    }
                }
                if (!found)
                    return false;
            }
            return true;
        }

        static_assert(behavior_covers_all_implements(),
                      "behavior() is out of sync with dispatch_traits: "
                      "add a case to behavior() AND an entry to kBehaviorHandledIds");
    } // namespace

    // ---- table_storage_t implementations ----

    table_storage_t::table_storage_t(std::pmr::memory_resource* resource)
        : mode_(storage_mode_t::IN_MEMORY)
        , buffer_pool_(resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
        , buffer_manager_(resource, fs_, buffer_pool_)
        , block_manager_(std::make_unique<components::table::storage::in_memory_block_manager_t>(
              buffer_manager_,
              components::table::storage::DEFAULT_BLOCK_ALLOC_SIZE))
        , table_(std::make_unique<components::table::data_table_t>(
              resource,
              *block_manager_,
              std::vector<components::table::column_definition_t>{})) {}

    table_storage_t::table_storage_t(std::pmr::memory_resource* resource,
                                     std::vector<components::table::column_definition_t> columns)
        : mode_(storage_mode_t::IN_MEMORY)
        , buffer_pool_(resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
        , buffer_manager_(resource, fs_, buffer_pool_)
        , block_manager_(std::make_unique<components::table::storage::in_memory_block_manager_t>(
              buffer_manager_,
              components::table::storage::DEFAULT_BLOCK_ALLOC_SIZE))
        , table_(std::make_unique<components::table::data_table_t>(resource, *block_manager_, std::move(columns))) {}

    table_storage_t::table_storage_t(std::pmr::memory_resource* resource,
                                     std::vector<components::table::column_definition_t> columns,
                                     const std::filesystem::path& otbx_path)
        : mode_(storage_mode_t::DISK)
        , buffer_pool_(resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
        , buffer_manager_(resource, fs_, buffer_pool_) {
        auto bm = std::make_unique<components::table::storage::single_file_block_manager_t>(buffer_manager_,
                                                                                            fs_,
                                                                                            otbx_path.string());
        bm->create_new_database();
        block_manager_ = std::move(bm);
        table_ = std::make_unique<components::table::data_table_t>(resource, *block_manager_, std::move(columns));
    }

    table_storage_t::table_storage_t(std::pmr::memory_resource* resource, const std::filesystem::path& otbx_path)
        : mode_(storage_mode_t::DISK)
        , buffer_pool_(resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
        , buffer_manager_(resource, fs_, buffer_pool_) {
        auto bm = std::make_unique<components::table::storage::single_file_block_manager_t>(buffer_manager_,
                                                                                            fs_,
                                                                                            otbx_path.string());
        bm->load_existing_database();
        block_manager_ = std::move(bm);

        components::table::storage::metadata_manager_t meta_mgr(*block_manager_);
        auto meta_block = block_manager_->meta_block();
        components::table::storage::meta_block_pointer_t meta_ptr;
        meta_ptr.block_pointer = meta_block;
        components::table::storage::metadata_reader_t reader(meta_mgr, meta_ptr);
        table_ = components::table::data_table_t::load_from_disk(resource, *block_manager_, reader);
    }

    void table_storage_t::checkpoint() {
        if (mode_ != storage_mode_t::DISK) {
            return;
        }

        components::table::storage::metadata_manager_t meta_mgr(*block_manager_);
        components::table::storage::metadata_writer_t writer(meta_mgr);
        table_->checkpoint(writer);
        writer.flush();

        auto* disk_bm = static_cast<components::table::storage::single_file_block_manager_t*>(block_manager_.get());
        // Set meta_block_ so write_header() persists it
        disk_bm->set_meta_block(writer.get_block_pointer().block_pointer);
        // Serialize free list to metadata blocks
        auto free_list_ptr = disk_bm->serialize_free_list();
        // W-TORN spec: durability of metadata + data blocks BEFORE header swap.
        // 1st fsync: ensure data/metadata blocks are on disk; without this, a crash after the
        // header write but before fsync of data could leave a header pointing to non-durable blocks.
        disk_bm->file_sync();
        components::table::storage::database_header_t header;
        header.initialize();
        header.free_list = free_list_ptr.block_pointer;
        disk_bm->write_header(header);
        // 2nd fsync: commit the new header — this is the atomic point of the checkpoint.
        disk_bm->file_sync();
    }

    void table_storage_t::checkpoint(wal::id_t new_wal_id) {
        if (mode_ != storage_mode_t::DISK) {
            return;
        }
        // First persist the data; if checkpoint() throws, fields stay unchanged.
        checkpoint();
        prev_checkpoint_wal_id_ = checkpoint_wal_id_;
        checkpoint_wal_id_ = new_wal_id;
    }

    void table_storage_t::add_column(components::table::column_definition_t& col) {
        auto new_table = std::make_unique<components::table::data_table_t>(*table_, col);
        table_ = std::move(new_table);
    }

    bool table_storage_t::drop_column(const std::string& attname) {
        // Physical column compaction. DISK is out of scope (would need segment
        // rewrites + checkpoint coordination); IN_MEMORY only.
        if (mode_ != storage_mode_t::IN_MEMORY) {
            return false;
        }
        if (!table_) {
            return false;
        }
        const auto& cols = table_->columns();
        std::uint64_t idx = 0;
        bool found = false;
        for (std::uint64_t i = 0; i < cols.size(); ++i) {
            if (cols[i].name() == attname) {
                idx = i;
                found = true;
                break;
            }
        }
        if (!found) {
            return false;
        }
        // The data_table_t(parent, removed_column) constructor performs the
        // rebuild: column_definitions_ minus idx, row_groups_ rebuilt via
        // collection_t::remove_column (per-segment column drop). All physical
        // storage for the dropped column is released when the previous
        // table_ unique_ptr goes away.
        auto new_table = std::make_unique<components::table::data_table_t>(*table_, idx);
        table_ = std::move(new_table);
        return true;
    }

    manager_disk_t::manager_disk_t(std::pmr::memory_resource* resource,
                                   actor_zeta::scheduler_raw scheduler,
                                   actor_zeta::scheduler_raw scheduler_disk,
                                   configuration::config_disk config,
                                   log_t& log)
        : actor_zeta::actor::actor_mixin<manager_disk_t>()
        , resource_(resource)
        , scheduler_(scheduler)
        , scheduler_disk_(scheduler_disk)
        , log_(log.clone())
        , config_(std::move(config)) {
        trace(log_, "manager_disk start");
        if (!config_.path.empty()) {
            create_directories(config_.path);
            create_agent(config.agent);
        }
        // This thread OWNS all message processing. Senders only push into inbox_
        // (lock-free) + notify pump_cv_; the loop-local in_flight list is private
        // to this thread, so the three phases below run lock-free.
        loop_thread_ = std::thread([this] {
            // this->resource(): the ctor parameter `resource` shadows the member fn.
            std::pmr::list<in_flight_entry_t> in_flight(this->resource());
            while (loop_running_.load(std::memory_order_acquire)) {
                actor_zeta::mailbox::message* raw = nullptr;
                while (inbox_.pop(raw)) {
                    in_flight.emplace_back();
                    in_flight.back().pending_msg = actor_zeta::mailbox::message_ptr{raw};
                }
                bool progress = true;
                while (progress) {
                    progress = false;
                    // (a) Create a behavior for the first slot that still needs one.
                    //     pending_msg STAYS in the slot: the coroutine holds a raw
                    //     pointer to the message across suspensions, so the message
                    //     must outlive the behavior. "needs one" marker = handle null.
                    for (auto& e : in_flight) {
                        if (e.pending_msg && !e.behavior) {
                            e.behavior = behavior(e.pending_msg.get());
                            progress = true;
                            break;
                        }
                    }
                    if (progress) {
                        continue;
                    }
                    // (b) Resume one ready awaited continuation, if any.
                    {
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
                            cont.resume(); // disk: no poll_pending — no pending_<T>_ containers.
                            progress = true;
                            continue;
                        }
                    }
                    // (c) Erase one done slot ("done" = handle non-null AND completed).
                    //     behavior_t + message_ptr destruct on this thread.
                    for (auto it = in_flight.begin(); it != in_flight.end(); ++it) {
                        if (it->behavior && it->behavior.done()) {
                            in_flight.erase(it);
                            progress = true;
                            break;
                        }
                    }
                }
                std::unique_lock<std::mutex> lk(mutex_);
                if (inbox_.empty())
                    pump_cv_.wait_for(lk, std::chrono::microseconds(100));
                // lock-free inbox trade: a push+notify may slip between empty() and
                // wait_for — bounded by the 100µs timeout (staleness, not loss).
            }
            // in_flight destructs on the loop thread — safe, no other thread ever
            // touches the in-flight state.
        });
        trace(log_, "manager_disk finish");
    }

    manager_disk_t::~manager_disk_t() {
        loop_running_.store(false, std::memory_order_release);
        pump_cv_.notify_one();
        if (loop_thread_.joinable()) {
            loop_thread_.join();
        }
        // Drain any messages delivered after the loop stopped: re-wrap each raw
        // pointer into a message_ptr temporary so it is destroyed (not leaked).
        actor_zeta::mailbox::message* raw = nullptr;
        while (inbox_.pop(raw)) {
            actor_zeta::mailbox::message_ptr drained{raw};
        }
        trace(log_, "delete manager_disk_t");
    }

    // Senders only deliver into inbox_ and wake the loop; loop_thread_ does all
    // processing (see ctor).
    std::pair<bool, actor_zeta::detail::enqueue_result>
    manager_disk_t::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
        inbox_.push(msg.release());
        pump_cv_.notify_one();
        return {false, actor_zeta::detail::enqueue_result::success};
    }

    actor_zeta::behavior_t manager_disk_t::behavior(actor_zeta::mailbox::message* msg) {
        switch (msg->command()) {
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::flush>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::flush, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::checkpoint_all>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::checkpoint_all, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::vacuum_all>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::vacuum_all, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::maybe_cleanup_many>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::maybe_cleanup_many, msg);
                break;
            }
            // Storage management
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::create_storage>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::create_storage, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::create_storage_with_columns>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::create_storage_with_columns, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::create_storage_disk>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::create_storage_disk, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::drop_storage_many>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::drop_storage_many, msg);
                break;
            }
            // Storage queries
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_types>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_types, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_total_rows>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_total_rows, msg);
                break;
            }
            // Storage data operations
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_scan>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_scan, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_scan_batched>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_scan_batched, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_fetch>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_fetch, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_scan_segment>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_scan_segment, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_append>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_append, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_update>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_update, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_delete_rows>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_delete_rows, msg);
                break;
            }
            // MVCC commit/revert
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_publish_commits>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_publish_commits, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_publish_deletes>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_publish_deletes, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_revert_appends>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_revert_appends, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_revert_deletes>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_revert_deletes, msg);
                break;
            }
            // resolve + invalidation pull
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::resolve_namespace>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::resolve_namespace, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::resolve_function_by_name>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::resolve_function_by_name, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::list_namespaces>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::list_namespaces, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::allocate_oids_batch>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::allocate_oids_batch, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::append_pg_catalog_row>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::append_pg_catalog_row, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::scan_by_keys>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::scan_by_keys, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::read_chunks_by_key>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::read_chunks_by_key, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::read_chunks_by_keys>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::read_chunks_by_keys, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::delete_pg_catalog_rows>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::delete_pg_catalog_rows, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::delete_pg_catalog_rows_many>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::delete_pg_catalog_rows_many, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::update_pg_attribute_commit_id_fields>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::update_pg_attribute_commit_id_fields, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::compact_relkind_g_storage>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::compact_relkind_g_storage, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::on_horizon_advanced>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::on_horizon_advanced, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::mark_storage_dropped_many>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::mark_storage_dropped_many, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_dropped_committed>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_dropped_committed, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_drop_aborted>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_drop_aborted, msg);
                break;
            }
            default:
                break;
        }
    }

    manager_disk_t::unique_future<void> manager_disk_t::on_horizon_advanced(uint64_t new_horizon) {
        trace(log_, "manager_disk::on_horizon_advanced , horizon : {}", new_horizon);

        std::pmr::vector<unique_future<void>> agent_futures{resource()};
        agent_futures.reserve(agents_.size());
        for (auto& agent_ptr : agents_) {
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent_ptr->address(),
                                                                   &agent_disk_t::on_horizon_advanced_inner,
                                                                   new_horizon);
            if (needs_sched) {
                scheduler_disk_->enqueue(agent_ptr.get());
            }
            agent_futures.emplace_back(std::move(fut));
        }
        for (auto& f : agent_futures) {
            co_await std::move(f);
        }

        co_return;
    }

    void manager_disk_t::set_manager_dispatcher_sync(actor_zeta::address_t address) {
        // Bootstrap-only (pre-scheduler-start), single-threaded — no locking.
        manager_dispatcher_ = address;

        // Fan the address (a mailbox handle, safe to copy) to every agent so each
        // on_horizon_advanced_inner can ack on_subscriber_empty(DISK_KIND) itself.
        for (auto& agent_ptr : agents_) {
            agent_ptr->set_manager_dispatcher_sync(address);
        }
    }

    void manager_disk_t::register_dropped_storage_sync(components::catalog::oid_t oid,
                                                       uint64_t dropped_at_commit_id,
                                                       std::filesystem::path path,
                                                       std::pmr::vector<std::filesystem::path> sidecar_paths) {
        // Bootstrap-only (base_spaces catalog scan rebuild); runtime DROP uses the
        // mark_storage_dropped_many mailbox handler below. Forwards an independent
        // deep-copy of path + sidecars into the owning agent's slice.
        if (!agents_.empty()) {
            const auto idx = pool_idx_for_oid(oid, agents_.size());
            std::pmr::vector<std::filesystem::path> agent_sidecars{resource()};
            agent_sidecars.reserve(sidecar_paths.size());
            for (const auto& sidecar : sidecar_paths) {
                agent_sidecars.push_back(sidecar);
            }
            agents_[idx]->register_dropped_storage_inner_sync(oid,
                                                               dropped_at_commit_id,
                                                               std::move(path),
                                                               std::move(agent_sidecars));
        }
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::mark_storage_dropped_many(session_id_t /*session*/,
                                              std::pmr::vector<components::catalog::oid_t> table_oids,
                                              uint64_t dropped_at_commit_id) {
        // Partition oids per owning agent (pool_idx_for_oid), then fan out one
        // mark_storage_dropped_many_inner per agent in PARALLEL (send all → await
        // all) — N per-oid singular marks would cost N round-trips; here they cost
        // one (at most num_agents parallel sends). The owning agent derives each
        // .otbx path + sidecars from its OWN still-live slice and records the GC
        // entry in mark_storage_dropped_many_inner — the manager no longer borrows
        // the agent's storage_entry across the actor boundary. Every oid in one
        // cascade shares the SAME dropped_at_commit_id (txn_id upper bound). Awaiting
        // all keeps this handler ordered w.r.t. operator_dynamic_cascade_delete's
        // subsequent drop_storage_many / cascade sends. Same partition-by-agent shape
        // as drop_storage_many.
        trace(log_,
              "manager_disk_t::mark_storage_dropped_many , oids : {} , commit_id : {}",
              table_oids.size(),
              dropped_at_commit_id);
        if (agents_.empty()) {
            co_return;
        }
        std::pmr::vector<std::pmr::vector<components::catalog::oid_t>> per_agent{resource()};
        per_agent.reserve(agents_.size());
        for (std::size_t i = 0; i < agents_.size(); ++i) {
            per_agent.emplace_back();
        }
        for (auto oid : table_oids) {
            const std::size_t pool_idx = pool_idx_for_oid(oid, agents_.size());
            per_agent[pool_idx].push_back(oid);
        }
        std::pmr::vector<unique_future<void>> agent_futures{resource()};
        agent_futures.reserve(per_agent.size());
        for (std::size_t i = 0; i < per_agent.size(); ++i) {
            if (per_agent[i].empty()) {
                continue;
            }
            auto& agent = agents_[i];
            if (agent == nullptr) {
                continue;
            }
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                   &agent_disk_t::mark_storage_dropped_many_inner,
                                                                   std::move(per_agent[i]),
                                                                   dropped_at_commit_id);
            if (needs_sched) {
                scheduler_disk_->enqueue(agent.get());
            }
            agent_futures.emplace_back(std::move(fut));
        }
        for (auto& f : agent_futures) {
            co_await std::move(f);
        }
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::storage_dropped_committed(session_id_t /*session*/, uint64_t txn_id, uint64_t commit_id) {
        // DROP-GC value-space remap. We do not know which agent owns the dropped
        // entry's oid here (the GC entry is keyed by oid, but the caller only has
        // the txn_id placeholder), so fan out to EVERY agent and let each rewrite
        // any of its own dropped_storages_ entries whose dropped_at_commit_id still
        // equals the TXN-ID placeholder. Mirrors on_horizon_advanced's broadcast.
        trace(log_,
              "manager_disk::storage_dropped_committed , txn_id : {} , commit_id : {}",
              txn_id,
              commit_id);

        std::pmr::vector<unique_future<void>> agent_futures{resource()};
        agent_futures.reserve(agents_.size());
        for (auto& agent_ptr : agents_) {
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent_ptr->address(),
                                                                  &agent_disk_t::storage_dropped_committed_inner,
                                                                  txn_id,
                                                                  commit_id);
            if (needs_sched) {
                scheduler_disk_->enqueue(agent_ptr.get());
            }
            agent_futures.emplace_back(std::move(fut));
        }
        for (auto& f : agent_futures) {
            co_await std::move(f);
        }
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::storage_drop_aborted(session_id_t /*session*/, uint64_t txn_id) {
        // DROP-rollback un-mark — the abort mirror of storage_dropped_committed. The GC
        // entry is keyed by oid but the caller only has the txn_id placeholder, so fan
        // out to EVERY agent and let each ERASE any of its own dropped_storages_ entries
        // whose dropped_at_commit_id still equals the TXN-ID placeholder. Erasing (not
        // remapping) un-marks the DROP so the still-live .otbx is never reclaimed.
        // Mirrors on_horizon_advanced's / storage_dropped_committed's broadcast.
        trace(log_, "manager_disk::storage_drop_aborted , txn_id : {}", txn_id);

        std::pmr::vector<unique_future<void>> agent_futures{resource()};
        agent_futures.reserve(agents_.size());
        for (auto& agent_ptr : agents_) {
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent_ptr->address(),
                                                                  &agent_disk_t::storage_drop_aborted_inner,
                                                                  txn_id);
            if (needs_sched) {
                scheduler_disk_->enqueue(agent_ptr.get());
            }
            agent_futures.emplace_back(std::move(fut));
        }
        for (auto& f : agent_futures) {
            co_await std::move(f);
        }
        co_return;
    }

} // namespace services::disk
