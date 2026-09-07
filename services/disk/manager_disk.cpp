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
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::checkpoint_all>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::vacuum_all>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::maybe_cleanup_many>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::create_storage_disk>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::drop_storage_many>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_types>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_total_rows>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_fetch_next_batch>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_close_cursor>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_reduce>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_fetch>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_append>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_update>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_delete_rows>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_publish_commits>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_publish_deletes>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_revert_appends>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_revert_deletes>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::resolve_namespace>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::resolve_function_by_name>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::find_cast_oid>,
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
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::drop_storage_column>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::rename_storage_column>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::on_horizon_advanced>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::mark_storage_dropped_many>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_dropped_committed>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_drop_aborted>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_open_scan_hold>,
            actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_compact_epoch>,
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

    table_storage_t::table_storage_t(std::pmr::memory_resource* resource,
                                     std::vector<components::table::column_definition_t> columns,
                                     const std::filesystem::path& otbx_path)
        : buffer_pool_(resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
        , buffer_manager_(resource, fs_, buffer_pool_)
        , pending_released_blocks_(resource) {
        auto bm = std::make_unique<components::table::storage::single_file_block_manager_t>(buffer_manager_,
                                                                                            fs_,
                                                                                            otbx_path.string());
        if (auto r = bm->create_new_database(); r.has_error()) {
            // error_on, not a plain copy: a plain copy would leave the text on the wrong resource.
            construction_error_ = core::error_on(resource, r.error());
            return;
        }
        block_manager_ = std::move(bm);
        table_ = std::make_unique<components::table::data_table_t>(resource, *block_manager_, std::move(columns));
    }

    table_storage_t::table_storage_t(std::pmr::memory_resource* resource,
                                     const std::filesystem::path& otbx_path,
                                     std::vector<components::table::column_definition_t> catalog_columns,
                                     bool allow_schemaless)
        : buffer_pool_(resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
        , buffer_manager_(resource, fs_, buffer_pool_)
        , pending_released_blocks_(resource) {
        auto bm = std::make_unique<components::table::storage::single_file_block_manager_t>(buffer_manager_,
                                                                                            fs_,
                                                                                            otbx_path.string());
        if (auto r = bm->load_existing_database(); r.has_error()) {
            construction_error_ = core::error_on(resource, r.error());
            return;
        }
        block_manager_ = std::move(bm);

        // INVALID_INDEX means the file is provably never checkpointed; feeding it to the metadata
        // reader would misread that as a read-past-end-of-chain corruption.
        if (block_manager_->meta_block() == components::table::storage::INVALID_INDEX) {
            if (catalog_columns.empty() && !allow_schemaless) {
                construction_error_ = core::error_t(
                    core::error_code_t::data_corruption,
                    std::pmr::string{otbx_path.string() +
                                         " has no checkpointed content (never checkpointed — legal), but the "
                                         "catalog supplies no columns for it; refusing to fabricate a schema-less "
                                         "table (the caller must defer the load until the catalog knows the table)",
                                     resource});
                return;
            }
            never_checkpointed_ = true;
            table_ = std::make_unique<components::table::data_table_t>(resource,
                                                                       *block_manager_,
                                                                       std::move(catalog_columns));
            return;
        }

        components::table::storage::metadata_manager_t meta_mgr(*block_manager_);
        auto meta_block = block_manager_->meta_block();
        components::table::storage::meta_block_pointer_t meta_ptr;
        meta_ptr.block_pointer = meta_block;
        components::table::storage::metadata_reader_t reader(meta_mgr, meta_ptr);
        auto loaded = components::table::data_table_t::load_from_disk(resource, *block_manager_, reader);
        if (loaded.has_error()) {
            construction_error_ = core::error_on(resource, loaded.error());
            return;
        }
        table_ = std::move(loaded.value());
#ifdef DEV_MODE
        capture_clean_fingerprint();
#endif
    }

#ifdef DEV_MODE
    // Skips in-place UPDATE by design (data_table_t::update sets the modified flag itself); this net
    // only catches a future mutating method that forgets to.
    void table_storage_t::capture_clean_fingerprint() noexcept {
        if (!table_) {
            clean_fingerprint_ = clean_fingerprint_t{};
            return;
        }
        auto collection = table_->row_group();
        clean_fingerprint_.total_rows = collection->total_rows();
        clean_fingerprint_.committed_rows = collection->committed_row_count();
        clean_fingerprint_.column_count = table_->column_count();
    }
#endif

    bool table_storage_t::needs_checkpoint() const noexcept {
        if (!table_) {
            return false;
        }
        if (!pending_released_blocks_.empty()) {
            return true;
        }
        if (table_->modified_since_checkpoint()) {
            return true;
        }
#ifdef DEV_MODE
        auto collection = table_->row_group();
        assert(collection->total_rows() == clean_fingerprint_.total_rows &&
               "clean table holds a different number of rows than the durable root was written from — a "
               "mutation path forgot to mark the table modified");
        assert(collection->committed_row_count() == clean_fingerprint_.committed_rows &&
               "clean table holds a different number of live rows than the durable root was written from — a "
               "mutation path forgot to mark the table modified");
        assert(table_->column_count() == clean_fingerprint_.column_count &&
               "clean table has a different column count than the durable root was written from — a "
               "mutation path forgot to mark the table modified");
#endif
        return false;
    }

    void table_storage_t::advance_wal_id_without_rewrite(wal::id_t new_wal_id) noexcept {
        prev_checkpoint_wal_id_ = checkpoint_wal_id_;
        checkpoint_wal_id_ = new_wal_id;
        checkpoint_wal_id_known_ = true;
    }

    bool table_storage_t::has_pending_update_overlay() {
        if (!table_) {
            return false;
        }
        for (const auto& info : table_->get_column_segment_info()) {
            if (info.has_updates) {
                return true;
            }
        }
        return false;
    }

    bool table_storage_t::has_versions_above(uint64_t watermark) const {
        if (!table_) {
            return false;
        }
        return table_->row_group()->has_version_above(watermark);
    }

    bool table_storage_t::storage_degraded() const noexcept {
        if (!block_manager_) {
            return false;
        }
        return block_manager_->degraded();
    }

    core::result_wrapper_t<bool> table_storage_t::checkpoint() {
        auto* disk_bm_check =
            static_cast<components::table::storage::single_file_block_manager_t*>(block_manager_.get());
        if (disk_bm_check->has_durability_error()) {
            disk_bm_check->roll_back_uncommitted_round();
            return core::error_t(disk_bm_check->durability_error());
        }
        if (disk_bm_check->has_allocation_error()) {
            disk_bm_check->roll_back_uncommitted_round();
            return core::error_t(disk_bm_check->allocation_error());
        }

        components::table::storage::metadata_manager_t meta_mgr(*block_manager_);
        components::table::storage::metadata_writer_t writer(meta_mgr);
        // A failure before write_header leaves the durable root unchanged; roll back this round's own
        // allocations rather than strand them (measured: ~655 KB/round leaked on a 7.8 MB table).
        auto cp_r = table_->checkpoint(writer);
        if (cp_r.has_error()) {
            disk_bm_check->roll_back_uncommitted_round();
            return cp_r;
        }
        if (auto flush_r = writer.flush(); flush_r.has_error()) {
            disk_bm_check->roll_back_uncommitted_round();
            return flush_r;
        }

        auto* disk_bm = static_cast<components::table::storage::single_file_block_manager_t*>(block_manager_.get());
        disk_bm->set_meta_block(writer.get_block_pointer().block_pointer);
        // Must run exactly here, between the new root's pointer stream and the free-list serialize.
        release_dropped_column_blocks();
        auto free_list_r = disk_bm->serialize_free_list();
        if (free_list_r.has_error()) {
            disk_bm->roll_back_uncommitted_round();
            return free_list_r.convert_error<bool>();
        }
        // W-TORN: this fsync makes metadata+data durable BEFORE the header swap, or a crash could
        // leave the header pointing at non-durable blocks.
        if (auto barrier_r = disk_bm->file_sync(); barrier_r.has_error()) {
            disk_bm->roll_back_uncommitted_round();
            return barrier_r;
        }
        components::table::storage::database_header_t header{};
        header.initialize();
        header.free_list = free_list_r.value().block_pointer;
        // The atomic point: write_header writes+fsyncs the slot in one call -- its fsync IS the commit.
        auto header_r = disk_bm->write_header(header);
        if (header_r.has_error()) {
            return header_r;
        }
        // Only here does the new root reach the device; every failure above must leave the entry dirty.
        table_->clear_modified_since_checkpoint();
#ifdef DEV_MODE
        capture_clean_fingerprint();
#endif
        return true;
    }

    core::result_wrapper_t<bool> table_storage_t::checkpoint(wal::id_t new_wal_id) {
        auto cp_r = checkpoint();
        if (cp_r.has_error()) {
            // Retried next round, but must NOT compact first -- see last_checkpoint_failed().
            last_checkpoint_failed_ = true;
            return cp_r;
        }
        last_checkpoint_failed_ = false;
        prev_checkpoint_wal_id_ = checkpoint_wal_id_;
        checkpoint_wal_id_ = new_wal_id;
        checkpoint_wal_id_known_ = true;
        return true;
    }

    void table_storage_t::add_column(components::table::column_definition_t& col) {
        auto new_table = std::make_unique<components::table::data_table_t>(*table_, col);
        table_ = std::move(new_table);
    }

    bool table_storage_t::drop_column(const std::string& attname) {
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
        // Names the blocks before the rebuild drops the only record of them (release happens later).
        if (block_manager_) {
            table_->collect_column_disk_block_ids(idx, pending_released_blocks_);
        }
        auto new_table = std::make_unique<components::table::data_table_t>(*table_, idx);
        table_ = std::move(new_table);
        return true;
    }

    core::result_wrapper_t<bool> table_storage_t::rename_column(const std::string& old_attname,
                                                                const std::string& new_attname) {
        if (!table_) {
            // The caller's catalog rename is already committed, so this can't just answer "nothing to do".
            std::pmr::string msg{"table_storage_t::rename_column: no loaded table for column '",
                                 pending_released_blocks_.get_allocator().resource()};
            msg += std::pmr::string{old_attname, pending_released_blocks_.get_allocator().resource()};
            msg += std::pmr::string{"'", pending_released_blocks_.get_allocator().resource()};
            return core::error_t{core::error_code_t::other_error, std::move(msg)};
        }
        return table_->rename_column(old_attname, new_attname);
    }

    // Deferred, not immediate: freeing a still-referenced block is worse than leaking it. Safe only
    // once the drop's superseded collection is gone (row_group() hands out counted copies BY VALUE).
    // Measured with the naming removed: 15 blocks (~3.75 MB on a 10k-row table) orphaned durably.
    void table_storage_t::release_dropped_column_blocks() {
        if (pending_released_blocks_.empty() || !block_manager_ || !table_) {
            return;
        }
        auto& block_manager = *block_manager_;
        // Same id can repeat (many segments pack into one block); dedup before the loop below.
        std::sort(pending_released_blocks_.begin(), pending_released_blocks_.end());
        pending_released_blocks_.erase(
            std::unique(pending_released_blocks_.begin(), pending_released_blocks_.end()),
            pending_released_blocks_.end());

        // NOT held across the frees below -- a holder that outlives them keeps handles alive past reclaim.
        std::pmr::vector<uint64_t> live(pending_released_blocks_.get_allocator().resource());
        {
            auto collection = table_->row_group();
            collection->collect_disk_block_ids(live);
        }
        std::sort(live.begin(), live.end());
        live.erase(std::unique(live.begin(), live.end()), live.end());

        for (uint64_t block_id : pending_released_blocks_) {
            if (block_id >= block_manager.total_blocks()) {
                block_manager.mark_as_free(block_id); // refuses the id and latches the corruption
                continue;
            }
            if (std::binary_search(live.begin(), live.end(), block_id)) {
                continue; // still carries a surviving column's segment (block packing)
            }
            if (block_manager.registry_alive(block_id)) {
                continue; // somebody still holds a handle for it
            }
            block_manager.mark_as_free(block_id);
            // ABA break: unregister only after the free, so no expired slot can be revived.
            block_manager.unregister_block(block_id);
        }
        pending_released_blocks_.clear();
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
        // This thread owns all message processing; senders only push into inbox_ and notify pump_cv_.
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
                    // pending_msg stays in the slot: the coroutine holds a raw pointer to it across suspensions.
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
#ifdef DEV_MODE
                            services::dispatcher::note_pump_hop();
#endif
                            cont.resume(); // disk: no poll_pending — no pending_<T>_ containers.
                            progress = true;
                            continue;
                        }
                    }
                    for (auto it = in_flight.begin(); it != in_flight.end(); ++it) {
                        if (it->behavior && it->behavior.done()) {
                            in_flight.erase(it);
                            progress = true;
                            break;
                        }
                    }
                }
                std::unique_lock<std::mutex> lk(mutex_);
                // In flight this timeout IS the per-hop latency (a statement crosses ~20 hops), so it's short.
                if (inbox_.empty())
                    pump_cv_.wait_for(lk,
                                      in_flight.empty() ? std::chrono::microseconds(100)
                                                        : std::chrono::microseconds(5));
            }
        });
        trace(log_, "manager_disk finish");
    }

    manager_disk_t::~manager_disk_t() {
        loop_running_.store(false, std::memory_order_release);
        pump_cv_.notify_one();
        if (loop_thread_.joinable()) {
            loop_thread_.join();
        }
        actor_zeta::mailbox::message* raw = nullptr;
        while (inbox_.pop(raw)) {
            actor_zeta::mailbox::message_ptr drained{raw};
        }
        trace(log_, "delete manager_disk_t");
    }

    std::pair<bool, actor_zeta::detail::enqueue_result>
    manager_disk_t::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
        // push refuses only under real memory exhaustion; reclaim rather than leak and hang the sender.
        auto* raw = msg.release();
        if (!inbox_.push(raw)) {
            actor_zeta::mailbox::message_ptr reclaimed{raw};
            error(log_,
                  "manager_disk_t::enqueue_impl: inbox push refused (allocation failure) — the message is "
                  "dropped and its future completes as abandoned");
            return {false, actor_zeta::detail::enqueue_result::queue_closed};
        }
        pump_cv_.notify_one();
        return {false, actor_zeta::detail::enqueue_result::success};
    }

    actor_zeta::behavior_t manager_disk_t::behavior(actor_zeta::mailbox::message* msg) {
        switch (msg->command()) {
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
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::create_storage_disk>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::create_storage_disk, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::drop_storage_many>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::drop_storage_many, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_types>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_types, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_total_rows>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_total_rows, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_fetch_next_batch>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_fetch_next_batch, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_close_cursor>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_close_cursor, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_open_scan_hold>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_open_scan_hold, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_compact_epoch>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_compact_epoch, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_reduce>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_reduce, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_fetch>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_fetch, msg);
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
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::resolve_namespace>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::resolve_namespace, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::resolve_function_by_name>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::resolve_function_by_name, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::find_cast_oid>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::find_cast_oid, msg);
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
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::drop_storage_column>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::drop_storage_column, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::rename_storage_column>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::rename_storage_column, msg);
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

    // Rule-3 pipeline bypass (core/pipeline_bypass.hpp): horizon keeps this off files a snapshot may read.
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
        // Bootstrap-only; the manager keeps no copy since only the agents send to the dispatcher.
        for (auto& agent_ptr : agents_) {
            agent_ptr->set_manager_dispatcher_sync(address);
        }
    }

    void manager_disk_t::register_dropped_storage_sync(components::catalog::oid_t oid,
                                                       uint64_t dropped_at_commit_id,
                                                       std::filesystem::path path,
                                                       std::pmr::vector<std::filesystem::path> sidecar_paths) {
        // Bootstrap-only (base_spaces catalog rebuild); runtime DROP uses mark_storage_dropped_many below.
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
        // Each agent derives its own path, so the manager never borrows agent state across the boundary.
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
        // Only the txn_id placeholder is known, not which agent owns the oid, so fan out to all.
        trace(log_, "manager_disk::storage_dropped_committed , txn_id : {} , commit_id : {}", txn_id, commit_id);

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

    manager_disk_t::unique_future<void> manager_disk_t::storage_drop_aborted(session_id_t /*session*/,
                                                                             uint64_t txn_id) {
        // Abort mirror of storage_dropped_committed: ERASES (not remaps) so the .otbx is never reclaimed.
        trace(log_, "manager_disk::storage_drop_aborted , txn_id : {}", txn_id);

        std::pmr::vector<unique_future<void>> agent_futures{resource()};
        agent_futures.reserve(agents_.size());
        for (auto& agent_ptr : agents_) {
            auto [needs_sched, fut] =
                actor_zeta::otterbrix::send(agent_ptr->address(), &agent_disk_t::storage_drop_aborted_inner, txn_id);
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
