#pragma once

#include "index_contract.hpp"

#include <actor-zeta.hpp>
#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/address.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/behavior_t.hpp>
#include <actor-zeta/detail/future.hpp>
#include <actor-zeta/detail/queue/enqueue_result.hpp>

#include "index_agent_disk.hpp"
#include <components/index/index_engine.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <core/file/local_file_system.hpp>
#include <mutex>

namespace services::index {

    class manager_index_t final : public actor_zeta::actor::actor_mixin<manager_index_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        using address_pack = std::tuple<actor_zeta::address_t>;
        using run_fn_t = std::function<void()>;

        manager_index_t(
            std::pmr::memory_resource* resource,
            actor_zeta::scheduler_raw scheduler,
            log_t& log,
            std::filesystem::path path_db = {},
            run_fn_t run_fn = [] { std::this_thread::yield(); });
        ~manager_index_t() = default;

        // Synchronous registration for initialization (before schedulers start)
        void register_collection_sync(session_id_t session, const collection_full_name_t& name);

        std::pmr::memory_resource* resource() const noexcept { return resource_; }
        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        [[nodiscard]] std::pair<bool, actor_zeta::detail::enqueue_result>
        enqueue_impl(actor_zeta::mailbox::message_ptr msg);

        void sync(address_pack pack);

        // Collection lifecycle
        unique_future<void> register_collection(session_id_t session, collection_full_name_t name);
        unique_future<void> unregister_collection(session_id_t session, collection_full_name_t name);

        // DML: bulk index operations
        unique_future<void> insert_rows(session_id_t session,
                                        collection_full_name_t name,
                                        std::unique_ptr<components::vector::data_chunk_t> data,
                                        uint64_t start_row_id,
                                        uint64_t count);
        unique_future<void> delete_rows(session_id_t session,
                                        collection_full_name_t name,
                                        std::unique_ptr<components::vector::data_chunk_t> data,
                                        std::pmr::vector<size_t> row_ids);
        unique_future<void> update_rows(session_id_t session,
                                        collection_full_name_t name,
                                        std::unique_ptr<components::vector::data_chunk_t> old_data,
                                        std::unique_ptr<components::vector::data_chunk_t> new_data,
                                        std::pmr::vector<size_t> row_ids);

        // DDL: index management
        unique_future<uint32_t> create_index(session_id_t session,
                                             collection_full_name_t name,
                                             index_name_t index_name,
                                             components::index::keys_base_storage_t keys,
                                             components::logical_plan::index_type type);
        unique_future<void> drop_index(session_id_t session, collection_full_name_t name, index_name_t index_name);

        // Query
        unique_future<std::pmr::vector<int64_t>> search(session_id_t session,
                                                        collection_full_name_t name,
                                                        components::index::keys_base_storage_t keys,
                                                        components::types::logical_value_t value,
                                                        components::expressions::compare_type compare);

        unique_future<bool> has_index(session_id_t session, collection_full_name_t name, index_name_t index_name);

        unique_future<std::pmr::vector<components::index::keys_base_storage_t>>
            get_indexed_keys(session_id_t session, collection_full_name_t name);

        using dispatch_traits = actor_zeta::implements<index_contract,
                                                       &manager_index_t::register_collection,
                                                       &manager_index_t::unregister_collection,
                                                       &manager_index_t::insert_rows,
                                                       &manager_index_t::delete_rows,
                                                       &manager_index_t::update_rows,
                                                       &manager_index_t::create_index,
                                                       &manager_index_t::drop_index,
                                                       &manager_index_t::search,
                                                       &manager_index_t::has_index,
                                                       &manager_index_t::get_indexed_keys>;

    private:
        std::pmr::memory_resource* resource_;
        actor_zeta::scheduler_raw scheduler_;
        run_fn_t run_fn_;
        log_t log_;
        std::filesystem::path path_db_;
        std::mutex mutex_;

        // Per-collection in-memory index engines
        std::pmr::unordered_map<collection_full_name_t, components::index::index_engine_ptr, collection_name_hash>
            engines_;

        // Per-index disk persistence (child actors)
        std::vector<index_agent_disk_ptr> disk_agents_;

        // Index metadata persistence (indexes_METADATA file)
        using file_ptr = std::unique_ptr<core::filesystem::file_handle_t>;
        core::filesystem::local_file_system_t fs_;
        file_ptr metafile_indexes_;

        void write_index_to_metafile(const components::logical_plan::node_create_index_ptr& index);
        void remove_index_from_metafile(const index_name_t& name);
        void remove_all_indexes_for_collection(const collection_name_t& collection);
        std::vector<components::logical_plan::node_create_index_ptr> read_indexes_from_metafile() const;

        // Address of manager_disk_t (for scan_segment when populating indexes)
        actor_zeta::address_t disk_address_ = actor_zeta::address_t::empty_address();

        // Find disk agent by address and schedule it if needed
        void schedule_agent(const actor_zeta::address_t& addr, bool needs_sched);

        // Pending futures
        std::pmr::vector<unique_future<void>> pending_void_;
        void poll_pending();

        actor_zeta::behavior_t current_behavior_;
    };

    using manager_index_ptr = std::unique_ptr<manager_index_t, actor_zeta::pmr::deleter_t>;

} // namespace services::index
