#pragma once

#include <unordered_map>
#include <variant>

#include <actor-zeta.hpp>
#include <actor-zeta/detail/memory_resource.hpp>

#include <core/excutor.hpp>
#include <core/spinlock/spinlock.hpp>

#include <components/catalog/catalog.hpp>
#include <components/cursor/cursor.hpp>
#include <components/document/document.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/physical_plan/base/operators/operator_write_data.hpp>
#include <services/disk/result.hpp>
#include <services/wal/base.hpp>
#include <services/wal/record.hpp>

#include "route.hpp"
#include "session.hpp"

namespace services::dispatcher {

    class manager_dispatcher_t;

    class dispatcher_t final : public actor_zeta::basic_actor<dispatcher_t> {
    public:
        using recomputed_types = components::base::operators::operator_write_data_t::updated_types_map_t;

        dispatcher_t(manager_dispatcher_t*,
                     actor_zeta::address_t&,
                     actor_zeta::address_t&,
                     actor_zeta::address_t&,
                     log_t& log);
        ~dispatcher_t();

        auto make_type() const noexcept -> const char* const;

        actor_zeta::behavior_t behavior();

        void load(const components::session::session_id_t& session, actor_zeta::address_t sender);
        void load_from_disk_result(const components::session::session_id_t& session,
                                   const services::disk::result_load_t& result);
        void load_from_memory_storage_result(const components::session::session_id_t& session);
        void load_from_wal_result(const components::session::session_id_t& session,
                                  std::vector<services::wal::record_t>& records);
        void execute_plan(const components::session::session_id_t& session,
                          components::logical_plan::node_ptr plan,
                          components::logical_plan::parameter_node_ptr params,
                          actor_zeta::address_t address);
        void execute_plan_finish(const components::session::session_id_t& session,
                                 components::cursor::cursor_t_ptr cursor);
        void execute_plan_delete_finish(const components::session::session_id_t& session,
                                        components::cursor::cursor_t_ptr cursor,
                                        recomputed_types updates);
        void size(const components::session::session_id_t& session,
                  std::string& database_name,
                  std::string& collection,
                  actor_zeta::base::address_t sender);
        void size_finish(const components::session::session_id_t&, components::cursor::cursor_t_ptr&& cursor);
        void close_cursor(const components::session::session_id_t& session);
        void wal_success(const components::session::session_id_t& session, services::wal::id_t wal_id);
        bool load_from_wal_in_progress(const components::session::session_id_t& session);

        const components::catalog::catalog& current_catalog();

    private:
        // Behaviors
        actor_zeta::behavior_t load_;
        actor_zeta::behavior_t load_from_disk_result_;
        actor_zeta::behavior_t load_from_memory_storage_result_;
        actor_zeta::behavior_t load_from_wal_result_;
        actor_zeta::behavior_t execute_plan_;
        actor_zeta::behavior_t execute_plan_finish_;
        actor_zeta::behavior_t execute_plan_delete_finish_;
        actor_zeta::behavior_t size_;
        actor_zeta::behavior_t size_finish_;
        actor_zeta::behavior_t close_cursor_;
        actor_zeta::behavior_t wal_success_;

        log_t log_;
        components::catalog::catalog catalog_;
        actor_zeta::address_t manager_dispatcher_;
        actor_zeta::address_t memory_storage_;
        actor_zeta::address_t manager_wal_;
        actor_zeta::address_t manager_disk_;
        session_storage_t session_to_address_;
        std::unordered_map<components::session::session_id_t, std::unique_ptr<components::cursor::cursor_t>> cursor_;
        std::unordered_map<components::session::session_id_t, components::cursor::cursor_t_ptr>
            result_storage_; // to be able return result from wal_success
        disk::result_load_t load_result_;
        recomputed_types update_result_;
        components::session::session_id_t load_session_;
        services::wal::id_t last_wal_id_{0};
        std::size_t load_count_answers_{0};

        components::cursor::cursor_t_ptr check_namespace_exists(const components::catalog::table_id id);
        components::cursor::cursor_t_ptr check_collectction_exists(const components::catalog::table_id id);
        components::cursor::cursor_t_ptr
        check_collections_format_(const components::logical_plan::node_ptr& logical_plan);

        components::logical_plan::node_ptr create_logic_plan(components::logical_plan::node_ptr plan);
        void update_catalog(components::logical_plan::node_ptr node);
        // TODO figure out what to do with records
        std::vector<services::wal::record_t> records_;
    };

    using dispatcher_ptr = std::unique_ptr<dispatcher_t, actor_zeta::pmr::deleter_t>;

    class manager_dispatcher_t final : public actor_zeta::cooperative_supervisor<manager_dispatcher_t> {
    public:
        using address_pack = std::tuple<actor_zeta::address_t, actor_zeta::address_t, actor_zeta::address_t>;

        enum class unpack_rules : uint64_t
        {
            memory_storage = 0,
            manager_wal = 1,
            manager_disk = 2
        };

        void sync(address_pack& pack) {
            memory_storage_ = std::get<static_cast<uint64_t>(unpack_rules::memory_storage)>(pack);
            manager_wal_ = std::get<static_cast<uint64_t>(unpack_rules::manager_wal)>(pack);
            manager_disk_ = std::get<static_cast<uint64_t>(unpack_rules::manager_disk)>(pack);
        }

        manager_dispatcher_t(std::pmr::memory_resource*, actor_zeta::scheduler_raw, log_t& log);

        ~manager_dispatcher_t() override;

        auto make_type() const noexcept -> const char* const;

        actor_zeta::behavior_t behavior();

        auto make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t*;

        ///-----
        void create_dispatcher() {
            actor_zeta::send(address(), address(), handler_id(route::create), components::session::session_id_t());
        }
        ///------
        void create(const components::session::session_id_t& session);
        void load(const components::session::session_id_t& session);
        void execute_plan(const components::session::session_id_t& session,
                          components::logical_plan::node_ptr plan,
                          components::logical_plan::parameter_node_ptr params);
        void
        size(const components::session::session_id_t& session, std::string& database_name, std::string& collection);
        void close_cursor(const components::session::session_id_t& session);

        const components::catalog::catalog& current_catalog();

        void get_schema(const components::session::session_id_t& session,
                        const std::pmr::vector<std::pair<database_name_t, collection_name_t>>& ids);

    protected:
        auto enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void override;

    private:
        // Behaviors
        actor_zeta::behavior_t create_;
        actor_zeta::behavior_t load_;
        actor_zeta::behavior_t execute_plan_;
        actor_zeta::behavior_t size_;
        actor_zeta::behavior_t schema_;
        actor_zeta::behavior_t close_cursor_;
        actor_zeta::behavior_t sync_;

        spin_lock lock_;
        log_t log_;
        actor_zeta::scheduler_raw e_;
        actor_zeta::address_t memory_storage_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t manager_wal_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t manager_disk_ = actor_zeta::address_t::empty_address();
        std::vector<dispatcher_ptr> dispatchers_;

        auto dispatcher() -> actor_zeta::address_t;
    };

} // namespace services::dispatcher
