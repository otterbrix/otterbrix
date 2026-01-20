#pragma once

#include <components/cursor/cursor.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/base/operators/operator.hpp>
#include <components/session/session.hpp>
#include <core/btree/btree.hpp>
#include <core/excutor.hpp>
#include <core/spinlock/spinlock.hpp>
#include <memory_resource>
#include <services/collection/executor.hpp>
#include <services/disk/result.hpp>

namespace services {

    class memory_storage_t final : public actor_zeta::cooperative_supervisor<memory_storage_t> {
        struct session_t {
            components::logical_plan::node_ptr logical_plan;
            actor_zeta::address_t sender;
            size_t count_answers;
        };

        struct load_buffer_t {
            std::pmr::vector<collection_full_name_t> collections;

            explicit load_buffer_t(std::pmr::memory_resource* resource);
        };

        using database_storage_t = std::pmr::set<database_name_t>;
        using collection_storage_t =
            core::pmr::btree::btree_t<collection_full_name_t, std::unique_ptr<collection::context_collection_t>>;
        using session_storage_t = core::pmr::btree::btree_t<components::session::session_id_t, session_t>;

    public:
        using address_pack = std::tuple<actor_zeta::address_t, actor_zeta::address_t>;
        enum class unpack_rules : uint64_t
        {
            manager_dispatcher = 0,
            manager_disk = 1
        };

        memory_storage_t(std::pmr::memory_resource* resource, actor_zeta::scheduler_raw scheduler, log_t& log);
        ~memory_storage_t();

        void sync(const address_pack& pack);
        void execute_plan(const components::session::session_id_t& session,
                          components::logical_plan::node_ptr logical_plan,
                          components::logical_plan::storage_parameters parameters,
                          components::catalog::used_format_t used_format);

        void register_udf(const components::session::session_id_t& session,
                          components::compute::function_ptr&& function);
        void register_udf_finish(const components::session::session_id_t& session,
                                 const std::string& function_name,
                                 components::compute::function_uid uid);

        void size(const components::session::session_id_t& session, collection_full_name_t&& name);
        void close_cursor(const components::session::session_id_t& session,
                          std::set<collection_full_name_t>&& collections);
        void load(const components::session::session_id_t& session, const disk::result_load_t& result);

        actor_zeta::scheduler_abstract_t* make_scheduler() noexcept;
        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior();

    private:
        actor_zeta::scheduler_raw e_;
        database_storage_t databases_;
        collection_storage_t collections_;
        log_t log_;

        // Behaviors
        actor_zeta::behavior_t sync_;
        actor_zeta::behavior_t load_;
        actor_zeta::behavior_t size_;
        actor_zeta::behavior_t create_documents_finish_;
        actor_zeta::behavior_t execute_plan_;
        actor_zeta::behavior_t execute_plan_finish_;
        actor_zeta::behavior_t execute_plan_delete_finish_;
        actor_zeta::behavior_t register_udf_;
        actor_zeta::behavior_t register_udf_finish_;

        actor_zeta::address_t manager_dispatcher_{actor_zeta::address_t::empty_address()};
        actor_zeta::address_t manager_disk_{actor_zeta::address_t::empty_address()};
        actor_zeta::address_t executor_address_{actor_zeta::address_t::empty_address()};

        session_storage_t sessions_;
        std::unique_ptr<load_buffer_t> load_buffer_;
        spin_lock lock_;
        collection::executor::executor_ptr executor_{nullptr,
                                                     actor_zeta::pmr::deleter_t(std::pmr::null_memory_resource())};

    private:
        void enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit* unit) override;

        void create_database_(const components::session::session_id_t& session,
                              components::logical_plan::node_ptr logical_plan);
        void drop_database_(const components::session::session_id_t& session,
                            components::logical_plan::node_ptr logical_plan);
        void create_collection_(const components::session::session_id_t& session,
                                components::logical_plan::node_ptr logical_plan);
        void drop_collection_(const components::session::session_id_t& session,
                              components::logical_plan::node_ptr logical_plan);

        void execute_plan_impl(const components::session::session_id_t& session,
                               components::logical_plan::node_ptr logical_plan,
                               components::logical_plan::storage_parameters parameters,
                               components::catalog::used_format_t used_format);

        void execute_plan_finish(const components::session::session_id_t& session,
                                 components::cursor::cursor_t_ptr cursor);

        void
        execute_plan_delete_finish(const components::session::session_id_t& session,
                                   components::cursor::cursor_t_ptr cursor,
                                   components::base::operators::operator_write_data_t::updated_types_map_t updates);

        void create_documents_finish(const components::session::session_id_t& session);
    };

} // namespace services
