#pragma once

#include <actor-zeta.hpp>
#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/detail/future.hpp>

#include <boost/filesystem.hpp>
#include <components/log/log.hpp>

#include <components/configuration/configuration.hpp>
#include <components/session/session.hpp>
#include <core/file/file_system.hpp>

#include "dto.hpp"
#include "forward.hpp"
#include "record.hpp"

#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_drop_collection.hpp>
#include <components/logical_plan/node_drop_database.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_update.hpp>

namespace services::wal {

    class wal_replicate_t : public actor_zeta::basic_actor<wal_replicate_t> {
        using session_id_t = components::session::session_id_t;
        using address_t = actor_zeta::address_t;
        using file_ptr = std::unique_ptr<core::filesystem::file_handle_t>;

    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        wal_replicate_t(std::pmr::memory_resource* resource, manager_wal_replicate_t* manager, log_t& log, configuration::config_wal config);
        virtual ~wal_replicate_t();

        unique_future<std::vector<record_t>> load(session_id_t session, services::wal::id_t wal_id);
        unique_future<services::wal::id_t> create_database(session_id_t session,
                                            components::logical_plan::node_create_database_ptr data);
        unique_future<services::wal::id_t> drop_database(session_id_t session,
                                          components::logical_plan::node_drop_database_ptr data);
        unique_future<services::wal::id_t> create_collection(session_id_t session,
                                              components::logical_plan::node_create_collection_ptr data);
        unique_future<services::wal::id_t> drop_collection(session_id_t session,
                                            components::logical_plan::node_drop_collection_ptr data);
        unique_future<services::wal::id_t> insert_one(session_id_t session,
                                       components::logical_plan::node_insert_ptr data);
        unique_future<services::wal::id_t> insert_many(session_id_t session,
                                        components::logical_plan::node_insert_ptr data);
        unique_future<services::wal::id_t> delete_one(session_id_t session,
                                       components::logical_plan::node_delete_ptr data,
                                       components::logical_plan::parameter_node_ptr params);
        unique_future<services::wal::id_t> delete_many(session_id_t session,
                                        components::logical_plan::node_delete_ptr data,
                                        components::logical_plan::parameter_node_ptr params);
        unique_future<services::wal::id_t> update_one(session_id_t session,
                                       components::logical_plan::node_update_ptr data,
                                       components::logical_plan::parameter_node_ptr params);
        unique_future<services::wal::id_t> update_many(session_id_t session,
                                        components::logical_plan::node_update_ptr data,
                                        components::logical_plan::parameter_node_ptr params);
        unique_future<services::wal::id_t> create_index(session_id_t session,
                                         components::logical_plan::node_create_index_ptr data);

        using dispatch_traits = actor_zeta::dispatch_traits<
            &wal_replicate_t::load,
            &wal_replicate_t::create_database,
            &wal_replicate_t::drop_database,
            &wal_replicate_t::create_collection,
            &wal_replicate_t::drop_collection,
            &wal_replicate_t::insert_one,
            &wal_replicate_t::insert_many,
            &wal_replicate_t::delete_one,
            &wal_replicate_t::delete_many,
            &wal_replicate_t::update_one,
            &wal_replicate_t::update_many,
            &wal_replicate_t::create_index
        >;

        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

    private:

        virtual void write_buffer(buffer_t& buffer);
        virtual void read_buffer(buffer_t& buffer, size_t start_index, size_t size) const;

        template<class T>
        void write_data_(T& data, components::logical_plan::parameter_node_ptr params);

        void init_id();
        bool find_start_record(services::wal::id_t wal_id, std::size_t& start_index) const;
        services::wal::id_t read_id(std::size_t start_index) const;
        record_t read_record(std::size_t start_index) const;
        size_tt read_size(size_t start_index) const;
        buffer_t read(size_t start_index, size_t finish_index) const;

        log_t log_;
        configuration::config_wal config_;
        core::filesystem::local_file_system_t fs_;
        atomic_id_t id_{0};
        crc32_t last_crc32_{0};
        file_ptr file_;

        std::pmr::vector<unique_future<std::vector<record_t>>> pending_load_;
        std::pmr::vector<unique_future<services::wal::id_t>> pending_id_;

        void poll_pending();

#ifdef DEV_MODE
    public:
        bool test_find_start_record(services::wal::id_t wal_id, std::size_t& start_index) const;
        services::wal::id_t test_read_id(std::size_t start_index) const;
        std::size_t test_next_record(std::size_t start_index) const;
        record_t test_read_record(std::size_t start_index) const;
        size_tt test_read_size(size_t start_index) const;
        buffer_t test_read(size_t start_index, size_t finish_index) const;
#endif
    };

    class wal_replicate_without_disk_t final : public wal_replicate_t {
        using session_id_t = components::session::session_id_t;
        using address_t = actor_zeta::address_t;

    public:
        wal_replicate_without_disk_t(std::pmr::memory_resource* resource, manager_wal_replicate_t* manager, log_t& log, configuration::config_wal config);

        unique_future<std::vector<record_t>> load(session_id_t session, services::wal::id_t wal_id);

    private:
        void write_buffer(buffer_t&) override;
        void read_buffer(buffer_t& buffer, size_t start_index, size_t size) const override;
    };

    using wal_replicate_ptr = std::unique_ptr<wal_replicate_t, actor_zeta::pmr::deleter_t>;

} //namespace services::wal