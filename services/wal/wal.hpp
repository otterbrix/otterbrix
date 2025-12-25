#pragma once

#include <actor-zeta.hpp>

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
        wal_replicate_t(manager_wal_replicate_t* manager, log_t& log, configuration::config_wal config);
        void load(const session_id_t& session, address_t& sender, services::wal::id_t wal_id);
        void create_database(const session_id_t& session,
                             address_t& sender,
                             components::logical_plan::node_create_database_ptr data);
        void drop_database(const session_id_t& session,
                           address_t& sender,
                           components::logical_plan::node_drop_database_ptr data);
        void create_collection(const session_id_t& session,
                               address_t& sender,
                               components::logical_plan::node_create_collection_ptr data);
        void drop_collection(const session_id_t& session,
                             address_t& sender,
                             components::logical_plan::node_drop_collection_ptr data);
        void insert_one(const session_id_t& session, address_t& sender, components::logical_plan::node_insert_ptr data);
        void
        insert_many(const session_id_t& session, address_t& sender, components::logical_plan::node_insert_ptr data);
        void delete_one(const session_id_t& session,
                        address_t& sender,
                        components::logical_plan::node_delete_ptr data,
                        components::logical_plan::parameter_node_ptr params);
        void delete_many(const session_id_t& session,
                         address_t& sender,
                         components::logical_plan::node_delete_ptr data,
                         components::logical_plan::parameter_node_ptr params);
        void update_one(const session_id_t& session,
                        address_t& sender,
                        components::logical_plan::node_update_ptr data,
                        components::logical_plan::parameter_node_ptr params);
        void update_many(const session_id_t& session,
                         address_t& sender,
                         components::logical_plan::node_update_ptr data,
                         components::logical_plan::parameter_node_ptr params);
        void create_index(const session_id_t& session,
                          address_t& sender,
                          components::logical_plan::node_create_index_ptr data);
        ~wal_replicate_t() override;

        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior();

    private:
        void send_success(const session_id_t& session, address_t& sender);

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

        actor_zeta::behavior_t load_;
        actor_zeta::behavior_t create_database_;
        actor_zeta::behavior_t drop_database_;
        actor_zeta::behavior_t create_collection_;
        actor_zeta::behavior_t drop_collection_;
        actor_zeta::behavior_t insert_one_;
        actor_zeta::behavior_t insert_many_;
        actor_zeta::behavior_t delete_one_;
        actor_zeta::behavior_t delete_many_;
        actor_zeta::behavior_t update_one_;
        actor_zeta::behavior_t update_many_;
        actor_zeta::behavior_t create_index_;

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
        wal_replicate_without_disk_t(manager_wal_replicate_t* manager, log_t& log, configuration::config_wal config);
        void load(const session_id_t& session, address_t& sender, services::wal::id_t wal_id);

    private:
        void write_buffer(buffer_t&) override;
        void read_buffer(buffer_t& buffer, size_t start_index, size_t size) const override;
    };

    using wal_replicate_ptr = std::unique_ptr<wal_replicate_t, actor_zeta::pmr::deleter_t>;

} //namespace services::wal
