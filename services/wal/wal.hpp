#pragma once

#include <actor-zeta.hpp>
#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/future.hpp>

#include <boost/filesystem.hpp>
#include <components/log/log.hpp>

#include <components/configuration/configuration.hpp>
#include <components/session/session.hpp>
#include <core/file/file_system.hpp>

#include "dto.hpp"
#include "forward.hpp"
#include "record.hpp"

namespace services::wal {

    class wal_replicate_t : public actor_zeta::basic_actor<wal_replicate_t> {
        using session_id_t = components::session::session_id_t;
        using address_t = actor_zeta::address_t;
        using file_ptr = std::unique_ptr<core::filesystem::file_handle_t>;

    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        wal_replicate_t(std::pmr::memory_resource* resource,
                        manager_wal_replicate_t* manager,
                        log_t& log,
                        configuration::config_wal config,
                        int worker_index = 0,
                        int worker_count = 1);
        virtual ~wal_replicate_t();

        unique_future<std::vector<record_t>> load(session_id_t session, services::wal::id_t wal_id);
        unique_future<services::wal::id_t> commit_txn(session_id_t session, uint64_t transaction_id);

        unique_future<void> truncate_before(session_id_t session, services::wal::id_t checkpoint_wal_id);

        // Physical WAL write methods
        unique_future<services::wal::id_t>
        write_physical_insert(session_id_t session,
                              std::string database,
                              std::string collection,
                              std::unique_ptr<components::vector::data_chunk_t> data_chunk,
                              uint64_t row_start,
                              uint64_t row_count,
                              uint64_t txn_id);

        unique_future<services::wal::id_t> write_physical_delete(session_id_t session,
                                                                 std::string database,
                                                                 std::string collection,
                                                                 std::pmr::vector<int64_t> row_ids,
                                                                 uint64_t count,
                                                                 uint64_t txn_id);

        unique_future<services::wal::id_t>
        write_physical_update(session_id_t session,
                              std::string database,
                              std::string collection,
                              std::pmr::vector<int64_t> row_ids,
                              std::unique_ptr<components::vector::data_chunk_t> new_data,
                              uint64_t count,
                              uint64_t txn_id);

        using dispatch_traits = actor_zeta::dispatch_traits<&wal_replicate_t::load,
                                                            &wal_replicate_t::commit_txn,
                                                            &wal_replicate_t::truncate_before,
                                                            &wal_replicate_t::write_physical_insert,
                                                            &wal_replicate_t::write_physical_delete,
                                                            &wal_replicate_t::write_physical_update>;

        services::wal::id_t current_id() const { return id_.load(); }

        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

    private:
        virtual void write_buffer(buffer_t& buffer);
        virtual void read_buffer(buffer_t& buffer, size_t start_index, size_t size) const;

        void init_id();
        bool find_start_record(services::wal::id_t wal_id, std::size_t& start_index) const;
        services::wal::id_t read_id(std::size_t start_index) const;
        record_t read_record(std::size_t start_index) const;
        size_tt read_size(size_t start_index) const;
        buffer_t read(size_t start_index, size_t finish_index) const;

        mutable log_t log_;
        configuration::config_wal config_;
        core::filesystem::local_file_system_t fs_;
        int worker_index_{0};
        int worker_count_{1};
        atomic_id_t id_{0};
        crc32_t last_crc32_{0};
        file_ptr file_;
        int current_segment_idx_{0};

        std::string wal_segment_name_(int segment_idx) const;
        void rotate_segment_();
        std::vector<std::filesystem::path> discover_segments_() const;
        services::wal::id_t last_id_in_file_(const std::filesystem::path& path);

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
        wal_replicate_without_disk_t(std::pmr::memory_resource* resource,
                                     manager_wal_replicate_t* manager,
                                     log_t& log,
                                     configuration::config_wal config,
                                     int worker_index = 0,
                                     int worker_count = 1);

        unique_future<std::vector<record_t>> load(session_id_t session, services::wal::id_t wal_id);

    private:
        void write_buffer(buffer_t&) override;
        void read_buffer(buffer_t& buffer, size_t start_index, size_t size) const override;
    };

    using wal_replicate_ptr = std::unique_ptr<wal_replicate_t, actor_zeta::pmr::deleter_t>;

} //namespace services::wal
