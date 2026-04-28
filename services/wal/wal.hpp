#pragma once

#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <actor-zeta/actor/basic_actor.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/future.hpp>

#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/wal/base.hpp>
#include <services/wal/record.hpp>
#include <services/wal/wal_binary.hpp>
#include <services/wal/wal_page_reader.hpp>
#include <services/wal/wal_page_writer.hpp>
#include <services/wal/wal_sync_mode.hpp>

namespace services::wal {

    class manager_wal_replicate_t;

    using session_id_t = components::session::session_id_t;

    class wal_worker_t final : public actor_zeta::actor::basic_actor<wal_worker_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        wal_worker_t(std::pmr::memory_resource* resource,
                     manager_wal_replicate_t* manager,
                     log_t& log,
                     configuration::config_wal config,
                     const std::string& database_name);

        ~wal_worker_t();

        auto make_type() const noexcept -> const char*;

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        // -----------------------------------------------------------------------
        // Internal methods (called by manager, NOT wal_contract)
        // -----------------------------------------------------------------------

        unique_future<std::vector<record_t>> load(session_id_t session, wal::id_t after_wal_id);

        unique_future<wal::id_t> commit_txn(session_id_t session,
                                            uint64_t transaction_id,
                                            wal_sync_mode sync_mode,
                                            wal::id_t wal_id);

        unique_future<void> truncate_before(session_id_t session, wal::id_t checkpoint_wal_id);

        unique_future<wal::id_t> current_wal_id(session_id_t session);

        unique_future<wal::id_t> write_physical_insert(session_id_t session,
                                                       std::string database,
                                                       std::string collection,
                                                       std::unique_ptr<components::vector::data_chunk_t> data_chunk,
                                                       uint64_t row_start,
                                                       uint64_t row_count,
                                                       uint64_t txn_id,
                                                       wal::id_t wal_id);

        unique_future<wal::id_t> write_physical_delete(session_id_t session,
                                                       std::string database,
                                                       std::string collection,
                                                       std::pmr::vector<int64_t> row_ids,
                                                       uint64_t count,
                                                       uint64_t txn_id,
                                                       wal::id_t wal_id);

        unique_future<wal::id_t> write_physical_update(session_id_t session,
                                                       std::string database,
                                                       std::string collection,
                                                       std::pmr::vector<int64_t> row_ids,
                                                       std::unique_ptr<components::vector::data_chunk_t> new_data,
                                                       uint64_t count,
                                                       uint64_t txn_id,
                                                       wal::id_t wal_id);

        using dispatch_traits = actor_zeta::dispatch_traits<
            &wal_worker_t::load,
            &wal_worker_t::commit_txn,
            &wal_worker_t::truncate_before,
            &wal_worker_t::current_wal_id,
            &wal_worker_t::write_physical_insert,
            &wal_worker_t::write_physical_delete,
            &wal_worker_t::write_physical_update>;

    private:
        // -----------------------------------------------------------------------
        // Startup helpers
        // -----------------------------------------------------------------------

        /// Discover existing segment files, recover max wal_id and last CRC.
        void recover_from_disk();

        /// Build a segment file path for the given segment index.
        std::filesystem::path segment_path(uint32_t seg_index) const;

        /// Collect all segment file paths sorted by index.
        std::vector<std::filesystem::path> discover_segments() const;

        /// Parse segment index from filename. Returns (uint32_t)-1 on failure.
        static uint32_t parse_segment_index(const std::filesystem::path& path,
                                            const std::string& db_name);

        /// Ensure the page writer is ready; rotate if the current segment is full.
        void ensure_writer();

        // -----------------------------------------------------------------------
        // State
        // -----------------------------------------------------------------------
        log_t log_;
        configuration::config_wal config_;
        std::string database_name_;
        std::filesystem::path database_dir_;

        atomic_id_t id_{0};
        crc32_t last_crc_{0};
        uint32_t current_segment_index_{0};

        std::unique_ptr<wal_page_writer_t> writer_;

        /// Temporary encode buffer, reused across writes to avoid re-allocation.
        buffer_t encode_buf_;
    };

    using wal_worker_ptr = std::unique_ptr<wal_worker_t, actor_zeta::pmr::deleter_t>;

} // namespace services::wal
