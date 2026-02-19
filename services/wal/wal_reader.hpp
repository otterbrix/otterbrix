#pragma once

#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <core/file/file_handle.hpp>
#include <core/file/local_file_system.hpp>
#include <services/wal/record.hpp>

#include <memory>
#include <vector>

namespace services::wal {

    class wal_reader_t {
    public:
        wal_reader_t(const configuration::config_wal& config,
                     std::pmr::memory_resource* resource,
                     log_t& log);

        std::vector<record_t> read_committed_records(id_t after_id);

    private:
        size_tt read_wal_size(core::filesystem::file_handle_t* file, std::size_t start_index) const;
        std::pmr::string read_wal_data(core::filesystem::file_handle_t* file, std::size_t start, std::size_t finish) const;
        record_t read_wal_record(core::filesystem::file_handle_t* file, std::size_t start_index) const;
        std::size_t next_wal_index(std::size_t start_index, size_tt size) const;

        std::pmr::memory_resource* resource_;
        mutable log_t log_;
        core::filesystem::local_file_system_t fs_;
        std::vector<std::unique_ptr<core::filesystem::file_handle_t>> wal_files_;
    };

} // namespace services::wal
