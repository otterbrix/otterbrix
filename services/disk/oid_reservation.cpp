#include "oid_reservation.hpp"

#include <core/file/local_file_system.hpp>

#include <absl/crc/crc32c.h>

#include <string>
#include <system_error>

namespace services::disk {

    namespace {

        core::error_t file_error(std::pmr::memory_resource* resource,
                                 core::error_code_t code,
                                 const char* what,
                                 const std::filesystem::path& path,
                                 const char* reason) {
            std::pmr::string message{what, resource};
            message += " ";
            message += path.string();
            message += ": ";
            message += reason;
            return core::error_t{code, std::move(message)};
        }

    } // namespace

    core::error_t replace_file_durably(std::pmr::memory_resource* resource,
                                       const std::filesystem::path& path,
                                       void* data,
                                       std::size_t size,
                                       const char* what) {
        core::filesystem::local_file_system_t fs;
        auto tmp_path = path;
        tmp_path += ".tmp";
        auto refuse = [&](const char* reason) {
            std::error_code remove_ec;
            std::filesystem::remove(tmp_path, remove_ec);
            return file_error(resource, core::error_code_t::io_error, what, path, reason);
        };

        std::error_code stale_ec;
        std::filesystem::remove(tmp_path, stale_ec);
        auto tmp = core::filesystem::open_file(fs,
                                               tmp_path,
                                               core::filesystem::file_flags::WRITE |
                                                   core::filesystem::file_flags::FILE_CREATE_NEW);
        if (tmp == nullptr) {
            return refuse("the staging file could not be opened");
        }
        const auto written = tmp->write(data, size);
        if (!written.complete || written.bytes_written != size) {
            tmp.reset();
            return refuse("the staging write was short");
        }
        if (!tmp->sync()) {
            tmp.reset();
            return refuse("the staging file could not be fsynced");
        }
        tmp.reset();
        if (!core::filesystem::move_files(fs, tmp_path, path)) {
            return refuse("the rename over the live file was refused");
        }
        auto dir = core::filesystem::open_file(fs, path.parent_path(), core::filesystem::file_flags::READ);
        if (dir == nullptr || !core::filesystem::file_sync(fs, *dir)) {
            return file_error(resource,
                              core::error_code_t::io_error,
                              what,
                              path,
                              "was renamed but its directory could not be fsynced");
        }
        return core::error_t::no_error();
    }

    std::uint32_t oid_sidecar_crc(const void* data, std::size_t size) noexcept {
        return static_cast<std::uint32_t>(absl::ComputeCrc32c({static_cast<const char*>(data), size}));
    }

    std::filesystem::path oid_reservation_path(const std::filesystem::path& db_path) {
        return db_path / "oid_reservation";
    }

    core::result_wrapper_t<components::catalog::oid_t> read_oid_reservation(std::pmr::memory_resource* resource,
                                                                            const std::filesystem::path& db_path) {
        core::filesystem::local_file_system_t fs;
        const auto path = oid_reservation_path(db_path);
        if (!core::filesystem::file_exists(fs, path)) {
            return components::catalog::INVALID_OID;
        }
        auto file = core::filesystem::open_file(fs, path, core::filesystem::file_flags::READ);
        if (file == nullptr) {
            return file_error(resource,
                              core::error_code_t::io_error,
                              "oid reservation",
                              path,
                              "exists but could not be opened");
        }
        // bound, crc32c(bound)
        std::uint32_t record[2] = {0, 0};
        if (file->file_size() != sizeof(record) ||
            core::filesystem::read(fs, *file, record, sizeof(record)) != static_cast<int64_t>(sizeof(record)) ||
            record[1] != oid_sidecar_crc(&record[0], sizeof(record[0]))) {
            return file_error(resource,
                              core::error_code_t::data_corruption,
                              "oid reservation",
                              path,
                              "does not hold an oid, so the oids handed out before the restart are unknown");
        }
        return components::catalog::oid_t{record[0]};
    }

    core::error_t persist_oid_reservation(std::pmr::memory_resource* resource,
                                          const std::filesystem::path& db_path,
                                          components::catalog::oid_t bound) {
        std::uint32_t record[2] = {static_cast<unsigned>(bound), 0};
        record[1] = oid_sidecar_crc(&record[0], sizeof(record[0]));
        return replace_file_durably(resource, oid_reservation_path(db_path), record, sizeof(record), "oid reservation");
    }

} // namespace services::disk
