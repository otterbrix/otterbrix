#include "oid_reservation.hpp"

#include <core/file/local_file_system.hpp>

#include <string>
#include <system_error>

namespace services::disk {

    namespace {

        std::filesystem::path staging_path(const std::filesystem::path& db_path) {
            auto path = oid_reservation_path(db_path);
            path += ".tmp";
            return path;
        }

        core::error_t reservation_error(std::pmr::memory_resource* resource,
                                        core::error_code_t code,
                                        const std::filesystem::path& db_path,
                                        const char* reason) {
            std::pmr::string what{"oid reservation ", resource};
            what += oid_reservation_path(db_path).string();
            what += ": ";
            what += reason;
            return core::error_t{code, std::move(what)};
        }

    } // namespace

    std::filesystem::path oid_reservation_path(const std::filesystem::path& db_path) {
        return db_path / "oid_reservation";
    }

    core::result_wrapper_t<components::catalog::oid_t>
    read_oid_reservation(std::pmr::memory_resource* resource, const std::filesystem::path& db_path) {
        core::filesystem::local_file_system_t fs;
        const auto path = oid_reservation_path(db_path);
        if (!core::filesystem::file_exists(fs, path)) {
            return components::catalog::INVALID_OID;
        }
        auto file = core::filesystem::open_file(fs, path, core::filesystem::file_flags::READ);
        if (file == nullptr) {
            return reservation_error(resource, core::error_code_t::io_error, db_path, "exists but could not be opened");
        }
        components::catalog::oid_t bound = components::catalog::INVALID_OID;
        if (core::filesystem::read(fs, *file, &bound, sizeof(bound)) != static_cast<int64_t>(sizeof(bound))) {
            return reservation_error(resource,
                                     core::error_code_t::data_corruption,
                                     db_path,
                                     "does not hold an oid, so the oids handed out before the restart are unknown");
        }
        return bound;
    }

    core::error_t persist_oid_reservation(std::pmr::memory_resource* resource,
                                          const std::filesystem::path& db_path,
                                          components::catalog::oid_t bound) {
        core::filesystem::local_file_system_t fs;
        const auto tmp_path = staging_path(db_path);
        auto refuse = [&](const char* reason) {
            std::error_code remove_ec;
            std::filesystem::remove(tmp_path, remove_ec);
            return reservation_error(resource, core::error_code_t::io_error, db_path, reason);
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
        const auto written = tmp->write(&bound, sizeof(bound));
        if (!written.complete || written.bytes_written != sizeof(bound)) {
            tmp.reset();
            return refuse("the staging write was short");
        }
        if (!tmp->sync()) {
            tmp.reset();
            return refuse("the staging file could not be fsynced");
        }
        tmp.reset();
        if (!core::filesystem::move_files(fs, tmp_path, oid_reservation_path(db_path))) {
            return refuse("the rename over the live reservation was refused");
        }
        auto dir = core::filesystem::open_file(fs, db_path, core::filesystem::file_flags::READ);
        if (dir == nullptr || !core::filesystem::file_sync(fs, *dir)) {
            return reservation_error(resource,
                                     core::error_code_t::io_error,
                                     db_path,
                                     "was renamed but its directory could not be fsynced");
        }
        return core::error_t::no_error();
    }

} // namespace services::disk
