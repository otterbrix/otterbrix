#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <core/result_wrapper.hpp>

#include <filesystem>
#include <memory_resource>

namespace services::disk {

    // How many oids one reservation hands out.
    inline constexpr std::uint32_t OID_RESERVATION_BLOCK = 8192;

    // Writes a staging file, fsyncs it, renames it over path and fsyncs the directory; what names the file in errors.
    [[nodiscard]] core::error_t replace_file_durably(std::pmr::memory_resource* resource,
                                                     const std::filesystem::path& path,
                                                     void* data,
                                                     std::size_t size,
                                                     const char* what);

    // The crc32c trailer both oid sidecars end with: a torn or foreign file is refused, never read as oids.
    [[nodiscard]] std::uint32_t oid_sidecar_crc(const void* data, std::size_t size) noexcept;

    std::filesystem::path oid_reservation_path(const std::filesystem::path& db_path);

    [[nodiscard]] core::result_wrapper_t<components::catalog::oid_t>
    read_oid_reservation(std::pmr::memory_resource* resource, const std::filesystem::path& db_path);

    [[nodiscard]] core::error_t persist_oid_reservation(std::pmr::memory_resource* resource,
                                                        const std::filesystem::path& db_path,
                                                        components::catalog::oid_t bound);

} // namespace services::disk
