#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <core/result_wrapper.hpp>

#include <filesystem>
#include <memory_resource>

namespace services::disk {

    inline constexpr components::catalog::oid_t OID_RESERVATION_BLOCK = 8192;

    std::filesystem::path oid_reservation_path(const std::filesystem::path& db_path);

    [[nodiscard]] core::result_wrapper_t<components::catalog::oid_t>
    read_oid_reservation(std::pmr::memory_resource* resource, const std::filesystem::path& db_path);

    [[nodiscard]] core::error_t persist_oid_reservation(std::pmr::memory_resource* resource,
                                                        const std::filesystem::path& db_path,
                                                        components::catalog::oid_t bound);

} // namespace services::disk
