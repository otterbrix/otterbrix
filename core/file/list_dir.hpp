#pragma once

#include <core/result_wrapper.hpp>

#include <filesystem>
#include <memory_resource>
#include <vector>

namespace core::filesystem {

    struct dir_entry_t {
        std::filesystem::path path;
        // The type of what the entry names, following a symlink; a dangling symlink is not_found.
        std::filesystem::file_type kind;
    };

    // Every entry of `directory`. A directory that cannot be listed and an entry whose kind cannot be
    // read are both io_error: a caller would otherwise skip what it cannot see.
    [[nodiscard]] core::result_wrapper_t<std::pmr::vector<dir_entry_t>>
    list_dir(std::pmr::memory_resource* resource, const std::filesystem::path& directory);

} // namespace core::filesystem
