#include "list_dir.hpp"

#include <string>
#include <system_error>

namespace core::filesystem {

    namespace {
        core::error_t listing_refused(std::pmr::memory_resource* resource, const std::string& what) {
            return core::error_t(core::error_code_t::io_error, std::pmr::string{what.data(), what.size(), resource});
        }
    } // namespace

    core::result_wrapper_t<std::pmr::vector<dir_entry_t>> list_dir(std::pmr::memory_resource* resource,
                                                                   const std::filesystem::path& directory) {
        std::pmr::vector<dir_entry_t> entries(resource);
        std::error_code ec;
        std::filesystem::directory_iterator it(directory, ec);
        for (const std::filesystem::directory_iterator end; !ec && it != end; it.increment(ec)) {
            std::error_code kind_ec;
            const auto kind = it->status(kind_ec).type();
            // A dangling symlink answers not_found with its ENOENT: it names nothing, which is no error.
            if (kind_ec && kind != std::filesystem::file_type::not_found) {
                return listing_refused(resource,
                                       "the entry " + it->path().string() +
                                           " could not be examined: " + kind_ec.message());
            }
            entries.push_back(dir_entry_t{it->path(), kind});
        }
        if (ec) {
            return listing_refused(resource,
                                   "the directory " + directory.string() + " could not be listed: " + ec.message());
        }
        return entries;
    }

} // namespace core::filesystem
