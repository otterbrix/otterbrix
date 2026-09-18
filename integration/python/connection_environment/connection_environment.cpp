#include "connection_environment.hpp"
#include <cassert>
#include <memory>

#include <components/configuration/configuration.hpp>
#include <integration/cpp/otterbrix.hpp>
#include <system_error>

namespace otterbrix {

    namespace {

        // `wal` and `log` are what config_wal/config_disk/config_log put under the base
        // directory (components/configuration/configuration.hpp; disk shares wal's dir).
        // `log` counts too: base_otterbrix_t opens it before anything else, so a run that
        // died during bootstrap must not be refused a reopen over its own logger directory.
        bool looks_like_otterbrix_database(const std::filesystem::path& path) {
            std::error_code ec;
            if (std::filesystem::exists(path / "wal", ec)) {
                return true;
            }
            return std::filesystem::exists(path / "log", ec);
        }

        bool directory_is_empty(const std::filesystem::path& path) {
            std::error_code ec;
            const bool empty = std::filesystem::is_empty(path, ec);
            return !ec && empty;
        }

        // No engine resource to borrow here: the engine's arena is a member of the space
        // (base_otterbrix_t::resource, integration/cpp/base_spaces.hpp) and every refusal
        // below returns before make_otterbrix builds one. `resource` is owned by the module
        // (main.cpp's PYBIND11_MODULE body) and passed in by `connect`; unused on success.
        core::error_t
        path_error(std::pmr::memory_resource* resource, core::error_code_t code, const std::string& what) {
            return core::error_t{code, std::pmr::string{what, resource}};
        }

    } // namespace

    std::shared_ptr<python_import_cache_t> connection_environment_t::import_cache_ = nullptr;

    core::result_wrapper_t<boost::intrusive_ptr<otterbrix_t>>
    connection_environment_t::make_space(std::pmr::memory_resource* resource, const std::filesystem::path& path) {
        // Without an arena the refusals below have nowhere to put their message.
        assert(resource != nullptr && "make_space needs an arena for its refusal messages");

        // Never remove_all()/recreate `path` here: that would destroy an existing database on
        // connect. Callers wanting a clean slate wipe it themselves before opening.
        //
        // `status` reports "absent" through the returned type, not `ec` (libc++ still fills
        // `ec` with ENOENT), so not_found is checked first and only a real stat failure is
        // treated as an io error.
        std::error_code ec;
        const auto status = std::filesystem::status(path, ec);
        if (status.type() == std::filesystem::file_type::not_found) {
            ec.clear();
            std::filesystem::create_directories(path, ec);
            if (ec) {
                return path_error(resource,
                                  core::error_code_t::io_error,
                                  "cannot create database directory '" + path.string() + "': " + ec.message());
            }
        } else if (ec) {
            return path_error(resource,
                              core::error_code_t::io_error,
                              "cannot inspect '" + path.string() + "': " + ec.message());
        } else if (!std::filesystem::is_directory(status)) {
            return path_error(resource,
                              core::error_code_t::invalid_parameter,
                              "'" + path.string() + "' exists and is not a directory");
        } else if (!directory_is_empty(path) && !looks_like_otterbrix_database(path)) {
            // A directory holding somebody else's files is a loud refusal,
            // not something to delete and replace with a fresh database.
            return path_error(resource,
                              core::error_code_t::invalid_parameter,
                              "'" + path.string() +
                                  "' is not empty and does not hold an otterbrix database; refusing to open it");
        }

        return make_otterbrix(configuration::config::create_config(path));
    }

    void connection_environment_t::cleanup() { import_cache_.reset(); }

    void connection_environment_t::throw_connection_exception() {
        throw std::runtime_error("Connection already closed!");
    }

    bool connection_environment_t::is_jupyter() { return false; }

    python_import_cache_t& connection_environment_t::import_cache() {
        if (!import_cache_) {
            import_cache_ = std::make_shared<python_import_cache_t>();
        }
        return *(import_cache_.get());
    }

} // namespace otterbrix
