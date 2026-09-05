#include "connection_environment.hpp"
#include <cassert>
#include <memory>

#include <components/configuration/configuration.hpp>
#include <integration/cpp/otterbrix.hpp>
#include <system_error>

namespace otterbrix {

    namespace {

        // What an otterbrix database leaves behind in its base directory. Both names
        // come from components/configuration/configuration.hpp: config_log puts the
        // logger under `<base>/log`, and config_wal / config_disk BOTH put their
        // trees under `<base>/wal` (the disk tree deliberately shares the WAL's
        // directory — see the comment on config_disk). Seeing either one is what
        // separates "a database of ours" from "somebody's folder".
        //
        // `log` counts on purpose: base_otterbrix_t opens the logger before anything
        // else, so a run that died during bootstrap leaves `log` alone. Refusing to
        // reopen that would strand the very directory the engine itself created.
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

        // There is no engine resource to borrow here: the engine's arena is a MEMBER of the
        // space (base_otterbrix_t::resource, integration/cpp/base_spaces.hpp), and every
        // refusal below returns before make_otterbrix builds one — borrowing it would mean
        // constructing the engine inside the very directory the refusal exists to leave
        // alone. This used to end in std::pmr::new_delete_resource() with a note saying the
        // only real exit was a resource ARGUMENT owned one level further up again.
        //
        // It is owned one level further up now: the module owns an arena (main.cpp's
        // PYBIND11_MODULE body), `connect` carries it in, and the refusal message is built
        // on it. Nothing on the success path touches it.
        core::error_t path_error(std::pmr::memory_resource* resource,
                                 core::error_code_t code,
                                 const std::string& what) {
            return core::error_t{code, std::pmr::string{what, resource}};
        }

    } // namespace

    std::shared_ptr<python_import_cache_t> connection_environment_t::import_cache_ = nullptr;

    core::result_wrapper_t<boost::intrusive_ptr<otterbrix_t>>
    connection_environment_t::make_space(std::pmr::memory_resource* resource, const std::filesystem::path& path) {
        // Rule 6: without an arena the refusals below have nowhere to put their message, and
        // a refusal that cannot speak is worse than the one it replaces.
        assert(resource != nullptr && "make_space needs an arena for its refusal messages");

        // Opening a database OPENS it. Never
        //     std::filesystem::remove_all(path);
        //     std::filesystem::create_directory(path);
        // here: that makes `otterbrix.connect("/my/db")` destroy /my/db before a single
        // statement runs — the user loses the database by connecting to it. Nothing asks
        // for that: every Python test in integration/python/tests that wants a clean
        // slate calls shutil.rmtree itself before opening. Wiping stays the caller's
        // explicit act; it is not a side effect of a constructor.
        //
        // `status` reports "absent" through the returned type, not through `ec`
        // (libc++ still fills `ec` with ENOENT in that case), so the not_found
        // branch is taken FIRST and only a genuine stat failure is an io error.
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
            // Rule 6: a directory holding somebody else's files is a loud refusal,
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
