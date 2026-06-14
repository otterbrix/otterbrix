#include "connection_environment.hpp"
#include <memory>

#include <components/configuration/configuration.hpp>
#include <integration/cpp/otterbrix.hpp>

namespace otterbrix {

    std::shared_ptr<python_import_cache_t> connection_environment_t::import_cache_ = nullptr;

    boost::intrusive_ptr<otterbrix_t> connection_environment_t::make_space(const std::filesystem::path& path) {
        std::filesystem::remove_all(path);
        std::filesystem::create_directory(path);
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
