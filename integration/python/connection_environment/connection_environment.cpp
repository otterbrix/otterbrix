#include "connection_environment.hpp"
#include <memory>

#include <components/configuration/configuration.hpp>
#include <integration/cpp/otterbrix.hpp>

namespace otterbrix {

    std::shared_ptr<PythonImportCache> ConnectionEnvironment::import_cache = nullptr;

    boost::intrusive_ptr<otterbrix_t> ConnectionEnvironment::MakeSpace(const std::filesystem::path& path) {
        std::filesystem::remove_all(path);
        std::filesystem::create_directory(path);
        return make_otterbrix(configuration::config::create_config(path));
    }

    void ConnectionEnvironment::Cleanup() { import_cache.reset(); }

    void ConnectionEnvironment::ThrowConnectionException() {
        throw std::runtime_error("Connection already closed!");
    }

    bool ConnectionEnvironment::IsJupyter() { return false; }

    PythonImportCache& ConnectionEnvironment::ImportCache() {
        if (!import_cache) {
            import_cache = std::make_shared<PythonImportCache>();
        }
        return *(import_cache.get());
    }

} // namespace otterbrix
