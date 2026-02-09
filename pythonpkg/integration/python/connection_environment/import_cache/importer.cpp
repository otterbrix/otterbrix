#include "importer.hpp"
#include "python_import_cache.hpp"
#include "python_import_cache_item.hpp"
#include "../connection_environment.hpp"

namespace otterbrix {

py::handle PythonImporter::Import(stack<std::reference_wrapper<PythonImportCacheItem>> &hierarchy, bool load) {
    auto &import_cache = ConnectionEnvironment::ImportCache();
    py::handle source(nullptr);
    while (!hierarchy.empty()) {
        // From top to bottom, import them
        auto &item = hierarchy.top();
        hierarchy.pop();
        source = item.get().Load(import_cache, source, load);
        if (!source) {
            // If load is false, or the module load fails and is not required, we return early
            break;
        }
    }
    return source;
}

} // namespace otterbrix

