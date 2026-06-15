#include "importer.hpp"
#include "python_import_cache.hpp"
#include "python_import_cache_item.hpp"
#include "../connection_environment.hpp"
#include <stack>

namespace otterbrix {

py::handle python_importer_t::import(std::stack<std::reference_wrapper<python_import_cache_item_t>> &hierarchy, bool load) {
    auto &import_cache = connection_environment_t::import_cache();
    py::handle source(nullptr);
    while (!hierarchy.empty()) {
        // From top to bottom, import them
        auto &item = hierarchy.top();
        hierarchy.pop();
        source = item.get().load(import_cache, source, load);
        if (!source) {
            // If load is false, or the module load fails and is not required, we return early
            break;
        }
    }
    return source;
}

} // namespace otterbrix

