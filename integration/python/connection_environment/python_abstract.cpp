#include "python_abstract.hpp"

#include "connection_environment.hpp"

namespace otterbrix::abc {
    // NOLINTNEXTLINE(readability-identifier-naming)
    bool is_list_like(py::handle obj) {
        if (py::isinstance<py::str>(obj) || py::isinstance<py::bytes>(obj)) {
            return false;
        }
        if (is_dict_like(obj)) {
            return false;
        }
        auto &import_cache = otterbrix::ConnectionEnvironment::ImportCache();
        auto iterable = import_cache.collections.abc.Iterable();
        return py::isinstance(obj, iterable);
    }
    
    // NOLINTNEXTLINE(readability-identifier-naming)
    bool is_dict_like(py::handle obj) {
        auto& import_cache = otterbrix::ConnectionEnvironment::ImportCache();
        auto mapping = import_cache.collections.abc.Mapping();
        return py::isinstance(obj, mapping);
    }

} // namespace otterbrix::abc
