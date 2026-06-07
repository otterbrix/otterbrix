#include "framework_object_detection.hpp"
#include "python_abstract.hpp"
#include "connection_environment.hpp"

#include <core/types/string.hpp>

#include <cassert>

namespace otterbrix {

    bool IsValidNumpyDimensions(const py::handle &object, int &dim) {
        // check the dimensions of numpy arrays
        // should only be called by IsAcceptedNumpyObject
        auto &import_cache = ConnectionEnvironment::ImportCache();
        if (!py::isinstance(object, import_cache.numpy.ndarray())) {
            return false;
        }
        auto shape = (py::cast<py::array>(object)).attr("shape");
        if (py::len(shape) != 1) {
            return false;
        }
        int cur_dim = (shape.attr("__getitem__")(0)).cast<int>();
        dim = dim == -1 ? cur_dim : dim;
        return dim == cur_dim;
    }


    NumpyObjectType FrameworkObjectDetection::GetNumpyObjectType(const py::object &object) {
        if (!ModuleIsLoaded<NumpyCacheItem>()) {
            return NumpyObjectType::INVALID;
        }
        auto &import_cache = ConnectionEnvironment::ImportCache();
        if (py::isinstance(object, import_cache.numpy.ndarray())) {
            auto len = py::len((py::cast<py::array>(object)).attr("shape"));
            switch (len) {
            case 1:
                return NumpyObjectType::NDARRAY1D;
            case 2:
                return NumpyObjectType::NDARRAY2D;
            default:
                return NumpyObjectType::INVALID;
            }
        } else if (abc::is_dict_like(object)) {
            int dim = -1;
            for (auto item : py::cast<py::dict>(object)) {
                if (!IsValidNumpyDimensions(item.second, dim)) {
                    return NumpyObjectType::INVALID;
                }
            }
            return NumpyObjectType::DICT;
        } else if (abc::is_list_like(object)) {
            int dim = -1;
            for (auto item : py::cast<py::list>(object)) {
                if (!IsValidNumpyDimensions(item, dim)) {
                    return NumpyObjectType::INVALID;
                }
            }
            return NumpyObjectType::LIST;
        }
        return NumpyObjectType::INVALID;
    }

    bool FrameworkObjectDetection::IsPandasDataframe(const py::object &object) {
        if (!ModuleIsLoaded<PandasCacheItem>()) {
            return false;
        }
        auto &import_cache_py = ConnectionEnvironment::ImportCache();
        return py::isinstance(object, import_cache_py.pandas.DataFrame());
    }

    bool FrameworkObjectDetection::IsPolarsDataframe(const py::object &object) {
        if (!ModuleIsLoaded<PolarsCacheItem>()) {
            return false;
        }
        auto &import_cache_py = ConnectionEnvironment::ImportCache();
        return py::isinstance(object, import_cache_py.polars.DataFrame());
    }

} // namespace otterbrix
