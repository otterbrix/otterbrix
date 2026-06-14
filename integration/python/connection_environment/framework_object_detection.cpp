#include "framework_object_detection.hpp"
#include "python_abstract.hpp"
#include "connection_environment.hpp"
#include "module_cheker.hpp"


#include <cassert>
#include <string>

namespace otterbrix {

    bool is_valid_numpy_dimensions(const py::handle &object, int &dim) {
        // check the dimensions of numpy arrays
        // should only be called by IsAcceptedNumpyObject
        auto &import_cache = connection_environment_t::import_cache();
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


    numpy_object_type_t framework_object_detection_t::get_numpy_object_type(const py::object &object) {
        if (!module_is_loaded<numpy_cache_item_t>()) {
            return numpy_object_type_t::INVALID;
        }
        auto &import_cache = connection_environment_t::import_cache();
        if (py::isinstance(object, import_cache.numpy.ndarray())) {
            auto len = py::len((py::cast<py::array>(object)).attr("shape"));
            switch (len) {
            case 1:
                return numpy_object_type_t::NDARRAY1D;
            case 2:
                return numpy_object_type_t::NDARRAY2D;
            default:
                return numpy_object_type_t::INVALID;
            }
        } else if (abc::is_dict_like(object)) {
            int dim = -1;
            for (auto item : py::cast<py::dict>(object)) {
                if (!is_valid_numpy_dimensions(item.second, dim)) {
                    return numpy_object_type_t::INVALID;
                }
            }
            return numpy_object_type_t::DICT;
        } else if (abc::is_list_like(object)) {
            int dim = -1;
            for (auto item : py::cast<py::list>(object)) {
                if (!is_valid_numpy_dimensions(item, dim)) {
                    return numpy_object_type_t::INVALID;
                }
            }
            return numpy_object_type_t::LIST;
        }
        return numpy_object_type_t::INVALID;
    }

    bool framework_object_detection_t::is_pandas_dataframe(const py::object &object) {
        if (!module_is_loaded<pandas_cache_item_t>()) {
            return false;
        }
        auto &import_cache_py = connection_environment_t::import_cache();
        return py::isinstance(object, import_cache_py.pandas.data_frame());
    }

    bool framework_object_detection_t::is_polars_dataframe(const py::object &object) {
        if (!module_is_loaded<polars_cache_item_t>()) {
            return false;
        }
        auto &import_cache_py = connection_environment_t::import_cache();
        return py::isinstance(object, import_cache_py.polars.data_frame());
    }

} // namespace otterbrix
