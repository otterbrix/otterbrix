#pragma once

#include "python_import_cache_modules.hpp"

#include <pybind11/pybind_wrapper.hpp>
#include <vector>

namespace otterbrix {

    struct python_import_cache_t {
    public:
        explicit python_import_cache_t();
        ~python_import_cache_t();

    public:
        pyarrow_cache_item_t pyarrow;
        pandas_cache_item_t pandas;
        datetime_cache_item_t datetime;
        decimal_cache_item_t decimal;
        ipython_cache_item_t IPython;
        ipywidgets_cache_item_t ipywidgets;
        numpy_cache_item_t numpy;
        pathlib_cache_item_t pathlib;
        polars_cache_item_t polars;
        pytz_cache_item_t pytz;
        types_cache_item_t types;
        typing_cache_item_t typing;
        uuid_cache_item_t uuid;
        collections_cache_item_t collections;

    public:
        py::handle add_cache(py::object item);

    private:
        std::vector<py::object> owned_objects;
    };

} // namespace otterbrix
