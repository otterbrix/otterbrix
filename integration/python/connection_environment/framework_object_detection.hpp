#pragma once

#include <pybind11/pybind_wrapper.hpp>

#include <cstdint>

namespace otterbrix {

    enum class numpy_object_type_t : uint8_t
    {
        //! To identify supported Numpy objects for scaning
        INVALID,   //! unsupported numpy object
        NDARRAY1D, //! numpy array with shape (n, )
        NDARRAY2D, //! numpy array with shape (n_rows, n_cols)
        LIST,      //! list of numpy arrays of shape (n,)
        DICT,      //! dict of numpy arrays of shape (n,)
    };

    class framework_object_detection_t {
    public:
        static numpy_object_type_t get_numpy_object_type(const py::object& object);
        static bool is_pandas_dataframe(const py::object& object);
        static bool is_polars_dataframe(const py::object& object);
    };

} // namespace otterbrix
