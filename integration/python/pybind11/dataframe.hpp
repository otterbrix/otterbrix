#pragma once

#include <pybind11/dataframe.hpp>
#include <pybind11/pybind_wrapper.hpp>

namespace otterbrix {

    class pandas_data_frame_t : public py::object {
    public:
        pandas_data_frame_t(const py::object& o)
            : py::object(o, borrowed_t{}) {}
        using py::object::object;

    public:
        static bool check_(const py::handle& object); // NOLINT
        static bool is_py_arrow_backed(const py::handle& df);
        static py::object to_arrow_table(const py::object& df);
    };

} // namespace otterbrix
