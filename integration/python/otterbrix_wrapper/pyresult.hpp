#pragma once

#include <pybind11/pybind_wrapper.hpp>
#include <pybind11/dataframe.hpp>
#include <native/python_objects.hpp>

#include <common/typedefs.hpp>

#include <components/cursor/cursor.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <vector>


namespace otterbrix {
    class py_connection_t;

    class py_result_t {
    public:
        py_result_t(py_connection_t* env, components::cursor::cursor_t_ptr result,
                const std::vector<components::table::column_definition_t>& columns);
        ~py_result_t();
        py_optional_t<py::tuple> fetchone();

        py::list fetchmany(idx_t size);

        py::list fetchall();
        
        pandas_data_frame_t fetch_df();

        void close();

        bool is_closed() const;

    private:
        py_connection_t* env;

        components::cursor::cursor_t_ptr result;
        std::vector<components::table::column_definition_t> columns;

    };

} // namespace otterbrix
