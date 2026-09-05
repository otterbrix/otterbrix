#pragma once

#include <native/python_objects.hpp>
#include <pybind11/dataframe.hpp>
#include <pybind11/pybind_wrapper.hpp>

#include <common/typedefs.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <components/cursor/cursor.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <cstddef>
#include <vector>

namespace otterbrix {
    class py_connection_t;
    class otterbrix_t;

    // The rows a statement produced, as Python sees them. This type existed and
    // was compiled from the start but was never registered with the module, so
    // nothing outside C++ could hold one; `OtterBrixPyConnection.execute` is what
    // hands it out (see py_result_t::initialize).
    class py_result_t {
    public:
        py_result_t(py_connection_t* env,
                    components::cursor::cursor_t_ptr result,
                    const std::vector<components::table::column_definition_t>& columns);
        ~py_result_t();

        static void initialize(py::handle& m);

        // Column descriptors for a cursor the engine produced from raw SQL. Names
        // and types both live in cursor->type_data() -- the same source
        // wrapper_cursor's `description` reads. A statement with no result set
        // (INSERT, UPDATE, every DDL) has none and gets an empty schema.
        static std::vector<components::table::column_definition_t>
        columns_of(const components::cursor::cursor_t_ptr& cursor);

        py_optional_t<py::tuple> fetchone();

        py::list fetchmany(idx_t size);

        py::list fetchall();

        pandas_data_frame_t fetch_df();

        // Rows the statement produced (SELECT) or wrote (INSERT/UPDATE/DELETE).
        std::size_t size() const;

        void close();

        bool is_closed() const;

    private:
        // No back-pointer to the connection: the result needs nothing from it once
        // it holds the batch and the space below, and keeping one only created a
        // raw pointer that outlived what it pointed at.

        // The result batch is pmr-allocated from the space's memory resource, so
        // the result has to keep the space alive on its own: a caller may close
        // the connection -- dropping what can be the last reference to the space
        // and with it the arena the rows live in -- and read the rows afterwards.
        // Declared before `result` so it outlives it during destruction.
        boost::intrusive_ptr<otterbrix_t> space;

        components::cursor::cursor_t_ptr result;
        std::vector<components::table::column_definition_t> columns;
    };

} // namespace otterbrix
