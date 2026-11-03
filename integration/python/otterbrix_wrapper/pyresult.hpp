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
#include <optional>
#include <vector>

namespace otterbrix {
    class py_connection_t;
    struct engine_arena_t;
    struct py_connection_state_t;

    // The rows a statement produced, as Python sees them (`OtterBrixPyConnection.execute` return type).
    class py_result_t {
    public:
        py_result_t(const py_connection_t& env,
                    components::cursor::cursor_t_ptr result,
                    const std::vector<components::table::column_definition_t>& columns);
        ~py_result_t();

        static void initialize(py::handle& m);

        // Names/types come from cursor->type_data(), the same source wrapper_cursor::description
        // reads. Empty for a statement with no result set (INSERT/UPDATE/DDL).
        static std::vector<components::table::column_definition_t>
        columns_of(const components::cursor::cursor_t_ptr& cursor);

        py_optional_t<py::tuple> fetchone();

        py::list fetchmany(idx_t size);

        py::list fetchall();

        pandas_data_frame_t fetch_df();

        // Rows of the result: what a SELECT or a RETURNING clause produced.
        std::size_t size() const;

        // DB-API rowcount: the rows an INSERT / UPDATE / DELETE wrote, else the rows of the result.
        int64_t rowcount() const;
        py::object created_oid() const;

        // None once the result is closed.
        std::optional<components::session::transaction_status_t> transaction_status() const;

        void close();

        bool is_closed() const;

    private:
        // The result batch is pmr-allocated from the engine's arena, which outlives the engine; declared before
        // `result` so it outlives it during destruction. The connection's state refuses reads once it is closed.
        boost::intrusive_ptr<engine_arena_t> arena_;
        boost::intrusive_ptr<py_connection_state_t> state_;

        components::cursor::cursor_t_ptr result;
        std::vector<components::table::column_definition_t> columns;
    };

} // namespace otterbrix
