#include "pyresult.hpp"

#include <pyconnection/pyconnection.hpp>
#include <util/convert_value.hpp>
#include <vector>

using namespace components;

namespace otterbrix {
    std::vector<components::table::column_definition_t>
    py_result_t::columns_of(const components::cursor::cursor_t_ptr& cursor) {
        std::vector<components::table::column_definition_t> defs;
        if (!cursor) {
            return defs;
        }
        const auto& types = cursor->type_data();
        defs.reserve(types.size());
        for (const auto& type : types) {
            defs.emplace_back(type.alias(), type);
        }
        return defs;
    }

    py_result_t::py_result_t(py_connection_t* env,
                             components::cursor::cursor_t_ptr result_p,
                             const std::vector<components::table::column_definition_t>& defs)
        : space(env ? env->space_ptr() : boost::intrusive_ptr<otterbrix_t>{})
        , result(std::move(result_p)) {
        if (!result) {
            throw std::runtime_error("PyResult created without a result object");
        }
        columns.reserve(defs.size());

        for (const auto& col : defs) {
            columns.emplace_back(col.name(), col.type());
        }
    }

    py_result_t::~py_result_t() {
        try {
            assert(py::gil_check());
            py::gil_scoped_release gil;
            result.reset();
        } catch (...) { // NOLINT
        }
    }

    py_optional_t<py::tuple> py_result_t::fetchone() {
        if (!result) {
            throw std::runtime_error("result closed");
        }
        // A statement with no result set -- INSERT, UPDATE, every DDL -- carries a
        // row COUNT in size() and no columns at all. Handing back that many empty
        // tuples would be noise; the count is what len() is for.
        if (columns.empty()) {
            return py::none();
        }
        bool has_data = false;
        {
            py::gil_scoped_release release;
            if (result->size() > 0 && result->has_next()) {
                result->advance();
                has_data = true;
            }
        }
        if (!has_data) {
            return py::none();
        }
        py::tuple res(columns.size());
        for (idx_t col_idx = 0; col_idx < columns.size(); col_idx++) {
            auto val = result->value(col_idx);
            const auto& type = columns[col_idx].type();
            res[col_idx] = util::logical_value_to_python(val, type);
        }
        return res;
    }

    py::list py_result_t::fetchmany(idx_t size) {
        py::list res;
        for (idx_t i = 0; i < size; i++) {
            auto fres = fetchone();
            if (fres.is_none()) {
                break;
            }
            res.append(fres);
        }
        return res;
    }

    py::list py_result_t::fetchall() {
        py::list res;
        while (true) {
            auto fres = fetchone();
            if (fres.is_none()) {
                break;
            }
            res.append(fres);
        }
        return res;
    }

    pandas_data_frame_t py_result_t::fetch_df() {
        if (!result) {
            throw std::runtime_error("result closed");
        }
        // Same reason as fetchone(): a statement with no result set has a row
        // count and no columns, and there is no frame to build out of that.
        if (columns.empty()) {
            return py::none();
        }
        if (result->size() == 0) {
            return py::none();
        }

        if (!result->has_next()) {
            return py::none();
        }

        py::list df_param;

        while (result->has_next()) {
            result->advance();
            auto row_idx = result->current_index();
            py::dict row = util::cursor_row_to_python_dict(result, static_cast<uint64_t>(row_idx), columns);
            df_param.append(row);
        }
        pandas_data_frame_t df =
            py::cast<pandas_data_frame_t>(py::module::import("pandas").attr("DataFrame")(df_param));
        return df;
    }

    std::size_t py_result_t::size() const { return result ? result->size() : 0u; }

    void py_result_t::close() { result = nullptr; }

    bool py_result_t::is_closed() const { return result == nullptr; }

    void py_result_t::initialize(py::handle& m) {
        py::class_<py_result_t>(m, "OtterBrixPyResult", py::module_local())
            .def("fetchone",
                 &py_result_t::fetchone,
                 "Fetch the next row as a tuple, or None once the result is exhausted")
            .def("fetchmany",
                 &py_result_t::fetchmany,
                 "Fetch the next `size` rows as a list of tuples",
                 py::arg("size") = 1)
            .def("fetchall", &py_result_t::fetchall, "Fetch every remaining row as a list of tuples")
            .def("df", &py_result_t::fetch_df, "Fetch every remaining row as a pandas DataFrame")
            .def("fetchdf", &py_result_t::fetch_df, "Fetch every remaining row as a pandas DataFrame")
            .def("to_df", &py_result_t::fetch_df, "Fetch every remaining row as a pandas DataFrame")
            .def("close", &py_result_t::close, "Release the result batch")
            .def("is_closed", &py_result_t::is_closed, "Whether the result batch has been released")
            .def("__len__",
                 &py_result_t::size,
                 "Rows the statement produced (SELECT) or wrote (INSERT/UPDATE/DELETE)");
    }

} // namespace otterbrix
