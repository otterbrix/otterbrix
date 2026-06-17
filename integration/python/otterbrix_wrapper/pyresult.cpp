#include "pyresult.hpp"

#include <util/convert_value.hpp>
#include <vector>

using namespace components;

namespace otterbrix {
    py_result_t::py_result_t(py_connection_t* env,
                             components::cursor::cursor_t_ptr result_p,
                             const std::vector<components::table::column_definition_t>& defs)
        : env(env)
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

    void py_result_t::close() { result = nullptr; }

    bool py_result_t::is_closed() const { return result == nullptr; }

} // namespace otterbrix
