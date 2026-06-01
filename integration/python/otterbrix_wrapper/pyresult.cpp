#include "pyresult.hpp"

#include <util/convert_value.hpp>

using namespace components;

namespace otterbrix {
    PyResult::PyResult(ConnectionEnvironment* env, components::cursor::cursor_t_ptr result_p,
            const vector<components::table::column_definition_t>& defs) 
        : env(env), result(std::move(result_p)) {
        if (!result) {
            throw std::runtime_error("PyResult created without a result object");
        }
        columns.reserve(defs.size());

        for (const auto& col : defs) {
            columns.emplace_back(col.name(), col.type());    
        }
    }

    PyResult::~PyResult() {
        try {
            assert(py::gil_check());
            py::gil_scoped_release gil;
            result.reset();
        } catch (...) { // NOLINT
        } 
    }

    Optional<py::tuple> PyResult::Fetchone() {
        py::gil_scoped_release release;
        if (!result) {
            throw std::runtime_error("result closed");
        }
        if (result->size() == 0) {
            return py::none();
        }
            
        if (!result->has_next()) {
            return py::none();
        }
        py::tuple res(columns.size());

        auto doc_ptr = result->next();

        for (idx_t col_idx = 0; col_idx < columns.size(); col_idx++) {
            const auto& name = columns[col_idx].name();
            const auto& type = columns[col_idx].type();
            auto doc_val = doc_ptr->get_value(name);
            if (doc_val.physical_type() == components::types::physical_type::NA) {
                res[col_idx] = py::none();
            }
            auto log_val = util::ToLogicalValue(doc_ptr, columns[col_idx]);
            res[col_idx] = PythonObject::FromValue(log_val, type);

        }
        return res;
    }

    py::list PyResult::Fetchmany(idx_t size) {
        py::list res;
        for (idx_t i = 0; i < size; i++) {
            auto fres = Fetchone();
            if (fres.is_none()) {
                break;
            }
            res.append(fres);
        }
        return res;

    }

    py::list PyResult::Fetchall() {
        py::list res;
        while (true) {
            auto fres = Fetchone();
            if (fres.is_none()) {
                break;
            }
            res.append(fres);
        }
        return res;
    }


    PandasDataFrame PyResult::FetchDF() {
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
            auto data = result->next();
            
            py::dict row = util::DocumentToPythonDict(data, columns);
            df_param.append(row);
        } 
        PandasDataFrame df = py::cast<PandasDataFrame>(
                py::module::import("pandas").attr("DataFrame")(df_param));
        return df;
    }

    void PyResult::Close() {
        result = nullptr;
    }

    bool PyResult::IsClosed() const {
       return result == nullptr; 
    }
    
    /*const vector<components::types::complex_logical_type>& PyResult::GetTypes() {
        return {};
    }

    const vector<string>& PyResult::GetNames() {
        return {};
    }*/

} // namespace otterbrix
