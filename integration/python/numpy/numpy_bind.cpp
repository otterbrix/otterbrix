#include "numpy_bind.hpp"
#include <iostream>
#include "array_wrapper.hpp"

#include <pandas/pandas_analyzer.hpp>
#include <pandas/column/pandas_numpy_column.hpp>
#include <pandas/pandas_bind.hpp>
#include "numpy_type.hpp"

#include <memory_resource>

namespace otterbrix {

using components::types::complex_logical_type;

core::error_t NumpyBind::Bind(std::pmr::memory_resource *resource, py::handle df, vector<PandasColumnBindData> &bind_columns,
                     vector<complex_logical_type> &return_types, vector<string> &names,
                     const configuration::config_pandas &cfg) {

	auto df_columns = py::list(df.attr("keys")());
	auto df_types = py::list();
	for (auto item : py::cast<py::dict>(df)) {
		if (string(py::str(item.second.attr("dtype").attr("char"))) == "U") {
			df_types.attr("append")(py::str("string"));
			continue;
		}
		df_types.attr("append")(py::str(item.second.attr("dtype")));
	}
	auto get_fun = df.attr("__getitem__");
	if (py::len(df_columns) == 0 || py::len(df_types) == 0 || py::len(df_columns) != py::len(df_types)) {
		return core::error_t{core::error_code_t::other_error,
		                     std::pmr::string{"Need a DataFrame with at least one column", resource}};
	}
	for (idx_t col_idx = 0; col_idx < py::len(df_columns); col_idx++) {
		complex_logical_type otterbrix_col_type;
		PandasColumnBindData bind_data;

		names.emplace_back(py::str(df_columns[col_idx]));
		auto numpy_type = ConvertNumpyType(resource, df_types[col_idx]);
		if (numpy_type.has_error()) {
			return numpy_type.error();
		}
		bind_data.numpy_type = numpy_type.value();

		auto column = get_fun(df_columns[col_idx]);

		if (bind_data.numpy_type.type == NumpyNullableType::FLOAT_16) {
			bind_data.pandas_col = make_unique<PandasNumpyColumn>(py::array(column.attr("astype")("float32")));
			bind_data.numpy_type.type = NumpyNullableType::FLOAT_32;
		} else {
			bind_data.pandas_col = make_unique<PandasNumpyColumn>(column);
		}
		auto col_type = NumpyToLogicalType(resource, bind_data.numpy_type);
		if (col_type.has_error()) {
			return col_type.error();
		}
		otterbrix_col_type = col_type.value();

		if (bind_data.numpy_type.type == NumpyNullableType::OBJECT) {
			PandasAnalyzer analyzer(resource, cfg);
			auto analyzed = analyzer.Analyze(get_fun(df_columns[col_idx]));
			if (analyzed.has_error()) {
				return analyzed.error();
			}
			if (analyzed.value()) {
				otterbrix_col_type = analyzer.AnalyzedType();
			}
		}

		return_types.push_back(otterbrix_col_type);
		bind_columns.push_back(std::move(bind_data));
	}
	return core::error_t::no_error();
}

} // namespace otterbrix
