#include "numpy_bind.hpp"
#include <memory>
#include <iostream>
#include "array_wrapper.hpp"

#include <pandas/pandas_analyzer.hpp>
#include <pandas/column/pandas_numpy_column.hpp>
#include <pandas/pandas_bind.hpp>
#include "numpy_type.hpp"

#include <memory_resource>
#include <string>
#include <vector>

namespace otterbrix {

using components::types::complex_logical_type;

core::error_t numpy_bind_t::bind(std::pmr::memory_resource *resource, py::handle df, std::vector<pandas_column_bind_data_t> &bind_columns,
                     std::vector<complex_logical_type> &return_types, std::vector<std::string> &names,
                     const configuration::config_pandas &cfg) {

	auto df_columns = py::list(df.attr("keys")());
	auto df_types = py::list();
	for (auto item : py::cast<py::dict>(df)) {
		if (std::string(py::str(item.second.attr("dtype").attr("char"))) == "U") {
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
		pandas_column_bind_data_t bind_data;

		names.emplace_back(py::str(df_columns[col_idx]));
		auto numpy_type = convert_numpy_type(resource, df_types[col_idx]);
		if (numpy_type.has_error()) {
			return numpy_type.error();
		}
		bind_data.numpy_type = numpy_type.value();

		auto column = get_fun(df_columns[col_idx]);

		if (bind_data.numpy_type.type == numpy_nullable_type_t::FLOAT_16) {
			bind_data.pandas_col = std::make_unique<pandas_numpy_column_t>(py::array(column.attr("astype")("float32")));
			bind_data.numpy_type.type = numpy_nullable_type_t::FLOAT_32;
		} else {
			bind_data.pandas_col = std::make_unique<pandas_numpy_column_t>(column);
		}
		auto col_type = numpy_to_logical_type(resource, bind_data.numpy_type);
		if (col_type.has_error()) {
			return col_type.error();
		}
		otterbrix_col_type = col_type.value();

		if (bind_data.numpy_type.type == numpy_nullable_type_t::OBJECT) {
			pandas_analyzer_t analyzer(resource, cfg);
			auto analyzed = analyzer.analyze(get_fun(df_columns[col_idx]));
			if (analyzed.has_error()) {
				return analyzed.error();
			}
			if (analyzed.value()) {
				otterbrix_col_type = analyzer.analyzed_type();
			}
		}

		return_types.push_back(otterbrix_col_type);
		bind_columns.push_back(std::move(bind_data));
	}
	return core::error_t::no_error();
}

} // namespace otterbrix
