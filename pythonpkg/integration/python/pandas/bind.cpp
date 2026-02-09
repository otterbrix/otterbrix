#include "pandas_bind.hpp"

#include "pandas_analyzer.hpp"
#include "column/pandas_numpy_column.hpp"

#include <numpy/numpy_type.hpp>

#include <components/types/types.hpp>
#include <components/vector/vector.hpp>
#include <core/types/vector.hpp>
#include <core/types/string.hpp>
#include <core/typedefs.hpp>

#include <cassert>
#include <string_view>
#include <stdexcept>

namespace otterbrix {
    using components::types::complex_logical_type;

namespace {

struct PandasBindColumn {
public:
	PandasBindColumn(py::handle name, py::handle type, py::object column)
	    : name(name), type(type), handle(std::move(column)) {
	}

public:
	py::handle name;
	py::handle type;
	py::object handle;
};

struct PandasDataFrameBind {
public:
	explicit PandasDataFrameBind(py::handle &df) {
        assert(hasattr(df, "columns"));
        assert(hasattr(df, "dtypes"));
        assert(hasattr(df, "__getitem__"));
		names = py::list(df.attr("columns"));
		types = py::list(df.attr("dtypes"));
		getter = df.attr("__getitem__");
	}
	PandasBindColumn operator[](idx_t index) const {
		assert(index < names.size());
		auto column = py::reinterpret_borrow<py::object>(getter(names[index]));
		auto type = types[index];
		auto name = names[index];
		return PandasBindColumn(name, type, column);
	}

public:
	py::list names;
	py::list types;

private:
	py::object getter;
};

}; // namespace

static complex_logical_type BindColumn(PandasBindColumn &column_p, PandasColumnBindData &bind_data) {
	complex_logical_type column_type;
	auto &column = column_p.handle;

	bind_data.numpy_type = ConvertNumpyType(column_p.type);
	bool column_has_mask = py::hasattr(column.attr("array"), "_mask");

	if (column_has_mask) {
		// masked object, fetch the internal data and mask array
		bind_data.mask = make_unique<RegisteredArray>(column.attr("array").attr("_mask"));
	}

	if (bind_data.numpy_type.type == NumpyNullableType::CATEGORY) {
        throw std::runtime_error("OtterBrix does\'t support Enum/Category");
	} else if (bind_data.numpy_type.type == NumpyNullableType::FLOAT_16) {
		auto pandas_array = column.attr("array");
		bind_data.pandas_col = make_unique<PandasNumpyColumn>(py::array(column.attr("to_numpy")("float32")));
		bind_data.numpy_type.type = NumpyNullableType::FLOAT_32;
		column_type = NumpyToLogicalType(bind_data.numpy_type);
	} else {
		auto pandas_array = column.attr("array");
		if (py::hasattr(pandas_array, "_data")) {
			// This means we can access the numpy array directly
			bind_data.pandas_col = make_unique<PandasNumpyColumn>(column.attr("array").attr("_data"));
		} else if (py::hasattr(pandas_array, "asi8")) {
			// This is a datetime object, has the option to get the array as int64_t's
			bind_data.pandas_col = make_unique<PandasNumpyColumn>(py::array(pandas_array.attr("asi8")));
		} else {
			// Otherwise we have to get it through 'to_numpy()'
			bind_data.pandas_col = make_unique<PandasNumpyColumn>(py::array(column.attr("to_numpy")()));
		}
		column_type = NumpyToLogicalType(bind_data.numpy_type);
	}
	// Analyze the inner data type of the 'object' column
	if (bind_data.numpy_type.type == NumpyNullableType::OBJECT) {
		PandasAnalyzer analyzer;
		if (analyzer.Analyze(column)) {
			column_type = analyzer.AnalyzedType();
		}
	}
	return column_type;
}

void Pandas::Bind(py::handle df_p, vector<PandasColumnBindData> &bind_columns,
                  vector<complex_logical_type> &return_types, vector<string> &names) {

	PandasDataFrameBind df(df_p);
	idx_t column_count = py::len(df.names);
	if (column_count == 0 || py::len(df.types) == 0 || column_count != py::len(df.types)) {
		throw std::runtime_error("Need a DataFrame with at least one column");
	}

	return_types.reserve(column_count);
	names.reserve(column_count);
	// loop over every column
	for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
		PandasColumnBindData bind_data;

		names.emplace_back(py::str(df.names[col_idx]));
		auto column = df[col_idx];
		auto column_type = BindColumn(column, bind_data);

		return_types.push_back(column_type);
		bind_columns.push_back(std::move(bind_data));
	}
}

} // namespace otterbrix
