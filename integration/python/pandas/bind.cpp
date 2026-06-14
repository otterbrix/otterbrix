#include "pandas_bind.hpp"
#include <memory>

#include "pandas_analyzer.hpp"
#include "column/pandas_numpy_column.hpp"

#include <numpy/numpy_type.hpp>

#include <components/types/types.hpp>
#include <components/vector/vector.hpp>
#include <common/typedefs.hpp>
#include <core/result_wrapper.hpp>

#include <cassert>
#include <string_view>
#include <memory_resource>
#include <string>
#include <vector>

namespace otterbrix {
    using components::types::complex_logical_type;

namespace {

struct pandas_bind_column_t {
public:
	pandas_bind_column_t(py::handle name, py::handle type, py::object column)
	    : name(name), type(type), handle(std::move(column)) {
	}

public:
	py::handle name;
	py::handle type;
	py::object handle;
};

struct pandas_data_frame_bind_t {
public:
	explicit pandas_data_frame_bind_t(py::handle &df) {
        assert(hasattr(df, "columns"));
        assert(hasattr(df, "dtypes"));
        assert(hasattr(df, "__getitem__"));
		names = py::list(df.attr("columns"));
		types = py::list(df.attr("dtypes"));
		getter = df.attr("__getitem__");
	}
	pandas_bind_column_t operator[](idx_t index) const {
		assert(index < names.size());
		auto column = py::reinterpret_borrow<py::object>(getter(names[index]));
		auto type = types[index];
		auto name = names[index];
		return pandas_bind_column_t(name, type, column);
	}

public:
	py::list names;
	py::list types;

private:
	py::object getter;
};

}; // namespace

static core::result_wrapper_t<complex_logical_type> bind_column(pandas_bind_column_t &column_p,
                                       pandas_column_bind_data_t &bind_data,
                                       std::pmr::memory_resource *resource,
                                       const configuration::config_pandas &cfg) {
	complex_logical_type column_type;
	auto &column = column_p.handle;

	auto numpy_type = convert_numpy_type(resource, column_p.type);
	if (numpy_type.has_error()) {
		return numpy_type.convert_error<complex_logical_type>();
	}
	bind_data.numpy_type = numpy_type.value();
	bool column_has_mask = py::hasattr(column.attr("array"), "_mask");

	if (column_has_mask) {
		// masked object, fetch the internal data and mask array
		bind_data.mask = std::make_unique<registered_array_t>(column.attr("array").attr("_mask"));
	}

	if (bind_data.numpy_type.type == numpy_nullable_type_t::CATEGORY) {
        return core::error_t(core::error_code_t::unimplemented_yet,
                             std::pmr::string("OtterBrix does't support Enum/Category", resource));
	} else if (bind_data.numpy_type.type == numpy_nullable_type_t::FLOAT_16) {
		auto pandas_array = column.attr("array");
		bind_data.pandas_col = std::make_unique<pandas_numpy_column_t>(py::array(column.attr("to_numpy")("float32")));
		bind_data.numpy_type.type = numpy_nullable_type_t::FLOAT_32;
		auto logical = numpy_to_logical_type(resource, bind_data.numpy_type);
		if (logical.has_error()) {
			return logical;
		}
		column_type = logical.value();
	} else {
		auto pandas_array = column.attr("array");
		if (py::hasattr(pandas_array, "_data")) {
			// This means we can access the numpy array directly
			bind_data.pandas_col = std::make_unique<pandas_numpy_column_t>(column.attr("array").attr("_data"));
		} else if (py::hasattr(pandas_array, "asi8")) {
			// This is a datetime object, has the option to get the array as int64_t's
			bind_data.pandas_col = std::make_unique<pandas_numpy_column_t>(py::array(pandas_array.attr("asi8")));
		} else {
			// Otherwise we have to get it through 'to_numpy()'
			bind_data.pandas_col = std::make_unique<pandas_numpy_column_t>(py::array(column.attr("to_numpy")()));
		}
		auto logical = numpy_to_logical_type(resource, bind_data.numpy_type);
		if (logical.has_error()) {
			return logical;
		}
		column_type = logical.value();
	}
	// analyze the inner data type of the 'object' column
	if (bind_data.numpy_type.type == numpy_nullable_type_t::OBJECT) {
		pandas_analyzer_t analyzer(resource, cfg);
		auto analyzed = analyzer.analyze(column);
		if (analyzed.has_error()) {
			return analyzed.convert_error<complex_logical_type>();
		}
		if (analyzed.value()) {
			column_type = analyzer.analyzed_type();
		}
	}
	return column_type;
}

core::error_t pandas_t::bind(std::pmr::memory_resource *resource, py::handle df_p,
                  std::vector<pandas_column_bind_data_t> &bind_columns,
                  std::vector<complex_logical_type> &return_types, std::vector<std::string> &names,
                  const configuration::config_pandas &cfg) {

	pandas_data_frame_bind_t df(df_p);
	idx_t column_count = py::len(df.names);
	if (column_count == 0 || py::len(df.types) == 0 || column_count != py::len(df.types)) {
		return core::error_t(core::error_code_t::invalid_parameter,
		                     std::pmr::string("Need a DataFrame with at least one column", resource));
	}

	return_types.reserve(column_count);
	names.reserve(column_count);
	// loop over every column
	for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
		pandas_column_bind_data_t bind_data;

		names.emplace_back(py::str(df.names[col_idx]));
		auto column = df[col_idx];
		auto column_type = bind_column(column, bind_data, resource, cfg);
		if (column_type.has_error()) {
			return column_type.error();
		}

		return_types.push_back(column_type.value());
		bind_columns.push_back(std::move(bind_data));
	}
	return core::error_t::no_error();
}

} // namespace otterbrix
