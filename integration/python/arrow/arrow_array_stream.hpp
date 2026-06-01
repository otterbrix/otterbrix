#pragma once

#include <pybind11/pybind_wrapper.hpp>

#include <components/arrow/arrow.hpp>
#include <components/arrow/arrow_wrapper.hpp>
//#include <components/function/table/arrow.hpp>
#include <core/typedefs.hpp>
#include <core/types/memory.hpp>
#include <core/types/unordered_map.hpp>

namespace otterbrix {

enum class PyArrowObjectType { Invalid, Table, RecordBatchReader, Scanner, Dataset, PyCapsule, PyCapsuleInterface };

namespace pyarrow {

class RecordBatchReader : public py::object {
public:
	RecordBatchReader(const py::object &o) : py::object(o, borrowed_t {}) {
	}
	using py::object::object;

public:
	static bool check_(const py::handle &object) {
		return !py::none().is(object);
	}
};

class Table : public py::object {
public:
	Table(const py::object &o) : py::object(o, borrowed_t {}) {
	}
	using py::object::object;

public:
	static bool check_(const py::handle &object) {
		return !py::none().is(object);
	}
};

} // namespace pyarrow


class PythonTableArrowArrayStreamFactory {
public:
    using components::arrow::ArrowArrayStreamWrapper;
    using components::arrow::ArrowSchemaWrapper;
    using components::function::table::ArrowStreamParameters;

	explicit PythonTableArrowArrayStreamFactory(PyObject *arrow_table)
	    : arrow_object(arrow_table) {};

	//! Produces an Arrow Scanner, should be only called once when initializing Scan States
	static unique_ptr<ArrowArrayStreamWrapper> Produce(uintptr_t factory, ArrowStreamParameters &parameters);

	//! Get the schema of the arrow object
	static void GetSchemaInternal(py::handle arrow_object, ArrowSchemaWrapper &schema);
	static void GetSchema(uintptr_t factory_ptr, ArrowSchemaWrapper &schema);

	//! Arrow Object (i.e., Scanner, Record Batch Reader, Table, Dataset)
	PyObject *arrow_object;

private:
	//! We transform a TableFilterSet to an Arrow Expression Object
    //todo std::unordered_map
	static py::object TransformFilter(components::table::table_filer_set_t &filters, unordered_map<idx_t, string> &columns,
	                                  unordered_map<idx_t, idx_t> filter_to_col, const ArrowTableType &arrow_table);

	static py::object ProduceScanner(py::object &arrow_scanner, py::handle &arrow_obj_handle,
	                                 ArrowStreamParameters &parameters);
};
} // namespace otterbrix

namespace pybind11 {
namespace detail {
template <>
struct handle_type_name<otterbrix::pyarrow::RecordBatchReader> {
	static constexpr auto name = _("pyarrow.lib.RecordBatchReader");
};
template <>
struct handle_type_name<otterbrix::pyarrow::Table> {
	static constexpr auto name = _("pyarrow.lib.Table");
};
} // namespace detail
} // namespace pybind11
