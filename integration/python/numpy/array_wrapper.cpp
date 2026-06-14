#include "array_wrapper.hpp"

#include <native/python_objects.hpp>
#include <connection_environment/connection_environment.hpp>
#include <util/util.hpp>

#include <components/types/types.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/vector.hpp>
#include <memory>

#include <absl/numeric/int128.h>

#include <chrono>
#include <memory_resource>
#include <string_view>
#include <string>

namespace otterbrix {
    using namespace components::vector;
    using namespace components::types;


namespace otterbrix_py_convert {

// Numeric conversion via static_cast. Used for both regular column conversion and
// integral->double conversion (HUGEINT/UHUGEINT, DECIMAL), with int128/uint128->double
// specializations defined below. For built-in types static_cast<T>(val) and T(val) are equivalent.
struct regular_convert_t {
	template <class OTTERBRIX_T, class NUMPY_T>
	static NUMPY_T convert_value(OTTERBRIX_T val, numpy_append_data_t &append_data) {
		(void)append_data;
		return static_cast<NUMPY_T>(val);
	}

	template <class NUMPY_T, bool PANDAS>
	static NUMPY_T null_value(bool &set_mask) {
		set_mask = true;
		return 0;
	}
};

struct string_convert_t {
	template <class OTTERBRIX_T, class NUMPY_T>
	static PyObject *convert_value(std::string_view val, numpy_append_data_t &append_data) {
		(void)append_data;
		auto data = const_data_ptr_cast(val.data());
		auto len = val.size();
		return PyUnicode_FromStringAndSize(const_char_ptr_cast(data), static_cast<Py_ssize_t>(len));
	}
	template <class NUMPY_T, bool PANDAS>
	static NUMPY_T null_value(bool &set_mask) {
		if (PANDAS) {
			set_mask = false;
			Py_RETURN_NONE;
		}
		set_mask = true;
		return nullptr;
	}
};

struct blob_convert_t {
	template <class OTTERBRIX_T, class NUMPY_T>
	static PyObject *convert_value(std::string_view val, numpy_append_data_t &append_data) {
		(void)append_data;
		return PyByteArray_FromStringAndSize(val.data(), static_cast<Py_ssize_t>(val.size()));
	}

	template <class NUMPY_T, bool PANDAS>
	static NUMPY_T null_value(bool &set_mask) {
		set_mask = true;
		return nullptr;
	}
};

struct bit_convert_t {
	template <class OTTERBRIX_T, class NUMPY_T>
	static PyObject *convert_value(std::string_view val, numpy_append_data_t &append_data) {
		(void)append_data;
		return PyBytes_FromStringAndSize(val.data(), static_cast<Py_ssize_t>(val.size()));
	}

	template <class NUMPY_T, bool PANDAS>
	static NUMPY_T null_value(bool &set_mask) {
		set_mask = true;
		return nullptr;
	}
};

struct uuid_convert_t {
	template <class OTTERBRIX_T, class NUMPY_T>
	static PyObject *convert_value(absl::int128 val, numpy_append_data_t &append_data) {
		(void)append_data;
		auto &import_cache = connection_environment_t::import_cache();
		py::handle h = import_cache.uuid.UUID()(util::parse_numeric_to_string(val)).release();
		return h.ptr();
	}

	template <class NUMPY_T, bool PANDAS>
	static NUMPY_T null_value(bool &set_mask) {
		set_mask = true;
		return nullptr;
	}
};

static py::object internal_create_list(vector_t &input,
    idx_t total_size, idx_t offset, idx_t size, numpy_append_data_t &append_data) {
	// initialize the array we'll append the list data to
	auto &type = input.type();
	array_wrapper_t result(type, append_data.pandas);
	if (auto init_err = result.initialize(append_data.resource, size); init_err.contains_error()) {
		if (!append_data.error.contains_error()) {
			append_data.error = std::move(init_err);
		}
		return py::none();
	}

	assert(offset + size <= total_size);
	if (auto append_err = result.append(append_data.resource, 0, input, total_size, offset, size);
	    append_err.contains_error()) {
		if (!append_data.error.contains_error()) {
			append_data.error = std::move(append_err);
		}
		return py::none();
	}
	return result.to_array();
}

struct list_convert_t {
	static py::object convert_value(vector_t &input, idx_t chunk_offset, numpy_append_data_t &append_data) {
		auto &list_data = append_data.idata;

		// get the list entry information from the parent
		const auto list_sel = *list_data.referenced_indexing;
		const auto list_entries = list_data.get_data<list_entry_t>();
		auto list_index = list_sel.get_index(chunk_offset);
		auto list_entry = list_entries[list_index];

		auto list_size = list_entry.length;
		auto list_offset = list_entry.offset;
		auto child_size = input.size(); 
		auto &child_vector = input.entry(); 

		return internal_create_list(child_vector, child_size, list_offset, list_size, append_data);
	}
};

struct array_convert_t {
	static py::object convert_value(vector_t &input, idx_t chunk_offset, numpy_append_data_t &append_data) {
		auto &array_data = append_data.idata;

		// get the list entry information from the parent
		const auto array_sel = *array_data.referenced_indexing;
		auto array_index = array_sel.get_index(chunk_offset);

		auto &array_type = input.type();
		assert(array_type.type() == logical_type::ARRAY);

		auto array_size = array_type.size();
		auto array_offset = array_index * array_size;
		auto child_size = 
            std::static_pointer_cast<components::vector::array_vector_buffer_t>(input.get_buffer())->size();
		auto &child_vector = input.entry();

		return internal_create_list(child_vector, child_size, array_offset, array_size, append_data);
	}
};

struct struct_convert_t {
	static py::dict convert_value(vector_t &input, idx_t chunk_offset, numpy_append_data_t &append_data) {
		py::dict py_struct;
		auto val = input.value(chunk_offset);
		auto &child_types = input.type().child_types();
		auto &struct_children = val.children();

		for (idx_t i = 0; i < struct_children.size(); i++) {
			auto &child_entry = child_types[i];
			auto &child_name = child_entry.alias();
			auto &child_type = child_entry;
			auto field = python_object_t::from_value(append_data.resource, struct_children[i], child_type);
			if (field.has_error()) {
				if (!append_data.error.contains_error()) {
					append_data.error = field.error();
				}
				return py_struct;
			}
			py_struct[child_name.c_str()] = field.value();
		}
		return py_struct;
	}
};
struct map_convert_t {
	static py::object convert_value(vector_t &input, idx_t chunk_offset, numpy_append_data_t &append_data) {
		auto val = input.value(chunk_offset);
		auto res = python_object_t::from_value(append_data.resource, val, input.type());
		if (res.has_error()) {
			if (!append_data.error.contains_error()) {
				append_data.error = res.error();
			}
			return py::none();
		}
		return res.value();
	}
};

template <>
double regular_convert_t::convert_value(absl::int128 val, numpy_append_data_t &append_data) {
	(void)append_data;
	return static_cast<double>(val);
}

template <>
double regular_convert_t::convert_value(absl::uint128 val, numpy_append_data_t &append_data) {
	(void)append_data;
	return static_cast<double>(val);
}

} // namespace otterbrix_py_convert

template <class OTTERBRIX_T, class NUMPY_T, class CONVERT, bool HAS_NULLS, bool PANDAS>
static bool convert_column_templated(numpy_append_data_t &append_data) {
	auto target_offset = append_data.target_offset;
	auto target_data = append_data.target_data;
	auto target_mask = append_data.target_mask;
	auto &idata = append_data.idata;
	auto count = append_data.count;
	auto source_offset = append_data.source_offset;

	auto src_ptr = idata.get_data<OTTERBRIX_T>();
	auto out_ptr = reinterpret_cast<NUMPY_T *>(target_data);
	bool mask_is_set = false;
	for (idx_t i = 0; i < count; i++) {
		idx_t src_idx = idata.referenced_indexing->get_index(i + source_offset);
		idx_t offset = target_offset + i;
		if (HAS_NULLS && !idata.validity.row_is_valid(src_idx)) {
			out_ptr[offset] = CONVERT::template null_value<NUMPY_T, PANDAS>(target_mask[offset]);
			mask_is_set = mask_is_set || target_mask[offset];
		} else {
			out_ptr[offset] = CONVERT::template convert_value<OTTERBRIX_T, NUMPY_T>(src_ptr[src_idx], append_data);
			target_mask[offset] = false;
		}
	}
	return mask_is_set;
}

template <class OTTERBRIX_T, class NUMPY_T, class CONVERT>
static bool convert_column(numpy_append_data_t &append_data) {
	auto &idata = append_data.idata;

	if (!idata.validity.all_valid()) {
		if (append_data.pandas) {
			return convert_column_templated<OTTERBRIX_T, NUMPY_T, CONVERT, /*has_nulls=*/true, /*pandas=*/true>(append_data);
		} else {
			return convert_column_templated<OTTERBRIX_T, NUMPY_T, CONVERT, /*has_nulls=*/true, /*pandas=*/false>(
			    append_data);
		}
	} else {
		if (append_data.pandas) {
			return convert_column_templated<OTTERBRIX_T, NUMPY_T, CONVERT, /*has_nulls=*/false, /*pandas=*/true>(
			    append_data);
		} else {
			return convert_column_templated<OTTERBRIX_T, NUMPY_T, CONVERT, /*has_nulls=*/false, /*pandas=*/false>(
			    append_data);
		}
	}
}

template <class NUMPY_T, class CONVERT>
static bool convert_nested(numpy_append_data_t &append_data) {
	auto target_offset = append_data.target_offset;
	auto target_data = append_data.target_data;
	auto target_mask = append_data.target_mask;
	auto &input = append_data.input;
	auto &idata = append_data.idata;
	auto count = append_data.count;
	auto source_offset = append_data.source_offset;

	auto out_ptr = reinterpret_cast<NUMPY_T *>(target_data);
	if (!idata.validity.all_valid()) {
		bool requires_mask = false;
		for (idx_t i = 0; i < count; i++) {
			idx_t index = i + source_offset;
			idx_t src_idx = idata.referenced_indexing->get_index(index);
			idx_t offset = target_offset + i;
			if (!idata.validity.row_is_valid(src_idx)) {
				out_ptr[offset] = py::none();
				requires_mask = true;
				target_mask[offset] = true;
			} else {
				out_ptr[offset] = CONVERT::convert_value(input, index, append_data);
				target_mask[offset] = false;
			}
		}
		return requires_mask;
	} else {
		for (idx_t i = 0; i < count; i++) {
			// NOTE: we do not apply the selection vector here,
			// because we use GetValue inside convert_value, which *also* applies the selection vector
			idx_t index = i + source_offset;
			idx_t offset = target_offset + i;
			out_ptr[offset] = CONVERT::convert_value(input, index, append_data);
			target_mask[offset] = false;
		}
		return false;
	}
}

template <class T>
static bool convert_column_regular(numpy_append_data_t &append_data) {
	return convert_column<T, T, otterbrix_py_convert::regular_convert_t>(append_data);
}

template <class OTTERBRIX_T>
static bool convert_decimal_internal(numpy_append_data_t &append_data, double division) {
	auto target_offset = append_data.target_offset;
	auto target_data = append_data.target_data;
	auto target_mask = append_data.target_mask;
	auto &idata = append_data.idata;
	auto count = append_data.count;
	auto source_offset = append_data.source_offset;

	auto src_ptr = idata.get_data<OTTERBRIX_T>();
	auto out_ptr = reinterpret_cast<double *>(target_data);
	if (!idata.validity.all_valid()) {
		for (idx_t i = 0; i < count; i++) {
			idx_t src_idx = idata.referenced_indexing->get_index(i + source_offset);
			idx_t offset = target_offset + i;
			if (!idata.validity.row_is_valid(src_idx)) {
				target_mask[offset] = true;
			} else {
				out_ptr[offset] =
				    otterbrix_py_convert::regular_convert_t::convert_value<OTTERBRIX_T, double>(src_ptr[src_idx], append_data) /
				    division;
				target_mask[offset] = false;
			}
		}
		return true;
	} else {
		for (idx_t i = 0; i < count; i++) {
			idx_t src_idx = idata.referenced_indexing->get_index(i + source_offset);
			idx_t offset = target_offset + i;
			out_ptr[offset] =
			    otterbrix_py_convert::regular_convert_t::convert_value<OTTERBRIX_T, double>(src_ptr[src_idx], append_data) /
			    division;
			target_mask[offset] = false;
		}
		return false;
	}
}

static bool convert_decimal(numpy_append_data_t &append_data) {
	auto &decimal_type = append_data.input.type();
    auto* decimal_extension = static_cast<decimal_logical_type_extension*>(decimal_type.extension());
	auto dec_scale = decimal_extension->scale();
	double division = pow(10, dec_scale);
	switch (decimal_extension->stored_as()) {
	case physical_type::INT16:
		return convert_decimal_internal<int16_t>(append_data, division);
	case physical_type::INT32:
		return convert_decimal_internal<int32_t>(append_data, division);
	case physical_type::INT64:
		return convert_decimal_internal<int64_t>(append_data, division);
	case physical_type::INT128:
		return convert_decimal_internal<int128_t>(append_data, division);
	default:
		append_data.error = core::error_t{core::error_code_t::other_error,
		                                  std::pmr::string{"unsupported decimal storage type in ConvertDecimal", append_data.resource}};
		return false;
	}
}

array_wrapper_t::array_wrapper_t(const complex_logical_type &type, bool pandas)
    : requires_mask(false), pandas(pandas) {
	data = std::make_unique<raw_array_wrapper_t>(type);
	mask = std::make_unique<raw_array_wrapper_t>(logical_type::BOOLEAN);
}

core::error_t array_wrapper_t::initialize(std::pmr::memory_resource *resource, idx_t capacity) {
	if (auto err = data->initialize(resource, capacity); err.contains_error()) {
		return err;
	}
	if (auto err = mask->initialize(resource, capacity); err.contains_error()) {
		return err;
	}
	return core::error_t::no_error();
}

void array_wrapper_t::resize(idx_t new_capacity) {
	data->resize(new_capacity);
	mask->resize(new_capacity);
}

core::error_t array_wrapper_t::append(std::pmr::memory_resource *resource, idx_t current_offset, vector_t &input, idx_t source_size, idx_t source_offset, idx_t count) {
	auto dataptr = data->data;
	auto maskptr = reinterpret_cast<bool *>(mask->data);
	assert(dataptr);
	assert(maskptr);
	assert(input.type() == data->type);
	bool may_have_null;

	unified_vector_format idata(input.resource(), source_size);
	input.to_unified_format(source_size, idata);

	if (count == components::table::storage::INVALID_INDEX) {
		assert(source_size != components::table::storage::INVALID_INDEX);
		count = source_size;
	}

	numpy_append_data_t append_data(resource, idata, input);
	append_data.target_offset = current_offset;
	append_data.target_data = dataptr;
	append_data.source_offset = source_offset;
	append_data.source_size = source_size;
	append_data.count = count;
	append_data.target_mask = maskptr;
	append_data.pandas = pandas;

	switch (input.type().type()) {
	case logical_type::BOOLEAN:
		may_have_null = convert_column_regular<bool>(append_data);
		break;
	case logical_type::TINYINT:
		may_have_null = convert_column_regular<int8_t>(append_data);
		break;
	case logical_type::SMALLINT:
		may_have_null = convert_column_regular<int16_t>(append_data);
		break;
	case logical_type::INTEGER:
		may_have_null = convert_column_regular<int32_t>(append_data);
		break;
	case logical_type::BIGINT:
		may_have_null = convert_column_regular<int64_t>(append_data);
		break;
	case logical_type::UTINYINT:
		may_have_null = convert_column_regular<uint8_t>(append_data);
		break;
	case logical_type::USMALLINT:
		may_have_null = convert_column_regular<uint16_t>(append_data);
		break;
	case logical_type::UINTEGER:
		may_have_null = convert_column_regular<uint32_t>(append_data);
		break;
	case logical_type::UBIGINT:
		may_have_null = convert_column_regular<uint64_t>(append_data);
		break;
	case logical_type::HUGEINT:
		may_have_null = convert_column<absl::int128, double, otterbrix_py_convert::regular_convert_t>(append_data);
		break;
	case logical_type::UHUGEINT:
		may_have_null = convert_column<absl::uint128, double, otterbrix_py_convert::regular_convert_t>(append_data);
		break;
	case logical_type::FLOAT:
		may_have_null = convert_column_regular<float>(append_data);
		break;
	case logical_type::DOUBLE:
		may_have_null = convert_column_regular<double>(append_data);
		break;
	case logical_type::DECIMAL:
		may_have_null = convert_decimal(append_data);
		break;
	case logical_type::TIMESTAMP:
		may_have_null = convert_column_regular<int64_t>(append_data);
		break;
	case logical_type::STRING_LITERAL:
		may_have_null = convert_column<std::string_view, PyObject *, otterbrix_py_convert::string_convert_t>(append_data);
		break;
	case logical_type::BLOB:
		may_have_null = convert_column<std::string_view, PyObject *, otterbrix_py_convert::blob_convert_t>(append_data);
		break;
	case logical_type::BIT:
		may_have_null = convert_column<std::string_view, PyObject *, otterbrix_py_convert::bit_convert_t>(append_data);
		break;
	case logical_type::LIST:
		may_have_null = convert_nested<py::object, otterbrix_py_convert::list_convert_t>(append_data);
		break;
	case logical_type::ARRAY:
		may_have_null = convert_nested<py::object, otterbrix_py_convert::array_convert_t>(append_data);
		break;
	case logical_type::MAP:
		may_have_null = convert_nested<py::object, otterbrix_py_convert::map_convert_t>(append_data);
		break;
	case logical_type::STRUCT:
		may_have_null = convert_nested<py::object, otterbrix_py_convert::struct_convert_t>(append_data);
		break;
	case logical_type::UUID:
		may_have_null = convert_column<absl::int128, PyObject *, otterbrix_py_convert::uuid_convert_t>(append_data);
		break;

	case logical_type::ENUM:
	case logical_type::UNION:
		return core::error_t{core::error_code_t::other_error,
		                     std::pmr::string{"type not yet supported for numpy conversion: " +
		                         std::to_string(static_cast<int>(input.type().type())), resource}};

	default:
		return core::error_t{core::error_code_t::other_error,
		                     std::pmr::string{"Unsupported type "+std::to_string(static_cast<int>(input.type().type())), resource}};
	}
	// A nested converter (LIST/ARRAY/MAP/STRUCT) or convert_decimal may have recorded an error.
	if (append_data.error.contains_error()) {
		return std::move(append_data.error);
	}
	if (may_have_null) {
		requires_mask = true;
	}
	data->count += count;
	mask->count += count;
	return core::error_t::no_error();
}

py::object array_wrapper_t::to_array() const {
	assert(data->array && mask->array);
	data->resize(data->count);
	if (!requires_mask) {
		return std::move(data->array);
	}
	mask->resize(mask->count);
	// construct numpy arrays from the data and the mask
	auto values = std::move(data->array);
	auto nullmask = std::move(mask->array);

	// create masked array and return it
	auto masked_array = py::module::import("numpy.ma").attr("masked_array")(values, nullmask);
	return masked_array;
}

} // namespace otterbrix
