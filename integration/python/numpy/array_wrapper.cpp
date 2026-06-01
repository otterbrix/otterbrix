#include "array_wrapper.hpp"

#include <native/python_objects.hpp>
#include <connection_environment/connection_environment.hpp>
#include <util/util.hpp>

#include <components/types/types.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/vector.hpp>
#include <core/types/memory.hpp>

#include <absl/numeric/int128.h>

#include <chrono>
#include <stdexcept>
#include <string_view>

namespace otterbrix {
    using namespace components::vector;
    using namespace components::types;


namespace otterbrix_py_convert {

struct RegularConvert {
	template <class OTTERBRIX_T, class NUMPY_T>
	static NUMPY_T ConvertValue(OTTERBRIX_T val, NumpyAppendData &append_data) {
		(void)append_data;
		return static_cast<NUMPY_T>(val);
	}

	template <class NUMPY_T, bool PANDAS>
	static NUMPY_T NullValue(bool &set_mask) {
		set_mask = true;
		return 0;
	}
};

struct StringConvert {
	template <class OTTERBRIX_T, class NUMPY_T>
	static PyObject *ConvertValue(std::string_view val, NumpyAppendData &append_data) {
		(void)append_data;
		auto data = const_data_ptr_cast(val.data());
		auto len = val.size();
		return PyUnicode_FromStringAndSize(const_char_ptr_cast(data), static_cast<Py_ssize_t>(len));
	}
	template <class NUMPY_T, bool PANDAS>
	static NUMPY_T NullValue(bool &set_mask) {
		if (PANDAS) {
			set_mask = false;
			Py_RETURN_NONE;
		}
		set_mask = true;
		return nullptr;
	}
};

struct BlobConvert {
	template <class OTTERBRIX_T, class NUMPY_T>
	static PyObject *ConvertValue(std::string_view val, NumpyAppendData &append_data) {
		(void)append_data;
		return PyByteArray_FromStringAndSize(val.data(), static_cast<Py_ssize_t>(val.size()));
	}

	template <class NUMPY_T, bool PANDAS>
	static NUMPY_T NullValue(bool &set_mask) {
		set_mask = true;
		return nullptr;
	}
};

struct BitConvert {
	template <class OTTERBRIX_T, class NUMPY_T>
	static PyObject *ConvertValue(std::string_view val, NumpyAppendData &append_data) {
		(void)append_data;
		return PyBytes_FromStringAndSize(val.data(), static_cast<Py_ssize_t>(val.size()));
	}

	template <class NUMPY_T, bool PANDAS>
	static NUMPY_T NullValue(bool &set_mask) {
		set_mask = true;
		return nullptr;
	}
};

struct UUIDConvert {
	template <class OTTERBRIX_T, class NUMPY_T>
	static PyObject *ConvertValue(absl::int128 val, NumpyAppendData &append_data) {
		(void)append_data;
		auto &import_cache = ConnectionEnvironment::ImportCache();
		py::handle h = import_cache.uuid.UUID()(util::ParseNumericToString(val)).release();
		return h.ptr();
	}

	template <class NUMPY_T, bool PANDAS>
	static NUMPY_T NullValue(bool &set_mask) {
		set_mask = true;
		return nullptr;
	}
};

static py::object InternalCreateList(vector_t &input, 
    idx_t total_size, idx_t offset, idx_t size, NumpyAppendData &append_data) {
	// Initialize the array we'll append the list data to
	auto &type = input.type();
	ArrayWrapper result(type, append_data.pandas);
	result.Initialize(size);

	assert(offset + size <= total_size);
	result.Append(0, input, total_size, offset, size);
	return result.ToArray();
}

struct ListConvert {
	static py::object ConvertValue(vector_t &input, idx_t chunk_offset, NumpyAppendData &append_data) {
		auto &list_data = append_data.idata;

		// Get the list entry information from the parent
		const auto list_sel = *list_data.referenced_indexing;
		const auto list_entries = list_data.get_data<list_entry_t>();
		auto list_index = list_sel.get_index(chunk_offset);
		auto list_entry = list_entries[list_index];

		auto list_size = list_entry.length;
		auto list_offset = list_entry.offset;
		auto child_size = input.size(); 
		auto &child_vector = input.entry(); 

		return InternalCreateList(child_vector, child_size, list_offset, list_size, append_data);
	}
};

struct ArrayConvert {
	static py::object ConvertValue(vector_t &input, idx_t chunk_offset, NumpyAppendData &append_data) {
		auto &array_data = append_data.idata;

		// Get the list entry information from the parent
		const auto array_sel = *array_data.referenced_indexing;
		auto array_index = array_sel.get_index(chunk_offset);

		auto &array_type = input.type();
		assert(array_type.type() == logical_type::ARRAY);

		auto array_size = array_type.size();
		auto array_offset = array_index * array_size;
		auto child_size = 
            std::static_pointer_cast<components::vector::array_vector_buffer_t>(input.get_buffer())->size();
		auto &child_vector = input.entry();

		return InternalCreateList(child_vector, child_size, array_offset, array_size, append_data);
	}
};

struct StructConvert {
	static py::dict ConvertValue(vector_t &input, idx_t chunk_offset, NumpyAppendData &append_data) {
		(void)append_data;
		py::dict py_struct;
		auto val = input.value(chunk_offset);
		auto &child_types = input.type().child_types();
		auto &struct_children = val.children(); 

		for (idx_t i = 0; i < struct_children.size(); i++) {
			auto &child_entry = child_types[i];
			auto &child_name = child_entry.alias(); 
			auto &child_type = child_entry;
			py_struct[child_name.c_str()] = PythonObject::FromValue(struct_children[i], child_type);
		}
		return py_struct;
	}
};
struct MapConvert {
	static py::dict ConvertValue(vector_t &input, idx_t chunk_offset, NumpyAppendData &append_data) {
		(void)append_data;
		auto val = input.value(chunk_offset);
		return PythonObject::FromValue(val, input.type());
	}
};

struct IntegralConvert {
	template <class OTTERBRIX_T, class NUMPY_T>
	static NUMPY_T ConvertValue(OTTERBRIX_T val, NumpyAppendData &append_data) {
		(void)append_data;
		return NUMPY_T(val);
	}

	template <class NUMPY_T, bool PANDAS>
	static NUMPY_T NullValue(bool &set_mask) {
		set_mask = true;
		return 0;
	}
};

template <>
double IntegralConvert::ConvertValue(absl::int128 val, NumpyAppendData &append_data) {
	(void)append_data;
	return static_cast<double>(val);
}

template <>
double IntegralConvert::ConvertValue(absl::uint128 val, NumpyAppendData &append_data) {
	(void)append_data;
	return static_cast<double>(val);
}

} // namespace otterbrix_py_convert

template <class OTTERBRIX_T, class NUMPY_T, class CONVERT, bool HAS_NULLS, bool PANDAS>
static bool ConvertColumnTemplated(NumpyAppendData &append_data) {
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
			out_ptr[offset] = CONVERT::template NullValue<NUMPY_T, PANDAS>(target_mask[offset]);
			mask_is_set = mask_is_set || target_mask[offset];
		} else {
			out_ptr[offset] = CONVERT::template ConvertValue<OTTERBRIX_T, NUMPY_T>(src_ptr[src_idx], append_data);
			target_mask[offset] = false;
		}
	}
	return mask_is_set;
}

template <class OTTERBRIX_T, class NUMPY_T, class CONVERT>
static bool ConvertColumn(NumpyAppendData &append_data) {
	auto &idata = append_data.idata;

	if (!idata.validity.all_valid()) {
		if (append_data.pandas) {
			return ConvertColumnTemplated<OTTERBRIX_T, NUMPY_T, CONVERT, /*has_nulls=*/true, /*pandas=*/true>(append_data);
		} else {
			return ConvertColumnTemplated<OTTERBRIX_T, NUMPY_T, CONVERT, /*has_nulls=*/true, /*pandas=*/false>(
			    append_data);
		}
	} else {
		if (append_data.pandas) {
			return ConvertColumnTemplated<OTTERBRIX_T, NUMPY_T, CONVERT, /*has_nulls=*/false, /*pandas=*/true>(
			    append_data);
		} else {
			return ConvertColumnTemplated<OTTERBRIX_T, NUMPY_T, CONVERT, /*has_nulls=*/false, /*pandas=*/false>(
			    append_data);
		}
	}
}

template <class OTTERBRIX_T, class NUMPY_T>
static bool ConvertColumnCategoricalTemplate(NumpyAppendData &append_data) {
	auto target_offset = append_data.target_offset;
	auto target_data = append_data.target_data;
	auto &idata = append_data.idata;
	auto count = append_data.count;
	auto source_offset = append_data.source_offset;

	auto src_ptr = idata.get_data<OTTERBRIX_T>();
	auto out_ptr = reinterpret_cast<NUMPY_T *>(target_data);
	if (!idata.validity.all_valid()) {
		for (idx_t i = 0; i < count; i++) {
			idx_t src_idx = idata.referenced_indexing->get_index(i + source_offset);
			idx_t offset = target_offset + i;
			if (!idata.validity.row_is_valid(src_idx)) {
				out_ptr[offset] = static_cast<NUMPY_T>(-1);
			} else {
				out_ptr[offset] = otterbrix_py_convert::RegularConvert::template ConvertValue<OTTERBRIX_T, NUMPY_T>(
				    src_ptr[src_idx], append_data);
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			idx_t src_idx = idata.referenced_indexing->get_index(i + source_offset);
			idx_t offset = target_offset + i;
			out_ptr[offset] = otterbrix_py_convert::RegularConvert::template ConvertValue<OTTERBRIX_T, NUMPY_T>(
			    src_ptr[src_idx], append_data);
		}
	}
	// Null values are encoded in the data itself
	return false;
}

template <class NUMPY_T, class CONVERT>
static bool ConvertNested(NumpyAppendData &append_data) {
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
				out_ptr[offset] = CONVERT::ConvertValue(input, index, append_data);
				target_mask[offset] = false;
			}
		}
		return requires_mask;
	} else {
		for (idx_t i = 0; i < count; i++) {
			// NOTE: we do not apply the selection vector here,
			// because we use GetValue inside ConvertValue, which *also* applies the selection vector
			idx_t index = i + source_offset;
			idx_t offset = target_offset + i;
			out_ptr[offset] = CONVERT::ConvertValue(input, index, append_data);
			target_mask[offset] = false;
		}
		return false;
	}
}

template <class NUMPY_T>
static bool ConvertColumnCategorical(NumpyAppendData &append_data) {
	auto physical_type = append_data.physical_type;
	switch (physical_type) {
	case physical_type::UINT8:
		return ConvertColumnCategoricalTemplate<uint8_t, NUMPY_T>(append_data);
	case physical_type::UINT16:
		return ConvertColumnCategoricalTemplate<uint16_t, NUMPY_T>(append_data);
	case physical_type::UINT32:
		return ConvertColumnCategoricalTemplate<uint32_t, NUMPY_T>(append_data);
	default:
		throw std::runtime_error("Enum Physical Type not Allowed");
	}
}

template <class T>
static bool ConvertColumnRegular(NumpyAppendData &append_data) {
	return ConvertColumn<T, T, otterbrix_py_convert::RegularConvert>(append_data);
}

template <class OTTERBRIX_T>
static bool ConvertDecimalInternal(NumpyAppendData &append_data, double division) {
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
				    otterbrix_py_convert::IntegralConvert::ConvertValue<OTTERBRIX_T, double>(src_ptr[src_idx], append_data) /
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
			    otterbrix_py_convert::IntegralConvert::ConvertValue<OTTERBRIX_T, double>(src_ptr[src_idx], append_data) /
			    division;
			target_mask[offset] = false;
		}
		return false;
	}
}

static bool ConvertDecimal(NumpyAppendData &append_data) {
	auto &decimal_type = append_data.input.type();
    auto* decimal_extension = static_cast<decimal_logical_type_extension*>(decimal_type.extension());
	auto dec_scale = decimal_extension->scale();
	double division = pow(10, dec_scale);
	switch (decimal_extension->stored_as()) {
	case physical_type::INT16:
		return ConvertDecimalInternal<int16_t>(append_data, division);
	case physical_type::INT32:
		return ConvertDecimalInternal<int32_t>(append_data, division);
	case physical_type::INT64:
		return ConvertDecimalInternal<int64_t>(append_data, division);
	case physical_type::INT128:
		return ConvertDecimalInternal<int128_t>(append_data, division);
	default:
		throw std::runtime_error("unsupported decimal storage type in ConvertDecimal");
	}
}

ArrayWrapper::ArrayWrapper(const complex_logical_type &type, bool pandas)
    : requires_mask(false), pandas(pandas) {
	data = make_unique<RawArrayWrapper>(type);
	mask = make_unique<RawArrayWrapper>(logical_type::BOOLEAN);
}

void ArrayWrapper::Initialize(idx_t capacity) {
	data->Initialize(capacity);
	mask->Initialize(capacity);
}

void ArrayWrapper::Resize(idx_t new_capacity) {
	data->Resize(new_capacity);
	mask->Resize(new_capacity);
}

void ArrayWrapper::Append(idx_t current_offset, vector_t &input, idx_t source_size, idx_t source_offset, idx_t count) {
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

	NumpyAppendData append_data(idata, input);
	append_data.target_offset = current_offset;
	append_data.target_data = dataptr;
	append_data.source_offset = source_offset;
	append_data.source_size = source_size;
	append_data.count = count;
	append_data.target_mask = maskptr;
	append_data.pandas = pandas;

	switch (input.type().type()) {
	case logical_type::BOOLEAN:
		may_have_null = ConvertColumnRegular<bool>(append_data);
		break;
	case logical_type::TINYINT:
		may_have_null = ConvertColumnRegular<int8_t>(append_data);
		break;
	case logical_type::SMALLINT:
		may_have_null = ConvertColumnRegular<int16_t>(append_data);
		break;
	case logical_type::INTEGER:
		may_have_null = ConvertColumnRegular<int32_t>(append_data);
		break;
	case logical_type::BIGINT:
		may_have_null = ConvertColumnRegular<int64_t>(append_data);
		break;
	case logical_type::UTINYINT:
		may_have_null = ConvertColumnRegular<uint8_t>(append_data);
		break;
	case logical_type::USMALLINT:
		may_have_null = ConvertColumnRegular<uint16_t>(append_data);
		break;
	case logical_type::UINTEGER:
		may_have_null = ConvertColumnRegular<uint32_t>(append_data);
		break;
	case logical_type::UBIGINT:
		may_have_null = ConvertColumnRegular<uint64_t>(append_data);
		break;
	case logical_type::HUGEINT:
		may_have_null = ConvertColumn<absl::int128, double, otterbrix_py_convert::IntegralConvert>(append_data);
		break;
	case logical_type::UHUGEINT:
		may_have_null = ConvertColumn<absl::uint128, double, otterbrix_py_convert::IntegralConvert>(append_data);
		break;
	case logical_type::FLOAT:
		may_have_null = ConvertColumnRegular<float>(append_data);
		break;
	case logical_type::DOUBLE:
		may_have_null = ConvertColumnRegular<double>(append_data);
		break;
	case logical_type::DECIMAL:
		may_have_null = ConvertDecimal(append_data);
		break;
	case logical_type::TIMESTAMP:
		may_have_null = ConvertColumnRegular<int64_t>(append_data);
		break;
	case logical_type::STRING_LITERAL:
		may_have_null = ConvertColumn<std::string_view, PyObject *, otterbrix_py_convert::StringConvert>(append_data);
		break;
	case logical_type::BLOB:
		may_have_null = ConvertColumn<std::string_view, PyObject *, otterbrix_py_convert::BlobConvert>(append_data);
		break;
	case logical_type::BIT:
		may_have_null = ConvertColumn<std::string_view, PyObject *, otterbrix_py_convert::BitConvert>(append_data);
		break;
	case logical_type::LIST:
		may_have_null = ConvertNested<py::object, otterbrix_py_convert::ListConvert>(append_data);
		break;
	case logical_type::ARRAY:
		may_have_null = ConvertNested<py::object, otterbrix_py_convert::ArrayConvert>(append_data);
		break;
	case logical_type::MAP:
		may_have_null = ConvertNested<py::object, otterbrix_py_convert::MapConvert>(append_data);
		break;
	case logical_type::STRUCT:
		may_have_null = ConvertNested<py::object, otterbrix_py_convert::StructConvert>(append_data);
		break;
	case logical_type::UUID:
		may_have_null = ConvertColumn<absl::int128, PyObject *, otterbrix_py_convert::UUIDConvert>(append_data);
		break;

	case logical_type::ENUM:
	case logical_type::UNION:
		throw std::runtime_error("type not yet supported for numpy conversion: " +
		                         to_string(static_cast<int>(input.type().type())));

	default:
		throw std::runtime_error("Unsupported type "+to_string(static_cast<int>(input.type().type())));
	}
	if (may_have_null) {
		requires_mask = true;
	}
	data->count += count;
	mask->count += count;
}

py::object ArrayWrapper::ToArray() const {
	assert(data->array && mask->array);
	data->Resize(data->count);
	if (!requires_mask) {
		return std::move(data->array);
	}
	mask->Resize(mask->count);
	// construct numpy arrays from the data and the mask
	auto values = std::move(data->array);
	auto nullmask = std::move(mask->array);

	// create masked array and return it
	auto masked_array = py::module::import("numpy.ma").attr("masked_array")(values, nullmask);
	return masked_array;
}

} // namespace otterbrix
