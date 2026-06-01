#include "numpy_scan.hpp"

#include <connection_environment/connection_environment.hpp>
#include <native/python_conversion.hpp>
#include <pandas/column/pandas_numpy_column.hpp>
#include <pandas/pandas_bind.hpp>

#include <components/types/types.hpp>
#include <components/vector/vector.hpp>

//#include "function/scalar/nested_functions.hpp"
#include <utf8proc.h>

#include <string_view>

namespace otterbrix {

using components::vector::vector_t;

// todo check data leak(set_data don't free old data, "get_data" is nullptr?
template <class T>
void ScanNumpyColumn(py::array &numpy_col, idx_t stride, idx_t offset, vector_t &out, idx_t count) {
	auto src_ptr = (T*)numpy_col.data();
	if (stride == sizeof(T)) {
		out.set_data(reinterpret_cast<std::byte*>(src_ptr + offset));
	} else {
		auto tgt_ptr = out.data<T>();
		for (idx_t i = 0; i < count; i++) {
			tgt_ptr[i] = src_ptr[stride / sizeof(T) * (i + offset)];
		}
	}
}

template <class T, class V>
void ScanNumpyCategoryTemplated(py::array &column, idx_t offset, vector_t &out, idx_t count) {
	auto src_ptr = (T*)column.data();
	auto tgt_ptr = out.data<T>(); 
	auto &tgt_mask = out.validity();
	for (idx_t i = 0; i < count; i++) {
		if (src_ptr[i + offset] == -1) {
			// Null value
			tgt_mask.set_invalid(i);
		} else {
			tgt_ptr[i] = src_ptr[i + offset];
		}
	}
}

template <class T>
void ScanNumpyCategory(py::array &column, idx_t count, idx_t offset, vector_t &out, string &src_type) {
	if (src_type == "int8") {
		ScanNumpyCategoryTemplated<int8_t, T>(column, offset, out, count);
	} else if (src_type == "int16") {
		ScanNumpyCategoryTemplated<int16_t, T>(column, offset, out, count);
	} else if (src_type == "int32") {
		ScanNumpyCategoryTemplated<int32_t, T>(column, offset, out, count);
	} else if (src_type == "int64") {
		ScanNumpyCategoryTemplated<int64_t, T>(column, offset, out, count);
	} else {
		throw std::runtime_error("The Pandas type " + src_type + " for categorical types is not implemented yet");
	}
}
//666
static void ApplyMask(PandasColumnBindData &bind_data, components::vector::validity_mask_t &validity, idx_t count, idx_t offset) {
    assert(bind_data.mask);
    auto mask = reinterpret_cast<const bool *>(bind_data.mask->numpy_array.data());
    for (idx_t i = 0; i < count; i++) {
        auto is_null = mask[offset + i];
        if (is_null) {
            validity.set_invalid(i);
        }
    }
}


template <class T>
void ScanNumpyMasked(PandasColumnBindData &bind_data, idx_t count, idx_t offset, vector_t &out) {
	assert(bind_data.pandas_col->Backend() == PandasColumnBackend::NUMPY);
	auto &numpy_col = reinterpret_cast<PandasNumpyColumn &>(*bind_data.pandas_col);
	ScanNumpyColumn<T>(numpy_col.array, numpy_col.stride, offset, out, count);
    if (bind_data.mask) {
        auto &result_mask = out.validity();
        ApplyMask(bind_data, result_mask, count, offset);
    }

}

template <class T>
void ScanNumpyFpColumn(PandasColumnBindData &bind_data, const T *src_ptr, idx_t stride, idx_t count, idx_t offset, vector_t &out) {
	auto &mask = out.validity(); 
	if (stride == sizeof(T)) {
		out.set_data((std::byte*)(src_ptr + offset)); // NOLINT
		// Turn NaN values into NULL
		auto tgt_ptr = out.data<T>();
		for (idx_t i = 0; i < count; i++) {
			if (std::isnan(tgt_ptr[i])) {
				mask.set_invalid(i);
			}
		}
	} else {
		auto tgt_ptr = out.data<T>();
		for (idx_t i = 0; i < count; i++) {
			tgt_ptr[i] = src_ptr[stride / sizeof(T) * (i + offset)];
			if (std::isnan(tgt_ptr[i])) {
				mask.set_invalid(i);
			}
		}
	}
    if (bind_data.mask) {
        auto &result_mask = out.validity();
        ApplyMask(bind_data, result_mask, count, offset);

    }
}


template <class T>
static std::string_view DecodePythonUnicode(T *codepoints, idx_t codepoint_count, vector_t &out) {
	// first figure out how many bytes to allocate
	idx_t utf8_length = 0;
	for (idx_t i = 0; i < codepoint_count; i++) {
        int cp = int(codepoints[i]);
        if (cp <= 0x7F) {
            utf8_length += 1;
        } else if (cp <= 0x7FF) {
            utf8_length += 2;
        } else if (0xd800 <= cp && cp <= 0xdfff) {
            assert(false);
        } else if (cp <= 0xFFFF) {
            utf8_length += 3;
        } else if (cp <= 0x10FFFF) {
            utf8_length += 4;
        } else {
            utf8_length += -1;
        }

		//int len = Utf8Proc::CodepointLength(int(codepoints[i]));
		assert(utf8_length >= 1);
	}
	int sz;
    auto buffer = static_cast<components::vector::string_vector_buffer_t*>(out.auxiliary().get());
    auto target = reinterpret_cast<utf8proc_uint8_t*>(buffer->empty_string(utf8_length));
    std::string_view result(reinterpret_cast<const char*>(target), utf8_length);
	//auto result = out.get_auxi(out, utf8_length);
	//auto target = result.GetDataWriteable();
    // utf8proc_reencode for array
	for (idx_t i = 0; i < codepoint_count; i++) {
		sz = utf8proc_encode_char(codepoints[i], target);
		assert(sz >= 1);
		target += sz;
	}
	return result;
}

static void SetInvalidRecursive(vector_t &out, idx_t index) {
	auto &validity = out.validity(); 
	validity.set_invalid(index);
	if (out.type().to_physical_type() == components::types::physical_type::STRUCT) {
		auto &children = out.entries(); 
		for (idx_t i = 0; i < children.size(); i++) {
			SetInvalidRecursive(*children[i], index);
		}
	}
}

//! 'count' is the amount of rows in the 'out' vector
//! 'offset' is the current row number within this vector
void ScanNumpyObject(PyObject *object, idx_t offset, vector_t &out) {

	// handle None
	if (object == Py_None) {
		SetInvalidRecursive(out, offset);
		return;
	}

	auto val = TransformPythonValue(object, out.type());
	// Check if the Value type is accepted for the logical_type of Vector
	out.set_value(offset, val);
}

// has no constraints
/*static void VerifyMapConstraints(vector_t &vec, idx_t count) {
	auto invalid_reason = MapVector::CheckMapValidity(vec, count);
	switch (invalid_reason) {
	case MapInvalidReason::VALID:
		return;
	case MapInvalidReason::DUPLICATE_KEY:
		throw std::runtime_error("Dict->Map conversion failed because 'key' list contains duplicates");
	case MapInvalidReason::NULL_KEY:
		throw std::runtime_error("Dict->Map conversion failed because 'key' list contains None");
	default:
		throw std::runtime_error("Option not implemented for MapInvalidReason");
	}
}*/

/*void VerifyTypeConstraints(vector_t &vec, idx_t count) {
	switch (vec.type().type()) {
        case components::types::logical_type::MAP: {
		VerifyMapConstraints(vec, count);
		break;
	}
	default:
		return;
	}
}*/

void NumpyScan::ScanObjectColumn(PyObject **col, idx_t stride, idx_t count, idx_t offset, vector_t &out) {
	// numpy_col is a sequential list of objects, that make up one "column" (Vector)
	out.set_vector_type(components::vector::vector_type::FLAT);
	auto &mask = out.validity();
	PythonGILWrapper gil; // We're creating python objects here, so we need the GIL

	if (stride == sizeof(PyObject *)) {
		auto src_ptr = col + offset;
		for (idx_t i = 0; i < count; i++) {
			ScanNumpyObject(src_ptr[i], i, out);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto src_ptr = col[stride / sizeof(PyObject *) * (i + offset)];
			ScanNumpyObject(src_ptr, i, out);
		}
	}
//	VerifyTypeConstraints(out, count);
}

//! 'offset' is the offset within the column
//! 'count' is the amount of values we will convert in this batch
void NumpyScan::Scan(PandasColumnBindData &bind_data, idx_t count, idx_t offset, vector_t &out) {
	assert(bind_data.pandas_col->Backend() == PandasColumnBackend::NUMPY);
	auto &numpy_col = reinterpret_cast<PandasNumpyColumn &>(*bind_data.pandas_col);
	auto &array = numpy_col.array;
	auto stride = numpy_col.stride;

	switch (bind_data.numpy_type.type) {
	case NumpyNullableType::BOOL:
		ScanNumpyMasked<bool>(bind_data, count, offset, out);
		break;
	case NumpyNullableType::UINT_8:
		ScanNumpyMasked<uint8_t>(bind_data, count, offset, out);
		break;
	case NumpyNullableType::UINT_16:
		ScanNumpyMasked<uint16_t>(bind_data, count, offset, out);
		break;
	case NumpyNullableType::UINT_32:
		ScanNumpyMasked<uint32_t>(bind_data, count, offset, out);
		break;
	case NumpyNullableType::UINT_64:
		ScanNumpyMasked<uint64_t>(bind_data, count, offset, out);
		break;
	case NumpyNullableType::INT_8:
		ScanNumpyMasked<int8_t>(bind_data, count, offset, out);
		break;
	case NumpyNullableType::INT_16:
		ScanNumpyMasked<int16_t>(bind_data, count, offset, out);
		break;
	case NumpyNullableType::INT_32:
		ScanNumpyMasked<int32_t>(bind_data, count, offset, out);
		break;
	case NumpyNullableType::INT_64:
		ScanNumpyMasked<int64_t>(bind_data, count, offset, out);
		break;
	case NumpyNullableType::FLOAT_32:
		ScanNumpyFpColumn<float>(bind_data, reinterpret_cast<const float *>(array.data()), numpy_col.stride, count, offset, out);
		break;
	case NumpyNullableType::FLOAT_64:
		ScanNumpyFpColumn<double>(bind_data, reinterpret_cast<const double *>(array.data()), numpy_col.stride, count, offset, out);
		break;
	case NumpyNullableType::STRING:
	case NumpyNullableType::OBJECT: {
		// Get the source pointer of the numpy array
		auto src_ptr = (PyObject **)array.data(); // NOLINT
		const bool is_object_col = bind_data.numpy_type.type == NumpyNullableType::OBJECT;
		if (is_object_col && out.type().type() != components::types::logical_type::STRING_LITERAL) {
			//! We have determined the underlying logical type of this object column
			return NumpyScan::ScanObjectColumn(src_ptr, numpy_col.stride, count, offset, out);
		}

		// Get the data pointer and the validity mask of the result vector
		auto tgt_ptr = out.data<std::string_view>(); 
		auto &out_mask = out.validity();
		unique_ptr<PythonGILWrapper> gil;
		auto &import_cache = ConnectionEnvironment::ImportCache();

		// Loop over every row of the arrays contents
		auto stride = numpy_col.stride;
		for (idx_t row = 0; row < count; row++) {
			auto source_idx = stride / sizeof(PyObject *) * (row + offset);

			// Get the pointer to the object
			PyObject *val = src_ptr[source_idx];
			if (!py::isinstance<py::str>(val)) {
				if (val == Py_None) {
					out_mask.set_invalid(row);
					continue;
				}
				if (import_cache.pandas.NaT(false)) {
					// If pandas is imported, check if this is pandas.NaT
					py::handle value(val);
					if (value.is(import_cache.pandas.NaT())) {
						out_mask.set_invalid(row);
						continue;
					}
				}
				if (import_cache.pandas.NA(false)) {
					// If pandas is imported, check if this is pandas.NA
					py::handle value(val);
					if (value.is(import_cache.pandas.NA())) {
						out_mask.set_invalid(row);
						continue;
					}
				}
				if (py::isinstance<py::float_>(val) && std::isnan(PyFloat_AsDouble(val))) {
					out_mask.set_invalid(row);
					continue;
				}
				if (!py::isinstance<py::str>(val)) {
					if (!gil) {
						gil = make_unique<PythonGILWrapper>();
					}
					bind_data.object_str_val.Push(std::move(py::str(val)));
					val = reinterpret_cast<PyObject *>(bind_data.object_str_val.LastAddedObject().ptr());
				}
			}
			// Python 3 string representation:
			// https://github.com/python/cpython/blob/3a8fdb28794b2f19f6c8464378fb8b46bce1f5f4/Include/cpython/unicodeobject.h#L79
			py::handle val_handle(val);
			if (!py::isinstance<py::str>(val_handle)) {
				out_mask.set_invalid(row);
				continue;
			}
			if (/*!PyUnicode_Check(val) &&*/ PyUnicode_IS_COMPACT_ASCII(val)) {
				// ascii string: we can zero copy
				tgt_ptr[row] = std::string_view(PyUtil::PyUnicodeData(val_handle), PyUtil::PyUnicodeGetLength(val_handle));
			} else {
				// unicode gunk
				auto ascii_obj = reinterpret_cast<PyASCIIObject *>(val);
				auto unicode_obj = reinterpret_cast<PyCompactUnicodeObject *>(val);
				// compact unicode string: is there utf8 data available?
				if (unicode_obj->utf8) {
					// there is! zero copy
					tgt_ptr[row] = std::string_view(const_char_ptr_cast(unicode_obj->utf8), unicode_obj->utf8_length);
				} else if (PyUtil::PyUnicodeIsCompact(unicode_obj) &&
				           !PyUtil::PyUnicodeIsASCII(unicode_obj)) { // NOLINT
					auto kind = PyUtil::PyUnicodeKind(val_handle);
					switch (kind) {
					case PyUnicode_1BYTE_KIND:
						tgt_ptr[row] = DecodePythonUnicode<Py_UCS1>(PyUtil::PyUnicode1ByteData(val_handle),
						                                            PyUtil::PyUnicodeGetLength(val_handle), out);
						break;
					case PyUnicode_2BYTE_KIND:
						tgt_ptr[row] = DecodePythonUnicode<Py_UCS2>(PyUtil::PyUnicode2ByteData(val_handle),
						                                            PyUtil::PyUnicodeGetLength(val_handle), out);
						break;
					case PyUnicode_4BYTE_KIND:
						tgt_ptr[row] = DecodePythonUnicode<Py_UCS4>(PyUtil::PyUnicode4ByteData(val_handle),
						                                            PyUtil::PyUnicodeGetLength(val_handle), out);
						break;
					default:
						throw std::runtime_error(
						    "Unsupported typekind constant " + to_string(int(kind)) + " for Python Unicode Compact decode");
					}
				} else {
					throw std::runtime_error("Unsupported string type: no clue what this string is");
				}
			}
		}
		break;
	}
	case NumpyNullableType::CATEGORY: 
			throw std::runtime_error("OtterBrix doen\'t support Categories/ENUMs");

	default:
		throw std::runtime_error("Unsupported pandas type");
	}
}

} // namespace otterbrix
