#include "numpy_scan.hpp"
#include <memory>

#include <connection_environment/connection_environment.hpp>
#include <native/python_conversion.hpp>
#include <pandas/column/pandas_numpy_column.hpp>
#include <pandas/pandas_bind.hpp>

#include <components/types/types.hpp>
#include <components/vector/vector.hpp>

#include <utf8proc.h>

#include <cstring>
#include <memory_resource>
#include <string_view>
#include <string>

namespace otterbrix {

using components::vector::vector_t;

static core::error_t make_error(std::pmr::memory_resource *resource, const std::string &what) {
	return core::error_t{core::error_code_t::other_error, std::pmr::string{what, resource}};
}

template <class T>
void scan_numpy_column(py::array &numpy_col, idx_t stride, idx_t offset, vector_t &out, idx_t count) {
	auto src_ptr = static_cast<const T*>(numpy_col.data());
	auto tgt_ptr = out.data<T>();
	if (stride == sizeof(T)) {
		std::memcpy(tgt_ptr, src_ptr + offset, count * sizeof(T));
	} else {
		const idx_t step = stride / sizeof(T);
		for (idx_t i = 0; i < count; i++) {
			tgt_ptr[i] = src_ptr[step * (i + offset)];
		}
	}
}

template <class T, class V>
void scan_numpy_category_templated(py::array &column, idx_t offset, vector_t &out, idx_t count) {
	auto src_ptr = static_cast<const T*>(column.data());
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
core::error_t scan_numpy_category(std::pmr::memory_resource *resource, py::array &column, idx_t count, idx_t offset, vector_t &out, std::string &src_type) {
	if (src_type == "int8") {
		scan_numpy_category_templated<int8_t, T>(column, offset, out, count);
	} else if (src_type == "int16") {
		scan_numpy_category_templated<int16_t, T>(column, offset, out, count);
	} else if (src_type == "int32") {
		scan_numpy_category_templated<int32_t, T>(column, offset, out, count);
	} else if (src_type == "int64") {
		scan_numpy_category_templated<int64_t, T>(column, offset, out, count);
	} else {
		return make_error(resource, "The Pandas type " + src_type + " for categorical types is not implemented yet");
	}
	return core::error_t::no_error();
}
static void apply_mask(pandas_column_bind_data_t &bind_data, components::vector::validity_mask_t &validity, idx_t count, idx_t offset) {
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
void scan_numpy_masked(pandas_column_bind_data_t &bind_data, idx_t count, idx_t offset, vector_t &out) {
	assert(bind_data.pandas_col->backend() == pandas_column_backend_t::NUMPY);
	auto &numpy_col = reinterpret_cast<pandas_numpy_column_t &>(*bind_data.pandas_col);
	scan_numpy_column<T>(numpy_col.array, numpy_col.stride, offset, out, count);
    if (bind_data.mask) {
        auto &result_mask = out.validity();
        apply_mask(bind_data, result_mask, count, offset);
    }

}

template <class T>
void scan_numpy_fp_column(pandas_column_bind_data_t &bind_data, const T *src_ptr, idx_t stride, idx_t count, idx_t offset, vector_t &out) {
	auto &mask = out.validity();
	auto tgt_ptr = out.data<T>();
	if (stride == sizeof(T)) {
		std::memcpy(tgt_ptr, src_ptr + offset, count * sizeof(T));
		for (idx_t i = 0; i < count; i++) {
			if (std::isnan(tgt_ptr[i])) {
				mask.set_invalid(i);
			}
		}
	} else {
		const idx_t step = stride / sizeof(T);
		for (idx_t i = 0; i < count; i++) {
			tgt_ptr[i] = src_ptr[step * (i + offset)];
			if (std::isnan(tgt_ptr[i])) {
				mask.set_invalid(i);
			}
		}
	}
    if (bind_data.mask) {
        auto &result_mask = out.validity();
        apply_mask(bind_data, result_mask, count, offset);

    }
}


template <class T>
static std::string_view decode_python_unicode(T *codepoints, idx_t codepoint_count, vector_t &out) {
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
            utf8_length -= 1;
        }

		assert(utf8_length >= 1);
	}
	int sz;
    auto buffer = static_cast<components::vector::string_vector_buffer_t*>(out.auxiliary().get());
    auto target = reinterpret_cast<utf8proc_uint8_t*>(buffer->empty_string(utf8_length));
    std::string_view result(reinterpret_cast<const char*>(target), utf8_length);
    // utf8proc_reencode for array
	for (idx_t i = 0; i < codepoint_count; i++) {
		sz = utf8proc_encode_char(static_cast<utf8proc_int32_t>(codepoints[i]), target);
		assert(sz >= 1);
		target += sz;
	}
	return result;
}

static void set_invalid_recursive(vector_t &out, idx_t index) {
	auto &validity = out.validity(); 
	validity.set_invalid(index);
	if (out.type().to_physical_type() == components::types::physical_type::STRUCT) {
		auto &children = out.entries(); 
		for (idx_t i = 0; i < children.size(); i++) {
			set_invalid_recursive(*children[i], index);
		}
	}
}

//! 'count' is the amount of rows in the 'out' vector
//! 'offset' is the current row number within this vector
core::error_t scan_numpy_object(std::pmr::memory_resource *resource, PyObject *object, idx_t offset, vector_t &out) {

	// handle None
	if (object == Py_None) {
		set_invalid_recursive(out, offset);
		return core::error_t::no_error();
	}

	auto val = transform_python_value(resource, object, out.type());
	if (val.has_error()) {
		return val.error();
	}
	// Check if the Value type is accepted for the logical_type of Vector
	out.set_value(offset, val.value());
	return core::error_t::no_error();
}

core::error_t numpy_scan_t::scan_object_column(std::pmr::memory_resource *resource, PyObject **col, idx_t stride, idx_t count, idx_t offset, vector_t &out) {
	// numpy_col is a sequential list of objects, that make up one "column" (Vector)
	out.set_vector_type(components::vector::vector_type::FLAT);
	python_gil_wrapper_t gil; // We're creating python objects here, so we need the GIL

	if (stride == sizeof(PyObject *)) {
		auto src_ptr = col + offset;
		for (idx_t i = 0; i < count; i++) {
			if (auto err = scan_numpy_object(resource, src_ptr[i], i, out); err.contains_error()) {
				return err;
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto src_ptr = col[stride / sizeof(PyObject *) * (i + offset)];
			if (auto err = scan_numpy_object(resource, src_ptr, i, out); err.contains_error()) {
				return err;
			}
		}
	}
	return core::error_t::no_error();
}

//! 'offset' is the offset within the column
//! 'count' is the amount of values we will convert in this batch
core::error_t numpy_scan_t::scan(std::pmr::memory_resource *resource, pandas_column_bind_data_t &bind_data, idx_t count, idx_t offset, vector_t &out) {
	assert(bind_data.pandas_col->backend() == pandas_column_backend_t::NUMPY);
	auto &numpy_col = reinterpret_cast<pandas_numpy_column_t &>(*bind_data.pandas_col);
	auto &array = numpy_col.array;

	switch (bind_data.numpy_type.type) {
	case numpy_nullable_type_t::BOOL:
		scan_numpy_masked<bool>(bind_data, count, offset, out);
		break;
	case numpy_nullable_type_t::UINT_8:
		scan_numpy_masked<uint8_t>(bind_data, count, offset, out);
		break;
	case numpy_nullable_type_t::UINT_16:
		scan_numpy_masked<uint16_t>(bind_data, count, offset, out);
		break;
	case numpy_nullable_type_t::UINT_32:
		scan_numpy_masked<uint32_t>(bind_data, count, offset, out);
		break;
	case numpy_nullable_type_t::UINT_64:
		scan_numpy_masked<uint64_t>(bind_data, count, offset, out);
		break;
	case numpy_nullable_type_t::INT_8:
		scan_numpy_masked<int8_t>(bind_data, count, offset, out);
		break;
	case numpy_nullable_type_t::INT_16:
		scan_numpy_masked<int16_t>(bind_data, count, offset, out);
		break;
	case numpy_nullable_type_t::INT_32:
		scan_numpy_masked<int32_t>(bind_data, count, offset, out);
		break;
	case numpy_nullable_type_t::INT_64:
		scan_numpy_masked<int64_t>(bind_data, count, offset, out);
		break;
	case numpy_nullable_type_t::FLOAT_32:
		scan_numpy_fp_column<float>(bind_data, reinterpret_cast<const float *>(array.data()), numpy_col.stride, count, offset, out);
		break;
	case numpy_nullable_type_t::FLOAT_64:
		scan_numpy_fp_column<double>(bind_data, reinterpret_cast<const double *>(array.data()), numpy_col.stride, count, offset, out);
		break;
	case numpy_nullable_type_t::STRING:
	case numpy_nullable_type_t::OBJECT: {
		// get the source pointer of the numpy array
		auto src_ptr = reinterpret_cast<PyObject **>(const_cast<void *>(array.data())); // NOLINT
		const bool is_object_col = bind_data.numpy_type.type == numpy_nullable_type_t::OBJECT;
		if (is_object_col && out.type().type() != components::types::logical_type::STRING_LITERAL) {
			//! We have determined the underlying logical type of this object column
			return numpy_scan_t::scan_object_column(resource, src_ptr, numpy_col.stride, count, offset, out);
		}

		// get the data pointer and the validity mask of the result vector
		auto tgt_ptr = out.data<std::string_view>(); 
		auto &out_mask = out.validity();
		std::unique_ptr<python_gil_wrapper_t> gil;
		auto &import_cache = connection_environment_t::import_cache();

		// Loop over every row of the arrays contents
		auto stride = numpy_col.stride;
		for (idx_t row = 0; row < count; row++) {
			auto source_idx = stride / sizeof(PyObject *) * (row + offset);

			// get the pointer to the object
			PyObject *val = src_ptr[source_idx];
			if (!py::isinstance<py::str>(val)) {
				if (val == Py_None) {
					out_mask.set_invalid(row);
					continue;
				}
				if (import_cache.pandas.na_t(false)) {
					// If pandas is imported, check if this is pandas.na_t
					py::handle value(val);
					if (value.is(import_cache.pandas.na_t())) {
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
						gil = std::make_unique<python_gil_wrapper_t>();
					}
					bind_data.object_str_val.push(std::move(py::str(val)));
					val = reinterpret_cast<PyObject *>(bind_data.object_str_val.last_added_object().ptr());
				}
			}
			// Python 3 string representation:
			// https://github.com/python/cpython/blob/3a8fdb28794b2f19f6c8464378fb8b46bce1f5f4/Include/cpython/unicodeobject.h#L79
			py::handle val_handle(val);
			if (!py::isinstance<py::str>(val_handle)) {
				out_mask.set_invalid(row);
				continue;
			}
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
			if (PyUnicode_IS_COMPACT_ASCII(val))
#pragma GCC diagnostic pop
			{
				// ascii string: we can zero copy
				tgt_ptr[row] = std::string_view(py_util_t::PyUnicodeData(val_handle), py_util_t::PyUnicodeGetLength(val_handle));
			} else {
				// unicode gunk
				auto unicode_obj = reinterpret_cast<PyCompactUnicodeObject *>(val);
				// compact unicode string: is there utf8 data available?
				if (unicode_obj->utf8) {
					// there is! zero copy
					tgt_ptr[row] = std::string_view(const_char_ptr_cast(unicode_obj->utf8), static_cast<std::string_view::size_type>(unicode_obj->utf8_length));
				} else if (py_util_t::PyUnicodeIsCompact(unicode_obj) &&
				           !py_util_t::PyUnicodeIsASCII(unicode_obj)) { // NOLINT
					auto kind = py_util_t::PyUnicodeKind(val_handle);
					switch (kind) {
					case PyUnicode_1BYTE_KIND:
						tgt_ptr[row] = decode_python_unicode<Py_UCS1>(py_util_t::PyUnicode1ByteData(val_handle),
						                                            py_util_t::PyUnicodeGetLength(val_handle), out);
						break;
					case PyUnicode_2BYTE_KIND:
						tgt_ptr[row] = decode_python_unicode<Py_UCS2>(py_util_t::PyUnicode2ByteData(val_handle),
						                                            py_util_t::PyUnicodeGetLength(val_handle), out);
						break;
					case PyUnicode_4BYTE_KIND:
						tgt_ptr[row] = decode_python_unicode<Py_UCS4>(py_util_t::PyUnicode4ByteData(val_handle),
						                                            py_util_t::PyUnicodeGetLength(val_handle), out);
						break;
					default:
						return make_error(resource,
						    "Unsupported typekind constant " + std::to_string(int(kind)) + " for Python Unicode Compact decode");
					}
				} else {
					return make_error(resource, "Unsupported string type: no clue what this string is");
				}
			}
		}
		break;
	}
	case numpy_nullable_type_t::CATEGORY:
			return make_error(resource, "OtterBrix doen\'t support Categories/ENUMs");

	default:
		return make_error(resource, "Unsupported pandas type");
	}
	return core::error_t::no_error();
}

} // namespace otterbrix
