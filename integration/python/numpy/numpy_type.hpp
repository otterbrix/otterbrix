#pragma once

#include <pybind11/pybind_wrapper.hpp>

#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>

namespace otterbrix {

// pandas_t has two different sets of types
// NumPy dtypes (e.g., bool, int8,...)
// pandas_t Specific Types (e.g., categorical, datetime_tz,...)
enum class numpy_nullable_type_t : uint8_t {
	//! NumPy dtypes
	BOOL,        //! bool_, bool8
	INT_8,       //! byte, int8
	UINT_8,      //! ubyte, uint8
	INT_16,      //! int16, short
	UINT_16,     //! uint16, ushort
	INT_32,      //! int32, intc
	UINT_32,     //! uint32, uintc,
	INT_64,      //! int64, int0, int_, intp, matrix
	UINT_64,     //! uint64, uint, uint0, uintp
	FLOAT_16,    //! float16, half
	FLOAT_32,    //! float32, single
	FLOAT_64,    //! float64, float_, double
	OBJECT,      //! object
	UNICODE,     //! <U1, unicode_, str_, str0
	DATETIME_S,  //! datetime64[s], <M8[s]
	DATETIME_MS, //! datetime64[ms], <M8[ms]
	DATETIME_NS, //! datetime64[ns], <M8[ns]
	DATETIME_US, //! datetime64[us], <M8[us]
	TIMEDELTA,   //! timedelta64[D], timedelta64

	//! ------------------------------------------------------------
	//! Extension Types
	//! ------------------------------------------------------------
	CATEGORY, //! category
	STRING,   //! string
};

struct numpy_type_t {
	numpy_nullable_type_t type;
	//! Optionally if the type is a DATETIME,
	//! this indicates whether the type has timezone information
	bool has_timezone = false;
};

enum class numpy_object_type_t : uint8_t {
	//! To identify supported Numpy objects for scaning
	INVALID,   //! unsupported numpy object
	NDARRAY1D, //! numpy array with shape (n, )
	NDARRAY2D, //! numpy array with shape (n_rows, n_cols)
	LIST,      //! list of numpy arrays of shape (n,)
	DICT,      //! dict of numpy arrays of shape (n,)
};

core::result_wrapper_t<numpy_type_t> convert_numpy_type(std::pmr::memory_resource *resource, const py::handle &col_type);
core::result_wrapper_t<components::types::complex_logical_type> numpy_to_logical_type(std::pmr::memory_resource *resource, const numpy_type_t &col_type);

} // namespace otterbrix
