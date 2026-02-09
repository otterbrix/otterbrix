#pragma once

#include <pybind11/pybind_wrapper.hpp>
#include <pybind11/gil_wrapper.hpp>
#include <native/python_objects.hpp>

#include <components/types/types.hpp>
#include <core/typedefs.hpp>

namespace otterbrix {

// TODO add to config
// sample to detect complex_logical_type
constexpr uint64_t PANDAS_ANALYZE_SAMPLE = 1000;

class PandasAnalyzer {
public:
	explicit PandasAnalyzer() {
		analyzed_type = components::types::logical_type::NA;

		sample_size = PANDAS_ANALYZE_SAMPLE; 
	}

public:
	components::types::complex_logical_type GetListType(py::object &ele, bool &can_convert);
	components::types::complex_logical_type DictToMap(const PyDictionary &dict, bool &can_convert);
	components::types::complex_logical_type DictToStruct(const PyDictionary &dict, bool &can_convert);
	components::types::complex_logical_type GetItemType(py::object ele, bool &can_convert);
	bool Analyze(py::object column);
	components::types::complex_logical_type AnalyzedType() {
		return analyzed_type;
	}

private:
	components::types::complex_logical_type InnerAnalyze(py::object column, bool &can_convert, idx_t increment);
	uint64_t GetSampleIncrement(idx_t rows);

private:
	uint64_t sample_size;
	//! Holds the gil to allow python object creation/destruction
	PythonGILWrapper gil;
	//! The resulting analyzed type
	components::types::complex_logical_type analyzed_type;
};

} // namespace otterbrix
