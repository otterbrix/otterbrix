#pragma once

#include <pybind11/pybind_wrapper.hpp>
#include <pybind11/gil_wrapper.hpp>
#include <native/python_objects.hpp>

#include <components/configuration/configuration.hpp>
#include <components/types/types.hpp>
#include <common/typedefs.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>

namespace otterbrix {

class pandas_analyzer_t {
public:
	explicit pandas_analyzer_t(std::pmr::memory_resource* resource,
	                        const configuration::config_pandas& cfg = {})
		: sample_size(cfg.analyze_sample_size)
		, analyzed_type_(components::types::logical_type::NA)
		, resource_(resource) {
	}

public:
	core::result_wrapper_t<components::types::complex_logical_type> get_list_type(py::object &ele, bool &can_convert);
	core::result_wrapper_t<components::types::complex_logical_type> dict_to_map(const py_dictionary_t &dict, bool &can_convert);
	core::result_wrapper_t<components::types::complex_logical_type> dict_to_struct(const py_dictionary_t &dict, bool &can_convert);
	core::result_wrapper_t<components::types::complex_logical_type> get_item_type(py::object ele, bool &can_convert);
	core::result_wrapper_t<bool> analyze(py::object column);
	components::types::complex_logical_type analyzed_type() {
		return analyzed_type_;
	}

private:
	core::result_wrapper_t<components::types::complex_logical_type> inner_analyze(py::object column, bool &can_convert, idx_t increment);
	uint64_t get_sample_increment(idx_t rows);

private:
	uint64_t sample_size;
	//! Holds the gil to allow python object creation/destruction
	python_gil_wrapper_t gil;
	//! The resulting analyzed type
	components::types::complex_logical_type analyzed_type_;
	//! Threaded memory resource for engine-data containers
	std::pmr::memory_resource* resource_;
};

} // namespace otterbrix
